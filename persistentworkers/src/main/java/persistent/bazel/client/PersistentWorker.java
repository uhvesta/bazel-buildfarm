// Copyright 2023-2025 The Buildfarm Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package persistent.bazel.client;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.devtools.build.lib.worker.WorkerProtocol.WorkRequest;
import com.google.devtools.build.lib.worker.WorkerProtocol.WorkResponse;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Optional;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import lombok.Getter;
import persistent.bazel.processes.ProtoWorkerRW;
import persistent.common.Worker;
import persistent.common.processes.ProcessWrapper;

/**
 * Wraps a persistent worker process, using ProtoWorkerRW. The process is created with a working
 * directory under the execRoot of the WorkerKey.
 *
 * <p>Maintains the metadata of the underlying process, i.e. its WorkerKey and the command used to
 * run it.
 *
 * <p>Also takes care of the underlying process's environment, i.e. directories and files.
 */
public class PersistentWorker implements Worker<WorkRequest, WorkResponse> {
  /** Services supporting being run as persistent workers need to parse this flag */
  public static final String PERSISTENT_WORKER_FLAG = "--persistent_worker";

  private static final Logger logger = Logger.getLogger(PersistentWorker.class.getName());

  /** How long a cancellable worker gets to acknowledge a cancel request. */
  public static final Duration DEFAULT_CANCEL_GRACE_PERIOD = Duration.ofSeconds(5);

  @Getter private final WorkerKey key;
  private final Duration cancelGracePeriod;
  @Getter private final ImmutableList<String> initCmd;
  @Getter private final Path execRoot;
  private final ProtoWorkerRW workerRW;

  public PersistentWorker(WorkerKey key, String workerDir) throws IOException {
    this(key, workerDir, DEFAULT_CANCEL_GRACE_PERIOD);
  }

  /**
   * @param cancelGracePeriod how long to wait for a worker to acknowledge a cancel request before
   *     giving up and killing it. Only relevant if the key is cancellable.
   */
  public PersistentWorker(WorkerKey key, String workerDir, Duration cancelGracePeriod)
      throws IOException {
    this.key = key;
    this.cancelGracePeriod = cancelGracePeriod;
    this.execRoot = key.getExecRoot().resolve(workerDir);
    this.initCmd =
        ImmutableList.<String>builder().addAll(key.getCmd()).addAll(key.getArgs()).build();

    Files.createDirectories(execRoot);

    final var logLevel = Level.FINE;
    if (logger.isLoggable(logLevel)) {
      Set<Path> workerFiles = ImmutableSet.copyOf(key.getWorkerFilesWithHashes().keySet());
      StringBuilder msg = new StringBuilder();
      msg.append("Starting Worker[");
      msg.append(key.getMnemonic());
      msg.append("]<");
      msg.append(execRoot);
      msg.append(">(");
      msg.append(initCmd);
      msg.append(") with files: \n");
      msg.append(workerFiles);
      logger.log(logLevel, msg.toString());
    }

    ProcessWrapper processWrapper = new ProcessWrapper(execRoot, initCmd, key.getEnv());
    this.workerRW = new ProtoWorkerRW(processWrapper);
  }

  @Override
  public WorkResponse doWork(WorkRequest request) throws InterruptedException {
    return doWork(request, null);
  }

  /**
   * Sends a request to the worker and waits for its response.
   *
   * <p>If the calling thread is interrupted, or {@code timeout} elapses first, the in-flight
   * request is cancelled: a cancellable worker is asked to abandon it and keeps running if it
   * acknowledges within the grace period, any other worker is killed. Either way this worker is
   * safe to hand back to the pool afterwards, because no stale response is left in its pipe.
   *
   * @param timeout how long to wait for the response, or null to wait indefinitely.
   * @return the response; a response with {@code was_cancelled} set if {@code timeout} elapsed;
   *     null if the worker died or the protocol failed.
   * @throws InterruptedException if the thread was interrupted; the request has been cancelled.
   */
  public WorkResponse doWork(WorkRequest request, Duration timeout) throws InterruptedException {
    WorkResponse response = null;
    try {
      logRequest(request);

      workerRW.write(request);
      response = timeout == null ? workerRW.waitAndRead() : workerRW.waitAndRead(timeout);
      if (response == null) {
        logger.log(Level.WARNING, "Worker timed out after " + timeout + ": " + initCmd);
        cancelInFlight(request);
        return WorkResponse.newBuilder()
            .setRequestId(request.getRequestId())
            .setWasCancelled(true)
            .build();
      }

      logIfBadResponse(response);
    } catch (InterruptedException e) {
      cancelInFlight(request);
      throw e;
    } catch (IOException e) {
      e.printStackTrace();
      logger.severe("IO Failing with : " + e.getMessage());
    } catch (Exception e) {
      e.printStackTrace();
      logger.severe("Failing with : " + e.getMessage());
    }
    return response;
  }

  /**
   * Abandons the in-flight request so this worker can be reused, or kills it if that isn't
   * possible.
   */
  private void cancelInFlight(WorkRequest request) {
    if (key.isCancellable() && requestCancel(request)) {
      return;
    }
    destroy();
  }

  /**
   * Sends a cancel request and waits for the worker to answer the original request, which it does
   * either with {@code was_cancelled} or with a regular response if it had already finished. That
   * response must be consumed so the pipe stays in sync with the next request.
   *
   * @return true if the worker acknowledged and is ready for new work.
   */
  private boolean requestCancel(WorkRequest request) {
    try {
      workerRW.write(
          WorkRequest.newBuilder().setRequestId(request.getRequestId()).setCancel(true).build());
      if (workerRW.waitAndRead(cancelGracePeriod) == null) {
        logger.log(
            Level.WARNING,
            "Worker did not acknowledge cancel within " + cancelGracePeriod + ": " + initCmd);
        return false;
      }
      // anything the cancelled request wrote to stderr is not meant for the next one
      flushStdErr();
      return true;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return false;
    } catch (IOException e) {
      logger.log(Level.WARNING, "Failed to cancel request: " + e.getMessage());
      return false;
    }
  }

  public Optional<Integer> getExitValue() {
    ProcessWrapper pw = workerRW.getProcessWrapper();
    return pw != null && !pw.isAlive() ? Optional.of(pw.exitValue()) : Optional.empty();
  }

  public String getStdErr() {
    try {
      return this.workerRW.getProcessWrapper().getErrorString();
    } catch (IOException e) {
      e.printStackTrace();
      return "getStdErr Exception: " + e;
    }
  }

  public String flushStdErr() {
    try {
      return this.workerRW.getProcessWrapper().flushErrorString();
    } catch (IOException e) {
      e.printStackTrace();
      return "flushStdErr Exception: " + e;
    }
  }

  private void logRequest(WorkRequest request) {
    logger.log(
        Level.FINE,
        "doWork()------<" + "Got request with args: " + request.getArgumentsList() + "------>");
  }

  private void logIfBadResponse(WorkResponse response) throws IOException {
    int returnCode = response.getExitCode();
    if (returnCode != 0 && logger.isLoggable(Level.FINE)) {
      StringBuilder sb = new StringBuilder();
      sb.append("logBadResponse(err)");
      sb.append("\nResponse non-zero exit_code: ");
      sb.append(returnCode);
      sb.append("\nResponse output: ");
      sb.append(response.getOutput());
      sb.append("\n\tProcess stderr: ");
      String stderr = workerRW.getProcessWrapper().getErrorString();
      sb.append(stderr);
      logger.log(Level.FINE, sb.toString());
    }
  }

  @Override
  public void destroy() {
    this.workerRW.getProcessWrapper().destroy();
  }
}
