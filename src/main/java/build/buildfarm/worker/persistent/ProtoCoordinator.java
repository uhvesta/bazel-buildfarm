// Copyright 2023 The Buildfarm Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package build.buildfarm.worker.persistent;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.devtools.build.lib.worker.WorkerProtocol.WorkRequest;
import com.google.devtools.build.lib.worker.WorkerProtocol.WorkResponse;
import com.google.protobuf.util.Durations;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;
import lombok.extern.java.Log;
import persistent.bazel.client.CommonsWorkerPool;
import persistent.bazel.client.PersistentWorker;
import persistent.bazel.client.WorkCoordinator;
import persistent.bazel.client.WorkerKey;
import persistent.bazel.client.WorkerSupervisor;

/**
 * Responsible for:
 *
 * <ol>
 *   <li>Initializing a new Worker's file environment correctly
 *   <li>pre-request requirements, e.g. ensuring tool input files
 *   <li>post-response requirements, i.e. putting output files in the right place
 * </ol>
 */
@Log
public class ProtoCoordinator extends WorkCoordinator<RequestCtx, ResponseCtx, CommonsWorkerPool> {
  private static final String WORKER_INIT_LOG_SUFFIX = ".initargs.log";

  // Synchronize writes to the tool input directory per WorkerKey
  // TODO: We only need a Set of WorkerKeys to synchronize on, but no ConcurrentHashSet
  private static final ConcurrentHashMap<WorkerKey, WorkerKey> toolInputSyncs =
      new ConcurrentHashMap<>();

  // Enforces locking on the same object given the same WorkerKey
  private static WorkerKey keyLock(WorkerKey key) {
    return toolInputSyncs.computeIfAbsent(key, k -> k);
  }

  public ProtoCoordinator(CommonsWorkerPool workerPool) {
    super(workerPool);
  }

  private ProtoCoordinator(WorkerSupervisor supervisor, int maxWorkersPerKey) {
    super(new CommonsWorkerPool(supervisor, maxWorkersPerKey));
  }

  // We copy tool inputs from the shared WorkerKey tools directory into our worker exec root,
  //    since there are multiple workers per key,
  //    and presumably there might be writes to tool inputs?
  // Tool inputs which are absolute-paths (e.g. /usr/bin/...) are not affected
  public static ProtoCoordinator ofCommonsPool(int maxWorkersPerKey) {
    WorkerSupervisor loadToolsOnCreate =
        new WorkerSupervisor() {
          @Override
          public PersistentWorker create(WorkerKey workerKey) throws Exception {
            Path keyExecRoot = workerKey.getExecRoot();
            String workerExecDir = getUniqueSubdir(keyExecRoot);
            Path workerExecRoot = keyExecRoot.resolve(workerExecDir);
            copyToolsIntoWorkerExecRoot(workerKey, workerExecRoot);

            Path initArgsLogFile = workerExecRoot.resolve(workerExecDir + WORKER_INIT_LOG_SUFFIX);
            if (!Files.exists(initArgsLogFile)) {
              StringBuilder initArgs = new StringBuilder();
              for (String s : workerKey.getCmd()) {
                initArgs.append(s).append('\n');
              }
              for (String s : workerKey.getArgs()) {
                initArgs.append(s).append('\n');
              }

              Files.write(initArgsLogFile, initArgs.toString().getBytes());
            }
            return new PersistentWorker(workerKey, workerExecDir);
          }
        };
    return new ProtoCoordinator(loadToolsOnCreate, maxWorkersPerKey);
  }

  public void copyToolInputsIntoWorkerToolRoot(WorkerKey key, WorkerInputs workerFiles)
      throws IOException {
    WorkerKey lock = keyLock(key);
    synchronized (lock) {
      try {
        // Copy tool inputs as needed
        Path workToolRoot = key.getToolRoot();
        for (Path opToolPath : workerFiles.opToolInputs) {
          Path workToolPath = workerFiles.relativizeInput(workToolRoot, opToolPath);
          if (!Files.exists(workToolPath)) {
            workerFiles.copyInputFile(opToolPath, workToolPath);
          }
        }
      } finally {
        toolInputSyncs.remove(key);
      }
    }
  }

  private static String getUniqueSubdir(Path workRoot) {
    String uuid = UUID.randomUUID().toString();
    while (Files.exists(workRoot.resolve(uuid))) {
      uuid = UUID.randomUUID().toString();
    }
    return uuid;
  }

  // copyToolInputsIntoWorkerToolRoot() should have been called before this.
  private static void copyToolsIntoWorkerExecRoot(WorkerKey key, Path workerExecRoot)
      throws IOException {
    log.log(Level.FINE, "loadToolsIntoWorkerRoot() into: " + workerExecRoot);

    Path toolInputRoot = key.getToolRoot();
    for (Path relPath : key.getWorkerFilesWithHashes().keySet()) {
      Path toolInputPath = toolInputRoot.resolve(relPath);
      Path execRootPath = workerExecRoot.resolve(relPath);

      FileAccessUtils.copyFile(toolInputPath, execRootPath);
    }
  }

  @Override
  public WorkRequest preWorkInit(WorkerKey key, RequestCtx request, PersistentWorker worker)
      throws IOException {
    checkNotNull(request.timeout);

    // Symlinking should hypothetically be faster+leaner than copying inputs, but it's buggy.
    copyNontoolInputs(request.workerInputs, worker.getExecRoot());

    return request.request;
  }

  /**
   * The request's timeout bounds the time the worker takes to respond. The worker enforces it
   * itself, in this thread, so that a timed out request is cancelled or killed in exactly the same
   * way as an interrupted one.
   *
   * @throws TimeoutException if the worker did not respond in time
   * @throws InterruptedException if the request was cancelled
   */
  @Override
  protected WorkResponse doWork(PersistentWorker worker, WorkRequest request, RequestCtx ctx)
      throws TimeoutException, InterruptedException {
    Duration timeout = Duration.ofNanos(Durations.toNanos(ctx.timeout));
    WorkResponse response = worker.doWork(request, timeout);
    if (response != null && response.getWasCancelled()) {
      throw new TimeoutException("Persistent worker did not respond within " + timeout);
    }
    return response;
  }

  // After the worker has finished, output files need to be visible in the operation directory
  @Override
  public ResponseCtx postWorkCleanup(
      WorkResponse response, PersistentWorker worker, RequestCtx request) throws IOException {
    if (response == null) {
      throw new RuntimeException("postWorkCleanup: WorkResponse was null!");
    }

    try {
      Path workerExecRoot = worker.getExecRoot();
      if (response.getExitCode() == 0) {
        moveOutputsToOperationRoot(request.filesContext, workerExecRoot);
      } else {
        deleteOutputs(request.filesContext, workerExecRoot);
      }
      cleanUpNontoolInputs(request.workerInputs, workerExecRoot);
    } catch (IOException e) {
      throw logBadCleanup(request, e);
    }

    return new ResponseCtx(response, worker.flushStdErr());
  }

  /**
   * A request that was cancelled, timed out or failed outright must not leave its inputs and
   * partial outputs behind in the worker's directory, since the worker goes back to the pool and
   * serves the next request from the same directory.
   */
  @Override
  protected void abortWork(PersistentWorker worker, RequestCtx request) throws IOException {
    Path workerExecRoot = worker.getExecRoot();
    try {
      deleteOutputs(request.filesContext, workerExecRoot);
    } finally {
      cleanUpNontoolInputs(request.workerInputs, workerExecRoot);
    }
  }

  private IOException logBadCleanup(RequestCtx request, IOException e) {
    WorkFilesContext context = request.filesContext;

    StringBuilder sb = new StringBuilder(122);
    sb.append("Output files failure debug for request with args<")
        .append(request.request.getArgumentsList())
        .append(">:\ngetOutputPathsList:\n")
        .append(context.outputPaths)
        .append("getOutputFilesList:\n")
        .append(context.outputFiles)
        .append("getOutputDirectoriesList:\n")
        .append(context.outputDirectories);

    log.log(Level.SEVERE, sb.toString(), e);

    return new IOException("Failed on postWorkCleanup", e);
  }

  private void copyNontoolInputs(WorkerInputs workerInputs, Path workerExecRoot)
      throws IOException {
    for (Path opPath : workerInputs.allInputs.keySet()) {
      if (!workerInputs.allToolInputs.contains(opPath)) {
        Path execPath = workerInputs.relativizeInput(workerExecRoot, opPath);
        workerInputs.copyInputFile(opPath, execPath);
      }
    }
  }

  // Make outputs visible to the rest of Worker machinery
  // see DockerExecutor::copyOutputsOutOfContainer
  void moveOutputsToOperationRoot(WorkFilesContext context, Path workerExecRoot)
      throws IOException {
    Path opRoot = context.opRoot;

    for (String outputDir : context.outputDirectories) {
      Path outputDirPath = Path.of(outputDir);
      Files.createDirectories(outputDirPath);
    }

    for (String relOutput : context.outputFiles) {
      Path execOutputPath = workerExecRoot.resolve(relOutput);
      Path opOutputPath = opRoot.resolve(relOutput);
      // Don't fail here if the action failed to produce a file.
      // The missing file will be handled just like it is for non-worker actions.
      if (Files.exists(execOutputPath)) {
        FileAccessUtils.moveFile(execOutputPath, opOutputPath);
      }
    }
  }

  // Outputs which won't be used must not be left for the worker's next request to trip over
  private void deleteOutputs(WorkFilesContext context, Path workerExecRoot) throws IOException {
    for (String relOutput : context.outputFiles) {
      FileAccessUtils.deleteFileIfExists(workerExecRoot.resolve(relOutput));
    }
  }

  private void cleanUpNontoolInputs(WorkerInputs workerInputs, Path workerExecRoot)
      throws IOException {
    for (Path opPath : workerInputs.allInputs.keySet()) {
      if (!workerInputs.allToolInputs.contains(opPath)) {
        workerInputs.deleteInputFileIfExists(workerExecRoot, opPath);
      }
    }
  }
}
