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

package persistent.bazel.processes;

import com.google.devtools.build.lib.worker.WorkerProtocol.WorkRequest;
import com.google.devtools.build.lib.worker.WorkerProtocol.WorkResponse;
import com.google.protobuf.GeneratedMessage;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.function.Supplier;
import lombok.Getter;
import org.apache.commons.io.IOUtils;
import persistent.common.processes.ProcessWrapper;

/**
 * Based off Google's ProtoWorkerProtocol Slightly generified to encapsulate read/writes Should be
 * used by both the PersistentWorker (client-side) and the WorkRequestHandler (in the persistent
 * worker process)
 *
 * <p>Writes WorkRequest protos to the persistent worker Reads WorkResponse protos from the
 * persistent worker
 *
 * <p>Static methods also expose some useful(?) utilities
 *
 * <p>TODO: What happens to input/output streams when the process dies? Presumably, it is closed (as
 * per tests).
 */
public class ProtoWorkerRW {
  @Getter private final ProcessWrapper processWrapper;

  private final InputStream readStream;

  private final OutputStream writeStream;

  public ProtoWorkerRW(ProcessWrapper processWrapper) {
    this.processWrapper = processWrapper;
    this.readStream = processWrapper.getStdOut();
    this.writeStream = processWrapper.getStdIn();
  }

  public void write(WorkRequest req) throws IOException {
    writeTo(req, this.writeStream);
  }

  public WorkResponse waitAndRead() throws IOException, InterruptedException {
    return waitAndRead(Long.MAX_VALUE);
  }

  /**
   * Waits up to {@code timeout} for the worker to respond.
   *
   * @return the response, or null if the worker did not respond within the timeout.
   */
  public WorkResponse waitAndRead(Duration timeout) throws IOException, InterruptedException {
    return waitAndRead(timeout.toNanos());
  }

  private WorkResponse waitAndRead(long timeoutNanos) throws IOException, InterruptedException {
    try {
      if (!awaitInput(processWrapper::isAlive, readStream, timeoutNanos)) {
        return null;
      }
    } catch (IOException e) {
      String stdErrMsg = processWrapper.getErrorString();
      String stdOut = "";
      try {
        if (processWrapper.isAlive() && readStream.available() > 0) {
          stdOut = IOUtils.toString(readStream, StandardCharsets.UTF_8);
        } else {
          stdOut = "no stream available";
        }
      } catch (IOException e2) {
        stdOut = "Exception trying to read stdout: " + e2;
      }
      throw new IOException(
          "IOException on waitForInput; stdErr: " + stdErrMsg + "\nStdout: " + stdOut, e);
    }
    return readResponse(readStream);
  }

  public static <R extends GeneratedMessage> void writeTo(R req, OutputStream outputStream)
      throws IOException {
    try {
      req.writeDelimitedTo(outputStream);
    } finally {
      outputStream.flush();
    }
  }

  public static WorkResponse readResponse(InputStream inputStream) throws IOException {
    return WorkResponse.parseDelimitedFrom(inputStream);
  }

  public static WorkRequest readRequest(InputStream inputStream) throws IOException {
    return WorkRequest.parseDelimitedFrom(inputStream);
  }

  public static void waitForInput(Supplier<Boolean> liveCheck, InputStream inputStream)
      throws IOException, InterruptedException {
    awaitInput(liveCheck, inputStream, Long.MAX_VALUE);
  }

  /**
   * @return true once input is available, false if {@code timeoutNanos} elapsed first.
   */
  private static boolean awaitInput(
      Supplier<Boolean> liveCheck, InputStream inputStream, long timeoutNanos)
      throws IOException, InterruptedException {
    String workerDeathMsg = "Worker process for died while waiting for response";
    long start = System.nanoTime();
    // TODO can we do better than spinning? i.e. condition variable?
    while (inputAvailable(inputStream, workerDeathMsg) == 0) {
      if (System.nanoTime() - start >= timeoutNanos) {
        return false;
      }
      Thread.sleep(10);
      if (!liveCheck.get()) {
        throw new IOException(workerDeathMsg + "\n");
      }
    }
    return true;
  }

  private static int inputAvailable(InputStream inputStream, String errorMsg) throws IOException {
    try {
      return inputStream.available();
    } catch (IOException e) {
      throw new IOException(errorMsg, e);
    }
  }
}
