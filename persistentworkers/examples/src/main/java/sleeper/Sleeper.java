// Copyright 2025 The Buildfarm Authors. All rights reserved.
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

package sleeper;

import static java.util.stream.Collectors.joining;

import com.google.devtools.build.lib.worker.WorkerProtocol.WorkRequest;
import com.google.devtools.build.lib.worker.WorkerProtocol.WorkResponse;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import persistent.bazel.processes.ProtoWorkerRW;
import persistent.bazel.processes.WorkRequestHandler;

/**
 * Example persistent worker for exercising request cancellation. A request is a command, either
 * given directly as its arguments or, like the real workers' argument files, as a single {@code
 * @file} argument naming a file with one argument per line. Commands, relative paths being
 * relative to the worker's working directory:
 *
 * <ul>
 *   <li>{@code echo <text>} answers with the text
 *   <li>{@code sleep <millis> [<started marker> [<partial output>]]} creates the partial output
 *       and then the absolute-path marker file, both optional, to say it is underway, blocks for
 *       that long and answers "slept"
 *   <li>{@code write <path> <text>} writes the file and answers "wrote"
 *   <li>{@code fail <code> [<path>]} writes the optional file, then exits with that code
 *   <li>{@code list <path>...} answers with a comma separated list of those paths which exist
 * </ul>
 *
 * <p>Started with {@code --persistent_worker} alone it handles one request at a time and does not
 * look at stdin while busy, like a worker which doesn't support cancellation. With {@code
 * --cancellable} as well it keeps reading requests while working, and abandons a request when it
 * receives a cancel request for it, answering with {@code was_cancelled}.
 */
public class Sleeper {
  private record Result(String output, int exitCode) {}

  public static void main(String[] args) {
    List<String> flags = List.of(args);
    if (!flags.contains("--persistent_worker")) {
      System.err.println("expected --persistent_worker");
      System.exit(2);
    }
    int exitCode;
    if (flags.contains("--cancellable")) {
      exitCode = processCancellable(System.in, System.out);
    } else {
      exitCode =
          new WorkRequestHandler(
                  (actionArgs, pw) -> {
                    try {
                      Result result = handle(actionArgs);
                      pw.write(result.output());
                      return result.exitCode();
                    } catch (InterruptedException | IOException e) {
                      Thread.currentThread().interrupt();
                      return 1;
                    }
                  })
              .processForever(System.in, System.out, System.err);
    }
    System.exit(exitCode);
  }

  private static Result handle(List<String> args) throws InterruptedException, IOException {
    if (args.size() == 1 && args.get(0).startsWith("@")) {
      return handle(Files.readAllLines(Path.of(args.get(0).substring(1))));
    }
    switch (args.isEmpty() ? "" : args.get(0)) {
      case "echo":
        return new Result(args.get(1), 0);
      case "sleep":
        if (args.size() > 3) {
          Files.writeString(Path.of(args.get(3)), "partial");
        }
        if (args.size() > 2) {
          Files.writeString(Path.of(args.get(2)), "started");
        }
        Thread.sleep(Long.parseLong(args.get(1)));
        return new Result("slept", 0);
      case "write":
        Files.writeString(Path.of(args.get(1)), args.get(2));
        return new Result("wrote", 0);
      case "fail":
        if (args.size() > 2) {
          Files.writeString(Path.of(args.get(2)), "partial");
        }
        return new Result("failed", Integer.parseInt(args.get(1)));
      case "list":
        return new Result(
            args.stream().skip(1).filter(p -> Files.exists(Path.of(p))).collect(joining(",")), 0);
      default:
        System.err.println("Cannot handle args: " + args);
        System.exit(2);
        return null;
    }
  }

  private static int processCancellable(InputStream in, PrintStream out) {
    Map<Integer, Thread> inFlight = new ConcurrentHashMap<>();
    Object writeLock = new Object();
    try {
      for (; ; ) {
        WorkRequest request = ProtoWorkerRW.readRequest(in);
        if (request == null) {
          return 0;
        }
        int id = request.getRequestId();
        if (request.getCancel()) {
          Thread work = inFlight.get(id);
          if (work != null) {
            work.interrupt();
          }
          continue;
        }
        Thread work =
            new Thread(
                () -> {
                  WorkResponse.Builder response = WorkResponse.newBuilder().setRequestId(id);
                  try {
                    Result result = handle(request.getArgumentsList());
                    response.setOutput(result.output()).setExitCode(result.exitCode());
                  } catch (InterruptedException e) {
                    response.setWasCancelled(true);
                  } catch (IOException e) {
                    response.setOutput(e.toString()).setExitCode(1);
                  }
                  inFlight.remove(id);
                  synchronized (writeLock) {
                    try {
                      ProtoWorkerRW.writeTo(response.build(), out);
                    } catch (IOException e) {
                      e.printStackTrace();
                    }
                  }
                });
        inFlight.put(id, work);
        work.start();
      }
    } catch (IOException e) {
      e.printStackTrace();
      return 1;
    }
  }
}
