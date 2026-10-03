// Copyright 2025 The Buildfarm Authors. All rights reserved.
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

import static com.google.common.truth.Truth.assertThat;
import static java.util.stream.Collectors.toSet;
import static org.junit.Assert.assertThrows;

import build.bazel.remote.execution.v2.ActionResult;
import build.bazel.remote.execution.v2.Command;
import build.buildfarm.v1test.Tree;
import build.buildfarm.worker.resources.ResourceLimits;
import build.buildfarm.worker.util.WorkerTestUtils;
import build.buildfarm.worker.util.WorkerTestUtils.TreeFile;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import com.google.protobuf.util.Durations;
import com.google.rpc.Code;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import persistent.common.processes.JavaProcessWrapper;
import persistent.testutil.ProcessUtils;

/**
 * Runs actions through {@link PersistentExecutor#runOnPersistentWorker} on real worker processes,
 * and looks at what they do to the outside world: the result, the operation directory, and which
 * processes the operating system says are running. An action asks the example {@code Sleeper}
 * worker to do something with a file of arguments, like the real workers' argument files.
 */
@RunWith(JUnit4.class)
public class PersistentExecutorTest {
  private static final Duration WAIT = Duration.ofSeconds(30);
  private static final String ARGS_FILE = "args.txt";
  private static final String OUTPUT = "output_file";
  private static final String EXTRA_INPUT = "input.txt";

  @Rule public final TestName testName = new TestName();

  private Path root;
  private Path jar;
  private Set<ProcessHandle> baseline;
  private int actions = 0;

  @Before
  public void setUp() throws Exception {
    baseline = descendants();
    root = Files.createTempDirectory("persistent-executor-test-");
    String filename = "sleeper-bin_deploy.jar";
    jar =
        ProcessUtils.retrieveFileResource(
            getClass().getClassLoader(), filename, root.resolve(filename));
  }

  @After
  public void tearDown() {
    workers().forEach(ProcessHandle::destroyForcibly);
  }

  private static Set<ProcessHandle> descendants() {
    return ProcessHandle.current().descendants().filter(ProcessHandle::isAlive).collect(toSet());
  }

  /** Processes started by the test, as seen by the operating system. */
  private Set<ProcessHandle> workers() {
    return Sets.difference(descendants(), baseline);
  }

  /**
   * One kind of worker and how actions declare it. Actions of the same scenario share a worker key,
   * so they share workers, but no two scenarios do.
   */
  private final class Scenario {
    final boolean workerSupportsCancel;
    final boolean declaredCancellable;

    Scenario(boolean workerSupportsCancel, boolean declaredCancellable) {
      this.workerSupportsCancel = workerSupportsCancel;
      this.declaredCancellable = declaredCancellable;
    }

    /** An action which has not been run. */
    Action action(String... workerArgs) throws IOException {
      return new Action(this, ImmutableList.copyOf(workerArgs), /* withExtraInput= */ false);
    }

    /** An action which also has an input that must not outlive it in the worker's directory. */
    Action actionWithExtraInput(String... workerArgs) throws IOException {
      return new Action(this, ImmutableList.copyOf(workerArgs), /* withExtraInput= */ true);
    }
  }

  private final class Action {
    final Scenario scenario;
    final Path opRoot;
    final ImmutableList<String> argsList;
    final WorkFilesContext context;
    final ActionResult.Builder result = ActionResult.newBuilder();

    Action(Scenario scenario, ImmutableList<String> workerArgs, boolean withExtraInput)
        throws IOException {
      this.scenario = scenario;
      opRoot = Files.createDirectories(root.resolve("operations").resolve("op" + actions++));
      Files.write(opRoot.resolve(ARGS_FILE), workerArgs);
      List<TreeFile> files = new ArrayList<>();
      files.add(new TreeFile(ARGS_FILE, String.join("\n", workerArgs)));
      // the worker's directory is made when it is given its tools
      files.add(new TreeFile("tools/tool", "tool", /* isTool= */ true));
      Files.createDirectories(opRoot.resolve("tools"));
      Files.writeString(opRoot.resolve("tools/tool"), "tool");
      if (withExtraInput) {
        files.add(new TreeFile(EXTRA_INPUT, "input"));
        Files.writeString(opRoot.resolve(EXTRA_INPUT), "input");
      }
      Tree tree = WorkerTestUtils.makeTree(opRoot.toString(), files);
      context =
          WorkFilesContext.fromContext(
              opRoot, tree, Command.newBuilder().addOutputFiles(OUTPUT).build());

      ImmutableList.Builder<String> args = ImmutableList.builder();
      args.add(JavaProcessWrapper.CURRENT_JVM_COMMAND, "-cp", jar.toString(), "sleeper.Sleeper");
      if (scenario.workerSupportsCancel) {
        args.add("--cancellable");
      }
      argsList = args.add("@" + ARGS_FILE).build();
    }

    Code run(Duration timeout) throws IOException, InterruptedException {
      return PersistentExecutor.runOnPersistentWorker(
          context,
          "operation-" + opRoot.getFileName(),
          argsList,
          // keeps this test's worker key to itself, since the worker pool is shared
          ImmutableMap.of("TEST", testName.getMethodName()),
          new ResourceLimits(),
          Durations.fromMillis(timeout.toMillis()),
          root.resolve("work-roots"),
          scenario.declaredCancellable,
          result);
    }

    Code run() throws IOException, InterruptedException {
      return run(WAIT);
    }

    /** Runs the action on its own thread, which the test may then interrupt. */
    InFlight runInBackground() {
      return new InFlight(this);
    }

    String stdout() {
      return result.getStdoutRaw().toStringUtf8();
    }
  }

  /** A running action, to cancel the way the operation's poller does: by interrupting its thread. */
  private final class InFlight {
    final CompletableFuture<Code> outcome = new CompletableFuture<>();
    final Thread thread;

    InFlight(Action action) {
      thread =
          new Thread(
              () -> {
                try {
                  outcome.complete(action.run());
                } catch (Throwable t) {
                  outcome.completeExceptionally(t);
                }
              });
      thread.start();
    }

    void interrupt() {
      thread.interrupt();
    }

    Throwable failure() throws Exception {
      return assertThrows(ExecutionException.class, () -> outcome.get(WAIT.toSeconds(), TimeUnit.SECONDS))
          .getCause();
    }
  }

  private void awaitFile(Path file) throws InterruptedException {
    long deadline = System.nanoTime() + WAIT.toNanos();
    while (!Files.exists(file)) {
      assertThat(System.nanoTime()).isLessThan(deadline);
      Thread.sleep(10);
    }
  }

  /** Starts a worker for the scenario, and returns it. */
  private Set<ProcessHandle> startWorker(Scenario scenario) throws Exception {
    assertThat(scenario.action("echo", "warmup").run()).isEqualTo(Code.OK);
    Set<ProcessHandle> started = Set.copyOf(workers());
    assertThat(started).isNotEmpty();
    return started;
  }

  /**
   * Starts an action which sleeps in the worker, after it has put partial output in its directory,
   * and returns once the worker is doing it.
   */
  private InFlight startSleeping(Scenario scenario) throws Exception {
    Path started = root.resolve("started-" + actions);
    InFlight inFlight =
        scenario
            .actionWithExtraInput("sleep", "60000", started.toString(), OUTPUT)
            .runInBackground();
    awaitFile(started);
    return inFlight;
  }

  /**
   * Runs an action which reports which of an earlier action's leftovers it can see from inside the
   * worker's directory.
   */
  private void assertNothingLeftBehind(Scenario scenario) throws Exception {
    Action probe = scenario.action("list", OUTPUT, EXTRA_INPUT);
    assertThat(probe.run()).isEqualTo(Code.OK);
    assertThat(probe.stdout()).isEmpty();
  }

  @Test
  public void completedActionDeliversOutputsAndKeepsItsWorker() throws Exception {
    Scenario scenario = new Scenario(/* workerSupportsCancel= */ false, /* declared= */ false);
    Action action = scenario.action("write", OUTPUT, "contents");

    assertThat(action.run()).isEqualTo(Code.OK);

    assertThat(action.result.getExitCode()).isEqualTo(0);
    assertThat(action.stdout()).isEqualTo("wrote");
    assertThat(Files.readString(action.opRoot.resolve(OUTPUT))).isEqualTo("contents");
    Set<ProcessHandle> worker = Set.copyOf(workers());
    assertThat(worker).isNotEmpty();

    assertThat(scenario.action("echo", "again").run()).isEqualTo(Code.OK);
    assertThat(workers()).isEqualTo(worker);
  }

  @Test
  public void failedActionDeliversNoOutputsAndLeavesNothingInTheWorker() throws Exception {
    Scenario scenario = new Scenario(/* workerSupportsCancel= */ false, /* declared= */ false);
    Action failing = scenario.actionWithExtraInput("fail", "3", OUTPUT);
    Set<ProcessHandle> worker = startWorker(scenario);

    assertThat(failing.run()).isEqualTo(Code.OK);

    assertThat(failing.result.getExitCode()).isEqualTo(3);
    assertThat(Files.exists(failing.opRoot.resolve(OUTPUT))).isFalse();
    assertNothingLeftBehind(scenario);
    assertThat(workers()).isEqualTo(worker);
  }

  @Test
  public void cancelledActionKeepsWorkerWhichSupportsCancel() throws Exception {
    Scenario scenario = new Scenario(/* workerSupportsCancel= */ true, /* declared= */ true);
    Set<ProcessHandle> worker = startWorker(scenario);
    InFlight cancelled = startSleeping(scenario);

    cancelled.interrupt();

    assertThat(cancelled.failure()).isInstanceOf(InterruptedException.class);
    assertThat(workers()).isEqualTo(worker);
    assertNothingLeftBehind(scenario);
    assertThat(workers()).isEqualTo(worker);
  }

  @Test
  public void cancelledActionReplacesWorkerWhichIgnoresCancel() throws Exception {
    Scenario scenario = new Scenario(/* workerSupportsCancel= */ false, /* declared= */ true);
    Set<ProcessHandle> worker = startWorker(scenario);
    InFlight cancelled = startSleeping(scenario);

    cancelled.interrupt();

    assertThat(cancelled.failure()).isInstanceOf(InterruptedException.class);
    assertThat(worker.stream().anyMatch(ProcessHandle::isAlive)).isFalse();
    assertNothingLeftBehind(scenario);
    assertThat(workers()).isNotEmpty();
    assertThat(Sets.intersection(workers(), worker)).isEmpty();
  }

  @Test
  public void cancelledActionReplacesWorkerNotDeclaredCancellable() throws Exception {
    Scenario scenario = new Scenario(/* workerSupportsCancel= */ true, /* declared= */ false);
    Set<ProcessHandle> worker = startWorker(scenario);
    InFlight cancelled = startSleeping(scenario);

    cancelled.interrupt();

    assertThat(cancelled.failure()).isInstanceOf(InterruptedException.class);
    assertThat(worker.stream().anyMatch(ProcessHandle::isAlive)).isFalse();
    assertNothingLeftBehind(scenario);
    assertThat(workers()).isNotEmpty();
    assertThat(Sets.intersection(workers(), worker)).isEmpty();
  }

  @Test
  public void timedOutActionKeepsWorkerWhichSupportsCancel() throws Exception {
    Scenario scenario = new Scenario(/* workerSupportsCancel= */ true, /* declared= */ true);
    Set<ProcessHandle> worker = startWorker(scenario);

    Action slow = scenario.actionWithExtraInput("sleep", "60000", "/dev/null", OUTPUT);
    assertThat(slow.run(Duration.ofSeconds(1))).isEqualTo(Code.DEADLINE_EXCEEDED);

    assertThat(slow.result.getStderrRaw().toStringUtf8()).contains("did not respond");
    assertThat(workers()).isEqualTo(worker);
    assertNothingLeftBehind(scenario);
    assertThat(workers()).isEqualTo(worker);
  }

  @Test
  public void timedOutActionReplacesWorkerNotDeclaredCancellable() throws Exception {
    Scenario scenario = new Scenario(/* workerSupportsCancel= */ true, /* declared= */ false);
    Set<ProcessHandle> worker = startWorker(scenario);

    Action slow = scenario.actionWithExtraInput("sleep", "60000", "/dev/null", OUTPUT);
    assertThat(slow.run(Duration.ofSeconds(1))).isEqualTo(Code.DEADLINE_EXCEEDED);

    assertThat(worker.stream().anyMatch(ProcessHandle::isAlive)).isFalse();
    assertNothingLeftBehind(scenario);
    assertThat(workers()).isNotEmpty();
    assertThat(Sets.intersection(workers(), worker)).isEmpty();
  }

  @Test
  public void workersAreNotCancellableByDefault() {
    assertThat(PersistentExecutor.isCancellable(new ResourceLimits(), "Javac", Set.of())).isFalse();
  }

  @Test
  public void workerIsCancellableWhenActionOrConfigurationDeclaresIt() {
    ResourceLimits declaring = new ResourceLimits();
    declaring.persistentWorkerCancellable = true;
    Set<String> mnemonics = Set.of("Javac", "Scalac");

    assertThat(PersistentExecutor.isCancellable(declaring, "MyTool", Set.of())).isTrue();
    assertThat(PersistentExecutor.isCancellable(new ResourceLimits(), "Javac", mnemonics)).isTrue();
    assertThat(PersistentExecutor.isCancellable(new ResourceLimits(), "Scalac", mnemonics))
        .isTrue();
    assertThat(PersistentExecutor.isCancellable(new ResourceLimits(), "MyTool", mnemonics))
        .isFalse();
  }
}
