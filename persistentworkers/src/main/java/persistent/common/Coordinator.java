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

package persistent.common;

import java.io.IOException;
import persistent.common.CtxAround.Id;

/**
 * Manages worker lifetimes and acts as the mediator between executors and workers. It also manages
 * pre-work initialization and post-work cleanup.
 *
 * @param <K> worker key type
 * @param <I> request type
 * @param <O> work response type
 * @param <W> worker type
 * @param <CI> request with extra context/info
 * @param <CO> response with extra context/info
 * @param <P> pool type
 */
public abstract class Coordinator<
    K,
    I,
    O,
    W extends Worker<I, O>,
    CI extends CtxAround<I>,
    CO extends CtxAround<O>,
    P extends ObjectPool<K, W>> {
  protected final P workerPool;

  public Coordinator(P workerPool) {
    this.workerPool = workerPool;
  }

  /**
   * Runs a request on a worker obtained from the pool.
   *
   * <p>The worker is always returned to the pool, whether the request completes, fails, or the
   * calling thread is interrupted. Workers which are no longer usable are expected to fail pool
   * validation on return and get discarded, so a failed or cancelled request cannot leak a pool
   * slot.
   */
  public CO runRequest(K workerKey, CI reqWithCtx) throws Exception {
    W worker = workerPool.obtain(workerKey);
    try {
      I request = preWorkInit(workerKey, reqWithCtx, worker);
      O workResponse = doWork(worker, request, reqWithCtx);
      return postWorkCleanup(workResponse, worker, reqWithCtx);
    } catch (Throwable t) {
      try {
        abortWork(worker, reqWithCtx);
      } catch (Throwable abortFailure) {
        t.addSuppressed(abortFailure);
      }
      throw t;
    } finally {
      workerPool.release(workerKey, worker);
    }
  }

  /** Hook for passing per-request context (e.g. a deadline) through to the worker. */
  protected O doWork(W worker, I request, CI reqWithCtx) throws Exception {
    return worker.doWork(request);
  }

  /**
   * Called when a request did not complete normally (an exception, or an interrupt) and before the
   * worker is returned to the pool. Implementations should undo what {@link #preWorkInit} did to
   * the worker's environment so the next request does not observe it.
   */
  protected void abortWork(W worker, CI request) throws IOException {}

  public abstract I preWorkInit(K workerKey, CI request, W worker) throws IOException;

  public abstract CO postWorkCleanup(O response, W worker, CI request) throws IOException;

  public static <K, I, O, W extends Worker<I, O>> SimpleCoordinator<K, I, O, W> simple(
      ObjectPool<K, W> workerPool) {
    return new SimpleCoordinator<>(workerPool);
  }

  /**
   * A Coordinator which doesn't have any extra metadata for the request and response types Its pool
   * type is also filled in as an ObjectPool interface
   */
  public static class SimpleCoordinator<K, I, O, W extends Worker<I, O>>
      extends Coordinator<K, I, O, W, Id<I>, Id<O>, ObjectPool<K, W>> {
    public SimpleCoordinator(ObjectPool<K, W> workerPool) {
      super(workerPool);
    }

    @Override
    public I preWorkInit(K workerKey, Id<I> request, W worker) {
      return request.get();
    }

    @Override
    public Id<O> postWorkCleanup(O response, W worker, Id<I> request) {
      return Id.of(response);
    }
  }
}
