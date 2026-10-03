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

public interface Worker<I, O> extends Destructable {
  /**
   * Performs the work for a request, blocking until it completes.
   *
   * <p>If the calling thread is interrupted while the work is in flight, the worker must bring
   * itself back to a state where it is either safe to reuse or has been destroyed, and then
   * propagate the {@link InterruptedException}.
   */
  O doWork(I request) throws InterruptedException;

  default void destroy() {}
}
