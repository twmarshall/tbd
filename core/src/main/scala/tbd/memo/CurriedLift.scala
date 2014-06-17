/**
 * Copyright (C) 2013 Carnegie Mellon University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package tbd.memo

import scala.collection.mutable.ArrayBuffer

import tbd.TBD
import tbd.mod.Mod

class CurriedLift[T](baseLift: Lift[T], curry: List[_]) extends Lift[T](null, 0) {

  override def memo(aArgs: List[_], func: () => T): T = {
    baseLift.memo(curry ++ aArgs, func)
  }
}
