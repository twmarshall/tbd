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
package tbd.ddg

import tbd.Constants.ModId

abstract class Tag
object Tag {
  case class Read(val readValue: Any, val reader: FunctionTag) extends Tag
  case class Write(val value: Any, val dest: ModId) extends Tag
  case class Memo(val function: FunctionTag, val args: List[Any]) extends Tag
  case class Mod(val dest: ModId, val initializer: FunctionTag) extends Tag
  case class Par(val fun1: FunctionTag, val fun2: FunctionTag) extends Tag
  case class Root() extends Tag
}

case class FunctionTag(val funcId: Int, val freeVars: List[(String, Any)])
