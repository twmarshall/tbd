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
package tdb.ddg

import tdb._
import tdb.Constants.{ModId, TaskId}
import tdb.list.ListInput

object Node {
  var id = 0

  def getId(): Int = {
    id = id + 1
    id
  }
}

sealed trait Node {
  var currentModId: ModId = -1

  var currentModId2: ModId = -1

  var updated = false
}

sealed trait ReexecutableNode extends Node

class MemoNode
    (val signature: Seq[Any],
     val memoizer: Memoizer[_]) extends Node {

  var value: Any = null
}

class ModNode
    (val modId1: ModId,
     val modId2: ModId) extends Node

class ParNode
    (val taskId1: TaskId,
     val taskId2: TaskId) extends Node {
  var pebble1 = false
  var pebble2 = false
}

class PutNode
    (val input: ListInput[Any, Any],
     val key: Any,
     val value: Any) extends Node

class PutAllNode
    (val input: ListInput[Any, Any],
     val values: Iterable[(Any, Any)]) extends Node

class PutInNode
    (val traceable: Traceable[Any, Any, Any],
     val parameters: Any) extends Node

class GetNode
    (val input: ListInput[Any, Any],
     val key: Any,
     val getter: Any => Unit) extends ReexecutableNode

class GetFromNode
    (val traceable: Traceable[Any, Any, Any],
     val parameters: Any,
     val getter: Any => Unit) extends ReexecutableNode

class ReadNode
    (val modId: ModId,
     val reader: Any => Changeable[Any])
  extends ReexecutableNode

class Read2Node
    (val modId1: ModId,
     val modId2: ModId,
     val reader: (Any, Any) => Changeable[Any]) extends ReexecutableNode

class Read3Node
    (val modId1: ModId,
     val modId2: ModId,
     val modId3: ModId,
     val reader: (Any, Any, Any) => Changeable[Any]) extends ReexecutableNode

class RootNode extends Node

class WriteNode(val modId: ModId, val modId2: ModId) extends Node
