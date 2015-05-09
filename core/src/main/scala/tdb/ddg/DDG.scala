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

import akka.pattern.ask
import scala.collection.mutable
import scala.concurrent.Await

import tdb._
import tdb.Constants._
import tdb.list.ListInput
import tdb.master.Master
import tdb.messages._

class DDG(_c: Context) {
  val reads = mutable.Map[ModId, mutable.Buffer[Timestamp]]()
  val keys = mutable.Map[InputId, mutable.Map[Any, mutable.Buffer[Timestamp]]]()
  val pars = mutable.Map[TaskId, Timestamp]()
  val nodes = mutable.Map[NodeId, Timestamp]()

  var updated = mutable.TreeSet[Timestamp]()((new TimestampOrdering()).reverse)

  val ordering = new Ordering()

  var root = ordering.append(new RootNode())
  _c.currentTime = root

  def addPut
      (input: ListInput[Any, Any],
       key: Any,
       value: Any,
       c: Context): Timestamp = {
    val putNode = new PutNode(input, key, value)
    val timestamp = nextTimestamp(putNode, c)

    timestamp
  }

  def addPutAll
      (input: ListInput[Any, Any],
       values: Iterable[(Any, Any)],
       c: Context): Timestamp = {
    val putNode = new PutAllNode(input, values)
    val timestamp = nextTimestamp(putNode, c)

    timestamp
  }

  def addPutIn
      (traceable: Traceable[Any, Any, Any],
       parameters: Any,
       c: Context): Timestamp = {
    val putNode = new PutInNode(traceable, parameters)
    val timestamp = nextTimestamp(putNode, c)

    timestamp
  }

  def addGet
      (input: ListInput[Any, Any],
       key: Any,
       getter: Any => Unit,
       c: Context): Timestamp = {
    val getNode = new GetNode(input, key, getter)
    val timestamp = nextTimestamp(getNode, c)

    if (!keys.contains(input.inputId)) {
      keys(input.inputId) = mutable.Map[Any, mutable.Buffer[Timestamp]]()
    }

    if (keys(input.inputId).contains(key)) {
      keys(input.inputId)(key) += timestamp
    } else {
      keys(input.inputId)(key) = mutable.Buffer(timestamp)
    }

    timestamp
  }

  def addRead
      (mod: Mod[Any],
       value: Any,
       reader: Any => Changeable[Any],
       c: Context): Timestamp = {
    val readNode = new ReadNode(mod.id, reader)
    val timestamp = nextTimestamp(readNode, c)

    if (reads.contains(mod.id)) {
      reads(mod.id) :+= timestamp
    } else {
      reads(mod.id) = mutable.Buffer(timestamp)
    }

    timestamp
  }

  def addRead2
      (mod1: Mod[Any],
       mod2: Mod[Any],
       value1: Any,
       value2: Any,
       reader: (Any, Any) => Changeable[Any],
       c: Context): Timestamp = {
    val readNode = new Read2Node(mod1.id, mod2.id, reader)
    val timestamp = nextTimestamp(readNode, c)

    if (reads.contains(mod1.id)) {
      reads(mod1.id) :+= timestamp
    } else {
      reads(mod1.id) = mutable.Buffer(timestamp)
    }

    if (reads.contains(mod2.id)) {
      reads(mod2.id) :+= timestamp
    } else {
      reads(mod2.id) = mutable.Buffer(timestamp)
    }

    timestamp
  }


  def addRead3
      (mod1: Mod[Any],
       mod2: Mod[Any],
       mod3: Mod[Any],
       value1: Any,
       value2: Any,
       value3: Any,
       reader: (Any, Any, Any) => Changeable[Any],
       c: Context): Timestamp = {
    val readNode = new Read3Node(mod1.id, mod2.id, mod3.id, reader)
    val timestamp = nextTimestamp(readNode, c)

    if (reads.contains(mod1.id)) {
      reads(mod1.id) :+= timestamp
    } else {
      reads(mod1.id) = mutable.Buffer(timestamp)
    }

    if (reads.contains(mod2.id)) {
      reads(mod2.id) :+= timestamp
    } else {
      reads(mod2.id) = mutable.Buffer(timestamp)
    }

    if (reads.contains(mod3.id)) {
      reads(mod3.id) :+= timestamp
    } else {
      reads(mod3.id) = mutable.Buffer(timestamp)
    }

    timestamp
  }

  def addMod
      (modId1: ModId,
       modId2: ModId,
       c: Context): Timestamp = {
    val modNode = new ModNode(modId1, modId2)
    val timestamp = nextTimestamp(modNode, c)

    timestamp
  }

  def addWrite
      (modId: ModId,
       modId2: ModId,
       c: Context): Timestamp = {
    val writeNode = new WriteNode(modId, modId2)
    val timestamp = nextTimestamp(writeNode, c)

    timestamp
  }

  def addPar
      (taskId1: TaskId,
       taskId2: TaskId,
       c: Context): ParNode = {
    val parNode = new ParNode(taskId1, taskId2)
    val timestamp = nextTimestamp(parNode, c)

    pars(taskId1) = timestamp
    pars(taskId2) = timestamp

    timestamp.end = c.ddg.nextTimestamp(parNode, c)

    parNode
  }

  def addMemo
      (signature: Seq[Any],
       memoizer: Memoizer[_],
       c: Context): Timestamp = {
    val memoNode = new MemoNode(signature, memoizer)
    val timestamp = nextTimestamp(memoNode, c)

    timestamp
  }

  def nextTimestamp(node: Node, c: Context): Timestamp = {
    val time =
      if (c.initialRun)
        ordering.append(node)
      else
        ordering.after(c.currentTime, node)

    c.currentTime = time

    time
  }

  def modUpdated(modId: ModId) {
    for (timestamp <- reads(modId)) {
      if (!timestamp.node.updated) {
        updated += timestamp

        timestamp.node.updated = true
      }
    }
  }

  def nodeUpdated(nodeId: NodeId) {
    val timestamp = nodes(nodeId)
    if (!timestamp.node.updated) {
      updated += timestamp

      timestamp.node.updated = true
    }
  }

  def keyUpdated(inputId: InputId, key: Any) {
    for (timestamp <- keys(inputId)(key)) {
      if (!timestamp.node.updated) {
        updated += timestamp

        timestamp.node.updated = true
      }
    }
  }

  def modRemoved(modId: ModId) {
    if (reads.contains(modId)) {
      for (timestamp <- reads(modId)) {
        timestamp.node.updated = false
      }
    }
  }

  def keyRemoved(inputId: InputId, key: Any) {
    if (keys.contains(inputId) && keys(inputId).contains(key)) {
      for (timestamp <- keys(inputId)(key)) {
        timestamp.node.updated = false
      }
    }
  }

  // Pebbles a par node. Returns true iff the pebble did not already exist.
  def parUpdated(taskId: TaskId): Boolean = {
    val timestamp = pars(taskId)
    val parNode = timestamp.node.asInstanceOf[ParNode]

    if (!parNode.pebble1 && !parNode.pebble2) {
      updated += timestamp
      parNode.updated = true
    }

    if (parNode.taskId1 == taskId) {
      val ret = !parNode.pebble1
      parNode.pebble1 = true
      ret
    } else {
      val ret = !parNode.pebble2
      parNode.pebble2 = true
      ret
    }
  }

  def startTime = ordering.base.next.base

  def endTime = ordering.base.base

  def getMods(): Iterable[ModId] = {
    val mods = mutable.Buffer[ModId]()

    var time = startTime.getNext()
    while (time != endTime) {
      val node = time.node

      if (time.end != null) {
        node match {
          case modNode: ModNode =>
            mods += modNode.modId1
            if (modNode.modId2 != -1)
              mods += modNode.modId2
          case _ =>
        }
      }

      time = time.getNext()
    }

    mods
  }

  override def toString = toString("")

  def toString(prefix: String): String = {
    val out = new StringBuffer("")
    def innerToString(time: Timestamp, prefix: String) {
      val thisString = time.node match {
        case memo: MemoNode =>
          prefix + memo + " time = " + time + " to " + time.end +
          " signature=" + memo.signature + "\n"
        case mod: ModNode =>
          prefix + mod + " time = " + time + " to " + time.end + "\n"
        case par: ParNode =>
          import scala.concurrent.ExecutionContext.Implicits.global
          val future1 = _c.resolver.send(par.taskId1, GetTaskDDGMessage)
          val future2 = _c.resolver.send(par.taskId2, GetTaskDDGMessage)

          val ddg1 = Await.result(future1.mapTo[DDG], DURATION)
          val ddg2 = Await.result(future2.mapTo[DDG], DURATION)

          prefix + par + " pebbles=(" + par.pebble1 +
          ", " + par.pebble2 + ")\n" + ddg1.toString(prefix + "|") +
          ddg2.toString(prefix + "|")
        case read: ReadNode =>
          prefix + read + " modId=(" + read.modId + ") " + " time=" +
          time + " to " + time.end + " updated=(" + read.updated + ")\n"
        case root: RootNode =>
          prefix + "RootNode=" + root + "\n"
        case write: WriteNode =>
          prefix + write + " modId=(" + write.modId + ")\n"
        case x => "???"
      }
      out.append(thisString)

      for (time <- ordering.getChildren(time, time.end)) {
        innerToString(time, prefix + "-")
      }
    }
    innerToString(startTime.getNext(), prefix)

    out.toString
  }
}
