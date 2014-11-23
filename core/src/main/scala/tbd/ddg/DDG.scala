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

import akka.actor.ActorRef
import akka.pattern.ask
import scala.collection.mutable.{Buffer, Map, MutableList, Set, TreeSet}
import scala.concurrent.Await

import tbd._
import tbd.Constants._
import tbd.master.Master
import tbd.messages._

class DDG {
  var root = RootNode.create()
  debug.TBD.nodes(root) = (Node.getId(), Tag.Root(), null)

  val reads = Map[ModId, Buffer[Timestamp]]()
  val pars = Map[ActorRef, Timestamp]()

  var updated = TreeSet[Timestamp]()((new TimestampOrdering()).reverse)

  private val ordering = new Ordering(root)

  val readers = Map[Int, Any => Changeable[Any]]()
  val read2ers = Map[Int, (Any, Any) => Changeable[Any]]()

  private val tasks = Map[TaskId, ActorRef]()

  var nextReadId = 0

  def addRead
      (mod: Mod[Any],
       value: Any,
       reader: Any => Changeable[Any],
       currentModId1: ModId,
       currentModId2: ModId,
       c: Context): Timestamp = {
    val readNodePointer = ReadNode.create(
      mod.id, nextReadId, currentModId1, currentModId2)
    readers(nextReadId) = reader
    nextReadId += 1

    val timestamp = nextTimestamp(readNodePointer, c)

    if (reads.contains(mod.id)) {
      reads(mod.id) :+= timestamp
    } else {
      reads(mod.id) = Buffer(timestamp)
    }

    timestamp
  }

  def addRead2
      (mod1: Mod[Any],
       mod2: Mod[Any],
       value1: Any,
       value2: Any,
       reader: (Any, Any) => Changeable[Any],
       currentModId1: ModId,
       currentModId2: ModId,
       c: Context): Timestamp = {
    val read2NodePointer = Read2Node.create(
      mod1.id, mod2.id, nextReadId, currentModId1, currentModId2)
    val timestamp = nextTimestamp(read2NodePointer, c)

    read2ers(nextReadId) = reader
    nextReadId += 1

    if (reads.contains(mod1.id)) {
      reads(mod1.id) :+= timestamp
    } else {
      reads(mod1.id) = Buffer(timestamp)
    }

    if (reads.contains(mod2.id)) {
      reads(mod2.id) :+= timestamp
    } else {
      reads(mod2.id) = Buffer(timestamp)
    }

    timestamp
  }

  def addMemo
      (signature: Seq[Any],
       memoizerId: MemoizerId,
       currentModId1: ModId,
       currentModId2: ModId,
       c: Context): Timestamp = {
    val memoNodePointer = MemoNode.create(
      memoizerId, signature, currentModId1, currentModId2)
    val timestamp = nextTimestamp(memoNodePointer, c)

    assert(Node.getCurrentModId2(timestamp.pointer) == currentModId2)
    assert(Node.getCurrentModId1(timestamp.pointer) == currentModId1)

    timestamp
  }

  def addMod
      (modId1: ModId,
       modId2: ModId,
       modizerId: ModizerId,
       key: Any,
       currentModId1: ModId,
       currentModId2: ModId,
       c: Context): Timestamp = {
    val modNodePointer = ModNode.create(
      modId1, modId2, modizerId, key, currentModId1, currentModId2)
    val timestamp = nextTimestamp(modNodePointer, c)

    timestamp
  }

  def addWrite
      (modId: ModId,
       modId2: ModId,
       c: Context): Timestamp = {
    val writeNodePointer = WriteNode.create(modId, modId2)
    val timestamp = nextTimestamp(writeNodePointer, c)

    timestamp
  }

  def addPar
      (taskRef1: ActorRef,
       taskRef2: ActorRef,
       taskId1: TaskId,
       taskId2: TaskId,
       c: Context): Timestamp = {
    val parNodePointer = ParNode.create(taskId1, taskId2)
    val timestamp = nextTimestamp(parNodePointer, c)

    tasks(taskId1) = taskRef1
    tasks(taskId2) = taskRef2
    pars(taskRef1) = timestamp
    pars(taskRef2) = timestamp

    timestamp.end = c.ddg.nextTimestamp(parNodePointer, c)

    timestamp
  }

  def getLeftTask(ptr: Pointer): ActorRef = {
    tasks(ParNode.getTaskId1(ptr))
  }

  def getRightTask(ptr: Pointer): ActorRef = {
    tasks(ParNode.getTaskId2(ptr))
  }

  def nextTimestamp(ptr: Pointer, c: Context): Timestamp = {
    val time =
      if (c.initialRun)
        ordering.append(ptr)
      else
        ordering.after(c.currentTime, ptr)

    c.currentTime = time

    time
  }

  def getChildren(start: Timestamp, end: Timestamp): Buffer[Timestamp] = {
    val children = Buffer[Timestamp]()

    var time = start.getNext()
    while (time != end) {
      children += time
      time = time.end.getNext()
    }

    children
  }

  def modUpdated(modId: ModId) {
    for (timestamp <- reads(modId)) {
      updated += timestamp
    }
  }

  // Pebbles a par node. Returns true iff the pebble did not already exist.
  def parUpdated(taskRef: ActorRef, c: Context): Boolean = {
    val timestamp = pars(taskRef)

    if (!ParNode.getPebble1(timestamp.pointer) &&
        !ParNode.getPebble2(timestamp.pointer)) {
      updated += timestamp
    }

    if (tasks(ParNode.getTaskId1(timestamp.pointer)) == taskRef) {
      val ret = !ParNode.getPebble1(timestamp.pointer)
      ParNode.setPebble1(timestamp.pointer, true)
      ret
    } else {
      val ret = !ParNode.getPebble2(timestamp.pointer)
      ParNode.setPebble2(timestamp.pointer, true)
      ret
    }
  }

  def splice(start: Timestamp, end: Timestamp, c: tbd.Context) {
    var time = start
    while (time < end) {
      if (time.end != null) {
        Node.getType(time.pointer) match {
          case Node.MemoNodeType =>
            c.getMemoizer(MemoNode.getMemoizerId(time.pointer))
              .removeEntry(time, MemoNode.getSignature(time.pointer))

          case Node.ModNodeType =>
            val modizerId = ModNode.getModizerId(time.pointer)

            if (modizerId != -1) {
              val key  = ModNode.getKey(time.pointer)

              if (c.getModizer(modizerId).remove(key)) {
                val modId1 = ModNode.getModId1(time.pointer)
                if (modId1 != -1) {
                  c.remove(modId1)
                }

                val modId2 = ModNode.getModId2(time.pointer)
                if (modId2 != -1) {
                  c.remove(modId2)
                }
              }
            } else {
              val modId1 = ModNode.getModId1(time.pointer)
              if (modId1 != -1) {
                c.remove(modId1)
              }

              val modId2 = ModNode.getModId2(time.pointer)
              if (modId2 != -1) {
                c.remove(modId2)
              }
            }

            MemoryAllocator.free(time.pointer)

          case Node.ParNodeType =>
            updated -= time

          case Node.ReadNodeType =>
            c.ddg.reads(ReadNode.getModId(time.pointer)) -= time
            updated -= time
            MemoryAllocator.free(time.pointer)

          case Node.Read2NodeType =>
            c.ddg.reads(Read2Node.getModId1(time.pointer)) -= time
            c.ddg.reads(Read2Node.getModId2(time.pointer)) -= time
            updated -= time

          case Node.WriteNodeType =>
        }

        if (time.end > end) {
          ordering.remove(time.end)
        }
      }

      time = time.getNext()
    }

    if (start.sublist == end.sublist) {
      start.previous.next = end
      end.previous = start.previous

      var size = 0
      var stamp = start.sublist.base.next
      while (stamp != start.sublist.base) {
        size += 1
        stamp = stamp.next
      }
      start.sublist.size = size
    } else {
      val startSublist =
        if (start.previous == start.sublist.base) {
          start.sublist.previous
        } else {
          start.previous.next = start.sublist.base
          start.sublist.base.previous = start.previous

          var size = 0
          var stamp = start.sublist.base.next
          while (stamp != start.sublist.base) {
            size += 1
            stamp = stamp.next
          }
          start.sublist.size = size

          start.sublist
        }

      end.previous = end.sublist.base
      end.sublist.base.next = end

      var size = 0
      var stamp = end.sublist.base.next
      while (stamp != end.sublist.base) {
        size += 1
        stamp = stamp.next
      }
      end.sublist.size = size

      startSublist.next = end.sublist
      end.sublist.previous = startSublist
    }
  }

  def startTime = ordering.base.next.base

  def endTime = ordering.base.base

  def toString(prefix: String): String = {
    val out = new StringBuffer("")
    def innerToString(time: Timestamp, prefix: String) {
      val thisString =
        Node.getType(time.pointer) match {
          case Node.MemoNodeType =>
            prefix + "MemoNode " + time.pointer + " time = " + time + " to " +
            time.end + " signature=" +
            MemoNode.getSignature(time.pointer) + "\n"
          case Node.ModNodeType =>
            prefix + "ModNode " + time.pointer + " modId1=)" +
            ModNode.getModId1(time.pointer) + ") modId2=(" +
            ModNode.getModId1(time.pointer) + ") time = " + time + " to " +
            time.end + "\n"
          case Node.ParNodeType =>
            val taskRef1 = tasks(ParNode.getTaskId1(time.pointer))
            val taskRef2 = tasks(ParNode.getTaskId2(time.pointer))

            val f1 = taskRef1 ? GetTaskDDGMessage
            val f2 = taskRef2 ? GetTaskDDGMessage

            val ddg1 = Await.result(f1.mapTo[DDG], DURATION)
            val ddg2 = Await.result(f2.mapTo[DDG], DURATION)

            prefix + "ParNode " + time.pointer  + " pebbles=(" +
            ParNode.getPebble1(time.pointer) + ", " +
            ParNode.getPebble2(time.pointer) + ")\n" +
            ddg1.toString(prefix + "|") + ddg2.toString(prefix + "|")
          case Node.RootNodeType =>
            prefix + "RootNode " + time.pointer + "\n"
          case Node.ReadNodeType =>
            prefix + "ReadNode " + time.pointer + " modId=(" +
            ReadNode.getModId(time.pointer) + ") time=" + time + " to " +
            time.end + "\n"
          case Node.Read2NodeType =>
            prefix + "Read2Node " + time.pointer + " modId1=(" +
            Read2Node.getModId1(time.pointer) + ") modId2=(" +
            Read2Node.getModId2(time.pointer) + ") time=" + time + " to " +
            time.end + "\n"
          case Node.WriteNodeType =>
            prefix + "WriteNode " + time.pointer + " modId=(" +
            WriteNode.getModId1(time.pointer) + ")\n"
        }

      out.append(thisString)

      for (time <- getChildren(time, time.end)) {
        innerToString(time, prefix + "-")
      }
    }
    innerToString(startTime.getNext(), prefix)

    out.toString
  }
}
