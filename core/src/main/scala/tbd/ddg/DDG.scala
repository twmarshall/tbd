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

  var updated = TreeSet[Timestamp]()(new TimestampOrdering())

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

    val end = c.ddg.nextTimestamp(parNodePointer, c)
    Timestamp.setEndPtr(timestamp.ptr, end.ptr)

    timestamp
  }

  def getLeftTask(ptr: Pointer): ActorRef = {
    tasks(ParNode.getTaskId1(ptr))
  }

  def getRightTask(ptr: Pointer): ActorRef = {
    tasks(ParNode.getTaskId2(ptr))
  }

  def nextTimestamp(ptr: Pointer, c: Context): Timestamp = {
    val timePtr =
      if (c.initialRun)
        ordering.append(ptr)
      else
        ordering.after(c.currentTime, ptr)

    c.currentTime = timePtr

    Timestamp.getTimestamp(timePtr)
  }

  def getChildren(start: Timestamp, end: Timestamp): Buffer[Timestamp] = {
    val children = Buffer[Timestamp]()

    var timePtr = Timestamp.getNext(start.ptr)
    while (timePtr != end.ptr) {
      children += Timestamp.getTimestamp(timePtr)
      timePtr = Timestamp.getNext(Timestamp.getEndPtr(timePtr))
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

    val nodePtr = Timestamp.getNodePtr(timestamp.ptr)
    if (!ParNode.getPebble1(nodePtr) &&
        !ParNode.getPebble2(nodePtr)) {
      updated += timestamp
    }

    if (tasks(ParNode.getTaskId1(nodePtr)) == taskRef) {
      val ret = !ParNode.getPebble1(nodePtr)
      ParNode.setPebble1(nodePtr, true)
      ret
    } else {
      val ret = !ParNode.getPebble2(nodePtr)
      ParNode.setPebble2(nodePtr, true)
      ret
    }
  }

  // Removes the subdddg between start and end, inclusive. This is the only
  // place in the code where Nodes are removed from the graph, so this is where
  // we must call free on their pointers.
  def splice(_start: Pointer, endPtr: Pointer, c: tbd.Context) {
    val start = Timestamp.getTimestamp(_start)
    val end = Timestamp.getTimestamp(endPtr)

    var time = start
    while (Timestamp.<(time.ptr, endPtr)) {
      val timeEndPtr = Timestamp.getEndPtr(time.ptr)
      if (timeEndPtr != -1) {
        val nodePtr = Timestamp.getNodePtr(time.ptr)

        Node.getType(nodePtr) match {
          case Node.MemoNodeType =>
            c.getMemoizer(MemoNode.getMemoizerId(nodePtr))
              .removeEntry(time, MemoNode.getSignature(nodePtr))

          case Node.Memo1NodeType =>
            c.getMemoizer(Memo1Node.getMemoizerId(nodePtr))
              .removeEntry(time, Memo1Node.getSignature(nodePtr))

          case Node.ModizerNodeType =>
            val modizerId = ModizerNode.getModizerId(nodePtr)

            val key  = ModizerNode.getKey(nodePtr)

            if (c.getModizer(modizerId).remove(key)) {
              val modId1 = ModizerNode.getModId1(nodePtr)
              if (modId1 != -1) {
                c.remove(modId1)
              }

              val modId2 = ModizerNode.getModId2(nodePtr)
              if (modId2 != -1) {
                c.remove(modId2)
              }
            }

          case Node.ModNodeType =>
            val modId1 = ModNode.getModId1(nodePtr)
            if (modId1 != -1) {
              c.remove(modId1)
            }

            val modId2 = ModNode.getModId2(nodePtr)
            if (modId2 != -1) {
              c.remove(modId2)
            }

          case Node.ParNodeType =>
            updated -= time

          case Node.ReadNodeType =>
            c.ddg.reads(ReadNode.getModId(nodePtr)) -= time
            updated -= time

          case Node.Read2NodeType =>
            c.ddg.reads(Read2Node.getModId1(nodePtr)) -= time
            c.ddg.reads(Read2Node.getModId2(nodePtr)) -= time
            updated -= time

          case Node.WriteNodeType =>
        }

        MemoryAllocator.free(nodePtr)

        if (Timestamp.>(timeEndPtr, endPtr)) {
          ordering.remove(timeEndPtr)
        }
      }

      val nextPtr = Timestamp.getNext(time.ptr)
      time = Timestamp.getTimestamp(nextPtr)
    }

    val startSublistPtr = Timestamp.getSublistPtr(start.ptr)
    val startSublistBasePtr = Sublist.getBasePtr(startSublistPtr)
    val endSublistPtr = Timestamp.getSublistPtr(end.ptr)
    if (startSublistPtr == endSublistPtr) {
      Timestamp.setNextTime(Timestamp.getPreviousTime(start.ptr), end.ptr)
      Timestamp.setPreviousTime(end.ptr, Timestamp.getPreviousTime(start.ptr))

      val newSize = Sublist.calculateSize(startSublistBasePtr)
      Sublist.setSize(startSublistPtr, newSize)
    } else {
      val newStartSublistPtr =
        if (Timestamp.getPreviousTime(start.ptr) ==
            Timestamp.getPreviousTime(startSublistBasePtr)) {
          Sublist.getPreviousSub(startSublistPtr)
        } else {
          Timestamp.setNextTime(Timestamp.getPreviousTime(start.ptr), startSublistBasePtr)
          Timestamp.setPreviousTime(startSublistBasePtr, Timestamp.getPreviousTime(start.ptr))

          val newSize = Sublist.calculateSize(startSublistBasePtr)
          Sublist.setSize(startSublistPtr, newSize)

          startSublistPtr
        }

      val startSublist = Sublist.getSublist(newStartSublistPtr)
      val endSublistBasePtr = Sublist.getBasePtr(endSublistPtr)

      Timestamp.setPreviousTime(end.ptr, endSublistBasePtr)
      Timestamp.setNextTime(endSublistBasePtr, end.ptr)

      val newSize = Sublist.calculateSize(endSublistBasePtr)
      Sublist.setSize(endSublistPtr, newSize)

      val endSublist = Sublist.getSublist(endSublistPtr)
      startSublist.nextSub = endSublist
      Sublist.setNextSub(startSublist.ptr, endSublistPtr)
      endSublist.previousSub = startSublist
    }
  }

  def startTime = ordering.base.nextSub.base

  def endTime = ordering.base.base

  def toString(prefix: String): String = {
    val out = new StringBuffer("")
    def innerToString(time: Timestamp, prefix: String) {
      val nodePtr = Timestamp.getNodePtr(time.ptr)

      val timeString = " time = " + time + " to " +
        Timestamp.getEndPtr(time.ptr)

      val thisString =
        Node.getType(nodePtr) match {
          case Node.MemoNodeType =>
            prefix + "MemoNode " + nodePtr + timeString + " signature=" +
            MemoNode.getSignature(nodePtr) + "\n"
          case Node.ModizerNodeType =>
            prefix + "ModizerNode " + nodePtr + " modId1=)" +
            ModizerNode.getModId1(nodePtr) + ") modId2=(" +
            ModizerNode.getModId1(nodePtr) + ")" + timeString + "\n"
          case Node.ModNodeType =>
            prefix + "ModNode " + nodePtr + " modId1=)" +
            ModNode.getModId1(nodePtr) + ") modId2=(" +
            ModNode.getModId1(nodePtr) + ")" + timeString + "\n"
          case Node.ParNodeType =>
            val taskRef1 = tasks(ParNode.getTaskId1(nodePtr))
            val taskRef2 = tasks(ParNode.getTaskId2(nodePtr))

            val f1 = taskRef1 ? GetTaskDDGMessage
            val f2 = taskRef2 ? GetTaskDDGMessage

            val ddg1 = Await.result(f1.mapTo[DDG], DURATION)
            val ddg2 = Await.result(f2.mapTo[DDG], DURATION)

            prefix + "ParNode " + nodePtr  + " pebbles=(" +
            ParNode.getPebble1(nodePtr) + ", " +
            ParNode.getPebble2(nodePtr) + ")\n" +
            ddg1.toString(prefix + "|") + ddg2.toString(prefix + "|")
          case Node.RootNodeType =>
            prefix + "RootNode " + nodePtr + "\n"
          case Node.ReadNodeType =>
            prefix + "ReadNode " + nodePtr + " modId=(" +
            ReadNode.getModId(nodePtr) + ")" + timeString + "\n"
          case Node.Read2NodeType =>
            prefix + "Read2Node " + nodePtr + " modId1=(" +
            Read2Node.getModId1(nodePtr) + ") modId2=(" +
            Read2Node.getModId2(nodePtr) + ")" + timeString + "\n"
          case Node.WriteNodeType =>
            prefix + "WriteNode " + nodePtr + " modId=(" +
            WriteNode.getModId1(nodePtr) + ")\n"
        }

      out.append(thisString)

      val end = Timestamp.getTimestamp(Timestamp.getEndPtr(time.ptr))
      for (time <- getChildren(time, end)) {
        innerToString(time, prefix + "-")
      }
    }
    val nextPtr = Timestamp.getNext(startTime.ptr)
    innerToString(Timestamp.getTimestamp(nextPtr), prefix)

    out.toString
  }
}
