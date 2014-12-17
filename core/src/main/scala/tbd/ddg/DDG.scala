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
import tbd.list.ListInput
import tbd.master.Master
import tbd.messages._

class DDG {
  var root = RootNode.create()
  debug.TBD.nodes(root) = (Node.getId(), Tag.Root(), null)

  val reads = Map[ModId, Buffer[Pointer]]()
  val pars = Map[ActorRef, Pointer]()

  var updated = TreeSet[Pointer]()(new TimestampOrdering())

  private val ordering = new Ordering(root)

  val readers = Map[Int, Any => Changeable[Any]]()
  val read2ers = Map[Int, (Any, Any) => Changeable[Any]]()

  private val tasks = Map[TaskId, ActorRef]()

  var nextReadId = 0

  private var nextPutId = 0

  private val puts = Map[Int, (ListInput[Any, Any], Any, Any)]()

  def addPut
      (input: ListInput[Any, Any],
       key: Any,
       value: Any,
       c: Context): Pointer = {
    val putNodePointer = PutNode.create(nextPutId, c.currentModId, -1)
    puts(nextPutId) = (input, key, value)

    val timePtr = nextTimestamp(putNodePointer, c)

    timePtr
  }

  def addRead
      (mod: Mod[Any],
       value: Any,
       reader: Any => Changeable[Any],
       currentModId1: ModId,
       currentModId2: ModId,
       c: Context): Pointer = {
    val readNodePointer = ReadNode.create(
      mod.id, nextReadId, currentModId1, currentModId2)
    readers(nextReadId) = reader
    nextReadId += 1

    val timePtr = nextTimestamp(readNodePointer, c)
    if (reads.contains(mod.id)) {
      reads(mod.id) :+= timePtr
    } else {
      reads(mod.id) = Buffer(timePtr)
    }

    timePtr
  }

  def addRead2
      (mod1: Mod[Any],
       mod2: Mod[Any],
       value1: Any,
       value2: Any,
       reader: (Any, Any) => Changeable[Any],
       currentModId1: ModId,
       currentModId2: ModId,
       c: Context): Pointer = {
    val read2NodePointer = Read2Node.create(
      mod1.id, mod2.id, nextReadId, currentModId1, currentModId2)
    val timePtr = nextTimestamp(read2NodePointer, c)

    read2ers(nextReadId) = reader
    nextReadId += 1
  
    if (reads.contains(mod1.id)) {
      reads(mod1.id) :+= timePtr
    } else {
      reads(mod1.id) = Buffer(timePtr)
    }
  
    if (reads.contains(mod2.id)) {
      reads(mod2.id) :+= timePtr
    } else {
      reads(mod2.id) = Buffer(timePtr)
    }

    timePtr
  }

  def addWrite
      (modId: ModId,
       modId2: ModId,
       c: Context): Pointer = {
    val writeNodePointer = WriteNode.create(modId, modId2)
    nextTimestamp(writeNodePointer, c)
  }
  
  def addPar
      (taskRef1: ActorRef,
       taskRef2: ActorRef,
       taskId1: TaskId,
       taskId2: TaskId,
       c: Context): Pointer = {
    val parNodePointer = ParNode.create(taskId1, taskId2)
    val timePtr = nextTimestamp(parNodePointer, c)

    tasks(taskId1) = taskRef1
    tasks(taskId2) = taskRef2
    pars(taskRef1) = timePtr
    pars(taskRef2) = timePtr

    val endPtr = c.ddg.nextTimestamp(parNodePointer, c)
    Timestamp.setEndPtr(timePtr, endPtr)

    timePtr
  }

  def getLeftTask(ptr: Pointer): ActorRef = {
    tasks(ParNode.getTaskId1(ptr))
  }

  def getRightTask(ptr: Pointer): ActorRef = {
    tasks(ParNode.getTaskId2(ptr))
  }

  def nextTimestamp(ptr: Pointer, c: Context): Pointer = {
    val timePtr =
      if (c.initialRun)
        ordering.append(ptr)
      else
        ordering.after(c.currentTime, ptr)
  
    c.currentTime = timePtr
  
    timePtr
  }

  def getChildren(startPtr: Pointer, endPtr: Pointer): Buffer[Pointer] = {
    val children = Buffer[Pointer]()

    var timePtr = Timestamp.getNext(startPtr)
    while (timePtr != endPtr) {
      children += timePtr
      timePtr = Timestamp.getNext(Timestamp.getEndPtr(timePtr))
    }

    children
  }

  def modUpdated(modId: ModId) {
    for (timePtr <- reads(modId)) {
      updated += timePtr
    }
  }

  // Pebbles a par node. Returns true iff the pebble did not already exist.
  def parUpdated(taskRef: ActorRef, c: Context): Boolean = {
    val timePtr = pars(taskRef)

    val nodePtr = Timestamp.getNodePtr(timePtr)
    if (!ParNode.getPebble1(nodePtr) &&
        !ParNode.getPebble2(nodePtr)) {
      updated += timePtr
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

 def getMods(): Iterable[ModId] = {
   val mods = Buffer[ModId]()

   var time = startTime
   while (time != endTime) {
     val nodePtr = Timestamp.getNodePtr(time)

     if (Timestamp.getEndPtr(time) != -1) {
       Node.getType(nodePtr) match {
         case Node.ModNodeType =>
           val modId1 = ModNode.getModId1(nodePtr)
           if (modId1 != -1) {
             mods += modId1
           }

           val modId2 = ModNode.getModId2(nodePtr)
           if (modId2 != -1) {
             mods += modId2
           }
         case _ =>
       }
     }

     time = Timestamp.getNext(time)
   }

   mods
 }

  // Removes the subdddg between start and end, inclusive. This is the only
  // place in the code where Nodes are removed from the graph, so this is where
  // we must call free on their pointers.
  def splice(startPtr: Pointer, endPtr: Pointer, c: tbd.Context) {
    val startSublistPtr = Timestamp.getSublistPtr(startPtr)
    val startSublistBasePtr = Sublist.getBasePtr(startSublistPtr)
    val startPreviousTime = Timestamp.getPreviousTime(startPtr)
    val endSublistPtr = Timestamp.getSublistPtr(endPtr)

    var timePtr = startPtr
    while (Timestamp.<(timePtr, endPtr)) {
      val timeEndPtr = Timestamp.getEndPtr(timePtr)
      if (timeEndPtr != -1) {
        val nodePtr = Timestamp.getNodePtr(timePtr)

        Node.getType(nodePtr) match {
          case Node.MemoNodeType =>
            c.getMemoizer(MemoNode.getMemoizerId(nodePtr))
              .removeEntry(timePtr, MemoNode.getSignature(nodePtr))

          case Node.Memo1NodeType =>
            c.getMemoizer(Memo1Node.getMemoizerId(nodePtr))
              .removeEntry(timePtr, Memo1Node.getSignature(nodePtr))

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
            updated -= timePtr

          case Node.ReadNodeType =>
            c.ddg.reads(ReadNode.getModId(nodePtr)) -= timePtr
            updated -= timePtr

          case Node.Read2NodeType =>
            c.ddg.reads(Read2Node.getModId1(nodePtr)) -= timePtr
            c.ddg.reads(Read2Node.getModId2(nodePtr)) -= timePtr
            updated -= timePtr

          case Node.PutNodeType =>
            val (input, key, value) = puts(PutNode.getPutId(timePtr))
            input.remove(key, value)

          case Node.WriteNodeType =>
        }

        MemoryAllocator.free(nodePtr)

        if (Timestamp.>(timeEndPtr, endPtr)) {
          ordering.remove(timeEndPtr)
          MemoryAllocator.free(timeEndPtr)
        }
      }

      val lastTimePtr = timePtr
      timePtr = Timestamp.getNext(timePtr)
      MemoryAllocator.free(lastTimePtr)
    }

    if (startSublistPtr == endSublistPtr) {
      Timestamp.setNextTime(startPreviousTime, endPtr)
      Timestamp.setPreviousTime(endPtr, startPreviousTime)

      val newSize = Sublist.calculateSize(startSublistBasePtr)
      Sublist.setSize(startSublistPtr, newSize)
    } else {
      val newStartSublistPtr =
        if (startPreviousTime ==
            Timestamp.getPreviousTime(startSublistBasePtr)) {
          Sublist.getPreviousSub(startSublistPtr)
        } else {
          Timestamp.setNextTime(startPreviousTime, startSublistBasePtr)
          Timestamp.setPreviousTime(startSublistBasePtr, startPreviousTime)

          val newSize = Sublist.calculateSize(startSublistBasePtr)
          Sublist.setSize(startSublistPtr, newSize)

          startSublistPtr
        }

      val endSublistBasePtr = Sublist.getBasePtr(endSublistPtr)

      Timestamp.setPreviousTime(endPtr, endSublistBasePtr)
      Timestamp.setNextTime(endSublistBasePtr, endPtr)

      val newSize = Sublist.calculateSize(endSublistBasePtr)
      Sublist.setSize(endSublistPtr, newSize)

      Sublist.setNextSub(newStartSublistPtr, endSublistPtr)
    }
  }

  def startTime = {
    Sublist.getBasePtr(Sublist.getNextSub(ordering.basePtr))
  }

  def endTime = {
    Sublist.getBasePtr(ordering.basePtr)
  }

  def toString(prefix: String): String = {
    val out = new StringBuffer("")
    def innerToString(timePtr: Pointer, prefix: String) {
      val nodePtr = Timestamp.getNodePtr(timePtr)

      val timeString = " time = " + Timestamp.getTime(timePtr) + " to " +
        Timestamp.getTime(Timestamp.getEndPtr(timePtr))

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

            prefix + "ParNode " + nodePtr + " pebbles=(" +
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
  
      for (time <- getChildren(timePtr, Timestamp.getEndPtr(timePtr))) {
          innerToString(time, prefix + "-")
        }
      }

    innerToString(Timestamp.getNext(startTime), prefix)

    out.toString
  }
}
