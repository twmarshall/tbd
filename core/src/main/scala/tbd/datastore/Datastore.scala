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
package tbd.datastore

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.pattern.ask
import java.io.{BufferedReader, File, FileReader}
import java.util.regex.Pattern
import scala.collection.mutable.{Buffer, Map}
import scala.concurrent.{Await, Future}

import tbd.Mod
import tbd.Constants._
import tbd.list._
import tbd.messages._

object Datastore {
  def props(): Props = Props(classOf[Datastore])
}

class Datastore extends Actor with ActorLogging {
  import context.dispatcher

  var workerId: String = _

  private val mods = Map[ModId, Any]()

  private var nextModId = 0

  private val lists = Map[String, ListInput[Any, Any]]()

  private var nextListId = 0

  private val dependencies = Map[ModId, Set[ActorRef]]()

  // Maps logical names of datastores to their references.
  private val datastores = Map[String, ActorRef]()

  private var misses = 0

  def createMod[T](value: T): Mod[T] = {
    val modId = workerId + ":" + nextModId
    nextModId += 1

    mods(modId) = value

    new Mod(modId)
  }

  def read[T](mod: Mod[T]): T = {
    mods(mod.id).asInstanceOf[T]
  }

  def getMod(modId: ModId, taskRef: ActorRef): Any = {
    if (mods.contains(modId)) {
      if (mods(modId) == null)
	NullMessage
      else
	mods(modId)
    } else {
      val workerId = modId.split(":")(0)
      val future = datastores(workerId) ? GetModMessage(modId, taskRef)

      /*misses += 1
      log.info(misses + " misses")*/

      Await.result(future, DURATION)
    }
  }

  def update[T](mod: Mod[T], value: T) {
    val futures = Buffer[Future[String]]()

    if (!mods.contains(mod.id) || mods(mod.id) != value) {
      mods(mod.id) = value

      if (dependencies.contains(mod.id)) {
	for (taskRef <- dependencies(mod.id)) {
	  futures += (taskRef ? ModUpdatedMessage(mod.id)).mapTo[String]
	}
      }
    }

    Await.result(Future.sequence(futures), DURATION)
  }

  def updateMod
      (modId: ModId,
       value: Any,
       task: ActorRef,
       respondTo: ActorRef) {
    val futures = Buffer[Future[String]]()

    if (!mods.contains(modId) || mods(modId) != value) {
      mods(modId) = value

      if (dependencies.contains(modId)) {
	for (taskRef <- dependencies(modId)) {
          if (taskRef != task) {
	    futures += (taskRef ? ModUpdatedMessage(modId)).mapTo[String]
          }
	}
      }
    }

    Future.sequence(futures).onComplete {
      case value => respondTo ! "done"
    }
  }

  def receive = {
    case CreateModMessage(value: Any) =>
      sender ! createMod(value)

    case CreateModMessage(null) =>
      sender ! createMod(null)

    case GetModMessage(modId: ModId, taskRef: ActorRef) =>
      sender ! getMod(modId, taskRef)

      if (dependencies.contains(modId)) {
	dependencies(modId) += taskRef
      } else {
	dependencies(modId) = Set(taskRef)
      }

    case GetModMessage(modId: ModId, null) =>
      sender ! getMod(modId, null)

    case UpdateModMessage(modId: ModId, value: Any, task: ActorRef) =>
      updateMod(modId, value, task, sender)

    case UpdateModMessage(modId: ModId, value: Any, null) =>
      updateMod(modId, value, null, sender)

    case UpdateModMessage(modId: ModId, null, task: ActorRef) =>
      updateMod(modId, null, task, sender)

    case UpdateModMessage(modId: ModId, null, null) =>
      updateMod(modId, null, null, sender)

    case RemoveModsMessage(modIds: Iterable[ModId]) =>
      for (modId <- modIds) {
	mods -= modId
	dependencies -= modId
      }

      sender ! "done"

    case CreateListMessage(conf: ListConf) =>
      val listId = nextListId + ""
      nextListId += 1
      val list =
	if (conf.chunkSize == 1) {
	  new ListModifier[Any, Any](this)
	} else {
	  new ChunkListModifier[Any, Any](this, conf)
	}

      lists(listId) = list

      if (conf.file != "") {
	val file = new File("wiki.xml")
	val fileSize = file.length()

	val in = new BufferedReader(new FileReader("wiki.xml"))
	val partitionSize = (fileSize / conf.partitions).toInt
	var buf = new Array[Char](partitionSize)

	in.skip(partitionSize * conf.partitionIndex)
	in.read(buf)

	val regex = Pattern.compile("""(?s)<key>(.*?)</key>[\s]*?<value>(.*?)</value>""")
	val str = new String(buf)
	val matcher = regex.matcher(str)

	var end = 0
	while (matcher.find()) {
	  list.put(matcher.group(1), matcher.group(2))
	  end = matcher.end()
	}

	if (conf.partitionIndex != conf.partitions - 1) {
	  var remaining = str.substring(end)
	  var done = false
	  while (!done) {
	    val smallBuf = new Array[Char](64)
	    in.read(smallBuf)

	    remaining += new String(smallBuf)
	    val matcher = regex.matcher(remaining)
	    if (matcher.find()) {
	      list.put(matcher.group(1), matcher.group(2))
	      done = true
	    }
	  }
	}
      }

      sender ! listId

    case GetAdjustableListMessage(listId: String) =>
      sender ! lists(listId).getAdjustableList()

    case LoadMessage(listId: String, data: Map[Any, Any]) =>
      lists(listId).load(data)
      sender ! "okay"

    case PutMessage(listId: String, key: Any, value: Any) =>
      lists(listId).put(key, value)
      sender ! "okay"

    case UpdateMessage(listId: String, key: Any, value: Any) =>
      lists(listId).update(key, value)
      sender ! "okay"

    case RemoveMessage(listId: String, key: Any) =>
      lists(listId).remove(key)
      sender ! "okay"

    case PutAfterMessage(listId: String, key: Any, newPair: (Any, Any)) =>
      lists(listId).putAfter(key, newPair)
      sender ! "okay"

    case RegisterDatastoreMessage(workerId: String, datastoreRef: ActorRef) =>
      datastores(workerId) = datastoreRef

    case SetIdMessage(_workerId: String) =>
      workerId = _workerId

    case x =>
      log.warning("Datastore actor received unhandled message " +
                  x + " from " + sender)
  }
}
