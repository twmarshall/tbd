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

import akka.actor.{Actor, ActorRef, ActorLogging, Props}
import scala.collection.mutable.{ArrayBuffer, Map, Set}
import scala.concurrent.Future

import tbd.Constants._
import tbd.{AdjustableConf, ListConf, TableConf}
import tbd.messages._
import tbd.mod._

object Datastore {
  def props(): Props = Props(classOf[Datastore])
}

class Datastore extends Actor with ActorLogging {
  import context.dispatcher
  private val mods = Map[Any, Mod[Any]]()

  // Maps the name of an input table to a set containing the Modifiers that were
  // returned containing elements from this table, so that we can inform them
  // when the table is updated.
  private val modifiers = Map[String, Set[Modifier[Any, Any]]]()

  private val inputs = Map[InputId, Modifier[Any, Any]]()

  private var nextInputId: InputId = 0

  private var nextModId = 0
  def createMod[T](value: T): Mod[T] = {
    val mod = new Mod[T](new ModId("d." + nextModId), value)
    nextModId += 1

    mods(mod.id) = mod.asInstanceOf[Mod[Any]]
    mod
  }

  def getMod(modId: ModId): Any = {
    mods(modId).value
  }

  def updateMod(modId: ModId, value: Any): ArrayBuffer[Future[String]] = {
    if (!mods.contains(modId)) {
      log.warning("Trying to update non-existent mod " + modId)
    }

    mods(modId).update(value)
  }

  def removeMod(modId: ModId) {
    if (!mods.contains(modId)) {
      log.warning("Trying to remove nonexistent mod " + modId)
    }
    mods -= modId
  }

  def receive = {
    case CreateAdjustableMessage(conf: AdjustableConf) => {
      val inputId = nextInputId
      nextInputId += 1

      val modifier =
	conf match {
	  case ListConf(file, partitions, chunkSize, chunkSizer, valueMod) =>
	    if (chunkSize > 1) {
              if (!valueMod) {
		if (partitions == 1) {
		  log.info("Creating new ChunkList.")
		  new ChunkListModifier[Any, Any](this, Map(), chunkSize,
						  chunkSizer)
		} else {
		  log.info("Creating new PartitionedChunkList.")
		  new PartitionedChunkListModifier[Any, Any](this, Map(), partitions,
                                                             chunkSize, chunkSizer)
		}
              } else {
		if (partitions == 1) {
		  log.info("Creating new DoubleChunkList.")
		  new DoubleChunkListModifier[Any, Any](
		    this,
		    Map(),
		    chunkSize,
		    chunkSizer)
		} else {
		  log.info("Creating new PartitionedDoubleChunkList.")
		  new PartitionedDoubleChunkListModifier[Any, Any](
		    this,
		    Map(),
		    partitions,
		    chunkSize,
		    chunkSizer)
		}
              }
	    } else {
	      if (!valueMod) {
		if (partitions == 1) {
		  log.info("Creating new ModList.")
		  new ModListModifier[Any, Any](this, Map())
		} else {
		  log.info("Creating new PartitionedModList.")
		  new PartitionedModListModifier[Any, Any](this, Map(), partitions)
		}
	      } else {
		if (partitions == 1) {
		  log.info("Creating new DoubleModList.")
		  new DMLModifier[Any, Any](this, Map())
		} else {
		  log.info("Creating new PartitionedDoubleModList.")
		  new PDMLModifier[Any, Any](this, Map(), partitions)
		}
              }
	    }
	  case TableConf() => {
	    new TableModifier(this)
	  }
	}
      inputs(inputId) = modifier

      sender ! inputId
    }

    case PutInputMessage(inputId: InputId, key: Any, value: Any) => {
      val futures = inputs(inputId).insert(key, value)

      sender ! Future.sequence(futures)
    }

    case UpdateInputMessage(inputId: InputId, key: Any, value: Any) => {
      val futures = inputs(inputId).update(key, value)

      sender ! Future.sequence(futures)
    }

    case RemoveInputMessage(inputId: InputId, key: Any) => {
      val futures = inputs(inputId).remove(key)

      sender ! Future.sequence(futures)
    }

    case GetInputMessage(inputId: InputId) => {
      sender ! inputs(inputId).getModifiable()
    }

    case CleanUpMessage(
        workerRef: ActorRef,
        removeLists: Set[AdjustableList[Any, Any]]) => {
      for (table <- modifiers.keys) {
        for (removeList <- removeLists) {
          modifiers(table) = modifiers(table)
                             .filter((modifier: Modifier[Any, Any]) => {
            removeList != modifier.getModifiable()
          })
        }
      }

      sender ! "done"
    }

    case x => {
      log.warning("Datastore actor received unhandled message " +
                  x + " from " + sender)
    }
  }
}
