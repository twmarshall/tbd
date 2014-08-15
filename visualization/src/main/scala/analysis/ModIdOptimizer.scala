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

package tbd.visualization.analysis

import tbd.visualization.graph._
import scala.collection.mutable.{Buffer, HashMap}
import tbd.Constants.ModId
import tbd.ddg.{Tag, SingleWriteTag}

class ModIdOptimizer  {

  case class CallTag(val reads: List[ModId], val funcId: Int)

  private val idPool = HashMap[CallTag, List[ModId]]()
  private val usedIds = HashMap[CallTag, List[ModId]]()

  private var nextFreeId = -1

  private def getNextId(): ModId = {
    nextFreeId += 1
    "gen." + nextFreeId.toString
  }

  private def getId(tag: CallTag): ModId = {
    val id = if(idPool.contains(tag)) {
      val ids = idPool(tag)
      if(ids.tail.isEmpty) {
        idPool.remove(tag)
      } else {
        idPool(tag) = ids.tail
      }
      ids.head
    } else {
      getNextId()
    }

    if(!usedIds.contains(tag)) {
      usedIds(tag) = List()
    }

    usedIds(tag) = id :: usedIds(tag)

    id
  }

  def optimize(a: DDG, b: DDG) {
    nextFreeId = 0
    idPool.clear()
    usedIds.clear()

    optimize(a)

    idPool ++= usedIds
    usedIds.clear()

    optimize(b)
  }

  private def optimize(ddg: DDG) {
    val iter = new DfsFirstIterator(
      ddg.root,
      ddg,
      (e: Edge) => (e.isInstanceOf[Edge.Control]))

    iter.foreach(node => {
      node.tag match {
        case tag:Tag.Write => {
          optimizeFromWrite(node, tag, ddg)
        }
        case _ => null
      }
    })

  }

  private def optimizeFromWrite(node: Node, tag: Tag.Write, ddg: DDG) {

    val modNodes = ddg.adj(node).filter(e => {
      e.isInstanceOf[Edge.WriteMod]
    }).map(_.destination)
    val readNodes = ddg.adj(node).filter(e => {
      e.isInstanceOf[Edge.WriteRead]
    }).map(_.destination)

    //Not sure if we should better track all reads to the modNode
    val readTrack = trackReads(ddg, node)

    val reads = readTrack.reads.map(_.mod)


    val newWriteTags = tag.writes.map(write => {
      val oldId = write.mod

      val originatingModNodes = modNodes.filter(node => {
        val tag = node.tag.asInstanceOf[Tag.Mod]
        tag.dests.contains(oldId)
      })

      if(originatingModNodes.isEmpty)
        throw new IllegalArgumentException("Mod node creating dest not found.")
      if(originatingModNodes.size > 1)
        throw new IllegalArgumentException("Mod node creating dest found " +
                                           "more than once.")

      val originatingModNode = originatingModNodes.head

      val modTag = originatingModNode.tag.asInstanceOf[Tag.Mod]

      val funcId = modTag.initializer.funcId

      val signature = CallTag(reads, funcId)
      val newId = getId(signature)

      originatingModNode.tag = new Tag.Mod(replace(modTag.dests, oldId, newId),
                             modTag.initializer)

      readNodes.foreach(node => {
        val tag = node.tag.asInstanceOf[Tag.Read]
        node.tag = new Tag.Read(tag.readValue,
                                tag.reader)(newId)
      })

      SingleWriteTag(newId, write.value)
    })

    node.tag = Tag.Write(newWriteTags)
  }

  private def replace[T](list: List[T], oldElem: T, newElem: T) = {
    list.map{
      case x if x == oldElem => newElem
      case x => x
    }
  }

  private def trackReads(ddg: DDG, write: Node): TrackingResult = {
    var reads = List[Tag.Read]()
    var rootMod: Tag.Mod = null
    import scala.util.control.Breaks._

    val iter = new DfsFirstIterator(
      write,
      ddg,
      (e: Edge) => (e.isInstanceOf[Edge.InverseControl]))

    breakable {
      iter.foreach(x => x.tag match {
        case mod:Tag.Mod => {
          if(!reads.isEmpty) {
            rootMod = mod
            break
          }
        }
        case read:Tag.Read => {
            reads = read :: reads
        }
        case _ => null
      })
    }

    TrackingResult(reads, rootMod)
  }

  case class TrackingResult(
    val reads: List[Tag.Read],
    val rootMod: Tag.Mod)
}
