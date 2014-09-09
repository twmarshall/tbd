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

/*
 * Represents a node tag, which holds valuable information
 * for debugging and analyzing for each node.
 *
 * Each tag has a primary and a secondary parameter list.
 * The primary parameter list contains attributes, which are relevant for
 * node equality. Basically the analysis package assumes that two nodes are
 * equal, iff the primary parameter lists of their tags are equal.
 * The secondary parameter lists can be used to hold additional information, for
 * example for debugging purposes. 
 */
abstract class Tag

object Tag {
  /*
   * A read tag, consisting of the value read by the read node and the tag of
   * the reader function.
   *
   * Addidtionally, the mod being read is stored in the second parameter list
   * for visualization purposes.
   */
  case class Read(val readValue: Any, val reader: FunctionTag)(val mod: ModId)
    extends Tag
  /*
   * A write tag, consisting of a list of the writes done.
   */
  case class Write(val writes: List[SingleWriteTag]) extends Tag
  /*
   * A memo tag, consisting of the tag of the function invoked by the memo and
   * the memo's parameter list to match against.
   */
  case class Memo(val function: FunctionTag, val args: Seq[Any]) extends Tag
  /*
   * A mod tag, consisting of a list of created mods and the tag of the
   * function invoked.
   */
  case class Mod(val mods: List[ModId], val initializer: FunctionTag) extends Tag
  /*
   * A par tag, consisting of two function tags for the two invoked functions.
   */
  case class Par(val fun1: FunctionTag, val fun2: FunctionTag) extends Tag
  /*
   * A root tag. Empty.
   */
  case class Root() extends Tag

  /*
   * Formats a tag for more easy-to-read output.
   */
  def formatTag(tag: Tag): String = {
    tag match{
      case Tag.Read(value, funcTag) => {
          "Read " + value +
          "\nReader " + formatFunctionTag(funcTag)
      }
      case Tag.Write(writes) => {
        writes.map(x =>  {
          "write " + x.value + " to " + x.mod
        }).reduceLeft(_ + "\n   " + _)
      }
      case Tag.Memo(funcTag, signature) => {
          "Memo" +
          "\n" + formatFunctionTag(funcTag) +
          "\nSignature:" + signature.foldLeft("")(_ + "\n   " + _)
      }
      case Tag.Mod(mod, initializer) => {
          "Mod id " + mod.reduceLeft(_ + ", " + _) +
          "\nInitializer " + formatFunctionTag(initializer)
      }
      case Tag.Par(fun1, fun2) => {
          "Par" +
          "\nFirst " + formatFunctionTag(fun1) +
          "\nSecond " + formatFunctionTag(fun2)
      }
      case _ => ""
    }
  }

  def formatFunctionTag(tag: FunctionTag): String = {
    "Function: " + tag.funcId +
    "\nBound free vars:" +
    tag.freeVars.map(x => x._1 + " = " + x._2).foldLeft("")(_ + "\n   " + _)
  }
}

/*
 * Represents a invoked function parameter. Consisting of the function ID, which
 * is unique for each function call in the source code and a list of variables
 * which are bound from an outside scope.
 */
case class FunctionTag(val funcId: Int, val freeVars: List[(String, Any)])
/*
 * Represents a single value written to a single mod, consisting of the mod id
 * and the value being written.
 */
case class SingleWriteTag(val mod: ModId, val value: Any)
