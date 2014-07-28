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
  case class Read(val readValue: Any, val reader: FunctionTag)(val mod: ModId) extends Tag
  case class Write(val value: Any, val dest: ModId) extends Tag
  case class Memo(val function: FunctionTag, val args: List[Any]) extends Tag
  case class Mod(val dest: ModId, val initializer: FunctionTag) extends Tag
  case class Par(val fun1: FunctionTag, val fun2: FunctionTag) extends Tag
  case class Root() extends Tag


  def formatTag(tag: Tag): String = {
    tag match{
      case Tag.Read(value, funcTag) => {
          "Read " + value +
          "\nReader " + formatFunctionTag(funcTag)
      }
      case Tag.Write(value, dest) => {
          "Write " + value + " to " + dest
      }
      case Tag.Memo(funcTag, signature) => {
          "Memo" +
          "\n" + formatFunctionTag(funcTag) +
          "\nSignature:" + signature.foldLeft("")(_ + "\n   " + _)
      }
      case Tag.Mod(dest, initializer) => {
          "Mod id " + dest +
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

case class FunctionTag(val funcId: Int, val freeVars: List[(String, Any)])
