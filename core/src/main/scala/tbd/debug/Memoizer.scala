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
package tbd.debug

import scala.collection.mutable.{ArrayBuffer, Map}
import scala.concurrent.{Await, Future}

import tbd.Context
import tbd.Constants._
import tbd.ddg.{FunctionTag, MemoNode, Node, Tag, Timestamp}
import tbd.macros.{TbdMacros, functionToInvoke}

class Memoizer[T](implicit c: Context) extends tbd.Memoizer[T]()(c) {
  import scala.language.experimental.macros

  @functionToInvoke("applyInternal")
  override def apply(args: Any*)(func: => T): T = macro TbdMacros.memoMacro[T]

  def applyInternal(
      signature: Seq[_],
      func: => T,
      funcId: Int,
      freeTerms: List[(String, Any)]): T = {
    val internalId = Node.getId()
    val stack = Thread.currentThread().getStackTrace()
    val ret = super.apply(signature)(func)

    val tag = Tag.Memo(FunctionTag(funcId, freeTerms), signature)
    val nodePtr = Timestamp.getNodePtr(c.currentTime.ptr)
    TBD.nodes(nodePtr) = (internalId, tag, stack)

    ret
  }
}
