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

import tbd.Context
import tbd.ddg.{FunctionTag, Node, ParNode, Tag}
import tbd.macros.{TbdMacros, functionToInvoke}

class Parizer[T]
    (one: Context => T,
     id1: Int,
     closedTerms1: List[(String, Any)]) extends tbd.Parizer(one) {
  import scala.language.experimental.macros

  @functionToInvoke("parTwoInternal")
  override def and[U](two: Context => U)(implicit c: Context): (T, U) =
    macro TbdMacros.parTwoMacro[(T, U)]

  def parTwoInternal[U](
      two: Context => U,
      c: Context,
      id2: Int,
      closedTerms2: List[(String, Any)]): (T, U) = {
    val internalId = Node.getId()
    val stack = Thread.currentThread().getStackTrace()
    val (oneRet, twoRet) = super.and(two)(c)

    val tag = Tag.Par(
      FunctionTag(id1, closedTerms1), FunctionTag(id2, closedTerms2))
    TBD.nodes(c.currentTime.pointer) = (internalId, tag, stack)

    (oneRet, twoRet)
  }
}
