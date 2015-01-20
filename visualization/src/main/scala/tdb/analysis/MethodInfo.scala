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

package tdb.visualization.analysis

import tdb.visualization.graph.Node

/*
 * Represents information about a method for debugging purposes.
 */
case class MethodInfo(val name: String, val file: String, val line: Int)

object MethodInfo {
  //Words which are internal function calls - we filter those out of the
  //stack trace for identifying the call site in user code.
  var reservedWords = List("<init>", "()", "addRead", "addMod", "mod2", "modLeft",
  "modRight", "addWrite", "addMemo", "createMod", "getStackTrace",
  "apply", "read", "memo", "par", "write", "mod")

  //Extracts method information from a stack trace.
  def extract(node: Node) = {
    val methods = node.stacktrace.map(y => {
      (y.getMethodName(),y.getFileName(), y.getLineNumber())
    })

    val currentMethods = methods.filter(x => {
      reservedWords.forall(y => !x._1.startsWith(y))
    })

    val methodName = if(!currentMethods.isEmpty) {
      var currentMethod = currentMethods(0)._1

      if(currentMethod.contains("$")) {
        currentMethod = currentMethod.substring(0, currentMethod.lastIndexOf("$"))
        currentMethod = currentMethod.substring(currentMethod.lastIndexOf("$") + 1)
      }

      currentMethod
    } else {
      "<unknown>"
    }

    val (fileName, lineNumber) = if(!currentMethods.isEmpty) {
      (currentMethods(0)._2, currentMethods(0)._3)
    } else {
      ("<unknown>", 0)
    }
    MethodInfo(methodName, fileName, lineNumber)
  }
}
