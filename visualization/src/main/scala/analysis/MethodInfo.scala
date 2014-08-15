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

import tbd.visualization.graph.Node

/*
 * Represents information about a method for debugging purposes.
 */
case class MethodInfo(val name: String, val file: String, val line: Int)


object MethodInfo {
  //Extracts method information from a stack trace. 
  def extract(node: Node) = {
    val methods = node.stacktrace.map(y => {
      (y.getMethodName(),y.getFileName(), y.getLineNumber())
    })

    val currentMethods = methods.filter(x => {
      val y = x._1
      (!y.startsWith("<init>")
      && !y.startsWith("()")
      && !y.startsWith("addRead")
      && !y.startsWith("addMod")
      && !y.startsWith("mod2")
      && !y.startsWith("modLeft")
      && !y.startsWith("modRight")
      && !y.startsWith("addWrite")
      && !y.startsWith("addMemo")
      && !y.startsWith("createMod")
      && !y.startsWith("getStackTrace")
      && !y.startsWith("apply")
      && !y.startsWith("read")
      && !y.startsWith("memo")
      && !y.startsWith("par")
      && !y.startsWith("write")
      && !y.startsWith("mod"))})

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
