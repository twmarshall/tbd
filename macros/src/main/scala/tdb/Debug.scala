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

package tdb.macros

import java.io.File
import scala.language.experimental.macros
import scala.reflect.macros.whitebox.Context
import scala.tools.reflect.ToolBox

object Debug {
  def lineImpl(c: Context): c.Expr[Int] = {
    import c.universe._

    val line = Literal(Constant(c.enclosingPosition.line))
    c.Expr[Int](line)
  }

  def fileImpl(c: Context): c.Expr[String] = {
    import c.universe._

    val absolute = c.enclosingPosition.source.file.file.toURI
    val base = new File(".").toURI

    val path = Literal(Constant(base.relativize(absolute).getPath))

    c.Expr[String](path)
  }

  def markImpl(c: Context): c.Expr[String] = {
    import c.universe._

    val line = c.enclosingPosition.line

    val absolute = c.enclosingPosition.source.file.file.toURI
    val base = new File(".").toURI
    val path = base.relativize(absolute).getPath

    val expr = Literal(Constant(path + "(" + line + ")"))

    c.Expr[String](expr)
  }
}
