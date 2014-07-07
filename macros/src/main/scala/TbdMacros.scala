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

package tbd.macros

import reflect.macros.Context
import language.experimental.macros
import scala.tools.reflect.ToolBox

object TbdMacros {
  def readMacro[T, U]
      (c: Context)(mod: c.Expr[_])(reader: c.Expr[(T => U)]): c.Expr[U] = {
    import c.universe._

    class TermExtractor extends Traverser {
      var terms = List[(Tree, String)]()

      override def traverse(tree: Tree): Unit = tree match {
//        case expr @ Select(fun, name) =>
//          terms = (tree, name.toString) :: terms
//          super.traverse(fun)
        case ident @ Ident(name) if !ident.symbol.isMethod => terms = (tree, name.toString) :: terms
        case _ => super.traverse(tree)
      }
    }

    val readerSymbol = reader.tree.symbol;

    class ValDefExtractor extends Traverser {
      var defs = List[(String, ValDef)]()
      var found = false

      def traverseChildTrees(trees: List[Tree]): Unit = {

        trees.foreach((subtree) => {
          val recursiveTraverser = new ValDefExtractor()
          recursiveTraverser.traverse(subtree)

          if(recursiveTraverser.found) {
            this.found = true
            this.defs = recursiveTraverser.defs ::: this.defs
          }
        })
      }

      override def traverse(tree: Tree): Unit = {

        if(readerSymbol.equals(tree.symbol)) {
          found = true
        }

        tree match {
          case expr @ ValDef(_, name, _, subtree) =>
            defs = (name.toString(), expr) :: defs
            super.traverse(subtree)
          case Block(trees, tree) => traverseChildTrees(tree :: trees)
          case Function(params, subtree) => traverseChildTrees(List(subtree) ::: params)
          case _ => super.traverse(tree)
        }
      }
    }

    println("#### enc: " + showRaw(c.enclosingDef))
    println("#### inc: " + showRaw(reader.tree))

    var termExtractor = new TermExtractor()
    termExtractor.traverse(reader.tree)

    var valDefExtractor = new ValDefExtractor()
    valDefExtractor.traverse(c.enclosingDef)

    if(!valDefExtractor.found) {
      c.abort(null, "Did not find own function in enclosing tree.")
    }
    val readFunc = Select(c.prefix.tree, TermName("readInternal"))

    println("Outer:")
    valDefExtractor.defs.foreach(x => println(x._1))
    println("Inner:")
    termExtractor.terms.foreach(x => println(x._2))
    println("End.")

    //freeVars holds all free variables which are closed in by the
    //enclosing function.
    val freeVars = termExtractor.terms.filter(x => {
        !valDefExtractor.defs.find(y => (y._1 == x._2)).isEmpty && x._2 != "f"
    }).map(x => {
      val name = Literal(Constant(x._2))
      val reader = x._1
      q"($name, $reader)"
    })

    println("### Passed List: " + freeVars)

    c.Expr[U](q"$readFunc($mod, $reader, $freeVars)")
  }
}