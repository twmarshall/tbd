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

import reflect.macros.whitebox.Context
import language.experimental.macros
import scala.tools.reflect.ToolBox

object TbdMacros {

  def memoMacro[T]
      (c: Context)(args: c.Tree*)(func: c.Tree): c.Expr[T] = {
    import c.universe._

    val closedVars = createFreeVariableList(c)(func)
    val memo = Select(c.prefix.tree, TermName("memoInternal"))
    c.Expr[T](q"$memo(List(..$args), $func, $closedVars)")
  }

  def readMacro[T, U]
      (c: Context)(mod: c.Tree)(reader: c.Tree): c.Expr[U] = {
    import c.universe._

    val closedVars = createFreeVariableList(c)(reader)
    val readFunc = Select(c.prefix.tree, TermName("readInternal"))
    c.Expr[U](q"$readFunc($mod, $reader, $closedVars)")
  }

  private def createFreeVariableList(c: Context)(func: c.Tree) = {
    import c.universe._

    findFreeVariabels(c)(func).map(x => {
      val name = Literal(Constant(x._2))
      val value = x._1
      q"($name, $value)"
    })
  }

  private def findFreeVariabels(c: Context)(func: c.Tree) = {
    import c.universe._

    //Pre-fetch the symbol of our reader func.
    val readerSymbol = func.symbol

    class ParentValDefExtractor(targetSymbol: Symbol) extends Traverser {
      var defs = List[(String, ValDef)]()
      var found = false

      def traverseChildTrees(trees: List[Tree], include: Boolean): Boolean = {

        var found = false;

        trees.foreach((subtree) => {
            found = found | traverseChildTree(subtree, include)
        })

        found
      }

      def traverseChildTree(tree: Tree, include: Boolean): Boolean = {
        val recursiveTraverser = new ParentValDefExtractor(targetSymbol)
        recursiveTraverser.traverse(tree)

        if(recursiveTraverser.found || include) {
          this.defs = recursiveTraverser.defs ::: this.defs
        }

        if(recursiveTraverser.found) {
          this.found = true
        }
        recursiveTraverser.found
      }

      override def traverse(tree: Tree): Unit = {

        if(targetSymbol.equals(tree.symbol)) {
          found = true
        }

        tree match {
          case expr @ ValDef(_, name, _, subtree) =>
            defs = (name.toString(), expr) :: defs
            super.traverse(subtree)
          case Block(trees, tree) => traverseChildTrees(tree :: trees, true)
          case Function(params, subtree) => {
              traverseChildTrees(params, traverseChildTree(subtree, false))
          }
          case _ => super.traverse(tree)
        }
      }
    }

    class IdentTermExtractor() extends Traverser {
      var idents = List[(Tree, String)]()

      override def traverse(tree: Tree): Unit = tree match {
        case ident @ Ident(name) if !ident.symbol.isMethod => idents = (tree, name.toString) :: idents
        case _ => super.traverse(tree)
      }
    }

    var termExtractor = new IdentTermExtractor()
    termExtractor.traverse(func)

    //Check if term is really free
    var freeTerms = termExtractor.idents.filter((x) => {
      //For each ident, look for a parent val def in our own function.
      val defExtractor = new ParentValDefExtractor(x._1.symbol)
      defExtractor.traverse(func)

      if(!defExtractor.found) {
        c.warning(c.enclosingPosition, "Macro Bug: Did not find variable symbol in enclosing tree. Symbol was: " + x._1.symbol)
      }

      defExtractor.defs.find(y => x._2 == y._1).isEmpty
    })

    //println("Inner free terms")
    //freeTerms.foreach(println(_))

    val distincFreeTerms = freeTerms.groupBy(x => x._2).map(x => x._2.head).toList

    var valDefExtractor = new ParentValDefExtractor(readerSymbol)
    //valDefExtractor.traverse(c.enclosingUnit.body) //TODO: Find the right scope
    valDefExtractor.traverse(c.enclosingUnit.body) //TODO: Find the right scope

    if(!valDefExtractor.found) {
      c.warning(c.enclosingPosition, "Macro Bug: Did not find closed function in enclosing tree. Symbol was: " + c.universe.showRaw(readerSymbol))
    }
    //println("Outer defs")
    //valDefExtractor.defs.foreach(println(_))

    distincFreeTerms.filter(x => {
        !valDefExtractor.defs.find(y => (y._1 == x._2)).isEmpty
    })
  }
}