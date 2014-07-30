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

  var funcId = 0
  def getFuncId(): Int = {
    //Well, this is dirty - we always have to do a clean build to get
    //unique ids.
    funcId = funcId + 1
    funcId
  }

  def memoMacro[T]
      (c: Context)(args: c.Tree*)(func: c.Tree): c.Expr[T] = {
    import c.universe._

    val closedVars = createFreeVariableList(c)(func)
    val memo = Select(c.prefix.tree, TermName("applyInternal"))
    val id = Literal(Constant(getFuncId()))
    c.Expr[T](q"$memo(List(..$args), $func, $id, $closedVars)")
  }

  def parOneMacro[Q]
      (c: Context)(one: c.Tree): c.Expr[Q] = {
    import c.universe._

    val closedVars = createFreeVariableList(c)(one)
    val par = Select(c.prefix.tree, TermName("parInternal"))
    val id = Literal(Constant(getFuncId()))
    c.Expr[Q](q"$par($one, $id, $closedVars)")
  }

  def parTwoMacro[Q]
      (con: Context)(two: con.Tree)(c: con.Tree): con.Expr[Q] = {
    import con.universe._

    val closedVars = createFreeVariableList(con)(two)
    val par = Select(con.prefix.tree, TermName("parTwoInternal"))
    val id = Literal(Constant(getFuncId()))
    con.Expr[Q](q"$par($two, $c, $id, $closedVars)")
  }

  def readMacro[T]
      (con: Context)(mod: con.Tree)(reader: con.Tree)(c: con.Tree): con.Expr[T] = {
    import con.universe._

    val closedVars = createFreeVariableList(con)(reader)
    val readFunc = Select(con.prefix.tree, TermName("readInternal"))
    val id = Literal(Constant(getFuncId()))
    con.Expr[T](q"$readFunc($mod, $reader, $c, $id, $closedVars)")
  }

  def read_2Macro[T]
      (con: Context)(mod: con.Tree)(reader: con.Tree)(c: con.Tree): con.Expr[T] = {
    import con.universe._

    val closedVars = createFreeVariableList(con)(reader)
    val readFunc = Select(con.prefix.tree, TermName("read_2Internal"))
    val id = Literal(Constant(getFuncId()))
    con.Expr[T](q"$readFunc($mod, $reader, $c, $id, $closedVars)")
  }

  def modMacro[T]
      (con: Context)(initializer: con.Tree)(c: con.Tree): con.Expr[T] = {
    import con.universe._

    val closedVars = createFreeVariableList(con)(initializer)
    val modFunc = Select(con.prefix.tree, TermName("modInternal"))
    val id = Literal(Constant(getFuncId()))
    con.Expr[T](q"$modFunc($initializer, $c, $id, $closedVars)")
  }

  /**
   * Generates a list using quasiquotes from free variables from
   * within a tree
   */
  private def createFreeVariableList(c: Context)(func: c.Tree) = {
    import c.universe._

    findFreeVariabels(c)(func).map(x => {
      val name = Literal(Constant(x._2))
      val value = x._1
      q"($name, $value)"
    })
  }

  /**
   * Finds free variables within an anonymous function, which
   * are bound from an outer scope.
   *
   * Static or class variables are not found.
   */
  private def findFreeVariabels(c: Context)(func: c.Tree): List[(c.Tree, String)] = {
    import c.universe._

    //Symbol of our function.
    def targetSymbol = func.symbol

    /**
     * A traverser which extracts all ValDef nodes from the AST,
     * which are ancestors of the node which hast the symbol targetSymbol.
     */
    class ParentValDefExtractor(targetSymbol: c.Symbol) extends Traverser {
      var defs = List[(String, ValDef)]()
      var found = false

      //Traverse each child tree, remember wheter we already found
      //our target symbol.
      def traverseChildTrees(trees: List[Tree], include: Boolean): Boolean = {

        var found = false;

        trees.foreach((subtree) => {
            found = found | traverseChildTree(subtree, include)
        })

        found
      }

      //Traverse a single child tree.
      //If the child tree, contains our target, we remember all
      //ValDefs from the child tree and mark this node as ancestor too.
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

      //Traverse the current tree.
      //Check whether we found the target. If so, stop traversion.
      //If not, extract all relevant child trees.
      override def traverse(tree: Tree): Unit = {

        if(targetSymbol == tree.symbol) {
          found = true
        }

        tree match {
          case expr @ ValDef(_, name, _, subtree) =>
            defs = (name.toString(), expr) :: defs
            super.traverse(subtree)
          case Block(trees, tree) => traverseChildTrees(tree :: trees, true)
          case Function(params, subtree) => {
              //Special case: If our target is in the subtree
              //of a function call, we also have to include the
              //params of our function in the case.
              traverseChildTrees(params, traverseChildTree(subtree, false))
          }
          case _ => super.traverse(tree)
        }
      }
    }

    /**
     * Traverser which simply extracts all Ident nodes
     * from a tree.
     */
    class IdentTermExtractor() extends Traverser {
      var idents = List[(Tree, String)]()

      override def traverse(tree: Tree): Unit = tree match {
        case ident @ Ident(name) if !ident.symbol.isMethod =>
          idents = (tree, name.toString) :: idents
        case _ => super.traverse(tree)
      }
    }

    //Extract all Idents from our function
    var termExtractor = new IdentTermExtractor()
    termExtractor.traverse(func)

    //Check if term is really free
    var freeTerms = termExtractor.idents.filter((x) => {
      //For each ident, look for a parent ValDef in our own function.
      val defExtractor = new ParentValDefExtractor(x._1.symbol)
      defExtractor.traverse(func)

      //If we define this val ourself, drop it.
      defExtractor.defs.find(y => x._2 == y._1).isEmpty
    })

    //Only keep each symbol once.
    val distincFreeTerms = freeTerms.groupBy(x => x._2)
                            .map(x => x._2.head).toList

    //Now, find all ValDefs which are ancestors of our function in
    //the AST.
    var valDefExtractor = new ParentValDefExtractor(targetSymbol)

    valDefExtractor.traverse(c.enclosingUnit.body)

    //If we did not find ourselfs, this macro is broken. Yay.
    if(!valDefExtractor.found) {
      c.warning(c.enclosingPosition, "Macro Bug: Did not find closed function"
                + " in enclosing tree. Symbol was: "
                + targetSymbol)

      //We are not allowed to return anything, as this list might contain
      //functions we try to evaluate without params. 
      List[(Tree, String)]()
    } else {
      //New return all Idents from our function which are also defined as
      //Vals in the outer scope. This is necassary to filter out methods,
      //which can be referenced via Ident, too.
      distincFreeTerms.filter(x => {
          !valDefExtractor.defs.find(y => (y._1 == x._2)).isEmpty
      })
    }
  }
}