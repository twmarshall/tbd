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

package thomasdb.macros

import reflect.macros.whitebox.Context
import language.experimental.macros
import scala.tools.reflect.ToolBox

//Static annotation for ThomasDB functions, which can be read by our macro.
//We use this to re-use code as we can use the same macro for different
//functions. Only the parameter list has to be equal.
class functionToInvoke(value: String) extends scala.annotation.StaticAnnotation

object ThomasDBMacros {

  var funcId = 0

  def getFuncId(): Int = {
    //We always have to do a clean build to get unique ids.
    funcId = funcId + 1
    funcId
  }

  /*
   * Macros to be invoked by their corresponding methods.
   * Since for a method and a macro, the signature has to match, we need
   * to have a macro for each signature type.
   */
  def memoMacro[T]
      (c: Context)(args: c.Tree*)(func: c.Tree): c.Expr[T] = {
    import c.universe._

    val closedVars = createFreeVariableList(c)(func)
    val memo = getFunctionToInvoke(c)
    val id = Literal(Constant(getFuncId()))
    c.Expr[T](q"$memo(List(..$args), $func, $id, $closedVars)")
  }

  def parOneMacro[Q]
      (c: Context)(one: c.Tree): c.Expr[Q] = {
    import c.universe._

    val closedVars = createFreeVariableList(c)(one)
    val par = getFunctionToInvoke(c)
    val id = Literal(Constant(getFuncId()))
    c.Expr[Q](q"$par($one, $id, $closedVars)")
  }

  def parTwoMacro[Q]
      (con: Context)(two: con.Tree)(c: con.Tree): con.Expr[Q] = {
    import con.universe._

    val closedVars = createFreeVariableList(con)(two)
    val par = getFunctionToInvoke(con)
    val id = Literal(Constant(getFuncId()))
    con.Expr[Q](q"$par($two, $c, $id, $closedVars)")
  }

  def readMacro[T]
      (con: Context)(mod: con.Tree)(reader: con.Tree)(c: con.Tree): con.Expr[T] = {
    import con.universe._

    val closedVars = createFreeVariableList(con)(reader)
    val readFunc = getFunctionToInvoke(con)
    val id = Literal(Constant(getFuncId()))
    con.Expr[T](q"$readFunc($mod, $reader, $c, $id, $closedVars)")
  }

  def modizerMacro[T]
      (con: Context)(key: con.Tree)(initializer: con.Tree)(c: con.Tree): con.Expr[T] = {
    import con.universe._

    val closedVars = createFreeVariableList(con)(initializer)
    val modFunc = getFunctionToInvoke(con)
    val id = Literal(Constant(getFuncId()))
    con.Expr[T](q"$modFunc($key, $initializer, $c, $id, $closedVars)")
  }

  def modMacro[T]
      (con: Context)(initializer: con.Tree)(c: con.Tree): con.Expr[T] = {
    import con.universe._

    val closedVars = createFreeVariableList(con)(initializer)
    val modFunc = getFunctionToInvoke(con)
    val id = Literal(Constant(getFuncId()))
    con.Expr[T](q"$modFunc($initializer, $c, $id, $closedVars)")
  }

  /**
   * Finds the the value of the functionToInvoke setting of the
   * called macro application, or alternatively, fails and stops makro
   * expansion.
   */
  private def getFunctionToInvoke(c: Context) = {
    import c.universe._
    val settingValue = c.macroApplication.symbol.annotations.filter(
      _.tree.tpe <:< typeOf[functionToInvoke]
    ).headOption.flatMap(
      _.tree.children.tail.collectFirst {
        case Literal(Constant(s: String)) => s
      }
    ).getOrElse(
      c.abort(c.enclosingPosition, "functionToInvoke annotation not found.")
    )

    Select(c.prefix.tree, TermName(settingValue))
  }
  /**
   * Generates a list using quasiquotes from free variables from
   * within a tree
   */
  private def createFreeVariableList(c: Context)(func: c.Tree) = {
    import c.universe._

    val freeVars = findFreeVariabels(c)(func)

    freeVars.map(x => {
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
  private def findFreeVariabels(c: Context)(func: c.Tree) = {
    import c.universe._

    //Symbol of our function.
    def targetSymbol = c.macroApplication.symbol

    /**
     * A traverser which extracts all ValDef nodes from the AST,
     * which are ancestors of the node which hast the symbol targetSymbol.
     */
    class ParentValDefExtractor(targetSymbol: c.Symbol) extends Traverser {
      var defs = List[(String, TreeApi)]()
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
            //We fund a val def.
            defs = (name.toString(), expr) :: defs
            super.traverse(subtree)
          case expr @ Bind(name, _) =>
            //We found a bind from a case/match. This is also important to
            //remember.
            defs = (name.toString(), expr) :: defs
          case Block(trees, tree) => traverseChildTrees(tree :: trees, false)
          case Function(params, subtree) => {
            //Special case: If our target is in the subtree
            //of a function call, we also have to include the
            //params of our function in the case.
            traverseChildTrees(params, traverseChildTree(subtree, false))
          }
          case CaseDef(valdef, _, matchexpr) => {
            //Special case: Pattern matching. Handle it similar as function.
            traverseChildTree(valdef, traverseChildTree(matchexpr, false))
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

    //Only keep each symbol once, also filter out packes and so on.
    val filteredTerms = termExtractor.idents.filter(x => {
                              !x._1.symbol.isPackage &&
                              !x._1.symbol.isMethod &&
                              !x._1.symbol.isModule &&
                              !x._1.symbol.isClass &&
                              !x._1.symbol.isType &&
                              x._2 != "_" //Exclude blank.
                            })

    //Check if all instances of term are really free
    var distinctFreeTerms = filteredTerms.filter((x) => {
      //For each ident, look for a parent ValDef in our own function.
      val defExtractor = new ParentValDefExtractor(x._1.symbol)
      defExtractor.traverse(func)

      //If we define this val ourself, drop it.
      val defs = defExtractor.defs
      defs.find(y => x._2 == y._1).isEmpty
    }).groupBy(x => x._2).map(x => x._2.head).toList

    distinctFreeTerms
  }
}
