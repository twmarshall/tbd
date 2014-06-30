/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package tbd.visualization.graph

import tbd.ddg

object NodeType extends Enumeration {
  type NodeType = Value
  val Write, Read, Par, Memo, Root = Value
}
import NodeType._

class Node(ddgNode: ddg.Node) {
  val nodeType = ddgNode match {
    case x:ddg.WriteNode => NodeType.Write
    case x:ddg.ReadNode => NodeType.Read
    case x:ddg.MemoNode => NodeType.Memo
    case x:ddg.ParNode => NodeType.Par
    case x:ddg.RootNode => NodeType.Root
  }

  val signature = ddgNode match {
    case x:ddg.WriteNode => x.mod.read()
    case x:ddg.ReadNode => x.mod.read() //Have to include reader function
    case x:ddg.MemoNode => null
    case x:ddg.ParNode => null
    case x:ddg.RootNode => null
  }

  def ~(that: Node): Boolean = {
    return this.nodeType == that.nodeType &&
    that.signature == this.signature
  }

  override def toString = {
    nodeType + " " + signature
  }
}
