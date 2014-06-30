/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package tbd.visualization.graph

object EdgeType extends Enumeration {
  type EdgeType = Value
  val Call, Dependency = Value
}
import EdgeType._

class Edge(tp: EdgeType, src: Node, dst: Node) {
  val destination = dst
  val source = src
  val edgeType = tp
}
