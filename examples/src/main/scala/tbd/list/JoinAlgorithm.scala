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
package tbd.examples.list

import scala.collection.GenIterable
import scala.collection.mutable.Map

import tbd.Context
import tbd.datastore.IntData
import tbd.list._

class JoinAlgorithm(_conf: Map[String, _], _listConf: ListConf)
    extends Algorithm[Int, AdjustableList[Int, (Int, Int)]](_conf, _listConf) {
  val input = ListInput[Int, Int](listConf)
  val data = new IntData(input, count, mutations)

  val input2 = ListInput[Int, Int](listConf)
  val data2 = new IntData(input2, count)

  def generateNaive() = {
    data.generate()
    data2.generate()
    data2.load()
  }

  def runNaive() {
    naiveHelper(data.table, data2.table)
  }

  private def naiveHelper(input: Map[Int, Int], input2: Map[Int, Int]) = {
    val output = Map[Int, (Int, Int)]()
    for ((key, value) <- input) {
      for ((key2, value2) <- input2) {
	if (key == key2) {
	  output(key) = (value, value2)
	}
      }
    }

    output
  }

  def checkOutput(
      input: Map[Int, Int],
      output: AdjustableList[Int, (Int, Int)]): Boolean = {
    val sortedOutput = output.toBuffer().map(_._2).sortWith(_._1 < _._1)
    val answer = naiveHelper(input, data2.table)
    val sortedAnswer = answer.values.toBuffer.sortWith(_._1 < _._1)

    //println(sortedAnswer)
    //println(sortedOutput)
    sortedAnswer == sortedOutput
  }

  def comparator(pair1: (Int, Int), pair2: (Int, Int)) = {
    pair1._1 == pair2._1
  }

  def run(implicit c: Context): AdjustableList[Int, (Int, Int)] = {
    val list = input.getAdjustableList()
    val list2 = input2.getAdjustableList().asInstanceOf[ModList[Int, Int]]

    list.join(list2, comparator)
  }
}

class ChunkJoinAlgorithm(_conf: Map[String, _], _listConf: ListConf)
    extends Algorithm[Int, AdjustableList[Int, (Int, Int)]](_conf, _listConf) {
  val input = ListInput[Int, Int](listConf)
  val data = new IntData(input, count, mutations)

  val input2 = ListInput[Int, Int](listConf)
  val data2 = new IntData(input2, count)

  def generateNaive() = {
    data.generate()
    data2.generate()
    data2.load()
  }

  def runNaive() {
    naiveHelper(data.table, data2.table)
  }

  private def naiveHelper(input: Map[Int, Int], input2: Map[Int, Int]) = {
    val output = Map[Int, (Int, Int)]()
    for ((key, value) <- input) {
      for ((key2, value2) <- input2) {
	if (key == key2) {
	  output(key) = (value, value2)
	}
      }
    }

    output
  }

  def checkOutput(
      input: Map[Int, Int],
      output: AdjustableList[Int, (Int, Int)]): Boolean = {
    val sortedOutput = output.toBuffer().map(_._2).sortWith(_._1 < _._1)
    val answer = naiveHelper(input, data2.table)
    val sortedAnswer = answer.values.toBuffer.sortWith(_._1 < _._1)

    //println(sortedAnswer)
    //println(sortedOutput)
    sortedAnswer == sortedOutput
  }

  def comparator(pair1: (Int, Int), pair2: (Int, Int)) = {
    pair1._1 == pair2._1
  }

  def run(implicit c: Context): AdjustableList[Int, (Int, Int)] = {
    val list = input.getAdjustableList()
    val list2 = input2.getAdjustableList().asInstanceOf[ChunkList[Int, Int]]

    list.join(list2, comparator)
  }
}
