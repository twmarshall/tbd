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

package tbd.visualization

import scala.collection.mutable.ArrayBuffer
import collection.mutable.{HashMap, MutableList}
import scala.util.Random

import tbd._
import tbd.list.ListInput

abstract class TestBase[T, V](algorithm: TestAlgorithm[T, V])
    extends ExperimentSource[V] {
  var initialSize = 10

  private val mutator = new Mutator()
  private val listConf = algorithm.getListConf()
  private val input = ListInput[Int, Int](listConf)
  protected var mutationCounter = 0

  private val table = new HashMap[Int, Int]
  protected val rand = new Random()
  private var freeList = List[Int]()

  private var keyCounter = 0
  protected var maxValue = 100

  protected var mutations: MutableList[Mutation] = null

  var listener: ExperimentSink[V] = null

  def addValue() {
    val newValue = rand.nextInt(maxValue)
    val newKey =
      if(freeList.size == 0 || rand.nextInt(2) == 1) {
        keyCounter += 1
        keyCounter
      } else {
        var (head::tail) = freeList
        freeList = tail
        head
      }

    addValue(newKey, newValue)
  }

  def addValue(key: Int, value: Int) {

    if(table.contains(key)) {
      //Element exists. Skip.
    } else {
      mutations += Deletion(key, value)

      input.put(key, value)
      table += (key -> value)
    }
  }

  def removeValue() {
    val keys = table.keys.toBuffer

    if(keys.length > 0)
    {
      val toRemove = keys(rand.nextInt(keys.length))

      removeValue(toRemove)
    }
  }

  def removeValue(key: Int) {

      mutations += Deletion(key, table(key))

      table -= key
      input.remove(key)
      freeList = (freeList :+ key)
  }

  def updateValue() {
    val keys = table.keys.toBuffer

    if(keys.length > 0)
    {
      val toUpdate = keys(rand.nextInt(keys.length))
      val newValue = rand.nextInt(maxValue)

      updateValue(toUpdate, newValue)
    }
  }

  def updateValue(key: Int, value: Int) {

      mutations += Update(key, value, table(key))

      table(key) = value
      input.update(key, value)
  }

  def randomMutation() {
    rand.nextInt(3) match {
      case 0 => updateValue()
      case 1 => removeValue()
      case 2 => addValue()
    }
  }

  def run() {

    mutations = MutableList[Mutation]()

    for(i <- 1 to initialSize)
      addValue()

    algorithm.input = input
    val output = mutator.run[T](algorithm)

    initialize()

    do {
      mutator.propagate()
      mutationCounter += 1

      val input = table.toMap
      val result = algorithm.getResult(output)
      val expectedResult = algorithm.getExpectedResult(table)

      val ddg = graph.DDG.create(mutator.getDDG().root)
      pushResult(new ExperimentResult(mutationCounter, input, mutations.toList,
                                      result, expectedResult, ddg))

      if(result != expectedResult) {
        println("//Check error!") //Todo: Handle this without println
      }

      mutations = MutableList[Mutation]()
    } while(step())

    dispose()

    mutator.shutdown()
  }

  def initialize()
  def step(): Boolean
  def dispose()
}

abstract class Mutation(key: Int)
case class Insertion(key: Int, value: Int) extends Mutation(key)
case class Deletion(key: Int, value: Int) extends Mutation(key)
case class Update(key: Int, newValue: Int, oldValue: Int) extends Mutation(key)
