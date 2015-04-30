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

package tdb.visualization

import scala.collection.mutable.ArrayBuffer
import collection.mutable.{HashMap, MutableList}
import scala.util.Random

import tdb._

/*
 * Base class for test generators.
 */
abstract class TestBase[T, V](algorithm: TestAlgorithm[T, V])
    extends ExperimentSource[V] {
  //Size of input for initial run.
  var initialSize = 10

  private val mutator = new Mutator()
  private val listConf = algorithm.getListConf()
  private val input = mutator.createList[Int, Int](listConf)

  //Counter keeping track of mutation rounds.
  protected var mutationCounter = 0

  //Input.
  private val table = new HashMap[Int, Int]

  protected val rand = new Random()

  //List keeping track of free id's.
  private var freeList = List[Int]()
  private var keyCounter = 0

  //Maximum of values in the input.
  protected var maxValue = 100

  //List holding all mutations of the current mutation round.
  protected var mutations: MutableList[Mutation] = null

  var listener: ExperimentSink[V] = null

  //Adds a random value with any free key.
  def addValue() {
    val newValue = rand.nextInt(maxValue)
    /*val newKey =
      if(freeList.size == 0 || rand.nextInt(2) == 1) {
        keyCounter += 1
        keyCounter
      } else {
        var (head::tail) = freeList
        freeList = tail
        head
      }*/
    var newKey = rand.nextInt(maxValue)
    while (table.contains(newKey)) {
      newKey = rand.nextInt(maxValue)
    }

    addValue(newKey, newValue)
  }

  //Adds a given value with a given key.
  def addValue(key: Int, value: Int) {

    if(table.contains(key)) {
      //Element exists. Skip.
    } else {
      mutations += Deletion(key, value)

      input.put(key, value)
      table += (key -> value)
    }
  }

  //Removes a random value.
  def removeValue() {
    val keys = table.keys.toBuffer

    if(keys.length > 0)
    {
      val toRemove = keys(rand.nextInt(keys.length))

      removeValue(toRemove)
    }
  }

  //Removes the value with the given key.
  def removeValue(key: Int) {

      mutations += Deletion(key, table(key))

      input.remove(key, table(key))
      table -= key

      freeList = (freeList :+ key)
  }

  //Updates a random value.
  def updateValue() {
    val keys = table.keys.toBuffer

    if(keys.length > 0)
    {
      val toUpdate = keys(rand.nextInt(keys.length))
      val newValue = rand.nextInt(maxValue)

      updateValue(toUpdate, newValue)
    }
  }

  //Updates the value associated with the given key to a new value.
  def updateValue(key: Int, value: Int) {

      mutations += Update(key, value, table(key))

      table(key) = value
      input.put(key, value)
  }

  //Does a random mutation, update, add or remove.
  def randomMutation() {
    rand.nextInt(3) match {
      case 0 => updateValue()
      case 1 => removeValue()
      case 2 => addValue()
    }
  }

  //Runs this test generator.
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
      val result = algorithm.getResult(output, mutator)
      val expectedResult = algorithm.getExpectedResult(table)

      val ddg = graph.DDG.create(mutator)
      pushResult(new ExperimentResult(mutationCounter, input, mutations.toList,
                                      result, expectedResult, ddg))

      if(result != expectedResult) {
        throw new IllegalArgumentException("Check Error. Expected: " + expectedResult +
                                    "Got: " + result)
      }

      mutations = MutableList[Mutation]()
    } while(step())

    dispose()

    mutator.shutdown()
  }

  //Called before test generation starts.
  def initialize()
  //Called when a new mutation round should be done.
  //If false is returned, the test generation stops.
  def step(): Boolean
  //Called after test generatrion stopped.
  def dispose()
}

/*
 * Classes representing mutations on the input.
 */
abstract class Mutation(key: Int)
case class Insertion(key: Int, value: Int) extends Mutation(key)
case class Deletion(key: Int, value: Int) extends Mutation(key)
case class Update(key: Int, newValue: Int, oldValue: Int) extends Mutation(key)
