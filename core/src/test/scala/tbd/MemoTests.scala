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
package tbd.test

import org.scalatest._

import tbd.{Adjustable, Changeable, Mutator, TableInput, TBD}
import tbd.ddg.{MemoNode, ReadNode, RootNode}
import tbd.master.Main
import tbd.mod.{Dest, Mod}

class MemoTest(input: TableInput[Int, Int]) extends Adjustable {
  // Note: real client applications should NOT have mutable state like this.
  // We are just using it to ensure that the memoized function doesn't get
  // reexecuted as appropriate.
  var count = 0

  def run(tbd: TBD): Mod[Int] = {
    val table = input.getTable()
    val one = table.get(1)
    val two = table.get(2)
    val memo = tbd.makeMemoizer[Mod[Int]]()

    tbd.mod((dest: Dest[Int]) => {
      tbd.read(one)(oneValue => {
        if (oneValue == 3) {
          tbd.mod((dest: Dest[Int]) => {
            tbd.read(one)(oneValueAgain => {
              tbd.write(dest, oneValueAgain)
            })
          })
        }
        val memoMod = memo(two) {
          tbd.mod((memoDest: Dest[Int]) => {
	    count += 1
	    tbd.read(two)(valueTwo => {
	      tbd.write(memoDest, valueTwo + 1)
	    })
          })
        }

        tbd.read(memoMod)(memoValue => {
          tbd.write(dest, oneValue + memoValue)
        })
      })
    })
  }
}

class AlreadyMatchedTest(input: TableInput[Int, Int]) extends Adjustable {
  var count1 = 0
  var count2 = 0

  /**
   * In the first run, one = 1, so the first call to memo doesn't get
   * executed, so count1 = 0, count2 = 1. In the second run, one = 3, so the
   * read of one is reexecuted, a memo match is found for the first call to
   * memo, but a memo match should not be found for the second call since we
   * can only match a memo entry once, so count1 = 0, count2 = 2.
   *
   * Note: it is generally not advisable to use a memo function in multiple
   * places in the code, as you can get weird results if the memoized block
   * isn't always the same.
   */
  def run(tbd: TBD): Mod[Int] = {
    val table = input.getTable()
    val one = table.get(1)
    val two = table.get(2)
    val memo = tbd.makeMemoizer[Changeable[Int]]()

    tbd.mod((dest: Dest[Int]) => {
      tbd.read(one)(oneValue => {
        if (oneValue == 3) {
          tbd.mod((dest: Dest[Int]) => {
            memo(two) {
	      count1 += 1
	      tbd.read(two)(twoValue => {
	        tbd.write(dest, twoValue + 2)
	      })
            }
          })
        }

        memo(two) {
	  count2 += 1
	  tbd.read(two)(twoValue => {
	    tbd.write(dest, twoValue + 1)
          })
        }
      })
    })
  }
}

class OutOfScopeTest(input: TableInput[Int, Int]) extends Adjustable {
  var num = 0

  def run(tbd: TBD): Int = {
    val table = input.getTable()
    val one = table.get(1)
    val two = table.get(2)
    val memo = tbd.makeMemoizer[Changeable[Int]]()

    tbd.mod((dest: Dest[Int]) => {
      tbd.read(one)(oneValue => {
	if (oneValue != 1) {
	  memo(two, dest) {
	    num += 1
	    tbd.write(dest, 0)
	  }
	} else {
	  tbd.write(dest, 1)
	}
      })
    })

    tbd.mod((dest: Dest[Int]) => {
      memo(two, dest) {
	tbd.write(dest, 2)
      }
    })

    0
  }
}

// Checks that if two calls to memo occur with the same signature, they both get
// matched during propagation.
class MatchingSignaturesTest(input: TableInput[Int, Int]) extends Adjustable {
  var count1 = 0
  var count2 = 0

  def run(tbd: TBD): Int = {
    val table = input.getTable()
    val one = table.get(1)
    val two = table.get(2)
    val memo = tbd.makeMemoizer[Changeable[Int]]()

    tbd.mod((dest: Dest[Int]) => {
      tbd.read(one)(oneValue => {
        memo(two, dest) {
          count1 += 1
          tbd.write(dest, 0)
        }
      })
    })

    tbd.mod((dest: Dest[Int]) => {
      tbd.read(one)(oneValue => {
        memo(two, dest) {
          count2 += 1
          tbd.write(dest, 0)
        }
      })
    })

    0
  }
}

// Checks that if a call to memo gets matched, a call that is also within the
// same reexecuting read but a parent of the already matched memo can't be
// matched (since part of its ddg has been cannabilized by the first match).
class MatchParentTest(input: TableInput[Int, Int]) extends Adjustable {
  var count1 = 0
  var count2 = 0
  var count3 = 0
  var count4 = 0

  def run(tbd: TBD): Mod[Int] = {
    val table = input.getTable()
    val one = table.get(1)
    val two = table.get(2)
    val three = table.get(3)
    val memo = tbd.makeMemoizer[Unit]()

    tbd.mod((dest: Dest[Int]) => {
      tbd.read(one)(oneValue => {
        if (oneValue == 1) {
          memo(two) {
            count1 += 1
            memo(three) {
              count2 += 1
            }
          }
        } else {
          memo(three) {
            count3 += 1
          }
          memo(two) {
            count4 += 1
          }
        }

        tbd.write(dest, 0)
      })
    })
  }
}

// Tests that change propagation is done through a memoized subddg before
// moving on.
class PropagateThroughMemoTest(input: TableInput[Int, Int]) extends Adjustable {
  var count = 0

  def run(tbd: TBD): Mod[Int] = {
    val table = input.getTable()
    val one = table.get(1)
    val two = table.get(2)
    val three = table.get(3)
    val memo = tbd.makeMemoizer[Mod[Int]]()

    tbd.mod((dest: Dest[Int]) =>
      tbd.read(one)(oneValue => {
        val mod = memo(two) {
          tbd.mod((dest: Dest[Int]) =>
            tbd.read(three)(threeValue => {
              tbd.write(dest, threeValue)
            }))
	}

        tbd.read(mod)(modValue => {
          count += 1
          tbd.write(dest, modValue)
        })
      }))
  }
}

// Tests that a single memo entry can be matched in multiple runs of change
// propagation.
class RepeatRunsTest(input: TableInput[Int, Int]) extends Adjustable {
  var count = 0

  def run(tbd: TBD): Mod[Int] = {
    val table = input.getTable()
    val one = table.get(1)
    val two = table.get(2)
    val memo = tbd.makeMemoizer[Changeable[Int]]()

    tbd.mod((dest: Dest[Int]) => {
      tbd.read(one)(oneValue => {
	memo(two) {
	  count += 1
	  tbd.write(dest, 0)
	}
      })
    })
  }
}

// Tests that if a memo match is made, a memo call that is in the current read's
// subddg but came before the matched memo can't be matched. This is not a
// fundamental aspect of memo, but just a limitation of our current
// implementation, resulting from the fact that making such matches would
// require renumbering timestamps. So, we may someday fix this problem.
class OutOfOrderMatchTest(input: TableInput[Int, Int]) extends Adjustable {
  var count2 = 0
  var count3 = 0

  def run(tbd: TBD): Any = {
    val table = input.getTable()
    val one = table.get(1)
    val two = table.get(2)
    val three = table.get(3)
    val memo = tbd.makeMemoizer[Unit]()

    tbd.mod((dest: Dest[Int]) => {
      tbd.read(one)(oneValue => {
	if (oneValue == 1) {
	  memo(two) {
	    count2 += 1
	  }
	  memo(three) {
	    count3 += 1
	  }
	} else {
	  memo(three) {
	    count3 += 1
	  }
	  memo(two) {
	    count2 += 1
	  }
	}

	tbd.write(dest, 0)
      })
    })
  }
}

class NoDestTest(input: TableInput[Int, Int]) extends Adjustable {
  def run(tbd: TBD): Mod[Int] = {
    val table = input.getTable()
    val one = table.get(1)
    val two = table.get(2)
    val three = table.get(3)
    val four = table.get(4)
    val memo = tbd.makeMemoizer[Changeable[Int]]()

    tbd.mod {
      tbd.read(one)(oneValue => {
        if (oneValue == 1) {
          tbd.mod {
            memo(two) {
              tbd.read(four)(fourValue =>
                tbd.write(fourValue))
            }
          }
          memo(three) {
            tbd.write(3)
          }
        } else {
          memo(two) {
            tbd.read(four)(fourValue =>
              tbd.write(fourValue))
          }
        }
      })
    }
  }
}

class NoDestTest2(input: TableInput[Int, Int]) extends Adjustable {
  def run(tbd: TBD): Mod[Int] = {
    val table = input.getTable()
    val one = table.get(1)
    val two = table.get(2)
    val three = table.get(3)
    val five = table.get(5)
    val memo = tbd.makeMemoizer[Changeable[Int]]()

    val six = tbd.mod {
      tbd.read(one)(one => {
        val four = tbd.mod {
          if (one == 1) {
            tbd.mod {
              memo(two) {
                tbd.read(five)(five =>
                  tbd.write(five))
              }
            }
            memo(three) {
              tbd.write(3)
            }
          } else {
            memo(two) {
              tbd.read(five)(five =>
                tbd.write(five))
            }
          }
        }

        tbd.read(four)(four => {
          tbd.write(four)
        })
      })
    }

    tbd.mod {
      tbd.read(six)(six => {
        tbd.write(six)
      })
    }
  }
}

class MemoTests extends FlatSpec with Matchers {
  "MemoTest" should "find the memo match" in {
    val mutator = new Mutator()
    val input = mutator.createTable[Int, Int]()
    input.put(1, 1)
    input.put(2, 10)
    val test = new MemoTest(input)
    val output = mutator.run[Mod[Int]](test)
    output.read() should be (12)
    test.count should be (1)

    val root = new MockRootNode(List(
      new MockReadNode(List(
        new MockMemoNode(List(
          new MockReadNode(List())
        )),
        new MockReadNode(List())
      ))
    ))

    val ddg = mutator.getDDG()

    if (!Main.debug) {
      assert(root.isEqual(ddg.root))
    }

    // Change the mod not read by the memoized function,
    // check that it isn't called.
    input.update(1, 3)
    mutator.propagate()
    output.read() should be (14)
    test.count should be (1)

    val root2 = new MockRootNode(List(
      new MockReadNode(List(
        new MockReadNode(List()),
        new MockMemoNode(List(
          new MockReadNode(List())
        )),
        new MockReadNode(List())
      ))
    ))

    val ddg2 = mutator.getDDG()
    if (!Main.debug) {
      assert(root2.isEqual(ddg2.root))
    }

    // Change the other mod, the memoized function should
    // be called.
    input.update(1, 2)
    input.update(2, 8)
    mutator.propagate()
    output.read() should be (11)
    test.count should be (2)

    val ddg3 = mutator.getDDG()
    if (!Main.debug) {
      assert(root.isEqual(ddg3.root))
    }

    mutator.shutdown()
  }

  "AlreadyMatchedTest" should "only reuse the memo entry once" in {
    val mutator = new Mutator()
    val input = mutator.createTable[Int, Int]()
    input.put(1, 1)
    input.put(2, 10)
    val test = new AlreadyMatchedTest(input)
    val output = mutator.run[Mod[Int]](test)
    output.read() should be (11)
    test.count1 should be (0)
    test.count2 should be (1)

    input.update(1, 3)
    mutator.propagate()
    output.read() should be (11)
    test.count1 should be (0)
    test.count2 should be (2)

    mutator.shutdown()
  }

  "OutOfScopeTest" should "not memo match outside of the reexecuted read" in {
    val mutator = new Mutator()
    val input = mutator.createTable[Int, Int]()
    input.put(1, 1)
    input.put(2, 2)
    val test = new OutOfScopeTest(input)
    mutator.run[Int](test)

    test.num should be (0)

    input.update(1, 3)
    mutator.propagate()

    test.num should be (1)

    mutator.shutdown()
  }

  "MatchingSignaturesTest" should "find both memo matches" in {
    val mutator = new Mutator()
    val input = mutator.createTable[Int, Int]()
    input.put(1, 1)
    input.put(2, 2)
    val test = new MatchingSignaturesTest(input)

    mutator.run[Int](test)
    test.count1 should be (1)
    test.count2 should be (1)

    input.update(1, 2)
    mutator.propagate()
    test.count1 should be (1)
    test.count2 should be (1)

    mutator.shutdown()
  }

  "MatchParentTest" should "not match the parent" in {
    val mutator = new Mutator()
    val input = mutator.createTable[Int, Int]()
    input.put(1, 1)
    input.put(2, 2)
    input.put(3, 3)
    val test = new MatchParentTest(input)

    mutator.run[Mod[Int]](test)
    test.count1 should be (1)
    test.count2 should be (1)
    test.count3 should be (0)
    test.count4 should be (0)

    input.update(1, 4)
    mutator.propagate()

    test.count1 should be (1)
    test.count2 should be (1)
    test.count3 should be (0)
    test.count4 should be (1)

    mutator.shutdown()
  }

  "PropagateThroughMemoTest" should "only reexecute the read once" in {
    val mutator = new Mutator()
    val input = mutator.createTable[Int, Int]()
    input.put(1, 1)
    input.put(2, 2)
    input.put(3, 3)
    val test = new PropagateThroughMemoTest(input)
    mutator.run[Mod[Int]](test)
    test.count should be (1)

    input.update(1, 4)
    input.update(3, 5)
    mutator.propagate()
    test.count should be (2)

    mutator.shutdown()
  }

  "RepeatRunsTest" should "memo match in multiple runs of propagation" in {
    val mutator = new Mutator()
    val input = mutator.createTable[Int, Int]()
    input.put(1, 1)
    input.put(2, 2)
    val test = new RepeatRunsTest(input)
    mutator.run[Mod[Int]](test)
    test.count should be (1)

    input.update(1, 3)
    mutator.propagate()
    test.count should be (1)

    input.update(1, 4)
    mutator.propagate()
    test.count should be (1)

    input.update(1, 5)
    mutator.propagate()
    test.count should be (1)

    mutator.shutdown()
  }

  "OutOfOrderMatchTest" should "not memo match if a later match has been made" in {
    val mutator = new Mutator()
    val input = mutator.createTable[Int, Int]()
    input.put(1, 1)
    input.put(2, 2)
    input.put(3, 3)
    val test = new OutOfOrderMatchTest(input)
    mutator.run[Any](test)
    test.count2 should be (1)
    test.count3 should be (1)

    input.update(1, 4)
    mutator.propagate()

    test.count2 should be (2)
    test.count3 should be (1)
    mutator.shutdown()
  }

  "NoDestTest" should "update the mod when making the memo match" in {
    val mutator = new Mutator()
    val input = mutator.createTable[Int, Int]()
    input.put(1, 1)
    input.put(2, 2)
    input.put(3, 3)
    input.put(4, 4)
    val test = new NoDestTest(input)
    val output = mutator.run[Mod[Int]](test)
    output.read() should be (3)

    input.update(1, 2)
    input.update(4, 5)
    mutator.propagate()
    output.read() should be (5)

    mutator.shutdown()
  }

  "NoDestTest2" should "write the matched value into the mod" in {
    val mutator = new Mutator()
    val input = mutator.createTable[Int, Int]()
    input.put(1, 1)
    input.put(2, 2)
    input.put(3, 3)
    input.put(5, 5)
    val test = new NoDestTest2(input)
    val output = mutator.run[Mod[Int]](test)
    output.read() should be (3)

    input.update(1, 2)
    input.update(5, 10)
    mutator.propagate()
    output.read() should be (10)
  }
}
