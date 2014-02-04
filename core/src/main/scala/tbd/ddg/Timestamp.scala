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
package tbd.ddg

class Timestamp(aSublist: Sublist, aTime: Double, aNext: Timestamp) {
  var sublist = aSublist
  var time = aTime
  var next = aNext
  var previous: Timestamp = null

  def <(that: Timestamp): Boolean = {
    if (sublist == that.sublist) {
      time < that.time
    } else {
      sublist.id < that.sublist.id
    }
  }

  def >(that: Timestamp): Boolean = {
    if (sublist == that.sublist) {
      time > that.time
    } else {
      sublist.id > that.sublist.id
    }
  }

  override def toString = "(" + sublist.id + " " + time.toString + ")"
}
