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
package tdb.util

import java.net.InetAddress;
import java.net.NetworkInterface;
import scala.io.StdIn

object Util {
  def tdbWait(n: Int) {
    print(n + ">")
    StdIn.readLine()
  }

  // Returns true if we are running on a 64 bit system.
  def is64(): Boolean = {
    val JVM_ARCH_PROPERTY = "sun.arch.data.model"

    val arch = System.getProperty(JVM_ARCH_PROPERTY)
    if (arch != null) {
      Integer.parseInt(arch) == 64
    } else {
      false
    }
  }

  /**
   * Returns a String representation of a relatively public (e.g. not loopback)
   * IP address for the machine we're currently running on.
   */
  def getIP(): String = {
    var ip = ""

    val en = NetworkInterface.getNetworkInterfaces()
    while(en.hasMoreElements()) {
      val n = en.nextElement().asInstanceOf[NetworkInterface]
      val ee = n.getInetAddresses()

      while (ee.hasMoreElements()) {
        val i = ee.nextElement().asInstanceOf[InetAddress]
        if (!i.isLoopbackAddress() && !i.isLinkLocalAddress()) {
          if (ip != "") {
            println("WARNING: found two possible ip addresses " + ip + " and " +
                    i.getHostAddress())
          }
          ip = i.getHostAddress();
        }
      }
    }

    ip
  }
}
