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
package tbd.reef;

import java.io.*;
import java.util.*;
import java.lang.*;

public class Test {
  public static void main(String[] args) throws IOException {
    
    System.out.println("hello");
    
    
    ProcessBuilder pb = new ProcessBuilder(new String[]{"java", "-Xmx2g", "-Xss4m", 
        "-cp", "/home/ubuntu/capstone/tbd/reef/target/scala-2.11/reef-assembly-0.1-SNAPSHOT.jar", 
        "tbd.master.Main", "-i", "127.0.53.53", "-p", "2555"});
    System.out.println("pb");
    
    //pb.inheritIO();
    //pb.redirectErrorStream(true);
    //pb.redirectOutput(new File("/tmp/masterOut.log"));
    //pb.redirectError(new File("/tmp/masterError.log"));
    //LOG.log(Level.INFO, "redirect");
    
    try {
      pb.redirectErrorStream(true); // merge stdout, stderr of process
      System.out.println("before start");
      Process p = pb.start();
      System.out.println("after start");
      InputStreamReader isr = new  InputStreamReader(p.getInputStream());
      BufferedReader br = new BufferedReader(isr);

      String lineRead;
      while ((lineRead = br.readLine()) != null) {
        System.out.println(lineRead);
      }

      int rc = p.waitFor();
      System.out.println(rc);
      // TODO error handling for non-zero rc
    }
    catch (IOException e) {
      System.out.println("master process IO exception");
    }
    catch (InterruptedException ie) {
      System.out.println("master process interrupted");
    }
    
  }
}
