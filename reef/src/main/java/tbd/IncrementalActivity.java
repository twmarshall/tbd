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
package edu.cmu.cs.incremental;

import com.microsoft.reef.activity.Activity;
import com.microsoft.tang.annotations.Parameter;
import com.microsoft.wake.remote.impl.ObjectSerializableCodec;

//import slacr.worker.Worker;
//import slacr.datastore.PostgreSQL;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * An Activity.
 */
public final class IncrementalActivity implements Activity {

  /** Standard java logger. */
  private static final Logger LOG = Logger.getLogger(IncrementalActivity.class.getName());

  /** A command to execute. */
  private final String command;

  /**
   * Activity constructor. Parameters are injected automatically by TANG.
   * @param command a command to execute.
   */
  @Inject
  private IncrementalActivity() {
    this.command = "asdf";
  }

  @Override
  public final byte[] call(final byte[] memento) {
    System.out.println("\n\n!@#$#@\n\n");
    //Worker w = new Worker(command);
    final ObjectSerializableCodec<String> codec = new ObjectSerializableCodec<>();
    return codec.encode("finished.");
  }
}
