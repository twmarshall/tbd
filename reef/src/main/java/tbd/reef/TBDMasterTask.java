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

import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.task.Task;

import java.util.logging.Level;
import java.util.logging.Logger;

import javax.inject.Inject;

/**
 * A master node.
 */
public final class TBDMasterTask implements Task {
  private static final Logger LOG =
      Logger.getLogger(TBDMasterTask.class.getName());
  private final int timeout;
  private final String masterIP;
  private final String masterPort;

  @Inject
  TBDMasterTask(@Parameter(TBDDriver.HostIP.class) final String ip,
      @Parameter(TBDDriver.HostPort.class) final String port,
      @Parameter(TBDLaunch.Timeout.class) final int tout) {
    masterIP = ip;
    masterPort = port;
    timeout = tout;
  }

  @Override
  public final byte[] call(final byte[] memento) {
    LOG.log(Level.INFO, "start master");
    LOG.log(Level.INFO, "master IP: {0}", masterIP);
    LOG.log(Level.INFO, "master port: {0}", masterPort);

   String[] args = {"-i", masterIP, "-p", masterPort};
   tbd.master.Main.main(args);

    LOG.log(Level.INFO, "master sleep");
    try {
      Thread.sleep(timeout*60*1000);
    } catch (InterruptedException e) {
      LOG.log(Level.INFO, "master sleep interrupted");
    }

    LOG.log(Level.INFO, "end master");
    return null;
  }
}
