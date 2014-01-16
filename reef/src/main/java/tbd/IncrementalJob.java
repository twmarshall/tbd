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

import com.microsoft.reef.client.*;
import com.microsoft.reef.util.EnvironmentUtils;
import com.microsoft.tang.JavaConfigurationBuilder;
import com.microsoft.tang.Tang;
import com.microsoft.tang.annotations.Parameter;
import com.microsoft.tang.annotations.Unit;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.tang.formats.ConfigurationModule;
import com.microsoft.wake.EventHandler;
import com.microsoft.wake.remote.impl.ObjectSerializableCodec;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

@Unit
class IncrementalJob {
  /**
   * Standard java logger.
   */
  private final static Logger LOG = Logger.getLogger(IncrementalJob.class.getName());

  /**
   * Reference to the REEF framework.
   */
  private final REEF reef;

  /**
   * Result of the IncrementalActivity execution on ALL nodes.
   */
  private String dsResult;

  /**
   * TBD client constructor. Parameters are injected automatically by TANG.
   *
   * @param reef      reference to the REEF framework.
   */
  @Inject
    IncrementalJob(final REEF reef, @Parameter(IncrementalClient.Files.class) final String resources) {
    this.reef = reef;
  }

  /**
   * Submits the shell command to be executed on every node of the resource manager.
   *
   * @param cmd shell command to execute.
   * @throws BindException configuration error.
   */
  public void submit() throws BindException {

    final String jobid = "incremental-" + System.currentTimeMillis();
    LOG.log(Level.INFO, "TBD - {0}", jobid);

    ConfigurationModule driverConf = DriverConfiguration.CONF
      .set(DriverConfiguration.DRIVER_IDENTIFIER, jobid)
      .set(DriverConfiguration.ON_ACTIVITY_COMPLETED, IncrementalDriver.CompletedActivityHandler.class)
      .set(DriverConfiguration.ON_EVALUATOR_ALLOCATED, IncrementalDriver.AllocatedEvaluatorHandler.class)
      .set(DriverConfiguration.ON_DRIVER_STARTED, IncrementalDriver.StartHandler.class)
      .set(DriverConfiguration.ON_DRIVER_STOP, IncrementalDriver.StopHandler.class);

    driverConf = EnvironmentUtils.addClasspath(driverConf, DriverConfiguration.GLOBAL_LIBRARIES);

    final JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder(driverConf.build());
    //this.reef.submit(cb.build());
  }

  /**
   * Receive message from the IncrementalDriver.
   * There is only one message, which comes at the end of the driver execution
   * and contains shell command output on each node.
   */
  final class JobMessageHandler implements EventHandler<JobMessage> {
    @Override
    public void onNext(final JobMessage message) {
      final ObjectSerializableCodec<String> codec = new ObjectSerializableCodec<>();
      final String msg = codec.decode(message.get());
      LOG.log(Level.INFO, "Got message: {0}", msg);
      synchronized (IncrementalJob.this) {
        assert (IncrementalJob.this.dsResult == null);
        IncrementalJob.this.dsResult = msg;
      }
    }
  }

  /**
   * Receive notification from IncrementalDriver that the job had completed successfully.
   */
  final class CompletedJobHandler implements EventHandler<CompletedJob> {
    @Override
    public void onNext(final CompletedJob job) {
      LOG.log(Level.INFO, "Completed job: {0}", job.getId());
      synchronized (IncrementalJob.this) {
        IncrementalJob.this.notify();
      }
    }
  }

  /**
   * Wait for the TBD job to complete and return the result.
   *
   * @return concatenated results from all distributed shells.
   */
  public String getResult() {
    while (this.dsResult == null) {
      LOG.info("Waiting for the TBD processes to complete.");
      try {
        synchronized (this) {
          this.wait();
        }
      } catch (final InterruptedException ex) {
        LOG.log(Level.WARNING, "Waiting for result interrupted.", ex);
      }
    }
    this.reef.close();
    return this.dsResult;
  }
}