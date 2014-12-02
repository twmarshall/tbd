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

import com.microsoft.reef.client.*;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.JavaConfigurationBuilder;
import com.microsoft.tang.Tang;
import com.microsoft.tang.annotations.NamedParameter;
import com.microsoft.tang.annotations.Parameter;
import com.microsoft.tang.annotations.Unit;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.reef.util.EnvironmentUtils;
import com.microsoft.wake.EventHandler;
import com.microsoft.wake.remote.impl.ObjectSerializableCodec;

import javax.inject.Inject;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * TBD REEF application client.
 */
@Unit
public class TBDClient {

  /**
   * Standard java logger.
   */
  private static final Logger LOG = Logger.getLogger(TBDClient.class.getName());

  /**
   * Codec to translate messages to and from the job driver
   */
  private static final ObjectSerializableCodec<String> CODEC = new ObjectSerializableCodec<>();

  /**
   * Reference to the REEF framework.
   * This variable is injected automatically in the constructor.
   */
  private final REEF reef;

  /**
   * Job Driver configuration.
   */
  private final Configuration driverConfiguration;

  /**
   * If true, take commands from stdin; otherwise, use -cmd parameter in batch mode.
   */
  private final boolean isInteractive;

  /**
   * Command prompt reader for the interactive mode (stdin).
   */
  private final BufferedReader prompt;

  /**
   * A reference to the running job that allows client to send messages back to the job driver
   */
  private RunningJob runningJob;

  /**
   * Start timestamp of the current task.
   */
  private long startTime = 0;

  /**
   * Total time spent performing tasks in Evaluators.
   */
  private long totalTime = 0;

  /**
   * Number of experiments ran so far.
   */
  private int numRuns = 0;

  /**
   * Set to false when job driver is done.
   */
  private boolean isBusy = true;

  /**
   * Last result returned from the job driver.
   */
  private String lastResult;

  private final int numWorkers;
  
  private final int timeout;
  
  /**
   * TBD REEF application client.
   * Parameters are injected automatically by TANG.
   *
   * @param reef    Reference to the REEF framework.
   */
  @Inject
  TBDClient(final REEF reef,
            @Parameter(TBDLaunch.NumWorkers.class) final Integer numWorkers,
            @Parameter(TBDLaunch.Timeout.class) final Integer timeout) throws BindException {

    this.reef = reef;
    this.numWorkers = numWorkers;
    this.timeout = timeout;

    this.isInteractive = false;
    this.prompt = null;
    /*
    // If command is not set, switch to interactive mode. (Yes, we compare pointers here)
    this.isInteractive = this.command ==
        TBDLaunch.Command.class.getAnnotation(NamedParameter.class).default_value();

    this.prompt = this.isInteractive ?
        new BufferedReader(new InputStreamReader(System.in)) : null;
    */

    final JavaConfigurationBuilder configBuilder = Tang.Factory.getTang().newConfigurationBuilder();
    configBuilder.addConfiguration(
        DriverConfiguration.CONF
            .set(DriverConfiguration.GLOBAL_LIBRARIES, TBDReefYarn.class.getProtectionDomain().getCodeSource().getLocation().getFile())
            .set(DriverConfiguration.DRIVER_IDENTIFIER, "TBDReefYarn")
            .set(DriverConfiguration.ON_EVALUATOR_ALLOCATED, TBDDriver.EvaluatorAllocatedHandler.class)
            .set(DriverConfiguration.ON_EVALUATOR_FAILED, TBDDriver.EvaluatorFailedHandler.class)
            .set(DriverConfiguration.ON_CONTEXT_ACTIVE, TBDDriver.ActiveContextHandler.class)
            .set(DriverConfiguration.ON_CONTEXT_CLOSED, TBDDriver.ClosedContextHandler.class)
            .set(DriverConfiguration.ON_CONTEXT_FAILED, TBDDriver.FailedContextHandler.class)
            .set(DriverConfiguration.ON_TASK_RUNNING, TBDDriver.RunningTaskHandler.class)
            //.set(DriverConfiguration.ON_TASK_COMPLETED, TBDDriver.CompletedTaskHandler.class)
            //.set(DriverConfiguration.ON_CLIENT_MESSAGE, TBDDriver.ClientMessageHandler.class)
            .set(DriverConfiguration.ON_DRIVER_STARTED, TBDDriver.StartHandler.class)
            .set(DriverConfiguration.ON_DRIVER_STOP, TBDDriver.StopHandler.class)
            .build()
    );
    configBuilder.bindNamedParameter(TBDLaunch.NumWorkers.class, "" + numWorkers);
    configBuilder.bindNamedParameter(TBDLaunch.Timeout.class, "" + timeout);
    this.driverConfiguration = configBuilder.build();
  }

  /**
   * @return a Configuration binding the ClientConfiguration.* event handlers to this Client.
   */
  public static Configuration getClientConfiguration() {
    return ClientConfiguration.CONF
        .set(ClientConfiguration.ON_JOB_RUNNING, TBDClient.RunningJobHandler.class)
        .set(ClientConfiguration.ON_JOB_MESSAGE, TBDClient.JobMessageHandler.class)
        .set(ClientConfiguration.ON_JOB_COMPLETED, TBDClient.CompletedJobHandler.class)
        .set(ClientConfiguration.ON_JOB_FAILED, TBDClient.FailedJobHandler.class)
        .set(ClientConfiguration.ON_RUNTIME_ERROR, TBDClient.RuntimeErrorHandler.class)
        .build();
  }

  /**
   * Launch the job driver.
   *
   * @throws BindException configuration error.
   */
  public void submit() {
    this.reef.submit(this.driverConfiguration);
  }

  /**
   * Send command to the job driver. Record timestamp when the command was sent.
   * If this.command is set, use it; otherwise, ask user for the command.
   */
  /*
  private synchronized void submitTask() {
    if (this.isInteractive) {
      String cmd;
      try {
        do {
          System.out.print("\nRE> ");
          cmd = this.prompt.readLine();
        } while (cmd != null && cmd.trim().isEmpty());
      } catch (final IOException ex) {
        LOG.log(Level.FINE, "Error reading from stdin: {0}", ex);
        cmd = null;
      }
      if (cmd == null || cmd.equals("exit")) {
        this.runningJob.close();
        stopAndNotify();
      } else {
        this.submitTask(cmd);
      }
    } else {
      // non-interactive batch mode:
      this.submitTask(this.command);
    }
  }
  */

  /**
   * Send command to the job driver. Record timestamp when the command was sent.
   *
   * @param cmd shell command to execute in all Evaluators.
   */
  /*
  private synchronized void submitTask(final String cmd) {
    LOG.log(Level.FINE, "Submit task {0} \"{1}\" to {2}",
        new Object[]{this.numRuns + 1, cmd, this.runningJob});
    this.startTime = System.currentTimeMillis();
    this.runningJob.send(CODEC.encode(cmd));
  }
  */

  /**
   * Notify the process in waitForCompletion() method that the main process has finished.
   */
  private synchronized void stopAndNotify() {
    this.runningJob = null;
    this.isBusy = false;
    this.notify();
  }

  /**
   * Wait for the job driver to complete. This method is called from Launch.main()
   */
  public String waitForCompletion() {
    while (this.isBusy) {
      LOG.log(Level.FINE, "Waiting for the Job Driver to complete.");
      try {
        synchronized (this) {
          this.wait();
        }
      } catch (final InterruptedException ex) {
        LOG.log(Level.WARNING, "Waiting for result interrupted.", ex);
      }
    }
    return this.lastResult;
  }

  public void close() {
    this.reef.close();
  }

  /**
   * Receive notification from the job driver that the job is running.
   */
  final class RunningJobHandler implements EventHandler<RunningJob> {
    @Override
    public void onNext(final RunningJob job) {
      LOG.log(Level.FINE, "Running job: {0}", job.getId());
      synchronized (TBDClient.this) {
        TBDClient.this.runningJob = job;
        //TBDClient.this.submitTask();
      }
    }
  }

  /**
   * Receive message from the job driver.
   * There is only one message, which comes at the end of the driver execution
   * and contains shell command output on each node.
   */
  final class JobMessageHandler implements EventHandler<JobMessage> {
    @Override
    public void onNext(final JobMessage message) {
      String masterAkka = CODEC.decode(message.get());
      System.out.println("master Akka:  " + masterAkka);
      /*
      synchronized (TBDClient.this) {

        lastResult = CODEC.decode(message.get());
        final long jobTime = System.currentTimeMillis() - startTime;
        totalTime += jobTime;
        ++numRuns;

        LOG.log(Level.FINE, "TIME: Task {0} completed in {1} msec.:\n{2}",
            new Object[]{"" + numRuns, "" + jobTime, lastResult});

        System.out.println(lastResult);

        if (runningJob != null) {
          if (isInteractive || numRuns < maxRuns) {
            submitTask();
          } else {
            LOG.log(Level.INFO,
                "All {0} tasks complete; Average task time: {1}. Closing the job driver.",
                new Object[]{maxRuns, totalTime / (double) maxRuns});
            runningJob.close();
            stopAndNotify();
          }
        }
      }
      */
    }
  }

  /**
   * Receive notification from the job driver that the job had failed.
   */
  final class FailedJobHandler implements EventHandler<FailedJob> {
    @Override
    public void onNext(final FailedJob job) {
      LOG.log(Level.SEVERE, "Failed job: " + job.getId(), job.getReason().orElse(null));
      stopAndNotify();
    }
  }

  /**
   * Receive notification from the job driver that the job had completed successfully.
   */
  final class CompletedJobHandler implements EventHandler<CompletedJob> {
    @Override
    public void onNext(final CompletedJob job) {
      LOG.log(Level.FINE, "Completed job: {0}", job.getId());
      stopAndNotify();
    }
  }

  /**
   * Receive notification that there was an exception thrown from the job driver.
   */
  final class RuntimeErrorHandler implements EventHandler<FailedRuntime> {
    @Override
    public void onNext(final FailedRuntime error) {
      LOG.log(Level.SEVERE, "Error in job driver: " + error, error.getReason().orElse(null));
      stopAndNotify();
    }
  }
}