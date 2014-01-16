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

import com.microsoft.reef.client.ClientConfiguration;
import com.microsoft.reef.client.DriverConfiguration;
import com.microsoft.reef.client.DriverLauncher;
import com.microsoft.reef.client.LauncherStatus;
import com.microsoft.reef.client.REEF;
import com.microsoft.reef.runtime.local.client.LocalRuntimeConfiguration;
import com.microsoft.reef.runtime.yarn.client.YarnClientConfiguration;
import com.microsoft.reef.util.EnvironmentUtils;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.Injector;
import com.microsoft.tang.Tang;
import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.NamedParameter;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.tang.exceptions.InjectionException;
import com.microsoft.tang.formats.ConfigurationFile;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The Client for TBD.
 */
public final class IncrementalClient {

  private static final int NUM_LOCAL_THREADS = 2;

  private static final Logger LOG = Logger.getLogger(IncrementalClient.class.getName());

  /**
   * Number of milliseconds to wait for the job to complete.
   */
  private static final int JOB_TIMEOUT = 10000; // 10 sec.

  /**
   * Parameter to the activity.
   */
  @NamedParameter(doc = "Parameter to activity.", short_name = "cmd")
  public static final class Command implements Name<String> {
  }

  static final String EMPTY_FILES = "EMPTY_FILES";
  /**
   * Command line parameter - additional files to submitActivity to the Evaluators.
   */
  @NamedParameter(doc = "Additional files to submitActivity to the evaluators.",
      short_name = "files", default_value = EMPTY_FILES)
  public static final class Files implements Name<String> {
  }

  /**
   * @param local should the job be launched locally or on YARN.
   * @return (immutable) TANG Configuration object.
   * @throws BindException      if configuration injector fails.
   * @throws InjectionException if the Local.class parameter is not injected.
   */
  private static Configuration getConfiguration(boolean local)
    throws BindException, InjectionException {
    final Configuration runtimeConfiguration;
    if (local) {
      LOG.log(Level.INFO, "Running DS on the local runtime");
      runtimeConfiguration = LocalRuntimeConfiguration.CONF
        .set(LocalRuntimeConfiguration.NUMBER_OF_THREADS, NUM_LOCAL_THREADS)
        .build();
    } else {
      /*LOG.log(Level.INFO, "Running DS on YARN");
      runtimeConfiguration = YarnClientConfiguration.CONF
        .set(YarnClientConfiguration.REEF_JAR_FILE, EnvironmentUtils.getClassLocationFile(REEF.class))
        .build();*/
      runtimeConfiguration = null;
    }

    /*final Configuration clientConfiguration = ClientConfiguration.CONF
      .set(ClientConfiguration.ON_JOB_MESSAGE, IncrementalJob.JobMessageHandler.class)
      .set(ClientConfiguration.ON_JOB_COMPLETED, IncrementalJob.CompletedJobHandler.class)
      .build();

    return Tang.Factory.getTang()
      .newConfigurationBuilder(runtimeConfiguration, clientConfiguration)
      .build();*/
    return runtimeConfiguration;
  }

  public static void runTBDJob(final Configuration aConfig)
      throws BindException, InjectionException {
    final Injector injector = Tang.Factory.getTang().newInjector(aConfig);
    final IncrementalJob shell = injector.getInstance(IncrementalJob.class);
    //shell.submit();
    //System.out.println(shell.getResult());
  }

  /**
   * Start TBD job. Runs method runTBDClient().
   *
   * @param args command line parameters.
   */
  //public static void main(final String[] args) throws BindException, InjectionException {
  public static String run() throws BindException, InjectionException {
    return run(true);
  }

  public static String run(boolean local) {
    try {
      final Configuration config = getConfiguration(local);
      LOG.log(Level.INFO, "TBD Configuration:\n--\n{0}--:",
              ConfigurationFile.toConfigurationString(config));

      runTBDJob(config);
      LOG.log(Level.INFO, "TBD job completed.");
    } catch (final BindException | InjectionException ex) {
      LOG.log(Level.SEVERE, "Unable to launch on REEF", ex);
    } catch (final Exception ex) {
      LOG.log(Level.SEVERE, "Unable to launch on REEFasdf", ex);
    }

    return "asdf";
  }
}
