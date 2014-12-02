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

import com.microsoft.reef.client.ClientConfiguration;
import com.microsoft.reef.runtime.local.client.LocalRuntimeConfiguration;
import com.microsoft.reef.runtime.yarn.client.YarnClientConfiguration;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.Injector;
import com.microsoft.tang.JavaConfigurationBuilder;
import com.microsoft.tang.Tang;
import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.NamedParameter;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.tang.exceptions.InjectionException;
import com.microsoft.tang.formats.AvroConfigurationSerializer;
import com.microsoft.tang.formats.CommandLine;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * TBD REEF application launcher - main class.
 */
public final class TBDLaunch {

  /**
   * Number of REEF worker threads in local mode.
   */
  private static final int NUM_LOCAL_THREADS = 4;
  /**
   * Standard Java logger
   */
  private static final Logger LOG = Logger.getLogger(TBDLaunch.class.getName());

  /**
   * This class should not be instantiated.
   */
  private TBDLaunch() {
    throw new RuntimeException("Do not instantiate this class!");
  }

  /**
   * Parse the command line arguments.
   *
   * @param args command line arguments, as passed to main()
   * @return Configuration object.
   * @throws BindException configuration error.
   * @throws IOException   error reading the configuration.
   */
  private static Configuration parseCommandLine(final String[] args)
      throws BindException, IOException {
    final JavaConfigurationBuilder confBuilder = Tang.Factory.getTang().newConfigurationBuilder();
    final CommandLine cl = new CommandLine(confBuilder);
    cl.registerShortNameOfClass(Local.class);
    cl.registerShortNameOfClass(NumWorkers.class);
    cl.registerShortNameOfClass(Timeout.class);
    cl.processCommandLine(args);
    return confBuilder.build();
  }

  private static Configuration cloneCommandLineConfiguration(final Configuration commandLineConf)
      throws InjectionException, BindException {
    final Injector injector = Tang.Factory.getTang().newInjector(commandLineConf);
    final JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
    cb.bindNamedParameter(NumWorkers.class, String.valueOf(injector.getNamedInstance(NumWorkers.class)));
    cb.bindNamedParameter(Timeout.class, String.valueOf(injector.getNamedInstance(Timeout.class)));
    return cb.build();
  }

  /**
   * Parse command line arguments and create TANG configuration ready to be submitted to REEF.
   *
   * @param args Command line arguments, as passed into main().
   * @return (immutable) TANG Configuration object.
   * @throws BindException      if configuration commandLineInjector fails.
   * @throws InjectionException if configuration commandLineInjector fails.
   * @throws IOException        error reading the configuration.
   */
  private static Configuration getClientConfiguration(final String[] args)
      throws BindException, InjectionException, IOException {

    final Configuration commandLineConf = parseCommandLine(args);

    final Configuration clientConfiguration = ClientConfiguration.CONF
        .set(ClientConfiguration.ON_JOB_RUNNING, TBDClient.RunningJobHandler.class)
        .set(ClientConfiguration.ON_JOB_MESSAGE, TBDClient.JobMessageHandler.class)
        .set(ClientConfiguration.ON_JOB_COMPLETED, TBDClient.CompletedJobHandler.class)
        .set(ClientConfiguration.ON_JOB_FAILED, TBDClient.FailedJobHandler.class)
        .set(ClientConfiguration.ON_RUNTIME_ERROR, TBDClient.RuntimeErrorHandler.class)
        .build();

    final Injector commandLineInjector = Tang.Factory.getTang().newInjector(commandLineConf);
    final boolean isLocal = commandLineInjector.getNamedInstance(Local.class);
    final Configuration runtimeConfiguration;
    if (isLocal) {
      LOG.log(Level.INFO, "Running on the local runtime");
      runtimeConfiguration = LocalRuntimeConfiguration.CONF
          .set(LocalRuntimeConfiguration.NUMBER_OF_THREADS, NUM_LOCAL_THREADS)
          .build();
    } else {
      LOG.log(Level.INFO, "Running on YARN");
      runtimeConfiguration = YarnClientConfiguration.CONF.build();
    }

    return Tang.Factory.getTang()
        .newConfigurationBuilder(runtimeConfiguration, clientConfiguration,
            cloneCommandLineConfiguration(commandLineConf))
        .build();
  }

  /**
   * Main method that starts the Retained Evaluators job.
   *
   * @return a string that contains last results from all evaluators.
   */
  public static String run(final Configuration config) throws InjectionException {
    final Injector injector = Tang.Factory.getTang().newInjector(config);
    final TBDClient client = injector.getInstance(TBDClient.class);
    client.submit();
    return client.waitForCompletion();
  }

  /**
   * Main method that starts the Retained Evaluators job.
   *
   * @param args command line parameters.
   */
  public static void main(final String[] args) {
    try {
      final Configuration config = getClientConfiguration(args);
      LOG.log(Level.FINEST, "Configuration:\n--\n{0}--",
          new AvroConfigurationSerializer().toString(config));
      run(config);
      LOG.info("Done!");
    } catch (final BindException | InjectionException | IOException ex) {
      LOG.log(Level.SEVERE, "Job configuration error", ex);
    }
  }

  /**
   * Command line parameter: number of evaluators to allocate.
   */
  @NamedParameter(doc = "Number of workers",
      short_name = "numWorkers", default_value = "2")
  public static final class NumWorkers implements Name<Integer> {
  }

  /**
   * Command line parameter = true to run locally, or false to run on YARN.
   */
  @NamedParameter(doc = "Whether or not to run on the local runtime",
      short_name = "local", default_value = "true")
  public static final class Local implements Name<Boolean> {
  }

  /**
   * Command line parameter = true to run locally, or false to run on YARN.
   */
  @NamedParameter(doc = "Timeout (in seconds), after which the REEF application will terminate",
      short_name = "timeout", default_value = "600000")
  public static final class Timeout implements Name<Integer> {
  }
}