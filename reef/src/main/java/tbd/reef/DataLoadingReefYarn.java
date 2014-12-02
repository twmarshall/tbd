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

import com.microsoft.reef.annotations.audience.ClientSide;
import com.microsoft.reef.client.DriverConfiguration;
import com.microsoft.reef.client.DriverLauncher;
import com.microsoft.reef.client.LauncherStatus;
import com.microsoft.reef.driver.evaluator.EvaluatorRequest;
import com.microsoft.reef.io.data.loading.api.DataLoadingRequestBuilder;
import com.microsoft.reef.runtime.local.client.LocalRuntimeConfiguration;
import com.microsoft.reef.runtime.yarn.client.YarnClientConfiguration;
import com.microsoft.reef.util.EnvironmentUtils;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.Injector;
import com.microsoft.tang.JavaConfigurationBuilder;
import com.microsoft.tang.Tang;
import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.NamedParameter;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.tang.exceptions.InjectionException;
import com.microsoft.tang.formats.CommandLine;
import org.apache.hadoop.mapred.TextInputFormat;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

@ClientSide
public class DataLoadingReefYarn {

  private static final Logger LOG = Logger.getLogger(DataLoadingReefYarn.class.getName());

  private static final int NUM_LOCAL_THREADS = 16;
  private static final int NUM_COMPUTE_EVALUATORS = 0;
  private static int NUM_SPLITS;

  @NamedParameter(doc = "Whether or not to run on the local runtime",
      short_name = "local", default_value = "false")
  public static final class Local implements Name<Boolean> {
  }

  @NamedParameter(doc = "Number of minutes before timeout",
      short_name = "timeout", default_value = "10")
  public static final class TimeOut implements Name<Integer> {
  }

  @NamedParameter(short_name = "input")
  public static final class InputDir implements Name<String> {
  }

  @NamedParameter(doc = "Number of partitions, i.e., number of workers",
      short_name = "partitions", default_value = "2")
  public static final class Partitions implements Name<Integer> {
  }

  @NamedParameter(doc = "This should be set to the same to chunkSizes in TBD",
      short_name = "chunkSizes", default_value = "1")
  public static final class ChunkSizes implements Name<Integer> {
  }

  public static void main(final String[] args)
      throws InjectionException, BindException, IOException {

    final Tang tang = Tang.Factory.getTang();

    final JavaConfigurationBuilder cb = tang.newConfigurationBuilder();

    new CommandLine(cb)
        .registerShortNameOfClass(Local.class)
        .registerShortNameOfClass(TimeOut.class)
        .registerShortNameOfClass(DataLoadingReefYarn.InputDir.class)
        .registerShortNameOfClass(Partitions.class)
        .registerShortNameOfClass(ChunkSizes.class)
        .processCommandLine(args);

    final Injector injector = tang.newInjector(cb.build());

    final boolean isLocal = injector.getNamedInstance(Local.class);
    final int jobTimeout = injector.getNamedInstance(TimeOut.class) * 60 * 1000;
    final String inputDir = injector.getNamedInstance(DataLoadingReefYarn.InputDir.class);
    final int partitions = injector.getNamedInstance(Partitions.class);
    final int chunkSizes = injector.getNamedInstance(ChunkSizes.class);

    NUM_SPLITS = partitions;

    final Configuration runtimeConfiguration;
    if (isLocal) {
      LOG.log(Level.INFO, "Running Data Loading demo on the local runtime");
      runtimeConfiguration = LocalRuntimeConfiguration.CONF
          .set(LocalRuntimeConfiguration.NUMBER_OF_THREADS, NUM_LOCAL_THREADS)
          .build();
    } else {
      LOG.log(Level.INFO, "Running Data Loading demo on YARN");
      runtimeConfiguration = YarnClientConfiguration.CONF.build();
    }

    final EvaluatorRequest computeRequest = EvaluatorRequest.newBuilder()
        .setNumber(NUM_COMPUTE_EVALUATORS)
        .setMemory(512)
        .setNumberOfCores(1)
        .build();

    final Configuration dataLoadConfiguration = new DataLoadingRequestBuilder()
        .setMemoryMB(3072)
        .setNumberOfCores(2)
        .setInputFormatClass(TextInputFormat.class)
        .setInputPath(inputDir)
        .setNumberOfDesiredSplits(NUM_SPLITS)
        //.setComputeRequest(computeRequest)
        .setDriverConfigurationModule(DriverConfiguration.CONF
            .set(DriverConfiguration.GLOBAL_LIBRARIES, EnvironmentUtils.getClassLocation(DataLoadingReefYarn.class))
            .set(DriverConfiguration.ON_CONTEXT_ACTIVE, DataLoadingDriver.ContextActiveHandler.class)
            .set(DriverConfiguration.ON_TASK_COMPLETED, DataLoadingDriver.TaskCompletedHandler.class)
            //.set(DriverConfiguration.ON_DRIVER_STARTED, DataLoadingDriver.DriverStartedHandler.class)
            .set(DriverConfiguration.DRIVER_IDENTIFIER, "DataLoadingREEF"))
        .build();

    

    //start a hardcoded master on localhost
    String cp = DataLoadingReefYarn.class.getProtectionDomain().getCodeSource().getLocation().getFile();
    LOG.log(Level.INFO, "cp: {0}", cp);
    ProcessBuilder pb = new ProcessBuilder("java", "-Xss4m", "-cp", cp, "tbd.master.Main", "-i", "127.0.0.1", "-p", "2555");
    LOG.log(Level.INFO, "pb");
    pb.redirectErrorStream(true);
    pb.inheritIO();
    pb.redirectErrorStream(true);
    Process p = null;
    try {
      LOG.log(Level.INFO, "before start");
      p = pb.start();
      LOG.log(Level.INFO, "after start");
    }
    catch (IOException e) {
      LOG.log(Level.INFO, "master process IO exception");
    }

    final LauncherStatus state =
        DriverLauncher.getLauncher(runtimeConfiguration).run(dataLoadConfiguration, jobTimeout);

    LOG.log(Level.INFO, "REEF job completed: {0}", state);

    LOG.log(Level.INFO, "end master");
    p.destroy();
  }
}