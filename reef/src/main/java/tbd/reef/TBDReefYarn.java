package tbd.reef;

import com.microsoft.reef.client.DriverConfiguration;
import com.microsoft.reef.client.DriverLauncher;
import com.microsoft.reef.client.LauncherStatus;
import com.microsoft.reef.runtime.yarn.client.YarnClientConfiguration;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.tang.exceptions.InjectionException;

import java.util.logging.Level;
import java.util.logging.Logger;

public final class TBDReefYarn {
  private static final Logger LOG = Logger.getLogger(TBDReefYarn.class.getName());
  private static final int JOB_TIMEOUT = 600000; // 600 sec.

  /**
   * @return the configuration of the REEF driver.
   */
  private static Configuration getDriverConfiguration() {
    
    System.out.println(TBDReefYarn.class.getProtectionDomain().getCodeSource().getLocation().getFile());
    /*
    System.out.println(TBDReefYarn.class.getProtectionDomain().getCodeSource().getLocation());
    System.out.println(TBDReefYarn.class.getProtectionDomain().getCodeSource());
    System.out.println(TBDReefYarn.class.getProtectionDomain());
    */
    return DriverConfiguration.CONF
        .set(DriverConfiguration.GLOBAL_LIBRARIES, TBDReefYarn.class.getProtectionDomain().getCodeSource().getLocation().getFile())
        .set(DriverConfiguration.DRIVER_IDENTIFIER, "TBDReefYarn")
        .set(DriverConfiguration.ON_DRIVER_STARTED, TBDDriver.StartHandler.class)
        .set(DriverConfiguration.ON_EVALUATOR_ALLOCATED, TBDDriver.EvaluatorAllocatedHandler.class)
        .set(DriverConfiguration.ON_CONTEXT_ACTIVE, TBDDriver.ActiveContextHandler.class)
        .set(DriverConfiguration.ON_TASK_RUNNING, TBDDriver.RunningTaskHandler.class)
        .build();
  }

  /**
   * Main method that launches the REEF job.
   *
   * @param args command line parameters.
   * @throws BindException      if configuration commandLineInjector fails.
   * @throws InjectionException if configuration commandLineInjector fails.
   */
  public static void main(final String[] args) throws BindException, InjectionException {
    LOG.log(Level.INFO, "Client started");
    final LauncherStatus status = DriverLauncher
        .getLauncher(YarnClientConfiguration.CONF.build())
        .run(getDriverConfiguration(), JOB_TIMEOUT);
    LOG.log(Level.INFO, "REEF job completed: "+status);
  }
}
