package tbd.reef;

import org.apache.reef.client.DriverConfiguration;
import org.apache.reef.client.DriverLauncher;
import org.apache.reef.client.LauncherStatus;
import org.apache.reef.runtime.yarn.client.YarnClientConfiguration;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.exceptions.BindException;
import org.apache.reef.tang.exceptions.InjectionException;

import java.util.logging.Level;
import java.util.logging.Logger;

public final class TBDReefYarn {

  private static final Logger LOG = Logger.getLogger(TBDReefYarn.class.getName());
  private static final int JOB_TIMEOUT = 120000; // 30 sec.

  /**
   * @return the configuration of the REEF driver.
   */
  private static Configuration getDriverConfiguration() {
    LOG.log(Level.INFO, "DriverConfig TBDReefYarn.class.getProtectionDomain(): {0}", 
        TBDReefYarn.class.getProtectionDomain());
    LOG.log(Level.INFO, "DriverConfig TBDReefYarn.class.getProtectionDomain().getCodeSource(): {0}", 
        TBDReefYarn.class.getProtectionDomain().getCodeSource());
    LOG.log(Level.INFO, "DriverConfig TBDReefYarn.class.getProtectionDomain().getCodeSource().getLocation(): {0}", 
        TBDReefYarn.class.getProtectionDomain().getCodeSource().getLocation());
    LOG.log(Level.INFO, "DriverConfig TBDReefYarn.class.getProtectionDomain().getCodeSource().getLocation().getFile(): {0}",
        TBDReefYarn.class.getProtectionDomain().getCodeSource().getLocation().getFile());
    
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
