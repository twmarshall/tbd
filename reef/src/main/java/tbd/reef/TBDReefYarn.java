package tbd.reef;

import org.apache.reef.client.DriverConfiguration;
import org.apache.reef.client.DriverLauncher;
import org.apache.reef.client.LauncherStatus;
import org.apache.reef.runtime.yarn.client.YarnClientConfiguration;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.exceptions.BindException;
import org.apache.reef.tang.exceptions.InjectionException;

public final class TBDReefYarn {

  private static final int JOB_TIMEOUT = 30000; // 30 sec.

  /**
   * @return the configuration of the REEF driver.
   */
  private static Configuration getDriverConfiguration() {
    System.out.println();
    System.out.println("DriverConfig TBDReefYarn.class.getProtectionDomain(): "+TBDReefYarn.class.getProtectionDomain());
    System.out.println("DriverConfig TBDReefYarn.class.getProtectionDomain().getCodeSource(): "
        +TBDReefYarn.class.getProtectionDomain().getCodeSource());
    System.out.println("DriverConfig TBDReefYarn.class.getProtectionDomain().getCodeSource().getLocation(): "
        +TBDReefYarn.class.getProtectionDomain().getCodeSource().getLocation());
    System.out.println("DriverConfig TBDReefYarn.class.getProtectionDomain().getCodeSource().getLocation().getFile(): "
        +TBDReefYarn.class.getProtectionDomain().getCodeSource().getLocation().getFile());
    System.out.println();
    
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
    
    System.out.println("Client started");
    final LauncherStatus status = DriverLauncher
        .getLauncher(YarnClientConfiguration.CONF.build())
        .run(getDriverConfiguration(), JOB_TIMEOUT);
    System.out.println("REEF job completed: "+status);
  }
}
