package tbd.reef;

import com.microsoft.tang.annotations.Parameter;
import com.microsoft.reef.task.Task;

import java.util.logging.Level;
import java.util.logging.Logger;

import javax.inject.Inject;

import tbd.master.Main;

/**
 * A master node.
 */
public final class TBDMasterTask implements Task {
  private static final Logger LOG = Logger.getLogger(TBDMasterTask.class.getName());
  private final String masterIP;
  private final String masterPort;

  @Inject
  TBDMasterTask(@Parameter(TBDDriver.HostIP.class) final String ip, @Parameter(TBDDriver.HostPort.class) final String port) {
    masterIP = ip;
    masterPort = port;
  }

  @Override
  public final byte[] call(final byte[] memento) {
    LOG.log(Level.INFO, "start master");
    LOG.log(Level.INFO, "master IP: {0}", masterIP);
    LOG.log(Level.INFO, "master port: {0}", masterPort);

    String[] args = new String[] {"-i", masterIP, "-p", masterPort};
    Main.main(args);

    LOG.log(Level.INFO, "master sleep");
    try {
      Thread.sleep(400000);
    } catch (InterruptedException e) {
      LOG.log(Level.INFO, "master sleep interrupted");
    }

    LOG.log(Level.INFO, "end master");
    return null;
  }
}
