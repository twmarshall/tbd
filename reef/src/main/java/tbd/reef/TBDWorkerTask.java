package tbd.reef;

import com.microsoft.tang.annotations.Parameter;
import com.microsoft.reef.task.Task;

import java.util.logging.Level;
import java.util.logging.Logger;

import javax.inject.Inject;

import tbd.worker.Main;

/**
 * A worker node.
 */
public final class TBDWorkerTask implements Task {
  private static final Logger LOG = Logger.getLogger(TBDWorkerTask.class.getName());
  private final String hostIP;
  private final String hostPort;
  private final String masterAkka;

  @Inject
  TBDWorkerTask(@Parameter(TBDDriver.HostIP.class) final String ip,
      @Parameter(TBDDriver.HostPort.class) final String port,
      @Parameter(TBDDriver.MasterAkka.class) final String master) {
    hostIP = ip;
    hostPort = port;
    masterAkka = master;
  }

  @Override
  public final byte[] call(final byte[] memento) {
    LOG.log(Level.INFO, "start worker");
    LOG.log(Level.INFO, "IP: {0}", hostIP);
    LOG.log(Level.INFO, "port: {0}", hostPort);
    LOG.log(Level.INFO, "master akka: {0}", masterAkka);

    try {
      Thread.sleep(10000);
    } catch (InterruptedException e) {
      LOG.log(Level.INFO, "worker sleep interrupted 1");
    }

    String[] args = new String[] {"-i", hostIP, "-p", hostPort, masterAkka};
    Main.main(args);

    LOG.log(Level.INFO, "worker sleep");
    try {
      Thread.sleep(400000);
    } catch (InterruptedException e) {
      LOG.log(Level.INFO, "worker sleep interrupted");
    }

    LOG.log(Level.INFO, "end worker");
    return null;
  }
}
