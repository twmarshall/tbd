package tbd.reef;

import com.microsoft.tang.annotations.Parameter;
import com.microsoft.reef.task.Task;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.inject.Inject;

/**
 * A master node.
 */
public final class TBDMasterTask implements Task {
  private static final Logger LOG = Logger.getLogger(TBDMasterTask.class.getName());
  private final int timeout;
  private final String masterIP;
  private final String masterPort;

  @Inject
  TBDMasterTask(@Parameter(TBDDriver.HostIP.class) final String ip, 
      @Parameter(TBDDriver.HostPort.class) final String port,
      @Parameter(TBDLaunch.Timeout.class) final int tout) {
    masterIP = ip;
    masterPort = port;
    timeout = tout;
  }

  @Override
  public final byte[] call(final byte[] memento) {
    LOG.log(Level.INFO, "start master");
    LOG.log(Level.INFO, "master IP: {0}", masterIP);
    LOG.log(Level.INFO, "master port: {0}", masterPort);

    String cp = TBDMasterTask.class.getProtectionDomain().getCodeSource().getLocation().getFile();
    LOG.log(Level.INFO, "cp: {0}", cp);
    
    /*
    String[] args = new String[] {"-i", masterIP, "-p", masterPort};
    Main.main(args);

    LOG.log(Level.INFO, "master sleep");
    try {
      Thread.sleep(400000);
    } catch (InterruptedException e) {
      LOG.log(Level.INFO, "master sleep interrupted");
    }
    */

    ProcessBuilder pb = new ProcessBuilder("java", "-Xss4m", "-cp", cp, "tbd.master.Main", "-i", masterIP, "-p", masterPort);
    //ProcessBuilder pb = new ProcessBuilder("java", "-Xmx2g", "-Xss4m", "-cp", cp, "tbd.master.Main", "-i", masterIP, "-p", masterPort);
    //ProcessBuilder pb = new ProcessBuilder("java","-cp", cp, "tbd.reef.Test");
    LOG.log(Level.INFO, "pb");
    
    pb.redirectErrorStream(true);
    pb.inheritIO();
    pb.redirectErrorStream(true);
    //pb.redirectErrorStream(true);
    //pb.redirectOutput(new File("/tmp/masterOut.log"));
    //pb.redirectError(new File("/tmp/masterError.log"));
    //LOG.log(Level.INFO, "redirect");
    Process p = null;
    try {
      //pb.redirectErrorStream(true); // merge stdout, stderr of process
      LOG.log(Level.INFO, "before start");
      p = pb.start();
      LOG.log(Level.INFO, "after start");
      /*
      InputStreamReader isr = new  InputStreamReader(p.getInputStream());
      BufferedReader br = new BufferedReader(isr);
      LOG.log(Level.INFO, "start");
      String lineRead;
      while ((lineRead = br.readLine()) != null) {
        LOG.log(Level.INFO, "redirect: {0}", lineRead);
      }
      LOG.log(Level.INFO, "done");
      int rc = p.waitFor();
      LOG.log(Level.INFO, "return code: {0}", rc);
      // TODO error handling for non-zero rc
      */
    }
    catch (IOException e) {
      LOG.log(Level.INFO, "master process IO exception");
    }
    /*
    catch (InterruptedException ie) {
      LOG.log(Level.INFO, "master process interrupted");
    }
    */
    
    LOG.log(Level.INFO, "master sleep");
    try {
      Thread.sleep(timeout);
    } catch (InterruptedException e) {
      LOG.log(Level.INFO, "master sleep interrupted");
    }
    
    p.destroy();
    

    LOG.log(Level.INFO, "end master");
    return null;
  }
}
