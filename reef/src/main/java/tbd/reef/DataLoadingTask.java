package tbd.reef;

import com.microsoft.reef.annotations.audience.TaskSide;
import com.microsoft.reef.io.data.loading.api.DataSet;
import com.microsoft.reef.io.network.util.Pair;
import com.microsoft.reef.task.Task;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import javax.inject.Inject;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;

@TaskSide
public class DataLoadingTask implements Task {

  private static final Logger LOG = Logger.getLogger(DataLoadingTask.class.getName());

  private final DataSet<LongWritable, Text> dataSet;

  @Inject
  public DataLoadingTask(final DataSet<LongWritable, Text> dataSet) {
    this.dataSet = dataSet;
  }

  @Override
  public byte[] call(final byte[] memento) throws Exception {
    LOG.log(Level.INFO, "task started");
    int numEx = 0;
    Random rand = new Random();
    File fout = new File("/tmp/reef"+rand.nextFloat());
    FileOutputStream fos = new FileOutputStream(fout);
    BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos));
    for (final Pair<LongWritable, Text> keyValue : dataSet) {
      bw.write(keyValue.toString());
      bw.newLine();
    }
    bw.close();
    
    String cp = DataLoadingTask.class.getProtectionDomain().getCodeSource().getLocation().getFile();
    LOG.log(Level.INFO, "cp: {0}", cp);
    
    ProcessBuilder pb = new ProcessBuilder("java", "-Xss4m", "-cp", cp, "tbd.worker.Main", "-i", hostIP, "-p", hostPort, masterAkka);
    //ProcessBuilder pb = new ProcessBuilder("java", "-Xmx2g", "-Xss4m", "-cp", cp, "tbd.worker.Main", "-i", hostIP, "-p", hostPort, masterAkka);
    LOG.log(Level.INFO, "pb");
    
    pb.redirectErrorStream(true);
    pb.inheritIO();
    pb.redirectErrorStream(true);
    
    Process p = null;
    try {
      LOG.log(Level.INFO, "before start");
      p = pb.start();
      LOG.log(Level.INFO, "after start");
    } catch (IOException e1) {
      LOG.log(Level.INFO, "worker start failed");
    }
    
    LOG.log(Level.INFO, "worker sleep");
    try {
      Thread.sleep(timeout);
    } catch (InterruptedException e) {
      LOG.log(Level.INFO, "worker sleep interrupted");
    }
    
    p.destroy();
    LOG.log(Level.INFO, "task finished: read {0} lines", numEx);
    return Integer.toString(numEx).getBytes();
  }
}