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
package tbd.reef.dataloading;

import com.microsoft.reef.annotations.audience.TaskSide;
import com.microsoft.reef.io.data.loading.api.DataSet;
import com.microsoft.reef.io.network.util.Pair;
import com.microsoft.reef.task.Task;
import com.microsoft.tang.annotations.Parameter;

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

  private static final Logger LOG =
      Logger.getLogger(DataLoadingTask.class.getName());

  private final DataSet<LongWritable, Text> dataSet;

  private Integer partitions;
  private Integer chunkSizes;
  private String masterAkka;
  private Integer timeout;
  private String ip;
  private String port;

  @Inject
  public DataLoadingTask(final DataSet<LongWritable, Text> dataSet,
      @Parameter(DataLoadingReefYarn.Partitions.class) final int partitions,
      @Parameter(DataLoadingReefYarn.ChunkSizes.class) final int chunkSizes,
      @Parameter(DataLoadingReefYarn.MasterAkka.class) final String akka,
      @Parameter(DataLoadingReefYarn.Timeout.class) final int timeout,
      @Parameter(DataLoadingDriver.HostIP.class) final String hostIp,
      @Parameter(DataLoadingDriver.HostPort.class) final String hostPort) {
    this.dataSet = dataSet;
    this.partitions = partitions;
    this.chunkSizes = chunkSizes;
    this.masterAkka = akka;
    this.timeout = timeout;
    this.ip = hostIp;
    this.port = hostPort;
  }

  @Override
  public byte[] call(final byte[] memento) throws Exception {
    LOG.log(Level.INFO, "task started");
    int numEx = 0;
    Random rand = new Random();
    String filename = "/tmp/reef_part_"+rand.nextFloat();
    LOG.log(Level.INFO, "file part: {0}", filename);
    File fout = new File(filename);
    FileOutputStream fos = new FileOutputStream(fout);
    BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos));
    for (final Pair<LongWritable, Text> keyValue : dataSet) {
      bw.write(keyValue.toString());
      bw.newLine();
      numEx++;
    }
    bw.close();

    String cp = DataLoadingTask.class.getProtectionDomain()
        .getCodeSource().getLocation().getFile();
    LOG.log(Level.INFO, "cp: {0}", cp);

    ProcessBuilder pb = new ProcessBuilder(
        "java",
        "-Xmx2g",
        "-Xss4m",
        "-cp", cp,
        "tbd.worker.Main",
        "-i", ip,
        "-p", port,
        "-d", filename,
        "-w", partitions.toString(),
        "-u", chunkSizes.toString(),
        masterAkka);
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
      Thread.sleep(timeout*60*1000);
    } catch (InterruptedException e) {
      LOG.log(Level.INFO, "worker sleep interrupted");
    }

    p.destroy();

    LOG.log(Level.INFO, "task finished: read {0} lines", numEx);
    return Integer.toString(numEx).getBytes();
  }
}