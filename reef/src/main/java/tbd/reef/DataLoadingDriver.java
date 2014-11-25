package tbd.reef;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.inject.Inject;

import com.microsoft.reef.annotations.audience.DriverSide;
import com.microsoft.reef.driver.context.ActiveContext;
import com.microsoft.reef.driver.context.ContextConfiguration;
import com.microsoft.reef.driver.task.CompletedTask;
import com.microsoft.reef.driver.task.TaskConfiguration;
import com.microsoft.reef.io.data.loading.api.DataLoadingService;
import com.microsoft.reef.poison.PoisonedConfiguration;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.Tang;
import com.microsoft.tang.annotations.Unit;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.wake.EventHandler;
import com.microsoft.wake.time.event.StartTime;

@DriverSide
@Unit
public class DataLoadingDriver {

  private static final Logger LOG = Logger.getLogger(DataLoadingDriver.class.getName());

  private final AtomicInteger ctrlCtxIds = new AtomicInteger();
  private final AtomicInteger lineCnt = new AtomicInteger();
  private final AtomicInteger completedDataTasks = new AtomicInteger();

  private final DataLoadingService dataLoadingService;

  private boolean firstTask = true;
  
  Map<String, ActiveContext> contexts = new HashMap<String, ActiveContext>();

  @Inject
  public DataLoadingDriver(final DataLoadingService dataLoadingService) {
    this.dataLoadingService = dataLoadingService;
    this.completedDataTasks.set(dataLoadingService.getNumberOfPartitions());
  }

  /*
  public class DriverStartedHandler implements EventHandler<StartTime> {

    @Override
    public void onNext(final StartTime startTime) {
      String cp = DataLoadingDriver.class.getProtectionDomain().getCodeSource().getLocation().getFile();
      ProcessBuilder pb = new ProcessBuilder("java", "-Xss4m", "-cp", cp, "tbd.master.Main", "-i", "127.0.0.1", "-p", "2555");
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
      LOG.log(Level.INFO, "master sleep");
      try {
        Thread.sleep(10*60*1000);
      } catch (InterruptedException e) {
        LOG.log(Level.INFO, "master sleep interrupted");
      }
      p.destroy();
    }
  }
  */

  public class ContextActiveHandler implements EventHandler<ActiveContext> {

    @Override
    public void onNext(final ActiveContext activeContext) {

      final String contextId = activeContext.getId();
      LOG.log(Level.INFO, "Context active: {0}", contextId);

      if (dataLoadingService.isDataLoadedContext(activeContext) && !contexts.keySet().contains(contextId)) {
        contexts.put(contextId, activeContext);
        
        final String taskId = "LineCountTask-" + ctrlCtxIds.getAndIncrement();
        LOG.log(Level.INFO, "Submit LineCount task {0} to: {1}", new Object[] { taskId, contextId });

        try {
          if (firstTask) {
            firstTask = false;
          activeContext.submitTask(TaskConfiguration.CONF
              .set(TaskConfiguration.IDENTIFIER, taskId)
              .set(TaskConfiguration.TASK, DataLoadingTask.class)
              .build());
          
          
          } else {
            activeContext.submitTask(TaskConfiguration.CONF
                .set(TaskConfiguration.IDENTIFIER, taskId)
                .set(TaskConfiguration.TASK, DataLoadingTask2.class)
                .build());
          }
          
        } catch (final BindException ex) {
          LOG.log(Level.INFO, "Configuration error in " + contextId, ex);
          throw new RuntimeException("Configuration error in " + contextId, ex);
        }
        /*
        final String lcContextId = "LineCountCtxt-" + ctrlCtxIds.getAndIncrement();
        LOG.log(Level.INFO, "Submit LineCount context {0} to: {1}",
            new Object[] { lcContextId, contextId });

        final Configuration poisonedConfiguration = PoisonedConfiguration.CONTEXT_CONF
            .set(PoisonedConfiguration.CRASH_PROBABILITY, "0.4")
            .set(PoisonedConfiguration.CRASH_TIMEOUT, "1")
            .build();

        activeContext.submitContext(Tang.Factory.getTang()
            .newConfigurationBuilder(poisonedConfiguration,
                ContextConfiguration.CONF.set(ContextConfiguration.IDENTIFIER, lcContextId).build())
            .build());
        */
        

      } else if (activeContext.getId().startsWith("LineCountCtxt")) {

        final String taskId = "LineCountTask-" + ctrlCtxIds.getAndIncrement();
        LOG.log(Level.INFO, "Submit LineCount task {0} to: {1}", new Object[] { taskId, contextId });

        try {
          //if (firstTask) {
          activeContext.submitTask(TaskConfiguration.CONF
              .set(TaskConfiguration.IDENTIFIER, taskId)
              .set(TaskConfiguration.TASK, DataLoadingTask.class)
              .build());
          firstTask = false;
          /*
          } else {
            activeContext.submitTask(TaskConfiguration.CONF
                .set(TaskConfiguration.IDENTIFIER, taskId)
                .set(TaskConfiguration.TASK, DataLoadingTask2.class)
                .build());
          }
          */
        } catch (final BindException ex) {
          LOG.log(Level.INFO, "Configuration error in " + contextId, ex);
          throw new RuntimeException("Configuration error in " + contextId, ex);
        }
      } else {
        LOG.log(Level.INFO, "Unrecognized context: {0}", contextId);
        //activeContext.close();
      }
    }
  }

  public class TaskCompletedHandler implements EventHandler<CompletedTask> {
    @Override
    public void onNext(final CompletedTask completedTask) {

      final String taskId = completedTask.getId();
      LOG.log(Level.INFO, "Completed Task: {0}", taskId);

      final byte[] retBytes = completedTask.get();
      final String retStr = retBytes == null ? "No RetVal": new String(retBytes);
      LOG.log(Level.INFO, "Line count from {0} : {1}", new String[] { taskId, retStr });

      lineCnt.addAndGet(Integer.parseInt(retStr));

      if (completedDataTasks.decrementAndGet() <= 0) {
        LOG.log(Level.INFO, "Total line count: {0}", lineCnt.get());
      }

      LOG.log(Level.INFO, "Releasing Context: {0}", taskId);
      completedTask.getActiveContext().close();
    }
  }
}