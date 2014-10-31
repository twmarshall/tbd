package tbd.reef;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.reef.driver.task.CompletedTask;
import org.apache.reef.driver.task.RunningTask;

import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.driver.context.ContextConfiguration;
import org.apache.reef.driver.evaluator.AllocatedEvaluator;
import org.apache.reef.driver.evaluator.EvaluatorRequest;
import org.apache.reef.driver.evaluator.EvaluatorRequestor;
import org.apache.reef.driver.task.TaskConfiguration;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.time.event.Alarm;
import org.apache.reef.wake.time.Clock;
import org.apache.reef.wake.time.event.StartTime;
import org.apache.reef.wake.time.event.StopTime;

import javax.inject.Inject;

@Unit
public final class TBDDriver {

  private final EvaluatorRequestor requestor;

  private final int numEvaluators=2;
  private final int numWorkers=numEvaluators-1;

  private int numEvalAlloced = 0;
  private int numWorkerContexts = 0;
  private boolean masterEvalAlloced = false;
  private boolean masterSubmitted = false;
  
  private String masterIP;
  
  private Map<String, ActiveContext> contexts = new HashMap<String, ActiveContext>();
  private Map<String, String> ctxt2akka = new HashMap<String, String>();

  /**
   * Job driver constructor - instantiated via TANG.
   *
   * @param requestor evaluator requestor object used to create new evaluator containers.
   */
  @Inject
  public TBDDriver(final EvaluatorRequestor requestor) {
    this.requestor = requestor;
    System.out.println("Instantiated 'TBDDriver'");
  }

  /**
   * Handles the StartTime event: Request Evaluators.
   */
  public final class StartHandler implements EventHandler<StartTime> {
    @Override
    public void onNext(final StartTime startTime) {
      System.out.println("TIME: Start Driver.");
      
      
      
      TBDDriver.this.requestor.submit(EvaluatorRequest.newBuilder()
          .setNumber(2)
          .setMemory(64)
          .setNumberOfCores(1)
          .build());
      System.out.println("Requested Evaluators.");
    }
  }

  /**
   * Handles AllocatedEvaluator: Submit the Task
   */
  public final class EvaluatorAllocatedHandler implements EventHandler<AllocatedEvaluator> {
    @Override
    public void onNext(final AllocatedEvaluator allocatedEvaluator) {
      System.out.println("TIME: Evaluator Allocated " + allocatedEvaluator.getId());
      System.out.println("Evaluator descriptor: " + allocatedEvaluator.getEvaluatorDescriptor());
      System.out.println("Node descriptor" + allocatedEvaluator.getEvaluatorDescriptor().getNodeDescriptor());
      System.out.println("Socket address" + allocatedEvaluator.getEvaluatorDescriptor().getNodeDescriptor().getInetSocketAddress());
      System.out.println("Host name" + allocatedEvaluator.getEvaluatorDescriptor().getNodeDescriptor().getInetSocketAddress().getHostName());
    	
      final int nEval;
      final boolean masterEval;
      final boolean workerEval;
      final boolean legalEval;

      synchronized (TBDDriver.this) {
        masterEval = !masterEvalAlloced;
        workerEval = masterEvalAlloced && (numEvalAlloced < numEvaluators);
        legalEval = masterEval || workerEval;
        if (masterEval) {
          ++numEvalAlloced;
          masterEvalAlloced = true;
        } else if (workerEval) {
          ++numEvalAlloced;
        }
        nEval = numEvalAlloced;
      }

      if (legalEval) {
        String contextId = "";
        if (masterEval) {
          contextId = String.format("context_master_%06d", 0);
        } else if (workerEval) {
          contextId = String.format("context_worker_%06d", nEval);
        }
        final JavaConfigurationBuilder contextConfigBuilder = Tang.Factory.getTang().newConfigurationBuilder();
        contextConfigBuilder.addConfiguration(ContextConfiguration.CONF
            .set(ContextConfiguration.IDENTIFIER, contextId)
            .build());
        allocatedEvaluator.submitContext(contextConfigBuilder.build());
        System.out.println("Submit context " + contextId + " to evaluator " + allocatedEvaluator.getId());
      } else {
        System.out.println("Close Evaluator " + allocatedEvaluator.getId());
        allocatedEvaluator.close();
      }
    }
  }
  
  /**
   * Receive notification that the Context is active.
   */
  public final class ActiveContextHandler implements EventHandler<ActiveContext> {
    @Override
    public void onNext(final ActiveContext context) {
      System.out.println("TIME: Active Context " + context.getId());
      
      final String contextId = context.getId();
      final String character = contextId.split("_")[1];
      final boolean masterCtxt = character.equals("master");
      final boolean workerCtxt = character.equals("worker");
      //final int knownWorkers;
      
      synchronized (TBDDriver.this) {
        if (masterCtxt) {
          masterSubmitted = true;
        } else if (workerCtxt){
          ++numWorkerContexts;
        }
      }

      if (masterCtxt) {
        contexts.put(contextId, context);
        final String taskId = String.format("task_master_%06d", 0);
        final Configuration taskConfiguration = TaskConfiguration.CONF
            .set(TaskConfiguration.IDENTIFIER, taskId)
            .set(TaskConfiguration.TASK, TBDMasterTask.class)
            .build();
        context.submitTask(taskConfiguration);
        System.out.println("Submit  " + taskId + " to context " + contextId);
      } else if (workerCtxt) {
        contexts.put(contextId, context);
        System.out.println("Context active: " + contextId);
      } else {
        System.out.println("Close context " + contextId.split("_")[1] + ": " + contextId);
        context.close();
      }
      
      /*
      if (masterSubmitted && knownWorkers == numWorkers) {
        
      }
      */
    }
  }
  
  /**
   * Receive notification that the Task is running.
   */
  public final class RunningTaskHandler implements EventHandler<RunningTask> {
    @Override
    public void onNext(final RunningTask task) {
      System.out.println("TIME: Running Task " + task.getId());
      
      final String contextId = task.getActiveContext().getId();
      final String character = contextId.split("_")[1];
      
      if (character.equals("worker")) {
        return;
      } else if (character.equals("master")) {
        waitAndSubmitWorkerTasks();
      }
    }
  }
  
  /*
  public final class TaskCompletedHandler implements EventHandler<CompletedTask> {
    @Override
    public void onNext(final CompletedTask completedTask) {

      System.out.println(completedTask.getActiveContext().getEvaluatorDescriptor());
      completedTask.getActiveContext().close();
    }
  }
  
  public final class StopHandler implements EventHandler<StopTime> {
    @Override
    public void onNext(final StopTime stopTime) {
      System.out.println("TIME: Stop Driver");
    }
  }
  */
  
  private void waitAndSubmitWorkerTasks() {
    if (numWorkerContexts == numWorkers) {
      for (Entry<String, ActiveContext> e : contexts.entrySet()) {
        String contextId = e.getKey();
        ActiveContext context = e.getValue();
        if (contextId.startsWith("context_worker")) {
          final String taskId = contextId.replaceFirst("context", "task");
          final Configuration taskConfiguration = TaskConfiguration.CONF
              .set(TaskConfiguration.IDENTIFIER, taskId)
              .set(TaskConfiguration.TASK, TBDWorkerTask.class)
              .build();
          context.submitTask(taskConfiguration);
          System.out.println("Submit  " + taskId + " to context " + contextId);
        }
      }
    } else {
      System.out.println("Sleep: wait worker contexts");
      /*
      Clock.scheduleAlarm(CHECK_UP_INTERVAL,
          new EventHandler<Alarm>() {
            @Override
            public void onNext(final Alarm time) {
              LOG.log(Level.INFO, "Alarm: {0}", time);
              schedule();
            }
          });
      */
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        e.printStackTrace();
        System.out.println("Sleep exception.");
      }
      waitAndSubmitWorkerTasks();
    }
  }
}
