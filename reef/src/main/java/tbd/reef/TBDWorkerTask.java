package tbd.reef;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.event.Logging;
import akka.event.LoggingAdapter;

import org.apache.reef.task.Task;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import javax.inject.Inject;

import tbd.datastore.Datastore;
import tbd.master.Master;
import tbd.master.MasterConnector;
import tbd.worker.Main;
import tbd.worker.Worker;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A 'hello REEF' Task.
 */
public final class TBDWorkerTask implements Task {
  
  @Inject
  TBDWorkerTask() {
  }

  @Override
  public final byte[] call(final byte[] memento) {
    System.out.println("start instatiation");

    //option 1
    String[] args = new String[] {"akka.tcp://masterSystem1@127.0.0.1:2552/user/master"};
    Main.main(args);
    
    try {
      this.wait();
    } catch (InterruptedException e) {
      // TODO Auto-generated catch block
      System.out.println("master wait exception");
    }
    
    //option 2
    /*
    final StringBuilder sb = new StringBuilder();
    try {
      // Execute the command
      final Process proc = Runtime.getRuntime().exec("");
      try (final BufferedReader input = new BufferedReader(new InputStreamReader(proc.getInputStream()))) {
        String line;
        while ((line = input.readLine()) != null) {
          sb.append(line).append('\n');
        }
      }
    } catch (IOException ex) {
      sb.append(ex);
    }
    */
    
    //option 3
    /*
    Config remoteConfig = ConfigFactory.parseString("akka.actor.provider=\"akka.remote.RemoteActorRefProvider\"").
        withFallback(ConfigFactory.parseString("akka.remote.enabled-transports=[\"akka.remote.netty.tcp\"]")).
        withFallback(ConfigFactory.parseString("akka.loglevel =\"WARNING\"")).
        withFallback(ConfigFactory.parseString("akka.remote.netty.tcp.hostname=\"127.0.0.1\"")).
        withFallback(ConfigFactory.parseString("akka.remote.netty.tcp.port=2553"));
    
    ActorSystem system = ActorSystem.create("workerSystem", ConfigFactory.load(remoteConfig));
    //ActorSelection selection = system.actorSelection("akka.tcp://masterSystem1@127.0.0.1:2552/user/master");
    
    try {
      this.wait();
    } catch (InterruptedException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    */
      
    System.out.println("end instantiation");
    return null;
  }
}
