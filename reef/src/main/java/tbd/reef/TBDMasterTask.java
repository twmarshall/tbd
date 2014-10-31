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
import tbd.master.Main;
import tbd.master.Master;
import tbd.master.MasterConnector;
import tbd.worker.Worker;

/**
 * A 'hello REEF' Task.
 */
public final class TBDMasterTask implements Task {
  
  @Inject
  TBDMasterTask() {
  }

  @Override
  public final byte[] call(final byte[] memento) {
    System.out.println("start instatiation");
    
    //option 1
    String[] args = new String[] {};
    Main.main(args);
    
    try {
      this.wait();
    } catch (InterruptedException e) {
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
        withFallback(ConfigFactory.parseString("akka.remote.netty.tcp.port=2552"));
    
    ActorSystem system = ActorSystem.create("masterSystem1", ConfigFactory.load(remoteConfig));
    ActorRef masterRef = system.actorOf(Master.props(), "master");
    system.actorOf(Worker.props(masterRef), "worker");

    MasterConnector connector = new MasterConnector(masterRef, system);
    
    System.out.println("New master started at: akka.tcp://masterSystem1@127.0.0.1:2552/user/master");
    
    try {
      this.wait();
    } catch (InterruptedException e) {
      System.out.println("master wait exception");
    }
    */

    System.out.println("end instantiation");
    return null;
  }
}
