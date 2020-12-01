/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.coretopology

import java.util.concurrent.CompletableFuture
import java.util.concurrent.Future

import akka.cluster.Cluster
import akka.cluster.ClusterEvent.ClusterShuttingDown
import akka.event.EventStream
import akka.remote.ThisActorSystemQuarantinedEvent
import com.neo4j.causalclustering.discovery.akka.AkkaActorSystemRestartStrategy.NeverRestart
import com.neo4j.causalclustering.discovery.akka.BaseAkkaIT
import com.neo4j.causalclustering.discovery.akka.Restartable
import org.mockito.Mockito.verify

class RestartNeededListeningActorIT extends BaseAkkaIT("Quarantine") {
  "RestartNeededListeningActor" when {
    "listening for quarantine" should {
      "subscribe on startup" in new Fixture {
        When("start")
        Then("subscribe")
        awaitAssert(verify(eventStream).subscribe(actorRef, classOf[ThisActorSystemQuarantinedEvent]), max = defaultWaitTime)
      }
      "respond at least once to event" in new Fixture {
        When("quarantine")
        actorRef ! quarantine

        Then("restart at least once")
        awaitCond(counter >= 1, max = defaultWaitTime)
      }
      "unsubscribe on event" in new Fixture {
        When("quarantine")
        actorRef ! quarantine

        Then("unsubscribe")
        awaitAssert(verify(eventStream).unsubscribe(actorRef, classOf[ThisActorSystemQuarantinedEvent]), max = defaultWaitTime)
      }
    }
    "listening for cluster shutdown" should {
      "subscribe on startup" ignore new Fixture {
        When("start")
        Then("subscribe")
        // Won't pass because of scala/java varargs interop
        awaitAssert(verify(cluster).subscribe(actorRef, ClusterShuttingDown.getClass: Class[_]), max = defaultWaitTime)
      }
      "respond at least once to event" in new Fixture {
        When("quarantine")
        actorRef ! ClusterShuttingDown

        Then("restart at least once")
        awaitCond(counter >= 1, max = defaultWaitTime)
      }
      "unsubscribe on event" in new Fixture {
        When("quarantine")
        actorRef ! ClusterShuttingDown

        Then("unsubscribe")
        awaitAssert(verify(cluster).unsubscribe(actorRef, ClusterShuttingDown.getClass), max = defaultWaitTime)
      }
    }
    "listening for any" should {
      "respond at most once to multiple events" in new Fixture {
        When("many quarantine")
        actorRef ! quarantine
        actorRef ! ClusterShuttingDown
        actorRef ! quarantine
        actorRef ! ClusterShuttingDown
        actorRef ! quarantine

        Then("restart at most once")
        awaitNotCond(counter > 1, max = defaultWaitTime, "Should not have restarted multiple times")
      }
    }
  }

  trait Fixture {
    val eventStream = mock[EventStream]
    val cluster = mock[Cluster]

    var counter = 0
    val restart = new Restartable {
      override def scheduleRestart(): Future[Boolean] = {
        counter = counter + 1
        return CompletableFuture.completedFuture(true)
      }
    }
    val quarantine = ThisActorSystemQuarantinedEvent(null, null)

    val actorRef = system.actorOf(RestartNeededListeningActor.props(restart, eventStream, cluster, new NeverRestart()))
  }
}
