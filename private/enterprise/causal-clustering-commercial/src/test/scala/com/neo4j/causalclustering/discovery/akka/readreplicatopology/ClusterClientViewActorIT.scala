/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.readreplicatopology

import akka.cluster.client._
import akka.testkit.TestProbe
import com.neo4j.causalclustering.discovery.akka.BaseAkkaIT
import org.neo4j.logging.NullLogProvider

import scala.collection.JavaConverters._

class ClusterClientViewActorIT extends BaseAkkaIT("LocallyConnectedReadReplicas") {
  "ClusterClientViewActor" when {
    "starting" should {
      "subscribe to receptionist" in new Fixture {
        Then("subscribe")
        receptionist.expectMsg(defaultWaitTime, SubscribeClusterClients)
      }
    }
    "stopping" should {
      "unsubscribe from receptionist" in new Fixture {
        Given("subscribe")
        receptionist.expectMsg(defaultWaitTime, SubscribeClusterClients)

        When("stop")
        system.stop(actorRef)

        Then("unsubscribe")
        receptionist.expectMsg(defaultWaitTime, UnsubscribeClusterClients)
      }
    }
    "running" should {
      "return view with initial cluster clients" in new Fixture {
        When("message with 2 clients")
        actorRef ! ClusterClients(bothClients)

        Then("receive view with both clients")
        parent.expectMsg(defaultWaitTime, new ClusterClientViewMessage(bothClients.asJava))
      }
      "return view with up cluster clients" in new Fixture {
        When("first client reachable")
        actorRef ! ClusterClientUp(client1)

        Then("receive view with first client")
        parent.expectMsg(defaultWaitTime, new ClusterClientViewMessage(Set(client1).asJava))

        When("second client reachable")
        actorRef ! ClusterClientUp(client2)

        Then("receive view with both clients")
        parent.expectMsg(defaultWaitTime, new ClusterClientViewMessage(bothClients.asJava))
      }
      "return view without unreachable cluster clients" in new Fixture {
        When("first client reachable")
        actorRef ! ClusterClientUp(client1)

        Then("receive view with first client")
        parent.expectMsg(defaultWaitTime, new ClusterClientViewMessage(Set(client1).asJava))

        When("second client reachable")
        actorRef ! ClusterClientUp(client2)

        Then("receive view with both clients")
        parent.expectMsg(defaultWaitTime, new ClusterClientViewMessage(bothClients.asJava))

        When("first client unreachable")
        actorRef ! ClusterClientUnreachable(client1)

        Then("receive view without first client")
        parent.expectMsg(defaultWaitTime, new ClusterClientViewMessage(Set(client2).asJava))
      }
    }
  }
  
  trait Fixture {
    val parent = TestProbe()
    val receptionist = TestProbe()

    val client1, client2 = TestProbe().ref
    val bothClients = Set(client1, client2)

    val props = ClusterClientViewActor.props(parent.ref, receptionist.ref, NullLogProvider.getInstance())
    val actorRef = system.actorOf(props)
  }
}
