/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.readreplicatopology

import akka.actor.Terminated
import akka.testkit.TestProbe
import com.neo4j.causalclustering.discovery.akka.BaseAkkaIT
import com.neo4j.causalclustering.discovery.akka.readreplicatopology.ClusterClientManager.ClusterClientFactory

import scala.concurrent.duration.DurationInt

class ClusterClientManagerIT extends BaseAkkaIT("ClusterClientManagerIT") {
  "ClusterClientManager" when {

    "receiving a message" should {

      "forward it to its child client" in {
        Given("A client manager and factory")
        val clusterClient = TestProbe("clusterClient")
        val manager = system.actorOf(ClusterClientManager.props(_ => clusterClient.ref))

        When("Sending a message to the manager")
        manager ! "Hi"

        Then("It should be received by the client")
        clusterClient.expectMsg(10.seconds, "Hi")
      }
    }

    "receiving a message from its child client" should {

      "not forward it back" in {
        Given("A client manager and factory")
        val clusterClient = TestProbe("clusterClient")
        val manager = system.actorOf(ClusterClientManager.props(_ => clusterClient.ref))

        When("Sending a message to the manager from the client")
        clusterClient.send(manager, "Hi")

        Then("The client should receive nothing")
        clusterClient.expectNoMessage()
      }
    }

    "receiving a terminate message" should {

      "recreate the cluster client" in {
        Given("A client manager with a monitoring factory")
        val clusterClient = TestProbe("clusterClient")
        val createMonitor = TestProbe("createMonitor")
        val message = "created"
        val monitoringFactory: ClusterClientFactory = _ => {
          createMonitor.ref ! message
          clusterClient.ref
        }
        val manager = system.actorOf(ClusterClientManager.props(monitoringFactory))

        When("Sending terminate message to manager")
        manager ! Terminated(clusterClient.ref)(existenceConfirmed = true, addressTerminated = true)

        Then("The client should be recreated")
        createMonitor.expectMsg(10.seconds, message)
      }
    }
  }
}
