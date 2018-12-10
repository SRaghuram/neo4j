/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.coretopology

import java.util.concurrent.atomic.AtomicInteger

import akka.cluster.{Cluster, MemberStatus}
import akka.testkit.TestProbe
import com.neo4j.causalclustering.discovery.akka.BaseAkkaIT
import org.mockito.{ArgumentMatchers, Mockito}
import org.neo4j.logging.NullLogProvider

class ClusterDowningActorIT extends BaseAkkaIT("ClusterDowningActor") {
  "ClusterDowningActor" should {
    "do nothing with empty topology view" in new Fixture {
      Given("empty cluster view")
      When("send message")
      actorRef ! ClusterViewMessage.EMPTY

      Then("do nothing")
      doNothing()
    }
    "do nothing with no unreachable members" in new Fixture {
      Given("one reachable member")
      val message = ClusterViewMessage.EMPTY
        .withMember(reachable1)

      When("send message")
      actorRef ! message

      Then("do nothing")
      doNothing()
    }
    "do nothing if fewer reachable than unreachable members" in new Fixture {
      Given("Three members, two of which are unreachable")
      val message = ClusterViewMessage.EMPTY
        .withMember(reachable1)
        .withMember(unreachable1)
        .withMember(unreachable2)
        .withUnreachable(unreachable1)
        .withUnreachable(unreachable2)

      When("send message")
      actorRef ! message

      Then("do nothing")
      doNothing()
    }
    "do nothing if equal reachable and unreachable members" in new Fixture {
      Given("Two members, one of which is unreachable")
      val unreachable = ClusterViewMessageTest.createMember(0, MemberStatus.Up)
      val message = ClusterViewMessage.EMPTY
        .withMember(ClusterViewMessageTest.createMember(1, MemberStatus.Up))
        .withMember(unreachable)
        .withUnreachable(unreachable)

      When("send message")
      actorRef ! message

      Then("do nothing")
      doNothing()
    }
    "down members if more reachable than unreachable" in new Fixture {
      Given("Three members, one of which is unreachable")
      When("receive message")
      actorRef ! messageWithMostReachable

      Then("down then unreachable member")
      awaitAssert(Mockito.verify(cluster).down(unreachable1.address), max = defaultWaitTime)
    }
    "send cleanup message to topology actor if more reachable than unreachabe" in new Fixture {
      Given("Three members, one of which is unreachable")
      When("send message")
      actorRef ! messageWithMostReachable

      Then("send cleanup message")
      metadataActor.expectMsg(defaultWaitTime, new CleanupMessage(unreachable1.uniqueAddress))
    }
  }

  trait Fixture {
    val cluster = mock[Cluster]
    val metadataActor = TestProbe("Metadata")
    val logProvider = NullLogProvider.getInstance()

    val actorRef = system.actorOf(ClusterDowningActor.props(cluster, metadataActor.ref, logProvider))

    var port = new AtomicInteger(0)
    val unreachable1, unreachable2, reachable1, reachable2 = ClusterViewMessageTest.createMember(port.getAndIncrement(), MemberStatus.Up)

    val messageWithMostReachable = ClusterViewMessage.EMPTY
      .withMember(reachable1)
      .withMember(reachable2)
      .withMember(unreachable1)
      .withUnreachable(unreachable1)
    
    def doNothing() = {
      awaitNotAssert(Mockito.verify(cluster, Mockito.atLeastOnce()).down(ArgumentMatchers.any()), defaultWaitTime, "Should not down")
      metadataActor.expectNoMessage(defaultWaitTime)
    }
  }
}
