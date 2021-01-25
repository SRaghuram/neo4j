/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.coretopology

import java.util.concurrent.atomic.AtomicInteger

import akka.actor.ActorRef
import akka.actor.ExtendedActorSystem
import akka.cluster.Cluster
import akka.cluster.ClusterEvent.CurrentClusterState
import akka.cluster.Member
import akka.cluster.MemberStatus
import akka.event.EventStream
import com.neo4j.causalclustering.discovery.akka.BaseAkkaIT
import com.neo4j.causalclustering.discovery.akka.system.JoinMessageSpy
import com.neo4j.configuration.CausalClusteringInternalSettings
import org.mockito.ArgumentMatchers
import org.mockito.Mockito
import org.mockito.invocation.InvocationOnMock
import org.neo4j.configuration.Config
import org.neo4j.logging.NullLogProvider

import scala.collection.JavaConverters.asScalaSetConverter
import scala.collection.immutable
import scala.collection.immutable.SortedSet

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
      val message = messageWithMostUnreachable

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
    "down unreachable members to allow a new member to join" in new Fixture {
      Given("Three members, two of which are unreachable")
      val message = messageWithMostUnreachable
      setClusterStateFromMessage(message)

      When("notified of InitJoin request")
      actorRef ! JoinMessageSpy.InitJoinRequestObserved.dummy()

      Then("down all unreachable members")
      awaitAssert(Mockito.verify(cluster).down(unreachable1.address), max = defaultWaitTime)
      awaitAssert(Mockito.verify(cluster).down(unreachable2.address), max = defaultWaitTime)
    }
  }

  trait Fixture {
    // Set up mocks
    private val eventStream = mock[EventStream]
    private val actorSystem = mock[ExtendedActorSystem]
    protected val cluster: Cluster = mock[Cluster]
    Mockito.when(cluster.system).thenReturn(actorSystem)
    Mockito.when(actorSystem.eventStream).thenReturn(eventStream)
    Mockito.when(eventStream.subscribe(ArgumentMatchers.any[ActorRef], ArgumentMatchers.eq(classOf[JoinMessageSpy.InitJoinRequestObserved])))
      .thenReturn(true)

    // Cannot mock this because it's a final class, so have to jump through some more hoops
    protected var clusterState: CurrentClusterState = _
    Mockito.when(cluster.state).thenAnswer((invocation: InvocationOnMock) => {
      if (cluster == null) {
        throw new Exception("You must set the expected cluster state for your test")
      }
      clusterState
    })

    val config = Config.newBuilder().build()
    config.set(CausalClusteringInternalSettings.middleware_akka_down_unreachable_on_new_joiner, Boolean.box(true));

    // create fixture objects
    protected val logProvider: NullLogProvider = NullLogProvider.getInstance()
    protected val actorRef: ActorRef = system.actorOf(ClusterDowningActor.props(cluster, config))

    var port = new AtomicInteger(0)
    val unreachable1, unreachable2, reachable1, reachable2 = ClusterViewMessageTest.createMember(port.getAndIncrement(), MemberStatus.Up)

    protected val messageWithMostReachable: ClusterViewMessage = ClusterViewMessage.EMPTY
      .withMember(reachable1)
      .withMember(reachable2)
      .withMember(unreachable1)
      .withUnreachable(unreachable1)

    protected val messageWithMostUnreachable: ClusterViewMessage = ClusterViewMessage.EMPTY
      .withMember(reachable1)
      .withMember(unreachable1)
      .withMember(unreachable2)
      .withUnreachable(unreachable1)
      .withUnreachable(unreachable2)

    protected def doNothing() = {
      awaitNotAssert(Mockito.verify(cluster, Mockito.atLeastOnce()).down(ArgumentMatchers.any()), defaultWaitTime, "Should not down")
    }

    def setClusterStateFromMessage(message: ClusterViewMessage) = {
      val members: SortedSet[Member] = immutable.SortedSet[Member]() ++ message.members().asScala
      val unreachable: SortedSet[Member] = immutable.SortedSet[Member]() ++ message.unreachable().asScala
      clusterState = CurrentClusterState.apply(members, unreachable, null, null, null)
    }
  }

}
