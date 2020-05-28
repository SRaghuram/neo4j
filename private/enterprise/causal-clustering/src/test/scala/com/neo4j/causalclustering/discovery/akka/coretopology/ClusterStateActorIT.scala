/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.coretopology

import java.time.Duration
import java.util
import java.util.Collections
import java.util.concurrent.Callable
import java.util.concurrent.TimeUnit

import akka.actor.Address
import akka.cluster.Cluster
import akka.cluster.ClusterEvent
import akka.cluster.ClusterEvent.ClusterDomainEvent
import akka.cluster.ClusterEvent.UnreachableMember
import akka.cluster.Member
import akka.cluster.MemberStatus
import akka.testkit.TestProbe
import com.neo4j.causalclustering.discovery.akka.BaseAkkaIT
import com.neo4j.causalclustering.discovery.akka.coretopology.ClusterStateActor.ClusterMonitorRefresh
import com.neo4j.causalclustering.discovery.akka.coretopology.ClusterViewMessageTest.createMember
import com.neo4j.causalclustering.discovery.akka.monitoring.ClusterSizeMonitor
import com.neo4j.configuration.CausalClusteringInternalSettings.akka_failure_detector_acceptable_heartbeat_pause
import com.neo4j.configuration.CausalClusteringInternalSettings.akka_failure_detector_heartbeat_interval
import org.mockito.ArgumentMatchers
import org.mockito.Mockito.atLeastOnce
import org.mockito.Mockito.verify
import org.neo4j.configuration.Config
import org.neo4j.internal.helpers.collection.Iterators
import org.neo4j.test.assertion.Assert.assertEventually

import scala.collection.immutable.SortedSet
import scala.concurrent.duration.FiniteDuration

class ClusterStateActorIT extends BaseAkkaIT("ClusterStateActorTest") {

  "ClusterStateActor" when {
    "pre starting" should {
      "subscribe to cluster events" in new Fixture {
        val check = () =>
          verify(cluster)
            .subscribe(
              clusterStateRef,
              ClusterEvent.initialStateAsSnapshot,
              classOf[ClusterDomainEvent],
              classOf[UnreachableMember]
            )

        awaitAssert(check, defaultWaitTime)
      }
    }
    "post stopping" should {
      "unsubscribe from cluster events" in new Fixture {
        When("system stops")
        system.stop(clusterStateRef)

        Then("unsubscribe")
        awaitAssert(verify(cluster).unsubscribe(clusterStateRef), defaultWaitTime)
      }
      "not leave cluster because it won't rejoin if restarting" in new Fixture {
        When("system stops")
        system.stop(clusterStateRef)

        Then("do not leave cluster")
        awaitNotAssert(verify(cluster, atLeastOnce()).leave(ArgumentMatchers.any()), message = "Should not have left cluster")
      }
    }
    "receiving cluster messages" should {
      "send updated cluster view" when {
        val upMembers = Seq.tabulate(3)(port => createMember(port, MemberStatus.up))
        val upAsSet = SortedSet[Member]() ++ upMembers
        val upAsJavaSet =
        {
          val temp = new util.TreeSet[Member](Member.ordering)
          upMembers.foreach(temp.add)
          temp
        }

        "initial cluster event" in new Fixture {
          Given("Initial cluster event")
          val event = ClusterEvent.CurrentClusterState(members = upAsSet)

          When("event sent")
          clusterStateRef ! event

          Then("receive ClusterViewMessage")
          coreTopologyProbe.expectMsg(max = defaultWaitTime, new ClusterViewMessage(false, upAsJavaSet, Collections.emptySet()))
        }
        "member unreachable event" in new Fixture {
          Given("An initial actor state")
          val initialEvent = ClusterEvent.CurrentClusterState(members = upAsSet)
          clusterStateRef ! initialEvent
          coreTopologyProbe.expectMsg(max = defaultWaitTime, new ClusterViewMessage(false, upAsJavaSet, Collections.emptySet()))

          And("an unreachable member event")
          val unreachableEvent = ClusterEvent.UnreachableMember(upMembers.head)

          When("event sent")
          clusterStateRef ! unreachableEvent

          Then("receive ClusterViewMessage with unreachable member")
          coreTopologyProbe.expectMsg(max = defaultWaitTime, new ClusterViewMessage(false, upAsJavaSet, Iterators.asSet(upMembers.head)))
        }
        "member reachable event" in new Fixture {
          Given("An initial actor state with an unreachable member")
          val initialEvent = ClusterEvent.CurrentClusterState(members = upAsSet, unreachable = Set(upMembers.head))
          clusterStateRef ! initialEvent
          coreTopologyProbe.expectMsg(max = defaultWaitTime, new ClusterViewMessage(false, upAsJavaSet, Iterators.asSet(upMembers.head)))

          And("a reachable member event")
          val reachableEvent = ClusterEvent.ReachableMember(upMembers.head)

          When("event sent")
          clusterStateRef ! reachableEvent

          Then("receive ClusterViewMessage with unreachable member removed")
          coreTopologyProbe.expectMsg(max = defaultWaitTime, new ClusterViewMessage(false, upAsJavaSet, Collections.emptySet()))
        }
        "member up event" in new Fixture {
          Given("member up event")
          val member = createMember(99, MemberStatus.up)
          val event = ClusterEvent.MemberUp(member)

          When("event sent")
          clusterStateRef ! event

          Then("receive ClusterViewMessage with additional member")
          coreTopologyProbe.expectMsg(max = defaultWaitTime, new ClusterViewMessage(false, Iterators.asSortedSet(Member.ordering, member), Collections.emptySet()))
        }
        "member weakly up event" in new Fixture {
          Given("member up event")
          val member = createMember(99, MemberStatus.weaklyUp)
          val event = ClusterEvent.MemberWeaklyUp(member)

          When("event sent")
          clusterStateRef ! event

          Then("receive ClusterViewMessage with additional member")
          coreTopologyProbe.expectMsg(max = defaultWaitTime, new ClusterViewMessage(false, Iterators.asSortedSet(Member.ordering, member), Collections.emptySet()))
        }
        "member removed event" in new Fixture {
          Given("A member")
          val member = createMember(99, MemberStatus.up)

          And("an initial state with member")
          val initialEvent = ClusterEvent.MemberUp(member)
          clusterStateRef ! initialEvent
          coreTopologyProbe.expectMsg(max = defaultWaitTime, new ClusterViewMessage(false, Iterators.asSortedSet(Member.ordering, member), Collections.emptySet()))

          And("a remove event")
          val event = ClusterEvent.MemberRemoved(member.copy(MemberStatus.removed), MemberStatus.up)

          When("event sent")
          clusterStateRef ! event

          Then("receive a ClusterViewMessage without that member")
          coreTopologyProbe.expectMsg(max = defaultWaitTime, ClusterViewMessage.EMPTY)

          And("send cleanup to metadata actor")
          metadataPrope.expectMsg(max = defaultWaitTime, new CleanupMessage(member.uniqueAddress))
        }
        "leader changed event" in new Fixture {
          Given("A leader change event with a leader")
          val event = ClusterEvent.LeaderChanged(Some(Address("protocol", "system")))

          When("Event sent")
          clusterStateRef ! event

          Then("Receive ClusterViewMessage with converged set")
          coreTopologyProbe.expectMsg(max = defaultWaitTime, ClusterViewMessage.EMPTY.withConverged(true))
        }
        "send latest cluster view to downing actor after inactivity" in new Fixture {
          Given("An initial actor state with an unreachable member")
          val initialEvent = ClusterEvent.CurrentClusterState(members = upAsSet, unreachable = Set(upMembers.head))
          clusterStateRef ! initialEvent

          And("a reachable member event")
          val reachableEvent = ClusterEvent.ReachableMember(upMembers.head)
          clusterStateRef ! reachableEvent

          When("time passes")
          Then("receive ClusterViewMessage with unreachable member removed")
          val timePasses =
            config.get(akka_failure_detector_heartbeat_interval) plus config.get(akka_failure_detector_acceptable_heartbeat_pause) multipliedBy  2
          val toWait = FiniteDuration(defaultWaitTime.toMillis + timePasses.toMillis, TimeUnit.MILLISECONDS)
          downingProbe.expectMsg(max = toWait, new ClusterViewMessage(false, upAsJavaSet, Collections.emptySet()))
        }
      }
      "update monitor" when {
        "tick received" in new Fixture {
          clusterStateRef ! ClusterMonitorRefresh.INSTANCE

          assertEventually(monitor.membersSet, TRUE_CONDITION, defaultWaitTime.toMillis, TimeUnit.MILLISECONDS )
          assertEventually(monitor.unreachableSet, TRUE_CONDITION, defaultWaitTime.toMillis, TimeUnit.MILLISECONDS )
          assertEventually(monitor.convergedSet, TRUE_CONDITION, defaultWaitTime.toMillis, TimeUnit.MILLISECONDS )
        }
        "cluster event received" in new Fixture {
          clusterStateRef ! ClusterEvent.MemberUp(createMember(99, MemberStatus.up))

          assertEventually(monitor.membersSet, TRUE_CONDITION, defaultWaitTime.toMillis, TimeUnit.MILLISECONDS )
          assertEventually(monitor.unreachableSet, TRUE_CONDITION, defaultWaitTime.toMillis, TimeUnit.MILLISECONDS )
          assertEventually(monitor.convergedSet, TRUE_CONDITION, defaultWaitTime.toMillis, TimeUnit.MILLISECONDS )
        }
      }
    }

  }

  trait Fixture {
    val cluster = mock[Cluster]
    val coreTopologyProbe = TestProbe("CoreTopology")
    val downingProbe = TestProbe("Downing")
    val metadataPrope = TestProbe("Metadata")
    val config = Config.newBuilder()
      .set(akka_failure_detector_heartbeat_interval, Duration.ofSeconds( 1 ) )
      .set(akka_failure_detector_acceptable_heartbeat_pause, Duration.ofSeconds( 1 ) )
      .build()
    val monitor = new ClusterSizeMonitor {
      var hasSetMembers = false
      var hasSetUnreachable = false
      var hasSetConverged = false

      def membersSet = new Callable[Boolean] {
        override def call(): Boolean = hasSetMembers
      }
      def unreachableSet = new Callable[Boolean] {
        override def call(): Boolean = hasSetUnreachable
      }
      def convergedSet = new Callable[Boolean] {
        override def call(): Boolean = hasSetConverged
      }

      override def setMembers(size: Int): Unit = hasSetMembers = true

      override def setUnreachable(size: Int): Unit = hasSetUnreachable = true

      override def setConverged(converged: Boolean): Unit = hasSetConverged = true
    }
    val props = ClusterStateActor.props(cluster, coreTopologyProbe.ref, downingProbe.ref, metadataPrope.ref, config, monitor)
    val clusterStateRef = system.actorOf(props)
  }

}
