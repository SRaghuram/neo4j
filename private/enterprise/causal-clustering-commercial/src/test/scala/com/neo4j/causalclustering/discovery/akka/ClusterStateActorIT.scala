/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka

import java.util
import java.util.Collections

import akka.actor.Address
import akka.cluster.ClusterEvent.{ClusterDomainEvent, UnreachableMember}
import akka.cluster.{Cluster, ClusterEvent, Member, MemberStatus}
import akka.testkit.TestProbe
import org.mockito.ArgumentMatchers
import org.mockito.Mockito.verify
import org.neo4j.helpers.collection.Iterators
import org.neo4j.logging.NullLogProvider

import scala.collection.immutable.SortedSet

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
        awaitNotAssert(verify(cluster).leave(ArgumentMatchers.any()), message = "Should not have left cluster")
      }
    }
    "receiving cluster messages" should {
      "send updated cluster view" when {
        val upMembers = Seq.tabulate(3)(port => ClusterViewTest.createMember(port, MemberStatus.up))
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

          Then("receive ClusterView")
          coreTopologyProbe.expectMsg(max = defaultWaitTime, new ClusterView(false, upAsJavaSet, Collections.emptySet()))
        }
        "member unreachable event" in new Fixture {
          Given("An initial actor state")
          val initialEvent = ClusterEvent.CurrentClusterState(members = upAsSet)
          clusterStateRef ! initialEvent
          coreTopologyProbe.expectMsg(max = defaultWaitTime, new ClusterView(false, upAsJavaSet, Collections.emptySet()))

          And("an unreachable member event")
          val unreachableEvent = ClusterEvent.UnreachableMember(upMembers.head)

          When("event sent")
          clusterStateRef ! unreachableEvent

          Then("receive ClusterView with unreachable member")
          coreTopologyProbe.expectMsg(max = defaultWaitTime, new ClusterView(false, upAsJavaSet, Iterators.asSet(upMembers.head)))
        }
        "member reachable event" in new Fixture {
          Given("An initial actor state with an unreachable member")
          val initialEvent = ClusterEvent.CurrentClusterState(members = upAsSet, unreachable = Set(upMembers.head))
          clusterStateRef ! initialEvent
          coreTopologyProbe.expectMsg(max = defaultWaitTime, new ClusterView(false, upAsJavaSet, Iterators.asSet(upMembers.head)))

          And("a reachable member event")
          val reachableEvent = ClusterEvent.ReachableMember(upMembers.head)

          When("event sent")
          clusterStateRef ! reachableEvent

          Then("receive ClusterView with unreachable member removed")
          coreTopologyProbe.expectMsg(max = defaultWaitTime, new ClusterView(false, upAsJavaSet, Collections.emptySet()))
        }
        "member up event" in new Fixture {
          Given("member up event")
          val member = ClusterViewTest.createMember(99, MemberStatus.up)
          val event = ClusterEvent.MemberUp(member)

          When("event sent")
          clusterStateRef ! event

          Then("receive ClusterView with additional member")
          coreTopologyProbe.expectMsg(max = defaultWaitTime, new ClusterView(false, Iterators.asSortedSet(Member.ordering, member), Collections.emptySet()))
        }
        "member weakly up event" in new Fixture {
          Given("member up event")
          val member = ClusterViewTest.createMember(99, MemberStatus.weaklyUp)
          val event = ClusterEvent.MemberWeaklyUp(member)

          When("event sent")
          clusterStateRef ! event

          Then("receive ClusterView with additional member")
          coreTopologyProbe.expectMsg(max = defaultWaitTime, new ClusterView(false, Iterators.asSortedSet(Member.ordering, member), Collections.emptySet()))
        }
        "member removed event" in new Fixture {
          Given("A member")
          val member = ClusterViewTest.createMember(99, MemberStatus.up)

          And("an initial state with member")
          val initialEvent = ClusterEvent.MemberUp(member)
          clusterStateRef ! initialEvent
          coreTopologyProbe.expectMsg(max = defaultWaitTime, new ClusterView(false, Iterators.asSortedSet(Member.ordering, member), Collections.emptySet()))

          And("a remove event")
          val event = ClusterEvent.MemberRemoved(member.copy(MemberStatus.removed), MemberStatus.up)

          When("event sent")
          clusterStateRef ! event

          Then("receive a ClusterView without that member")
          coreTopologyProbe.expectMsg(max = defaultWaitTime, ClusterView.EMPTY)
        }
        "leader changed event" in new Fixture {
          Given("A leader change event with a leader")
          val event = ClusterEvent.LeaderChanged(Some(Address("protocol", "system")))

          When("Event sent")
          clusterStateRef ! event

          Then("Receive ClusterView with converged set")
          coreTopologyProbe.expectMsg(max = defaultWaitTime, ClusterView.EMPTY.withConverged(true))
        }
      }
    }

  }

  trait Fixture {
    val cluster = mock[Cluster]
    val coreTopologyProbe = TestProbe("CoreTopology")
    val clusterStateRef = system.actorOf(ClusterStateActor.props(cluster, coreTopologyProbe.ref, NullLogProvider.getInstance()))
  }

}
