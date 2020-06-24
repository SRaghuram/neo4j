/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.readreplicatopology

import java.time.Duration
import java.time.Instant
import java.util.concurrent.TimeUnit
import java.util.Collections
import java.util.UUID

import akka.Done
import akka.cluster.client.ClusterClientReceptionist
import akka.cluster.pubsub.DistributedPubSub
import akka.cluster.pubsub.DistributedPubSubMediator.CurrentTopics
import akka.cluster.pubsub.DistributedPubSubMediator.GetTopics
import akka.cluster.pubsub.DistributedPubSubMediator.Publish
import akka.pattern.ask
import akka.testkit.TestProbe
import com.neo4j.causalclustering.discovery.TestTopology
import com.neo4j.causalclustering.discovery.akka.BaseAkkaIT
import com.neo4j.causalclustering.discovery.akka.readreplicatopology.ReadReplicaViewActor.Tick
import com.neo4j.causalclustering.identity.MemberId
import org.neo4j.time.Clocks

import scala.collection.JavaConverters.mapAsJavaMapConverter
import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

class ReadReplicaViewActorIT extends BaseAkkaIT("GlobalReadReplica") {

  "ReadReplicaViewActor" when {
    "running" should {
      "send map with added read replicas to parent" in new Fixture {
        When("incoming read replica message 1")
        publish(rrMessage1)

        Then("receive read replica view")
        fishForParentMessage(new ReadReplicaViewMessage(Map(clusterClient1.path -> new ReadReplicaViewRecord(rrMessage1, clock)).asJava))

        When("incoming read replica message 2")
        publish(rrMessage2)

        Then("receive read replica view")
        fishForParentMessage( new ReadReplicaViewMessage(Map(
          clusterClient1.path -> new ReadReplicaViewRecord(rrMessage1, clock),
          clusterClient2.path -> new ReadReplicaViewRecord(rrMessage2, clock)).asJava) )
      }
      "send map without removed read replicas to parent" in new Fixture {
        When("incoming read replica message 1")
        publish(rrMessage1)

        Then("receive read replica view")
        fishForParentMessage( new ReadReplicaViewMessage(Map(clusterClient1.path -> new ReadReplicaViewRecord(rrMessage1, clock)).asJava) )

        When("incoming read replica message 2")
        publish(rrMessage2)

        Then("receive read replica view")
        fishForParentMessage( new ReadReplicaViewMessage(Map(
          clusterClient1.path -> new ReadReplicaViewRecord(rrMessage1, clock),
          clusterClient2.path -> new ReadReplicaViewRecord(rrMessage2, clock)).asJava) )

        When("incoming read replica removal message")
        publish(new ReadReplicaRemovalMessage(clusterClient1))

        Then("receive read replica view without removed RR")
        fishForParentMessage( new ReadReplicaViewMessage(Map(clusterClient2.path -> new ReadReplicaViewRecord(rrMessage2, clock)).asJava) )

      }
      "remove old entries after a period of time" in new Fixture {
        When("incoming read replica message 1")
        publish(rrMessage1)

        Then("receive read replica view")
        val rrRecord1 = new ReadReplicaViewRecord(rrMessage1, clock)
        fishForParentMessage( new ReadReplicaViewMessage(Map(clusterClient1.path -> rrRecord1).asJava) )

        When("time passes")
        clock.forward(refresh)

        And("incoming read replica message 2")
        publish(rrMessage2)

        Then("receive read replica view")
        val rrRecord2 = new ReadReplicaViewRecord(rrMessage2, clock)
        fishForParentMessage( new ReadReplicaViewMessage(Map(
          clusterClient1.path -> rrRecord1,
          clusterClient2.path -> rrRecord2).asJava) )

        When("time passes")
        val enoughTimeForFirstButNotSecondToExpire = refresh
          .multipliedBy(ReadReplicaViewActor.TICKS_BEFORE_REMOVE_READ_REPLICA - 1)
          .plus(Duration.ofMillis(1))
        clock.forward(enoughTimeForFirstButNotSecondToExpire)

        Then("receive read replica view without expired RR")
        fishForParentMessage(new ReadReplicaViewMessage(Map(clusterClient2.path -> rrRecord2).asJava), wait = waitWithRefresh )
      }
      "send periodic tick to parent" in new Fixture {
        Then("receive tick")
        parent.expectMsg(waitWithRefresh, Tick.INSTANCE)
      }
    }
  }

  trait Fixture {
    val parent = TestProbe()
    val clock = Clocks.fakeClock(Instant.now())
    val refresh = Duration.ofSeconds(1)
    val receptionist = ClusterClientReceptionist.get(system)

    val waitWithRefresh = defaultWaitTime + FiniteDuration(refresh.toMillis, TimeUnit.MILLISECONDS)

    val rrInfo1 = TestTopology.addressesForReadReplica(1)
    val rrInfo2 = TestTopology.addressesForReadReplica(2)
    
    val memberId1,memberId2 = new MemberId(UUID.randomUUID())
    
    val clusterClient1, clusterClient2, topologyClient1, topologyClient2 = TestProbe().ref

    val rrMessage1 = new ReadReplicaRefreshMessage(rrInfo1, memberId1, clusterClient1, topologyClient1, Collections.emptyMap())
    val rrMessage2 = new ReadReplicaRefreshMessage(rrInfo2, memberId2, clusterClient2, topologyClient2, Collections.emptyMap())

    val props = ReadReplicaViewActor.props(parent.ref, receptionist, clock, refresh)
    val actorRef = system.actorOf(props)
    
    val pubSub = DistributedPubSub.get(system)

    var hasSubscribed = false
    def askSubscribed(): Future[Done] = {
      ask(pubSub.mediator, GetTopics)
        .flatMap {
          case CurrentTopics(topics) if topics.contains(ReadReplicaViewActor.READ_REPLICA_TOPIC) =>
            hasSubscribed = true
            Future.successful(Done)
          case _ =>
            askSubscribed()
        }
    }
    askSubscribed()

    def publish(msg: Any)= {
      awaitCond(hasSubscribed, defaultWaitTime)
      pubSub.mediator ! Publish(ReadReplicaViewActor.READ_REPLICA_TOPIC, msg)
    }

    def fishForParentMessage(expected: Any, wait: FiniteDuration = defaultWaitTime) = {
      parent.fishForMessage(wait) {
        case msg if expected == msg => true
        case _: Tick => false
      }
    }
  }
}
