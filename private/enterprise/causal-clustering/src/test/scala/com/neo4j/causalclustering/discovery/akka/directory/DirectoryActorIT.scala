/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.directory

import java.util
import java.util.{Collections, UUID}

import akka.actor.ActorRef
import akka.cluster.ddata._
import akka.stream.javadsl.Source
import akka.stream.scaladsl.Sink
import akka.stream.{ActorMaterializer, OverflowStrategy}
import akka.testkit.TestProbe
import com.neo4j.causalclustering.core.consensus.LeaderInfo
import com.neo4j.causalclustering.discovery.akka.{BaseAkkaIT, DirectoryUpdateSink}
import com.neo4j.causalclustering.identity.MemberId
import org.neo4j.kernel.database.DatabaseId
import org.neo4j.kernel.database.TestDatabaseIdRepository.randomDatabaseId
import org.neo4j.logging.NullLogProvider

import scala.collection.JavaConverters._
import scala.util.Random

class DirectoryActorIT extends BaseAkkaIT("DirectoryActorTest") {

  "Metadata actor" should {
    behave like replicatedDataActor(new Fixture())

    "update replicated data on receipt of leader info message" in new Fixture {
      Given("leader info update")
      val event = new LeaderInfoSettingMessage(newReplicatedLeaderInfo.leaderInfo, randomDatabaseId())

      When("message received")
      replicatedDataActorRef ! event

      Then("replicated data was updated")
      expectReplicatorUpdates(replicator, dataKey)
    }

    "send incoming data to read replica actor and outside world" in new Fixture {
      Given("an incoming update")
      val update1 = ORMap.empty[DatabaseId,ReplicatedLeaderInfo].put(cluster, randomDatabaseId(), newReplicatedLeaderInfo)

      And("another incoming update")
      val update2 = ORMap.empty[DatabaseId,ReplicatedLeaderInfo].put(cluster, randomDatabaseId(), newReplicatedLeaderInfo)

      When("first update received")
      replicatedDataActorRef ! Replicator.Changed(dataKey)(update1)

      And("second update received")
      replicatedDataActorRef ! Replicator.Changed(dataKey)(update2)

      Then("first update sent to read replicas")
      rrActor.expectMsg(defaultWaitTime, new LeaderInfoDirectoryMessage(update1.entries.mapValues(_.leaderInfo).asJava))

      And("merged updates sent to read replicas")
      val merged = update1.merge(update2)
      rrActor.expectMsg(defaultWaitTime, new LeaderInfoDirectoryMessage(merged.entries.mapValues(_.leaderInfo).asJava))

      And("merged updates sent to outside world")
      awaitAssert(actualLeaderPerDb shouldBe merged.entries.mapValues(_.leaderInfo).asJava)
    }
  }

  class Fixture extends ReplicatedDataActorFixture[ORMap[DatabaseId,ReplicatedLeaderInfo]] {
    private val random = new Random()
    def newReplicatedLeaderInfo = {
      new ReplicatedLeaderInfo(new LeaderInfo(new MemberId(UUID.randomUUID()), random.nextLong()))
    }

    var actualLeaderPerDb = Collections.emptyMap[DatabaseId, LeaderInfo]

    val updateSink = new DirectoryUpdateSink {
      override def onDbLeaderUpdate(leaderPerDb: util.Map[DatabaseId, LeaderInfo]) = actualLeaderPerDb = leaderPerDb
    }

    val discoverySink = Source.queue[java.util.Map[DatabaseId,LeaderInfo]](1, OverflowStrategy.dropHead)
      .to(Sink.foreach(updateSink.onDbLeaderUpdate))
      .run(ActorMaterializer())

    val rrActor = TestProbe("ReadReplicaActor")

    val props = DirectoryActor.props(cluster, replicator.ref, discoverySink, rrActor.ref)
    override val replicatedDataActorRef: ActorRef = system.actorOf(props)
    override val dataKey: Key[ORMap[DatabaseId,ReplicatedLeaderInfo]] = ORMapKey(DirectoryActor.PER_DB_LEADER_KEY)
  }
}
