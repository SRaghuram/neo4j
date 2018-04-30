/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka

import java.util
import java.util.{Collections, UUID}

import akka.actor.ActorRef
import akka.cluster.ddata.{Key, LWWMap, LWWMapKey, Replicator}
import akka.stream.javadsl.Source
import akka.stream.scaladsl.Sink
import akka.stream.{ActorMaterializer, OverflowStrategy}
import akka.testkit.TestProbe
import org.neo4j.causalclustering.core.consensus.LeaderInfo
import org.neo4j.causalclustering.identity.MemberId
import org.neo4j.logging.NullLogProvider

import scala.util.Random

class DirectoryActorIT extends BaseAkkaIT("DirectoryActorTest") {

  "Metadata actor" should {
    behave like replicatedDataActor(new Fixture())

    "update replicated data on receipt of leader info message" in new Fixture {
      Given("leader info update")
      val event = new LeaderInfoForDatabase(newLeaderInfo, "dbName")

      When("message received")
      replicatedDataActorRef ! event

      Then("replicated data was updated")
      expectReplicatorUpdates(replicator, dataKey)
    }

    "send incoming data to read replica actor and outside world" in new Fixture {
      Given("an incoming update")
      val update1 = LWWMap.empty[String,LeaderInfo].put(cluster, "db1", newLeaderInfo)

      And("another incoming update")
      val update2 = LWWMap.empty[String,LeaderInfo].put(cluster, "db2", newLeaderInfo)

      When("first update received")
      replicatedDataActorRef ! Replicator.Changed(dataKey)(update1)

      And("second update received")
      replicatedDataActorRef ! Replicator.Changed(dataKey)(update2)

      Then("first update sent to read replicas")
      rrActor.expectMsg(defaultWaitTime, new DatabaseLeaderInfoMessage(update1.getEntries()))

      And("merged updates sent to read replicas")
      val merged = update1.merge(update2)
      rrActor.expectMsg(defaultWaitTime, new DatabaseLeaderInfoMessage(merged.getEntries()))

      And("merged updates sent to outside world")
      awaitAssert(actualLeaderPerDb shouldBe merged.getEntries())
    }
  }

  class Fixture extends ReplicatedDataActorFixture[LWWMap[String,LeaderInfo]] {
    private val random = new Random()
    def newLeaderInfo = {
      new LeaderInfo(new MemberId(UUID.randomUUID()), random.nextLong())
    }

    var actualLeaderPerDb = Collections.emptyMap[String, LeaderInfo]

    val updateSink = new DirectoryUpdateSink {
      override def onDbLeaderUpdate(leaderPerDb: util.Map[String, LeaderInfo]) = actualLeaderPerDb = leaderPerDb
    }

    val discoverySink = Source.queue[java.util.Map[String,LeaderInfo]](1, OverflowStrategy.dropHead)
      .to(Sink.foreach(updateSink.onDbLeaderUpdate))
      .run(ActorMaterializer())

    val rrActor = TestProbe("ReadReplicaActor")

    val props = DirectoryActor.props(cluster, replicator.ref, discoverySink, rrActor.ref, NullLogProvider.getInstance())
    override val replicatedDataActorRef: ActorRef = system.actorOf(props)
    override val dataKey: Key[LWWMap[String, LeaderInfo]] = LWWMapKey(DirectoryActor.PER_DB_LEADER_KEY)
  }
}
