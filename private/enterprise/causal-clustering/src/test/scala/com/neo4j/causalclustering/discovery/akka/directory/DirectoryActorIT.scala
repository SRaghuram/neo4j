/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.directory

import java.util
import java.util.Collections

import akka.actor.ActorRef
import akka.cluster.ddata.Key
import akka.cluster.ddata.ORMap
import akka.cluster.ddata.ORMapKey
import akka.cluster.ddata.Replicator
import akka.stream.ActorMaterializer
import akka.stream.OverflowStrategy
import akka.stream.javadsl.Source
import akka.stream.scaladsl.Sink
import akka.testkit.TestProbe
import com.neo4j.causalclustering.core.consensus.LeaderInfo
import com.neo4j.causalclustering.discovery.TestDiscoveryMember
import com.neo4j.causalclustering.discovery.akka.BaseAkkaIT
import com.neo4j.causalclustering.discovery.akka.DirectoryUpdateSink
import com.neo4j.causalclustering.discovery.akka.PublishInitialData
import com.neo4j.causalclustering.discovery.akka.monitoring.ReplicatedDataIdentifier
import com.neo4j.causalclustering.identity.IdFactory
import com.neo4j.causalclustering.identity.StubClusteringIdentityModule
import org.neo4j.dbms.DatabaseStateService
import org.neo4j.kernel.database.DatabaseId
import org.neo4j.kernel.database.TestDatabaseIdRepository.randomNamedDatabaseId
import org.neo4j.kernel.database.TestDatabaseIdRepository.randomDatabaseId

import scala.collection.JavaConverters.mapAsJavaMapConverter
import scala.util.Random

class DirectoryActorIT extends BaseAkkaIT("DirectoryActorTest") {

  "Directory actor" should {
    behave like replicatedDataActor(new Fixture())

    "update replicated data on receipt of leader info setting message" in new Fixture {
      Given("leader info update")
      val event = new LeaderInfoSettingMessage(randomReplicatedLeaderInfo.leaderInfo, randomNamedDatabaseId())

      When("message received")
      replicatedDataActorRef ! event

      Then("replicated data was updated")
      expectReplicatorUpdates(replicator, dataKey)
    }

    "send initial data to replicator when asked" in new Fixture {
      Given("some initial leaderships and a DirectoryActor")
      val databaseIds = List.fill(3)(randomNamedDatabaseId).toSet
      val leadershipSnapshot = databaseIds.map(_.databaseId -> randomLeaderInfo).toMap
      val actor = system.actorOf(DirectoryActor.props(cluster, replicator.ref, discoverySink, rrActor.ref, monitor))
      val stateService = databaseStateService(databaseIds)
      val identityModule = new StubClusteringIdentityModule

      When("PublishInitialData request received")
      actor ! new PublishInitialData(new TestDiscoveryMember(identityModule,stateService,leadershipSnapshot.asJava))

      Then("the initial leaderships should be published")
      val update = expectReplicatorUpdates(replicator, dataKey)
      val ddata = update.modify(Option(ORMap.empty))
      ddata.size shouldBe 3
      ddata.entries.mapValues(_.leaderInfo) shouldBe leadershipSnapshot
    }

    "send incoming data to read replica actor and outside world" in new Fixture {
      Given("incoming updates")
      val update1, update2 = ORMap.empty[DatabaseId,ReplicatedLeaderInfo].put(cluster, randomNamedDatabaseId().databaseId(), randomReplicatedLeaderInfo)

      When("first update received")
      replicatedDataActorRef ! Replicator.Changed(dataKey)(update1)

      And("second update received")
      replicatedDataActorRef ! Replicator.Changed(dataKey)(update2)

      Then("first update sent to read replicas")
      val expectedFirst = new LeaderInfoDirectoryMessage(update1.entries.map{ case (db,rrInfo) => Tuple2[DatabaseId,LeaderInfo](db, rrInfo.leaderInfo) }.asJava)
      rrActor.expectMsg(defaultWaitTime, expectedFirst)

      And("merged updates sent to read replicas")
      val merged = update1.merge(update2)
      val expectedSecond = new LeaderInfoDirectoryMessage(merged.entries.map{ case (db,rrInfo) => Tuple2[DatabaseId,LeaderInfo](db, rrInfo.leaderInfo) }.asJava)
      rrActor.expectMsg(defaultWaitTime, expectedSecond)

      And("merged updates sent to outside world")
      awaitAssert(actualLeaderPerDb shouldBe merged.entries.mapValues(_.leaderInfo).asJava)
    }
  }

  class Fixture extends ReplicatedDataActorFixture[ORMap[DatabaseId,ReplicatedLeaderInfo]] {
    private val random = new Random()

    def randomLeaderInfo = new LeaderInfo(IdFactory.randomRaftMemberId, random.nextLong)
    def randomReplicatedLeaderInfo = new ReplicatedLeaderInfo(randomLeaderInfo)

    var actualLeaderPerDb = Collections.emptyMap[DatabaseId, LeaderInfo]

    val updateSink = new DirectoryUpdateSink {
      override def onDbLeaderUpdate(leaderPerDb: util.Map[DatabaseId, LeaderInfo]): Unit = actualLeaderPerDb = leaderPerDb
    }

    val discoverySink = Source.queue[java.util.Map[DatabaseId,LeaderInfo]](1, OverflowStrategy.dropHead)
      .to(Sink.foreach(updateSink.onDbLeaderUpdate))
      .run(ActorMaterializer())

    val rrActor = TestProbe("ReadReplicaActor")

    val props = DirectoryActor.props(cluster, replicator.ref, discoverySink, rrActor.ref, monitor)
    override val replicatedDataActorRef: ActorRef = system.actorOf(props)
    override val dataKey: Key[ORMap[DatabaseId,ReplicatedLeaderInfo]] = ORMapKey(ReplicatedDataIdentifier.DIRECTORY.keyName())
    override val data = ORMap.create[DatabaseId,ReplicatedLeaderInfo]()
  }
}
