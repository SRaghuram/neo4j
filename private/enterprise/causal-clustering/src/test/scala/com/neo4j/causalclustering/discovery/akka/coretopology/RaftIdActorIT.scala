/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.coretopology

import akka.actor.ActorRef
import akka.cluster.ddata.Key
import akka.cluster.ddata.LWWMap
import akka.cluster.ddata.LWWMapKey
import akka.cluster.ddata.Replicator
import akka.testkit.TestProbe
import com.neo4j.causalclustering.core.consensus.LeaderInfo
import com.neo4j.causalclustering.discovery.TestDiscoveryMember
import com.neo4j.causalclustering.discovery.akka.BaseAkkaIT
import com.neo4j.causalclustering.discovery.akka.PublishInitialData
import com.neo4j.causalclustering.discovery.akka.monitoring.ReplicatedDataIdentifier
import com.neo4j.causalclustering.discovery.member.DiscoveryMemberFactory
import com.neo4j.causalclustering.identity.IdFactory
import com.neo4j.causalclustering.identity.RaftId
import com.neo4j.causalclustering.identity.RaftMemberId
import com.neo4j.causalclustering.identity.StubClusteringIdentityModule
import org.neo4j.kernel.database.DatabaseId
import org.neo4j.kernel.database.TestDatabaseIdRepository.randomNamedDatabaseId

import scala.collection.JavaConverters.mapAsJavaMapConverter

class RaftIdActorIT extends BaseAkkaIT("RaftIdActorTest") {

  "RaftIdActor" should {
    behave like replicatedDataActor(new Fixture())

    "update replicator with raft ID from this core server" in new Fixture {
      When("send raft ID locally")
      val memberId = IdFactory.randomRaftMemberId()
      replicatedDataActorRef ! new RaftIdSetRequest(RaftId.from(randomNamedDatabaseId.databaseId()), memberId, java.time.Duration.ofSeconds( 2 ))

      Then("update metadata")
      expectReplicatorUpdates(replicator, dataKey)
    }

    "send bootstrapped RaftId->RaftMemberId mappings to core topology actor from replicator" in new Fixture {
      Given("RaftId->RaftMemberId mappings")
      val memberId = IdFactory.randomRaftMemberId()
      val db1Id, db2Id = randomNamedDatabaseId.databaseId()
      val bootstrappedRaft1 = LWWMap.empty.put(cluster, RaftId.from(db1Id), memberId)
      val bootstrappedRaft2 = LWWMap.empty.put(cluster, RaftId.from(db2Id), memberId)

      When("mappings updated from replicator")
      replicatedDataActorRef ! Replicator.Changed(dataKey)(bootstrappedRaft1)
      replicatedDataActorRef ! Replicator.Changed(dataKey)(bootstrappedRaft2)

      Then("send those updates to core topology actor")
      coreTopologyProbe.expectMsg(new BootstrappedRaftsMessage(bootstrappedRaft1.getEntries))
      coreTopologyProbe.expectMsg(new BootstrappedRaftsMessage(bootstrappedRaft1.merge(bootstrappedRaft2).getEntries))
    }

    "should send initial data to replicator when asked" in new Fixture {
      Given("some initial raft membership and a RaftIdActor")
      val dbId = randomNamedDatabaseId
      val stateService = databaseStateService(Set(dbId))
      val snapshotFactory: DiscoveryMemberFactory = TestDiscoveryMember.factory _
      val expectedMapping = Map(RaftId.from(dbId.databaseId) -> identityModule.memberId(dbId))
      val replicatorProbe = TestProbe("replicatorProbe")
      val actor = system.actorOf(RaftIdActor.props(cluster, replicatorProbe.ref, coreTopologyProbe.ref, monitor, 3))

      When("a new RaftIdActor is started")
      actor ! new PublishInitialData(snapshotFactory.createSnapshot( identityModule, stateService, Map.empty[DatabaseId,LeaderInfo].asJava))

      Then("the initial raftId mapping should be published pre-start")
      val update = expectReplicatorUpdates(replicatorProbe, dataKey)
      val ddata = update.modify.apply(Option(LWWMap.create()))
      ddata.size shouldBe 1
      ddata.entries shouldBe expectedMapping
    }
  }

  class Fixture extends ReplicatedDataActorFixture[LWWMap[RaftId,RaftMemberId]] {
    override val dataKey: Key[LWWMap[RaftId,RaftMemberId]] = LWWMapKey(ReplicatedDataIdentifier.RAFT_ID.keyName())
    override val data = LWWMap.empty[RaftId,RaftMemberId]
    val coreTopologyProbe = TestProbe("coreTopologyActor")
    val identityModule = new StubClusteringIdentityModule
    val db1 = randomNamedDatabaseId
    val memberSnapshotFactory: DiscoveryMemberFactory = TestDiscoveryMember.factory _
    val props = RaftIdActor.props(cluster, replicator.ref, coreTopologyProbe.ref, monitor, 3)
    override val replicatedDataActorRef: ActorRef = system.actorOf(props)
  }
}
