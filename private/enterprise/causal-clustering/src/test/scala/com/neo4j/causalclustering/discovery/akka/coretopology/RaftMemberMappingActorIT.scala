/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.coretopology

import akka.cluster.ddata.LWWMap
import akka.cluster.ddata.LWWMapKey
import akka.cluster.ddata.Replicator
import akka.testkit.TestProbe
import com.neo4j.causalclustering.core.consensus.LeaderInfo
import com.neo4j.causalclustering.discovery.akka.BaseAkkaIT
import com.neo4j.causalclustering.discovery.akka.PublishInitialData
import com.neo4j.causalclustering.discovery.akka.common.DatabaseStoppedMessage
import com.neo4j.causalclustering.discovery.akka.common.RaftMemberKnownMessage
import com.neo4j.causalclustering.discovery.akka.database.state.DatabaseServer
import com.neo4j.causalclustering.discovery.akka.monitoring.ReplicatedDataIdentifier
import com.neo4j.causalclustering.discovery.member.DefaultServerSnapshot
import com.neo4j.causalclustering.discovery.member.TestCoreServerSnapshot
import com.neo4j.causalclustering.identity.IdFactory
import com.neo4j.causalclustering.identity.InMemoryCoreServerIdentity
import com.neo4j.causalclustering.identity.RaftMemberId
import org.neo4j.dbms.identity.ServerId
import org.neo4j.kernel.database.DatabaseId
import org.neo4j.kernel.database.NamedDatabaseId
import org.neo4j.kernel.database.TestDatabaseIdRepository
import org.neo4j.kernel.database.TestDatabaseIdRepository.randomNamedDatabaseId

import scala.collection.JavaConverters.mapAsJavaMapConverter

class RaftMemberMappingActorIT extends BaseAkkaIT("MappingActorIT") {
  "mapping actor" should {

    behave like replicatedDataActor(new Fixture())

    "send initial data to replicator when asked" in new Fixture {
      When("PublishInitialData request received")
      replicatedDataActorRef ! new PublishInitialData(snapshot)

      Then("the initial mappings should be published")
      expectUpdateWithDatabase(data, namedDatabaseIds, raftMemberIds, 1)
    }

    "send initial data for non cores (databases with memberships)" in new Fixture {
      // Standalone will also start the RaftMemberMappingActor through the Core Topology Actor
      When("PublishInitialData request received for databases without raft memberships")
      val snapDbsWithoutMemberships =
        new DefaultServerSnapshot(Map.empty[DatabaseId, RaftMemberId].asJava, databaseMemberships.asJava, Map.empty[DatabaseId, LeaderInfo].asJava)

      replicatedDataActorRef ! new PublishInitialData(snapDbsWithoutMemberships)

      Then("publish an empty initial mappings")
      expectUpdateWithDatabase(LWWMap.empty[DatabaseServer, RaftMemberId], Set.empty[NamedDatabaseId], Set.empty[RaftMemberId], Set.empty[ServerId], 1)
    }

    "send metadata to core topology actor on update" in new Fixture {
      Given("new database mapping")
      val databaseToServer1 = new DatabaseServer(databaseIdRepository.getRaw("one").databaseId, myself)
      val raftMemberId1 = IdFactory.randomRaftMemberId

      val databaseToServer2 = new DatabaseServer(databaseIdRepository.getRaw("two").databaseId, myself)
      val raftMemberId2 = IdFactory.randomRaftMemberId

      val mapping1 = LWWMap.empty.put(cluster, databaseToServer1, raftMemberId1)
      val mapping2 = LWWMap.empty.put(cluster, databaseToServer2, raftMemberId2)

      When("metadata updates received")
      replicatedDataActorRef ! Replicator.Changed(dataKey)(mapping1)
      replicatedDataActorRef ! Replicator.Changed(dataKey)(mapping2)

      Then("send metadata to core topology actor")
      coreTopologyProbe.expectMsg(new RaftMemberMappingMessage(mapping1))
      coreTopologyProbe.expectMsg(new RaftMemberMappingMessage(mapping2))
    }

    "cleanup metadata" in new Fixture {
      Given("a cleanup message")
      val message = new RaftMemberMappingActor.CleanupMessage(myself)

      And("initial update")
      replicatedDataActorRef ! new PublishInitialData(snapshot)
      val replicatedData = collectReplicatorUpdates(data, 1)
      replicatedDataActorRef ! Replicator.Changed(dataKey)(replicatedData)

      When("receive cleanup message")
      replicatedDataActorRef ! message

      Then("update replicator")
      val finalData = collectReplicatorUpdates(replicatedData, namedDatabaseIds.size)
      finalData.entries.size should equal(0)
    }

    "update data on database start" in new Fixture {
      Given("database IDs to start")
      val namedDatabaseId1, namedDatabaseId2 = randomNamedDatabaseId()
      val raftMemberId1 = identityModule.raftMemberId(namedDatabaseId1)
      val raftMemberId2 = identityModule.raftMemberId(namedDatabaseId2)

      And("initial update")
      replicatedDataActorRef ! new PublishInitialData(snapshot)
      collectReplicatorUpdates(data, 1)

      When("receive both start messages")
      replicatedDataActorRef ! new RaftMemberKnownMessage(namedDatabaseId1, raftMemberId1)
      replicatedDataActorRef ! new RaftMemberKnownMessage(namedDatabaseId2, raftMemberId2)

      Then("update replicator")
      expectUpdateWithDatabase(namedDatabaseId1, raftMemberId1)
      expectUpdateWithDatabase(namedDatabaseId2, raftMemberId2)
    }

    "update data on database stop" in new Fixture {
      Given("database IDs to start and stop")
      val namedDatabaseId1, namedDatabaseId2 = randomNamedDatabaseId()
      val raftMemberId1 = identityModule.raftMemberId(namedDatabaseId1)
      val raftMemberId2 = identityModule.raftMemberId(namedDatabaseId2)

      And("initial update")
      replicatedDataActorRef ! new PublishInitialData(snapshot)
      And("both databases started")
      replicatedDataActorRef ! new RaftMemberKnownMessage(namedDatabaseId1, raftMemberId1)
      replicatedDataActorRef ! new RaftMemberKnownMessage(namedDatabaseId2, raftMemberId2)

      val replicatedData = expectUpdateWithDatabase(data,
        namedDatabaseIds + namedDatabaseId1 + namedDatabaseId2, raftMemberIds + raftMemberId1 + raftMemberId2, 3)

      When("receive both stop messages")
      replicatedDataActorRef ! new DatabaseStoppedMessage(namedDatabaseId1)
      replicatedDataActorRef ! new DatabaseStoppedMessage(namedDatabaseId2)

      Then("update replicator")
      expectUpdateWithDatabase(replicatedData, namedDatabaseIds, raftMemberIds, 2)
    }
  }

  class Fixture extends ReplicatedDataActorFixture[LWWMap[DatabaseServer, RaftMemberId]] {
    val coreTopologyProbe = TestProbe("topology")
    val identityModule = new InMemoryCoreServerIdentity(0)
    val myself = identityModule.serverId()
    val dataKey = LWWMapKey.create[DatabaseServer, RaftMemberId](ReplicatedDataIdentifier.RAFT_MEMBER_MAPPING.keyName())
    val data = LWWMap.empty[DatabaseServer, RaftMemberId]

    val databaseIdRepository = new TestDatabaseIdRepository()
    val namedDatabaseIds = Set("system", "not_system").map(databaseIdRepository.getRaw)
    val raftMemberIds = namedDatabaseIds.map(identityModule.raftMemberId)
    val stateService = databaseStateService(namedDatabaseIds)
    val databaseMemberships = namedDatabaseIds.map(namedDatabaseId => (namedDatabaseId.databaseId(), stateService.stateOfDatabase(namedDatabaseId))).toMap
    val snapshot = new TestCoreServerSnapshot(identityModule, stateService, Map.empty[DatabaseId, LeaderInfo].asJava)

    val replicatedDataActorRef = system.actorOf(RaftMemberMappingActor.props(
      cluster, replicator.ref, coreTopologyProbe.ref, identityModule.serverId(), monitor))

    def collectReplicatorUpdates(original: LWWMap[DatabaseServer, RaftMemberId], count: Int): LWWMap[DatabaseServer, RaftMemberId] = {
      val update = expectReplicatorUpdates(replicator, dataKey)
      val data = update.modify.apply(Some(original))
      if (count > 1) {
        collectReplicatorUpdates(data, count - 1)
      } else {
        data
      }
    }

    def expectUpdateWithDatabase(namedDatabaseId: NamedDatabaseId, raftMemberId: RaftMemberId): LWWMap[DatabaseServer, RaftMemberId] = {
      expectUpdateWithDatabase(LWWMap.create[DatabaseServer, RaftMemberId], Set(namedDatabaseId), Set(raftMemberId), Set(myself), 1)
    }

    def expectUpdateWithDatabase(initial: LWWMap[DatabaseServer, RaftMemberId],
                                 namedDatabaseIds: Set[NamedDatabaseId],
                                 raftMemberIds: Set[RaftMemberId],
                                 messageCount: Int): LWWMap[DatabaseServer, RaftMemberId] = {
      expectUpdateWithDatabase(initial, namedDatabaseIds, raftMemberIds, Set(myself), messageCount)
    }

    def expectUpdateWithDatabase(initial: LWWMap[DatabaseServer, RaftMemberId],
                                 namedDatabaseIds: Set[NamedDatabaseId],
                                 raftMemberIds: Set[RaftMemberId],
                                 serverIds : Set[ServerId],
                                 messageCount: Int): LWWMap[DatabaseServer, RaftMemberId] = {
      namedDatabaseIds.size should equal(raftMemberIds.size)
      val state = collectReplicatorUpdates(initial, messageCount)
      val data = state.entries
      data.keySet.map(_.databaseId) should contain theSameElementsAs namedDatabaseIds.map(_.databaseId)
      data.keySet.map(_.serverId) should contain theSameElementsAs  serverIds
      data.values should contain theSameElementsAs raftMemberIds
      state
    }
  }
}
