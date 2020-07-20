/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.coretopology

import java.util.UUID

import akka.actor.Address
import akka.cluster.UniqueAddress
import akka.cluster.ddata.LWWMap
import akka.cluster.ddata.LWWMapKey
import akka.cluster.ddata.Replicator
import akka.testkit.TestProbe
import com.neo4j.causalclustering.discovery.TestDiscoveryMember
import com.neo4j.causalclustering.discovery.TestTopology
import com.neo4j.causalclustering.discovery.akka.BaseAkkaIT
import com.neo4j.causalclustering.discovery.akka.common.DatabaseStartedMessage
import com.neo4j.causalclustering.discovery.akka.common.DatabaseStoppedMessage
import com.neo4j.causalclustering.discovery.akka.monitoring.ReplicatedDataIdentifier
import org.neo4j.configuration.Config
import org.neo4j.dbms.identity.ServerId
import org.neo4j.kernel.database.DatabaseId
import org.neo4j.kernel.database.NamedDatabaseId
import org.neo4j.kernel.database.TestDatabaseIdRepository
import org.neo4j.kernel.database.TestDatabaseIdRepository.randomNamedDatabaseId

import scala.collection.JavaConverters.setAsJavaSetConverter

class MetadataActorIT extends BaseAkkaIT("MetadataActorIT") {
  "metadata actor" should {

    behave like replicatedDataActor(new Fixture())

    "update data on pre start" in new Fixture {
      expectUpdateWithDatabases(namedDatabaseIds)
    }

    "send metadata to core topology actor on update" in new Fixture {
      Given("new member metadata")
      val member1Address = UniqueAddress(Address("udp", system.name, "1.2.3.4", 8213), 1L)
      val member1Info = new CoreServerInfoForServerId(
        new ServerId(UUID.randomUUID()),
        TestTopology.addressesForCore(0, false)
      )
      val member2Address = UniqueAddress(Address("udp", system.name, "1.2.3.5", 8214), 2L)
      val member2Info = new CoreServerInfoForServerId(
        new ServerId(UUID.randomUUID()),
        TestTopology.addressesForCore(1, false)
      )

      val metadata1 = LWWMap.empty.put(cluster, member1Address, member1Info)
      val metadata2 = LWWMap.empty.put(cluster, member2Address, member2Info)

      When("metadata updates received")
      replicatedDataActorRef ! Replicator.Changed(dataKey)(metadata1)
      replicatedDataActorRef ! Replicator.Changed(dataKey)(metadata2)

      Then("send metadata to core topology actor")
      coreTopologyProbe.expectMsg(new MetadataMessage(metadata1))
      coreTopologyProbe.expectMsg(new MetadataMessage(metadata1.merge(metadata2)))
    }

    "cleanup metadata" in new Fixture {
      Given("a cleanup message")
      val memberAddress = UniqueAddress(Address("udp", system.name, "1.2.3.4", 8213), 1L)
      val message = new CleanupMessage(memberAddress)

      And("initial update")
      expectReplicatorUpdates(replicator, dataKey)

      When("receive cleanup message")
      replicatedDataActorRef ! message

      Then("update replicator")
      expectReplicatorUpdates(replicator, dataKey)
    }

    "update data on database start" in new Fixture {
      Given("database IDs to start")
      val namedDatabaseId1, namedDatabaseId2 = randomNamedDatabaseId()

      And("initial update")
      expectReplicatorUpdates(replicator, dataKey)

      When("receive both start messages")
      replicatedDataActorRef ! new DatabaseStartedMessage(namedDatabaseId1)
      replicatedDataActorRef ! new DatabaseStartedMessage(namedDatabaseId2)

      Then("update replicator")
      expectUpdateWithDatabases(namedDatabaseIds + namedDatabaseId1)
      expectUpdateWithDatabases(namedDatabaseIds + namedDatabaseId1 + namedDatabaseId2)
    }

    "update data on database stop" in new Fixture {
      Given("database IDs to start and stop")
      val namedDatabaseId1, namedDatabaseId2 = randomNamedDatabaseId()

      And("initial update")
      expectReplicatorUpdates(replicator, dataKey)

      And("both databases started")
      replicatedDataActorRef ! new DatabaseStartedMessage(namedDatabaseId1)
      replicatedDataActorRef ! new DatabaseStartedMessage(namedDatabaseId2)
      expectUpdateWithDatabases(namedDatabaseIds + namedDatabaseId1)
      expectUpdateWithDatabases(namedDatabaseIds + namedDatabaseId1 + namedDatabaseId2)

      When("receive both stop messages")
      replicatedDataActorRef ! new DatabaseStoppedMessage(namedDatabaseId2)
      replicatedDataActorRef ! new DatabaseStoppedMessage(namedDatabaseId1)

      Then("update replicator")
      expectUpdateWithDatabases(namedDatabaseIds + namedDatabaseId1)
      expectUpdateWithDatabases(namedDatabaseIds)
    }
  }

  class Fixture extends ReplicatedDataActorFixture[LWWMap[UniqueAddress, CoreServerInfoForServerId]] {
    val coreTopologyProbe = TestProbe("topology")
    val myself = new ServerId(UUID.randomUUID())
    val dataKey = LWWMapKey.create[UniqueAddress, CoreServerInfoForServerId](ReplicatedDataIdentifier.METADATA.keyName())
    val data = LWWMap.empty[UniqueAddress, CoreServerInfoForServerId]

    val databaseIdRepository = new TestDatabaseIdRepository()
    val namedDatabaseIds = Set("system", "not_system").map(databaseIdRepository.getRaw)
    var discoveryMember = new TestDiscoveryMember(myself, namedDatabaseIds.asJava)

    val coreServerInfo = TestTopology.addressesForCore(0, false, (Set.empty[DatabaseId] ++ namedDatabaseIds.map(_.databaseId())).asJava)

    val config = {
      val conf = Config.newBuilder().fromConfig(TestTopology.configFor(coreServerInfo)).build();
      conf
    }

    val replicatedDataActorRef = system.actorOf(MetadataActor.props(
      discoveryMember, cluster, replicator.ref, coreTopologyProbe.ref, config, monitor))

    def expectUpdateWithDatabases(namedDatabaseIds: Set[NamedDatabaseId]): Unit = {
      val update = expectReplicatorUpdates(replicator, dataKey)
      val data = update.modify.apply(Some(LWWMap.create()))
      val infoForMemberId = data.entries(cluster.selfUniqueAddress)
      infoForMemberId.serverId() should equal(discoveryMember.id())
      val serverInfo = infoForMemberId.coreServerInfo()
      serverInfo.startedDatabaseIds should contain theSameElementsAs namedDatabaseIds.map(_.databaseId())
    }
  }
}
