/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.coretopology

import java.util.UUID

import akka.actor.Address
import akka.cluster.UniqueAddress
import akka.cluster.ddata.{LWWMap, LWWMapKey, Replicator}
import akka.testkit.TestProbe
import com.neo4j.causalclustering.discovery.akka.BaseAkkaIT
import com.neo4j.causalclustering.discovery.akka.common.{DatabaseStartedMessage, DatabaseStoppedMessage}
import com.neo4j.causalclustering.discovery.akka.monitoring.ReplicatedDataIdentifier
import com.neo4j.causalclustering.discovery.{TestDiscoveryMember, TestTopology}
import com.neo4j.causalclustering.identity.MemberId
import org.neo4j.configuration.Config
import org.neo4j.kernel.database.TestDatabaseIdRepository.randomDatabaseId
import org.neo4j.kernel.database.{DatabaseId, TestDatabaseIdRepository}

import scala.collection.JavaConverters._

class MetadataActorIT extends BaseAkkaIT("MetadataActorIT") {
  "metadata actor" should {

    behave like replicatedDataActor(new Fixture())

    "update data on pre start" in new Fixture {
      expectUpdateWithDatabases(databaseIds)
    }

    "send metadata to core topology actor on update" in new Fixture {
      Given("new member metadata")
      val member1Address = UniqueAddress(Address("udp", system.name, "1.2.3.4", 8213), 1L)
      val member1Info = new CoreServerInfoForMemberId(
        new MemberId(UUID.randomUUID()),
        TestTopology.addressesForCore(0, false)
      )
      val member2Address = UniqueAddress(Address("udp", system.name, "1.2.3.5", 8214), 2L)
      val member2Info = new CoreServerInfoForMemberId(
        new MemberId(UUID.randomUUID()),
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
      val databaseId1, databaseId2 = randomDatabaseId()

      And("initial update")
      expectReplicatorUpdates(replicator, dataKey)

      When("receive both start messages")
      replicatedDataActorRef ! new DatabaseStartedMessage(databaseId1)
      replicatedDataActorRef ! new DatabaseStartedMessage(databaseId2)

      Then("update replicator")
      expectUpdateWithDatabases(databaseIds + databaseId1)
      expectUpdateWithDatabases(databaseIds + databaseId1 + databaseId2)
    }

    "update data on database stop" in new Fixture {
      Given("database IDs to start and stop")
      val databaseId1, databaseId2 = randomDatabaseId()

      And("initial update")
      expectReplicatorUpdates(replicator, dataKey)

      And("both databases started")
      replicatedDataActorRef ! new DatabaseStartedMessage(databaseId1)
      replicatedDataActorRef ! new DatabaseStartedMessage(databaseId2)
      expectUpdateWithDatabases(databaseIds + databaseId1)
      expectUpdateWithDatabases(databaseIds + databaseId1 + databaseId2)

      When("receive both stop messages")
      replicatedDataActorRef ! new DatabaseStoppedMessage(databaseId2)
      replicatedDataActorRef ! new DatabaseStoppedMessage(databaseId1)

      Then("update replicator")
      expectUpdateWithDatabases(databaseIds + databaseId1)
      expectUpdateWithDatabases(databaseIds)
    }
  }

  class Fixture extends ReplicatedDataActorFixture[LWWMap[UniqueAddress, CoreServerInfoForMemberId]] {
    val coreTopologyProbe = TestProbe("topology")
    val myself = new MemberId(UUID.randomUUID())
    val dataKey = LWWMapKey.create[UniqueAddress, CoreServerInfoForMemberId](ReplicatedDataIdentifier.METADATA.keyName())
    val data = LWWMap.empty[UniqueAddress, CoreServerInfoForMemberId]

    val databaseIdRepository = new TestDatabaseIdRepository()
    val databaseIds = Set(databaseIdRepository.getRaw("system"), databaseIdRepository.getRaw("not_system"))
    var discoveryMember = new TestDiscoveryMember(myself, databaseIds.asJava)

    val coreServerInfo = TestTopology.addressesForCore(0, false, databaseIds.asJava)

    val config = {
      val conf = Config.newBuilder().fromConfig(TestTopology.configFor(coreServerInfo)).build();
      conf
    }

    val replicatedDataActorRef = system.actorOf(MetadataActor.props(
      discoveryMember, cluster, replicator.ref, coreTopologyProbe.ref, config, monitor))

    def expectUpdateWithDatabases(databaseIds: Set[DatabaseId]): Unit = {
      val update = expectReplicatorUpdates(replicator, dataKey)
      val data = update.modify.apply(Some(LWWMap.create()))
      val infoForMemberId = data.entries(cluster.selfUniqueAddress)
      infoForMemberId.memberId() should equal(discoveryMember.id())
      val serverInfo = infoForMemberId.coreServerInfo()
      serverInfo.getDatabaseIds should contain theSameElementsAs databaseIds
    }
  }
}
