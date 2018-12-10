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
import com.neo4j.causalclustering.discovery.TestTopology
import com.neo4j.causalclustering.discovery.akka.BaseAkkaIT
import com.neo4j.causalclustering.identity.MemberId
import org.neo4j.kernel.configuration.Config
import org.neo4j.logging.NullLogProvider

class MetadataActorIT extends BaseAkkaIT("MetadataActorTest") {
  "metadata actor" should {

    behave like replicatedDataActor(new Fixture())

    "update data on pre start" in new Fixture {
      expectReplicatorUpdates(replicator, dataKey)
    }

    "remove data on post stop" in new Fixture {
      Given("initial update")
      expectReplicatorUpdates(replicator, dataKey)

      When("topping actor")
      system.stop(replicatedDataActorRef)

      Then("final update")
      expectReplicatorUpdates(replicator, dataKey)
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
  }

  class Fixture extends ReplicatedDataActorFixture[LWWMap[UniqueAddress, CoreServerInfoForMemberId]] {
    val coreTopologyProbe = TestProbe("topology")
    val myself = new MemberId(UUID.randomUUID())
    val dataKey = LWWMapKey.create[UniqueAddress, CoreServerInfoForMemberId](MetadataActor.MEMBER_DATA_KEY)

    val coreServerInfo = TestTopology.addressesForCore(0, false)

    val config = {
      val conf = Config.defaults()
      conf.augment(TestTopology.configFor(coreServerInfo))
      conf
    }

    val replicatedDataActorRef = system.actorOf(MetadataActor.props(myself, cluster, replicator.ref, coreTopologyProbe.ref, config, NullLogProvider.getInstance()))
  }
}
