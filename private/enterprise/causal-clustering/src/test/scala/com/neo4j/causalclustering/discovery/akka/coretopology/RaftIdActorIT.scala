/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.coretopology

import java.util.UUID

import akka.actor.ActorRef
import akka.cluster.ddata.Key
import akka.cluster.ddata.LWWMap
import akka.cluster.ddata.LWWMapKey
import akka.cluster.ddata.Replicator
import akka.testkit.TestProbe
import com.neo4j.causalclustering.discovery.akka.BaseAkkaIT
import com.neo4j.causalclustering.discovery.akka.monitoring.ReplicatedDataIdentifier
import com.neo4j.causalclustering.identity.MemberId
import com.neo4j.causalclustering.identity.RaftId
import org.neo4j.kernel.database.TestDatabaseIdRepository.randomNamedDatabaseId

class RaftIdActorIT extends BaseAkkaIT("RaftIdActorTest") {

  "RaftIdActor" should {
    behave like replicatedDataActor(new Fixture())

    "update replicator with raft ID from this core server" in new Fixture {
      When("send raft ID locally")
      val memberId = new MemberId(UUID.randomUUID)
      replicatedDataActorRef ! new RaftIdSetRequest(RaftId.from(randomNamedDatabaseId.databaseId()), memberId, java.time.Duration.ofSeconds( 2 ))

      Then("update metadata")
      expectReplicatorUpdates(replicator, dataKey)
    }

    "send bootstrapped raft IDs to core topology actor from replicator" in new Fixture {
      Given("raft IDs for databases")
      val memberId = new MemberId(UUID.randomUUID)
      val db1Id, db2Id = randomNamedDatabaseId.databaseId()
      val bootstrappedRaft1 = LWWMap.empty.put(cluster, RaftId.from(db1Id), memberId)
      val bootstrappedRaft2 = LWWMap.empty.put(cluster, RaftId.from(db2Id), memberId)

      When("raft IDs updated from replicator")
      replicatedDataActorRef ! Replicator.Changed(dataKey)(bootstrappedRaft1)
      replicatedDataActorRef ! Replicator.Changed(dataKey)(bootstrappedRaft2)

      Then("send updates to core topology actor")
      coreTopologyProbe.expectMsg(new BootstrappedRaftsMessage(bootstrappedRaft1.getEntries.keySet))
      coreTopologyProbe.expectMsg(new BootstrappedRaftsMessage(bootstrappedRaft1.merge(bootstrappedRaft2).getEntries.keySet))
    }
  }

  class Fixture extends ReplicatedDataActorFixture[LWWMap[RaftId,MemberId]] {
    override val dataKey: Key[LWWMap[RaftId,MemberId]] = LWWMapKey(ReplicatedDataIdentifier.RAFT_ID.keyName())
    override val data = LWWMap.empty[RaftId,MemberId]
    val coreTopologyProbe = TestProbe("coreTopologyActor")
    val props = RaftIdActor.props(cluster, replicator.ref, coreTopologyProbe.ref, monitor, 3)
    override val replicatedDataActorRef: ActorRef = system.actorOf(props)
  }
}
