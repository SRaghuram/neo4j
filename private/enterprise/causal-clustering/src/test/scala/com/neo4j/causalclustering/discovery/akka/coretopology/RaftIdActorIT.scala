/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.coretopology

import java.util.UUID

import akka.actor.ActorRef
import akka.cluster.ddata.{Key, LWWMap, LWWMapKey, Replicator}
import akka.testkit.TestProbe
import com.neo4j.causalclustering.discovery.akka.BaseAkkaIT
import com.neo4j.causalclustering.discovery.akka.monitoring.ReplicatedDataIdentifier
import com.neo4j.causalclustering.identity.{MemberId, RaftId}
import org.neo4j.kernel.database.TestDatabaseIdRepository.randomDatabaseId

class RaftIdActorIT extends BaseAkkaIT("RaftIdActorTest") {

  "RaftIdActor" should {
    behave like replicatedDataActor(new Fixture())

    "update replicator with raft ID from this core server" in new Fixture {
      When("send raft ID locally")
      val memberId = new MemberId(UUID.randomUUID)
      replicatedDataActorRef ! new RaftIdSetRequest(RaftId.from(randomDatabaseId), memberId, java.time.Duration.ofSeconds( 2 ))

      Then("update metadata")
      expectReplicatorUpdates(replicator, dataKey)
    }

    "send bootstrapped raft IDs to core topology actor from replicator" in new Fixture {
      Given("raft IDs for databases")
      val memberId = new MemberId(UUID.randomUUID)
      val db1Id = randomDatabaseId
      val db2Id = randomDatabaseId
      val bootstrappedRaft1 = LWWMap.empty.put(cluster, RaftId.from(db1Id), memberId)
      val bootstrappedRaft2 = LWWMap.empty.put(cluster, RaftId.from(db2Id), memberId)

      When("raft IDs updated from replicator")
      replicatedDataActorRef ! Replicator.Changed(dataKey)(bootstrappedRaft1)
      replicatedDataActorRef ! Replicator.Changed(dataKey)(bootstrappedRaft2)

      Then("send updates to core topology actor")
      coreTopologyProbe.expectMsg(new BootstrappedRaftsMessage(bootstrappedRaft1.getEntries.keySet))
      coreTopologyProbe.expectMsg(new BootstrappedRaftsMessage(bootstrappedRaft1.merge(bootstrappedRaft2).getEntries.keySet))
    }

    "refuse to update replicators with additional raft ID" in new Fixture {
      Given("a raft id and 2 publishers")
      val databaseId = randomDatabaseId
      val raftId = RaftId.from(databaseId)
      val memberId1 = new MemberId(UUID.randomUUID)
      val memberId2 = new MemberId(UUID.randomUUID)

      When("2 members are both sending both raft IDs")
      Then("Only the first cluster ID should be persisted")
      val expected = LWWMap.empty.put(cluster, raftId, memberId1)

      replicatedDataActorRef ! new RaftIdSetRequest(raftId, memberId1, java.time.Duration.ofSeconds( 2 ))
      expectClusterIdReplicatorUpdatesWithOutcome(LWWMap.empty, expected)
      replicatedDataActorRef ! new RaftIdSetRequest(raftId, memberId2, java.time.Duration.ofSeconds( 2 ))
      expectClusterIdReplicatorUpdatesWithOutcome(expected, expected)
    }
  }

  class Fixture extends ReplicatedDataActorFixture[LWWMap[RaftId,MemberId]] {
    override val dataKey: Key[LWWMap[RaftId,MemberId]] = LWWMapKey(ReplicatedDataIdentifier.RAFT_ID.keyName())
    override val data = LWWMap.empty[RaftId,MemberId]
    val coreTopologyProbe = TestProbe("coreTopologyActor")
    val props = RaftIdActor.props(cluster, replicator.ref, coreTopologyProbe.ref, monitor, 3)
    override val replicatedDataActorRef: ActorRef = system.actorOf(props)

    def expectClusterIdReplicatorUpdatesWithOutcome(initial: LWWMap[RaftId,MemberId], expected: LWWMap[RaftId,MemberId]): Unit =
    {
      replicator.fishForSpecificMessage(defaultWaitTime) {
        case update @ Replicator.Update(`dataKey`, _, _) =>
          val mapUpdate = update.asInstanceOf[Replicator.Update[LWWMap[RaftId,MemberId]]]
          val result: java.util.Map[RaftId,MemberId] = mapUpdate.modify(Option(initial)).getEntries()
          assert(result == expected.getEntries())
      }
    }
  }
}
