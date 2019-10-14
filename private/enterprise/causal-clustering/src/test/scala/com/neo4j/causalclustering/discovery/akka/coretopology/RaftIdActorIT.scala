/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.coretopology

import akka.actor.ActorRef
import akka.cluster.ddata.{Key, LWWMap, LWWMapKey, Replicator}
import akka.testkit.TestProbe
import com.neo4j.causalclustering.discovery.akka.BaseAkkaIT
import com.neo4j.causalclustering.discovery.akka.monitoring.ReplicatedDataIdentifier
import com.neo4j.causalclustering.identity.RaftId
import org.neo4j.kernel.database.DatabaseId
import org.neo4j.kernel.database.TestDatabaseIdRepository.randomDatabaseId

class RaftIdActorIT extends BaseAkkaIT("RaftIdActorTest") {

  "RaftIdActor" should {
    behave like replicatedDataActor(new Fixture())

    "update replicator with raft ID from this core server" in new Fixture {
      When("send raft ID locally")
      val databaseId = randomDatabaseId()
      replicatedDataActorRef ! new RaftIdSettingMessage(RaftId.from(databaseId), databaseId)

      Then("update metadata")
      expectReplicatorUpdates(replicator, dataKey)
    }

    "send raft ID to core topology actor from replicator" in new Fixture {
      Given("raft IDs for databases")
      val db1Id = randomDatabaseId()
      val db2Id = randomDatabaseId()
      val db1 = LWWMap.empty.put(cluster, db1Id, RaftId.from(db1Id))
      val db2 = LWWMap.empty.put(cluster, db2Id, RaftId.from(db2Id))

      When("raft IDs updated from replicator")
      replicatedDataActorRef ! Replicator.Changed(dataKey)(db1)
      replicatedDataActorRef ! Replicator.Changed(dataKey)(db2)

      Then("send updates to core topology actor")
      coreTopologyProbe.expectMsg(new RaftIdDirectoryMessage(db1))
      coreTopologyProbe.expectMsg(new RaftIdDirectoryMessage(db1.merge(db2)))
    }
  }

  class Fixture extends ReplicatedDataActorFixture[LWWMap[DatabaseId, RaftId]] {
    override val dataKey: Key[LWWMap[DatabaseId, RaftId]] = LWWMapKey(ReplicatedDataIdentifier.RAFT_ID.keyName())
    override val data = LWWMap.empty[DatabaseId, RaftId]
    val coreTopologyProbe = TestProbe("coreTopologyActor")
    val props = RaftIdActor.props(cluster, replicator.ref, coreTopologyProbe.ref, monitor)
    override val replicatedDataActorRef: ActorRef = system.actorOf(props)
  }
}
