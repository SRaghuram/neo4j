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
import com.neo4j.causalclustering.identity.RaftId
import org.neo4j.kernel.database.DatabaseId
import org.neo4j.logging.NullLogProvider

class RaftIdActorIT extends BaseAkkaIT("RaftIdActorTest") {

  "RaftIdActor" should {
    behave like replicatedDataActor(new Fixture())

    "update replicator with raft ID from this core server" in new Fixture {
      When("send raft ID locally")
      replicatedDataActorRef ! new RaftIdSettingMessage(new RaftId(UUID.randomUUID()), new DatabaseId("dbName"))

      Then("update metadata")
      expectReplicatorUpdates(replicator, dataKey)
    }

    "send raft ID to core topology actor from replicator" in new Fixture {
      Given("raft IDs for databases")
      val db1 = LWWMap.empty.put(cluster, new DatabaseId("db1"), new RaftId(UUID.randomUUID()))
      val db2 = LWWMap.empty.put(cluster, new DatabaseId("db2"), new RaftId(UUID.randomUUID()))

      When("raft IDs updated from replicator")
      replicatedDataActorRef ! Replicator.Changed(dataKey)(db1)
      replicatedDataActorRef ! Replicator.Changed(dataKey)(db2)

      Then("send updates to core topology actor")
      coreTopologyProbe.expectMsg(new RaftIdDirectoryMessage(db1))
      coreTopologyProbe.expectMsg(new RaftIdDirectoryMessage(db1.merge(db2)))
    }
  }

  class Fixture extends ReplicatedDataActorFixture[LWWMap[DatabaseId, RaftId]] {
    override val dataKey: Key[LWWMap[DatabaseId, RaftId]] = LWWMapKey(RaftIdActor.RAFT_ID_PER_DB_KEY)
    val coreTopologyProbe = TestProbe("coreTopologyActor")
    val props = RaftIdActor.props(cluster, replicator.ref, coreTopologyProbe.ref, NullLogProvider.getInstance())
    override val replicatedDataActorRef: ActorRef = system.actorOf(props)
  }
}
