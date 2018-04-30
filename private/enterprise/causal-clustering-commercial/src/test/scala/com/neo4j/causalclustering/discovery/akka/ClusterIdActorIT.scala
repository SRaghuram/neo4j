/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka

import java.util.UUID

import akka.actor.ActorRef
import akka.cluster.ddata.{Key, LWWMap, LWWMapKey, Replicator}
import akka.testkit.TestProbe
import org.neo4j.causalclustering.identity.ClusterId
import org.neo4j.logging.NullLogProvider

class ClusterIdActorIT extends BaseAkkaIT("ClusterIdActorTest") {

  "ClusterIdActor" should {
    behave like replicatedDataActor(new Fixture())

    "update replicator with cluster ID from this core server" in new Fixture {
      When("send cluster ID locally")
      replicatedDataActorRef ! new ClusterIdForDatabase(new ClusterId(UUID.randomUUID()), "dbName")

      Then("update metadata")
      expectReplicatorUpdates(replicator, dataKey)
    }

    "send cluster ID to core topology actor from replicator" in new Fixture {
      Given("cluster IDs for databases")
      val db1 = LWWMap.empty.put(cluster, "db1", new ClusterId(UUID.randomUUID()))
      val db2 = LWWMap.empty.put(cluster, "db2", new ClusterId(UUID.randomUUID()))

      When("cluster IDs updated from replicator")
      replicatedDataActorRef ! Replicator.Changed(dataKey)(db1)
      replicatedDataActorRef ! Replicator.Changed(dataKey)(db2)

      Then("send updates to core topology actor")
      coreTopologyProbe.expectMsg(new ClusterIdDirectoryMessage(db1))
      coreTopologyProbe.expectMsg(new ClusterIdDirectoryMessage(db1.merge(db2)))
    }
  }

  class Fixture extends ReplicatedDataActorFixture[LWWMap[String, ClusterId]] {
    override val dataKey: Key[LWWMap[String, ClusterId]] = LWWMapKey(ClusterIdActor.CLUSTER_ID_PER_DB_KEY)
    val coreTopologyProbe = TestProbe("coreTopologyActor")
    val props = ClusterIdActor.props(cluster, replicator.ref, coreTopologyProbe.ref, NullLogProvider.getInstance())
    override val replicatedDataActorRef: ActorRef = system.actorOf(props)
  }
}
