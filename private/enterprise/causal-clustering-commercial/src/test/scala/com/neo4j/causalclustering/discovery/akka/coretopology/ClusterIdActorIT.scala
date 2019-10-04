/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.coretopology

import java.time.Duration
import java.util.UUID

import akka.actor.ActorRef
import akka.cluster.ddata.{Key, LWWMap, LWWMapKey, Replicator}
import akka.testkit.TestProbe
import com.neo4j.causalclustering.discovery.akka.BaseAkkaIT
import org.neo4j.causalclustering.discovery.akka.monitoring.ReplicatedDataIdentifier.CLUSTER_ID
import org.neo4j.causalclustering.identity.ClusterId

class ClusterIdActorIT extends BaseAkkaIT("ClusterIdActorTest") {

  "ClusterIdActor" should {
    behave like replicatedDataActor(new Fixture())

    "update replicator with cluster ID from this core server" in new Fixture {
      When("send cluster ID locally")
      replicatedDataActorRef ! new ClusterIdSetRequest(new ClusterId(UUID.randomUUID()), "dbName", Duration.ofSeconds(2))

      Then("update metadata")
      expectReplicatorUpdates(replicator, dataKey)
    }

    "refuse to update replicators with additional cluster ID" in new Fixture {
      Given("2 cluster IDs for the same database")
      val clusterId1 = new ClusterId(UUID.randomUUID())
      val clusterId2 = new ClusterId(UUID.randomUUID())

      When("Sending both cluster IDs")
      Then("Only the first cluster ID should be persisted")
      val expected = LWWMap.empty.put(cluster, "db1", clusterId1)

      replicatedDataActorRef ! new ClusterIdSetRequest(clusterId1, "db1", Duration.ofSeconds(2))
      expectClusterIdReplicatorUpdatesWithOutcome(LWWMap.empty, expected)
      replicatedDataActorRef ! new ClusterIdSetRequest(clusterId2, "db1", Duration.ofSeconds(2))
      expectClusterIdReplicatorUpdatesWithOutcome(expected, expected)
      replicatedDataActorRef ! new ClusterIdSetRequest(clusterId2, "db2", Duration.ofSeconds(2))
      expectClusterIdReplicatorUpdatesWithOutcome(expected, expected.put(cluster, "db2", clusterId2))
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
    override val dataKey: Key[LWWMap[String, ClusterId]] = LWWMapKey(CLUSTER_ID.keyName())
    override val data = LWWMap.create[String, ClusterId]()
    val coreTopologyProbe = TestProbe("coreTopologyActor")
    val props = ClusterIdActor.props(cluster, replicator.ref, coreTopologyProbe.ref, 3, monitor)
    override val replicatedDataActorRef: ActorRef = system.actorOf(props)

    def expectClusterIdReplicatorUpdatesWithOutcome(initial: LWWMap[String,ClusterId], expected: LWWMap[String,ClusterId]): Unit =
    {
      replicator.fishForSpecificMessage(defaultWaitTime) {
        case update @ Replicator.Update(`dataKey`, _, _) =>
          val mapUpdate = update.asInstanceOf[Replicator.Update[LWWMap[String, ClusterId]]]
          val result: java.util.Map[String,ClusterId] = mapUpdate.modify(Option(initial)).getEntries()
          assert(result == expected.getEntries())
      }
    }
  }
}
