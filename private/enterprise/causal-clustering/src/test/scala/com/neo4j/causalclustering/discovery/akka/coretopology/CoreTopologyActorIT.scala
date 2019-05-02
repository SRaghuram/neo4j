/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.coretopology

import java.util
import java.util.{Collections, UUID}

import akka.actor.Address
import akka.cluster.ddata.LWWMapKey
import akka.cluster.{Cluster, Member, MemberStatus, UniqueAddress}
import akka.stream.javadsl.Source
import akka.stream.scaladsl.Sink
import akka.stream.{ActorMaterializer, OverflowStrategy}
import akka.testkit.TestProbe
import com.neo4j.causalclustering.discovery.akka._
import com.neo4j.causalclustering.discovery.{DatabaseCoreTopology, _}
import com.neo4j.causalclustering.identity.{ClusterId, MemberId}
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.verify
import org.mockito.{ArgumentMatchers, Mockito}
import org.neo4j.configuration.Config
import org.neo4j.kernel.database.DatabaseId
import org.neo4j.logging.NullLogProvider

import scala.collection.JavaConverters._

class CoreTopologyActorIT extends BaseAkkaIT("CoreTopologyActorTest") {

  "CoreTopologyActor" when {
    "receiving messages" should {
      "update topology" when {
        "cluster id updated" in new Fixture {
          Given("updated cluster ID data")
          makeTopologyActorKnowAboutCoreMember()
          val clusterIdData = Map(databaseId -> clusterId).asJava
          val event = new ClusterIdDirectoryMessage(clusterIdData)

          When("data received")
          topologyActorRef ! event

          Then("update topology")
          awaitAssert(
            verify(topologyBuilder).buildCoreTopology(ArgumentMatchers.eq(databaseId), ArgumentMatchers.eq(clusterId), any(), any())
          )
          awaitExpectedCoreTopology()
        }

        "metadata updated" in new Fixture {
          Given("updated metadata")
          val event = newMetadataMessage()

          When("metadata received")
          topologyActorRef ! event

          Then("update topology")
          awaitAssert(
            verify(topologyBuilder).buildCoreTopology(ArgumentMatchers.eq(databaseId), any(), any(), ArgumentMatchers.eq(event))
          )
          awaitExpectedCoreTopology()
        }

        "cluster view updated" in new Fixture {
          Given("a cluster view")
          makeTopologyActorKnowAboutCoreMember()
          val members = new util.TreeSet[Member](Member.ordering)
          members.add(ClusterViewMessageTest.createMember(1, MemberStatus.Up))
          val clusterView = new ClusterViewMessage(false, members, Collections.emptySet() )

          When("cluster view received")
          topologyActorRef ! clusterView

          Then("update topology")
          awaitAssert(
            verify(topologyBuilder).buildCoreTopology(ArgumentMatchers.eq(databaseId), any(), ArgumentMatchers.eq(clusterView), any())
          )
          awaitExpectedCoreTopology()
        }

        "cluster id updated without change in cluster membership" in new Fixture {
          Given("cluster view in known state")
          val members = new util.TreeSet[Member](Member.ordering)
          members.add(ClusterViewMessageTest.createMember(1, MemberStatus.Up))
          val clusterView = new ClusterViewMessage(false, members, Collections.emptySet() )
          val nullClusterIdTopology = new DatabaseCoreTopology(databaseId, null, expectedCoreTopology.canBeBootstrapped, expectedCoreTopology.members())
          Mockito.when(topologyBuilder.buildCoreTopology(ArgumentMatchers.eq(databaseId), any(), any(), any()))
              .thenReturn(nullClusterIdTopology)
          topologyActorRef ! clusterView
          makeTopologyActorKnowAboutCoreMember(nullClusterIdTopology)

          When("update cluster ID")
          val clusterIdData = Map(databaseId -> clusterId).asJava
          val event = new ClusterIdDirectoryMessage(clusterIdData)
          Mockito.when(topologyBuilder.buildCoreTopology(ArgumentMatchers.eq(databaseId), any(), any(), any()))
            .thenReturn(expectedCoreTopology)
          topologyActorRef ! event

          Then("update topology")
          awaitExpectedCoreTopology()
        }

        "database core topology updated to empty when database does not exist" in new Fixture {
          Given("metadata updated with default database")
          makeTopologyActorKnowAboutCoreMember()

          val otherDatabaseId = new DatabaseId("non-default")
          val otherTopology = new DatabaseCoreTopology(otherDatabaseId, clusterId, false, Map(new MemberId(UUID.randomUUID()) -> coreServerInfo(42)).asJava)
          val emptyTopology = new DatabaseCoreTopology(databaseId, clusterId, false, Map.empty[MemberId, CoreServerInfo].asJava)

          Mockito.when(topologyBuilder.buildCoreTopology(ArgumentMatchers.eq(otherDatabaseId), any(), any(), any())).thenReturn(otherTopology)
          Mockito.when(topologyBuilder.buildCoreTopology(ArgumentMatchers.eq(databaseId), any(), any(), any())).thenReturn(emptyTopology)

          When("received metadata with a different database")
          val message = newMetadataMessage(otherDatabaseId)
          topologyActorRef ! message

          Then("update default database topology to empty")
          awaitExpectedCoreTopology(emptyTopology) // receive empty topology for default database because it was removed
          awaitExpectedCoreTopology(otherTopology) // receive non-empty topology for non-default database because it was added
        }
      }
    }
  }

  trait Fixture {
    val coreTopologyReceiver = TestProbe("CoreTopologyReceiver")

    val materializer = ActorMaterializer()

    val topologySink = Source.queue[CoreTopologyMessage](1, OverflowStrategy.dropHead)
      .to(Sink.foreach(msg => coreTopologyReceiver.ref ! msg.coreTopology()))
      .run(materializer)

    val config = {
      val conf = Config.defaults()
      val myCoreServerConfig = TestTopology.configFor(coreServerInfo(0))
      conf.augment(myCoreServerConfig)
      conf
    }

    val clusterId = new ClusterId(UUID.randomUUID())
    val databaseId = new DatabaseId("default")

    val replicatorProbe = TestProbe("replicator")

    val memberDataKey = LWWMapKey[UniqueAddress, CoreServerInfoForMemberId](MetadataActor.MEMBER_DATA_KEY)
    val clusterIdKey = LWWMapKey[String, ClusterId](ClusterIdActor.CLUSTER_ID_PER_DB_KEY)

    val topologyBuilder = mock[TopologyBuilder]
    val expectedCoreTopology = new DatabaseCoreTopology(
      databaseId,
      clusterId,
      false,
      Map(
        new MemberId(UUID.randomUUID()) -> coreServerInfo(0),
        new MemberId(UUID.randomUUID()) -> coreServerInfo(1)
      ).asJava
    )
    Mockito.when(topologyBuilder.buildCoreTopology(ArgumentMatchers.eq(databaseId), any(), any(), any()))
      .thenReturn(expectedCoreTopology)

    val readReplicaProbe = TestProbe("readReplicaActor")
    val cluster = mock[Cluster]
    val myAddress = Address("akka", system.name, "myHost", 12)
    Mockito.when(cluster.selfAddress).thenReturn(myAddress)

    val props = CoreTopologyActor.props(
      new TestDiscoveryMember(),
      topologySink,
      readReplicaProbe.ref,
      replicatorProbe.ref,
      cluster,
      topologyBuilder,
      config,
      NullLogProvider.getInstance())
    val topologyActorRef = system.actorOf(props)

    def awaitExpectedCoreTopology(newCoreTopology: DatabaseCoreTopology = expectedCoreTopology) = {
      awaitAssert(coreTopologyReceiver.receiveOne(defaultWaitTime) should equal(newCoreTopology), max = defaultWaitTime)
      readReplicaProbe.expectMsg(newCoreTopology)
    }

    def makeTopologyActorKnowAboutCoreMember(newCoreTopology: DatabaseCoreTopology = expectedCoreTopology): Unit = {
      val metadataEvent = newMetadataMessage(databaseId)
      topologyActorRef ! metadataEvent
      awaitExpectedCoreTopology(newCoreTopology)
    }

    def newMetadataMessage(databaseId: DatabaseId = databaseId): MetadataMessage = {
      val info = new CoreServerInfoForMemberId(new MemberId(UUID.randomUUID()), coreServerInfo(1, databaseId))
      val metadata = Map(UniqueAddress(Address("protocol", "system"), 1L) -> info).asJava
      new MetadataMessage(metadata)
    }

    def coreServerInfo(serverId: Int, databaseId: DatabaseId = databaseId): CoreServerInfo =
      TestTopology.addressesForCore(serverId, false, Set(databaseId).asJava)
  }
}
