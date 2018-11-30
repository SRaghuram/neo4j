/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.verify
import org.mockito.{ArgumentMatchers, Mockito}
import org.neo4j.causalclustering.discovery._
import org.neo4j.causalclustering.identity.{ClusterId, MemberId}
import org.neo4j.kernel.configuration.Config
import org.neo4j.logging.NullLogProvider

import scala.collection.JavaConverters._

class CoreTopologyActorIT extends BaseAkkaIT("CoreTopologyActorTest") {

  "CoreTopologyActor" when {
    "receiving messages" should {
      "update topology" when {
        "cluster id updated" in new Fixture {
          Given("updated cluster ID data")
          val clusterIdData = Map(databaseName -> clusterId).asJava
          val event = new ClusterIdDirectoryMessage(clusterIdData)

          When("data received")
          topologyActorRef ! event

          Then("update topology")
          awaitAssert(
            verify(topologyBuilder).buildCoreTopology(ArgumentMatchers.eq(clusterId), any(), any())
          )
          awaitExpectedCoreTopology()
        }

        "metadata updated" in new Fixture {
          Given("updated metadata")
          val metadata = Map(
            UniqueAddress(Address("protocol", "system", "host", 1),1L) ->
              new CoreServerInfoForMemberId(new MemberId(UUID.randomUUID()), TestTopology.addressesForCore(1, false))).asJava
          val event = new MetadataMessage(metadata)

          When("metadata received")
          topologyActorRef ! event

          Then("update topology")
          awaitAssert(
            verify(topologyBuilder).buildCoreTopology(any(), any(), ArgumentMatchers.eq(event))
          )
          awaitExpectedCoreTopology()
        }

        "cluster view updated" in new Fixture {
          Given("a cluster view")
          val members = new util.TreeSet[Member](Member.ordering)
          members.add(ClusterViewMessageTest.createMember(1, MemberStatus.Up))
          val clusterView = new ClusterViewMessage(false, members, Collections.emptySet() )

          When("cluster view received")
          topologyActorRef ! clusterView

          Then("update topology")
          awaitAssert(
            verify(topologyBuilder).buildCoreTopology(any(), ArgumentMatchers.eq(clusterView), any())
          )
          awaitExpectedCoreTopology()
        }

        "cluster id updated without change in cluster membership" in new Fixture {
          Given("cluster view in known state")
          val members = new util.TreeSet[Member](Member.ordering)
          members.add(ClusterViewMessageTest.createMember(1, MemberStatus.Up))
          val clusterView = new ClusterViewMessage(false, members, Collections.emptySet() )
          val nullClusterIdTopology = new CoreTopology(null, expectedCoreTopology.canBeBootstrapped, expectedCoreTopology.members())
          Mockito.when(topologyBuilder.buildCoreTopology(any(), any(), any()))
              .thenReturn(nullClusterIdTopology)
          topologyActorRef ! clusterView
          awaitExpectedCoreTopology(nullClusterIdTopology)

          When("update cluster ID")
          val clusterIdData = Map(databaseName -> clusterId).asJava
          val event = new ClusterIdDirectoryMessage(clusterIdData)
          Mockito.when(topologyBuilder.buildCoreTopology(any(), any(), any()))
            .thenReturn(expectedCoreTopology)
          topologyActorRef ! event

          Then("update topology")
          awaitExpectedCoreTopology()
        }
      }
      "not update topology" when {
        "core topology has not changed" in new Fixture {
          Given("actor has a known topology")
          topologyActorRef ! ClusterViewMessage.EMPTY
          awaitExpectedCoreTopology()

          And("received state is reset")
          actualCoreTopology = CoreTopology.EMPTY

          When("generate a new topology with same members")
          topologyActorRef ! ClusterViewMessage.EMPTY

          Then("no further topology changes")
          awaitNoChangeToCoreTopology()
        }
      }

    }
  }

  trait Fixture {
    var actualCoreTopology = CoreTopology.EMPTY
    var actualReadReplicaTopology = ReadReplicaTopology.EMPTY

    val updateSink = new TopologyUpdateSink {
      override def onTopologyUpdate(topology: CoreTopology) = actualCoreTopology = topology

      override def onTopologyUpdate(topology: ReadReplicaTopology) = actualReadReplicaTopology = topology
    }

    val materializer = ActorMaterializer()

    val topologySink = Source.queue[CoreTopologyMessage](1, OverflowStrategy.dropHead)
      .to(Sink.foreach(msg => updateSink.onTopologyUpdate(msg.coreTopology)))
      .run(materializer)

    val config = {
      val conf = Config.defaults()
      val myCoreServerConfig = TestTopology.configFor(TestTopology.addressesForCore(0, false))
      conf.augment(myCoreServerConfig)
      conf
    }

    val clusterId = new ClusterId(UUID.randomUUID())
    val databaseName = "default"

    val replicatorProbe = TestProbe("replicator")

    val memberDataKey = LWWMapKey[UniqueAddress, CoreServerInfoForMemberId](MetadataActor.MEMBER_DATA_KEY)
    val clusterIdKey = LWWMapKey[String, ClusterId](ClusterIdActor.CLUSTER_ID_PER_DB_KEY)

    val topologyBuilder = mock[TopologyBuilder]
    val expectedCoreTopology = new CoreTopology(
      clusterId,
      false,
      Map(
        new MemberId(UUID.randomUUID()) -> TestTopology.addressesForCore(0, false),
        new MemberId(UUID.randomUUID()) -> TestTopology.addressesForCore(1, false)
      ).asJava
    )
    Mockito.when(topologyBuilder.buildCoreTopology(any(), any(), any()))
      .thenReturn(expectedCoreTopology)

    val readReplicaProbe = TestProbe("readReplicaActor")
    val cluster = mock[Cluster]
    val myAddress = Address("akka", system.name, "myHost", 12)
    Mockito.when(cluster.selfAddress).thenReturn(myAddress)

    val props = CoreTopologyActor.props(
      new MemberId(UUID.randomUUID()),
      topologySink,
      readReplicaProbe.ref,
      replicatorProbe.ref,
      cluster,
      topologyBuilder,
      config,
      NullLogProvider.getInstance())
    val topologyActorRef = system.actorOf(props)

    def awaitExpectedCoreTopology(newCoreTopology: CoreTopology = expectedCoreTopology) = {
      awaitCond(
        actualCoreTopology == newCoreTopology && actualCoreTopology.clusterId() == newCoreTopology.clusterId(),
        max = defaultWaitTime,
        message = s"Expected $newCoreTopology but was $actualCoreTopology"
      )
      readReplicaProbe.expectMsg(newCoreTopology)
    }

    def awaitNoChangeToCoreTopology() = awaitNotCond(
      actualCoreTopology == expectedCoreTopology && actualCoreTopology.clusterId() == expectedCoreTopology.clusterId(),
      max = defaultWaitTime,
      message = "Should not have updated core topology")
  }
}
