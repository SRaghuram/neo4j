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
import com.neo4j.causalclustering.identity.{MemberId, RaftId}
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.verify
import org.mockito.{ArgumentMatchers, Mockito}
import org.neo4j.configuration.Config
import org.neo4j.kernel.database.{DatabaseId, TestDatabaseIdRepository}
import org.neo4j.logging.NullLogProvider

import scala.collection.JavaConverters._

class CoreTopologyActorIT extends BaseAkkaIT("CoreTopologyActorIT") {

  "CoreTopologyActor" when {
    "receiving messages" should {
      "update topology" when {
        "raft id updated" in new Fixture {
          Given("updated raft ID data")
          makeTopologyActorKnowAboutCoreMember()
          val raftIdData = Map(databaseId -> raftId).asJava
          val event = new RaftIdDirectoryMessage(raftIdData)

          When("data received")
          topologyActorRef ! event

          Then("update topology")
          awaitAssert(
            verify(topologyBuilder).buildCoreTopology(ArgumentMatchers.eq(databaseId), ArgumentMatchers.eq(raftId), any(), any())
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

        "raft view updated" in new Fixture {
          Given("a raft view")
          makeTopologyActorKnowAboutCoreMember()
          val members = new util.TreeSet[Member](Member.ordering)
          members.add(ClusterViewMessageTest.createMember(1, MemberStatus.Up))
          val clusterView = new ClusterViewMessage(false, members, Collections.emptySet() )

          When("raft view received")
          topologyActorRef ! clusterView

          Then("update topology")
          awaitAssert(
            verify(topologyBuilder).buildCoreTopology(ArgumentMatchers.eq(databaseId), any(), ArgumentMatchers.eq(clusterView), any())
          )
          awaitExpectedCoreTopology()
        }

        "raft id updated without change in cluster membership" in new Fixture {
          Given("cluster view in known state")
          val members = new util.TreeSet[Member](Member.ordering)
          members.add(ClusterViewMessageTest.createMember(1, MemberStatus.Up))
          val clusterView = new ClusterViewMessage(false, members, Collections.emptySet() )
          val nullRaftIdTopology = new DatabaseCoreTopology(databaseId, null, expectedCoreTopology.members())
          Mockito.when(topologyBuilder.buildCoreTopology(ArgumentMatchers.eq(databaseId), any(), any(), any()))
              .thenReturn(nullRaftIdTopology)
          topologyActorRef ! clusterView
          makeTopologyActorKnowAboutCoreMember(nullRaftIdTopology)

          When("update raft ID")
          val raftIdData = Map(databaseId -> raftId).asJava
          val event = new RaftIdDirectoryMessage(raftIdData)
          Mockito.when(topologyBuilder.buildCoreTopology(ArgumentMatchers.eq(databaseId), any(), any(), any()))
            .thenReturn(expectedCoreTopology)
          topologyActorRef ! event

          Then("update topology")
          awaitExpectedCoreTopology()
        }

        "database core topology updated to empty when database does not exist" in new Fixture {
          Given("metadata updated with default database")
          makeTopologyActorKnowAboutCoreMember()

          val otherDatabaseId = databaseIdRepository.get("non-default")
          val otherTopology = new DatabaseCoreTopology(otherDatabaseId, raftId, Map(new MemberId(UUID.randomUUID()) -> coreServerInfo(42)).asJava)
          val emptyTopology = new DatabaseCoreTopology(databaseId, raftId, Map.empty[MemberId, CoreServerInfo].asJava)

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

      "update bootstrap state" when {

        "metadata updated" in new Fixture {
          Given("metadata message")
          val metadataMessage = newMetadataMessage()

          When("message received")
          topologyActorRef ! metadataMessage

          Then("update bootstrap state")
          val expectedBootstrapState = new BootstrapState(ClusterViewMessage.EMPTY, metadataMessage, myUniqueAddress, config)
          bootstrapStateReceiver.receiveOne(defaultWaitTime) should equal(expectedBootstrapState)
        }

        "cluster id updated" in new Fixture {
          Given("metadata and cluster ID messages")
          val metadataMessage = newMetadataMessage(databaseId)
          val raftIdMessage = new RaftIdDirectoryMessage(Map(databaseId -> raftId).asJava)

          When("messages received")
          topologyActorRef ! metadataMessage
          topologyActorRef ! raftIdMessage

          Then("update bootstrap state")
          val expectedBootstrapState = new BootstrapState(ClusterViewMessage.EMPTY, metadataMessage, myUniqueAddress, config)
          bootstrapStateReceiver.receiveOne(defaultWaitTime) should equal(expectedBootstrapState)
          bootstrapStateReceiver.receiveOne(defaultWaitTime) should equal(expectedBootstrapState)
        }

        "cluster view updated" in new Fixture {
          Given("metadata and cluster view messages")
          val metadataMessage = newMetadataMessage(databaseId)
          val members = new util.TreeSet[Member](Member.ordering)
          members.add(ClusterViewMessageTest.createMember(1, MemberStatus.Up))
          val clusterViewMessage = new ClusterViewMessage(false, members, Collections.emptySet())

          When("messages received")
          topologyActorRef ! metadataMessage
          topologyActorRef ! clusterViewMessage

          Then("update bootstrap state")
          val expectedBootstrapState1 = new BootstrapState(ClusterViewMessage.EMPTY, metadataMessage, myUniqueAddress, config)
          bootstrapStateReceiver.receiveOne(defaultWaitTime) should equal(expectedBootstrapState1)

          val expectedBootstrapState2 = new BootstrapState(clusterViewMessage, metadataMessage, myUniqueAddress, config)
          bootstrapStateReceiver.receiveOne(defaultWaitTime) should equal(expectedBootstrapState2)
        }
      }
    }
  }

  trait Fixture {
    val coreTopologyReceiver = TestProbe("CoreTopologyReceiver")
    val bootstrapStateReceiver = TestProbe("BootstrapStateReceiver")

    val materializer = ActorMaterializer()

    val topologySink = Source.queue[CoreTopologyMessage](1, OverflowStrategy.dropHead)
      .to(Sink.foreach(msg => coreTopologyReceiver.ref ! msg.coreTopology()))
      .run(materializer)

    val bootstrapStateSink = Source.queue[BootstrapState](1, OverflowStrategy.dropHead)
      .to(Sink.foreach(msg => bootstrapStateReceiver.ref ! msg))
      .run(materializer)

    val config = {
      val conf = Config.defaults()
      val myCoreServerConfig = TestTopology.configFor(coreServerInfo(0))
      conf.augment(myCoreServerConfig)
      conf
    }

    val databaseIdRepository = new TestDatabaseIdRepository()
    val databaseId = databaseIdRepository.get("default")
    val raftId = new RaftId(UUID.randomUUID())

    val replicatorProbe = TestProbe("replicator")

    val memberDataKey = LWWMapKey[UniqueAddress, CoreServerInfoForMemberId](MetadataActor.MEMBER_DATA_KEY)
    val raftIdKey = LWWMapKey[String, RaftId](RaftIdActor.RAFT_ID_PER_DB_KEY)

    val topologyBuilder = mock[TopologyBuilder]
    val expectedCoreTopology = new DatabaseCoreTopology(
      databaseId,
      raftId,
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
    val myUniqueAddress = UniqueAddress(myAddress, 42L)
    Mockito.when(cluster.selfAddress).thenReturn(myAddress)
    Mockito.when(cluster.selfUniqueAddress).thenReturn(myUniqueAddress)

    val props = CoreTopologyActor.props(
      new TestDiscoveryMember(),
      topologySink,
      bootstrapStateSink,
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
