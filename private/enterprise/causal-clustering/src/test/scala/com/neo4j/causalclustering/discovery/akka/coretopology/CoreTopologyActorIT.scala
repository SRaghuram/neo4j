/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.coretopology

import java.util
import java.util.Collections

import akka.actor.Address
import akka.cluster.Cluster
import akka.cluster.Member
import akka.cluster.MemberStatus
import akka.cluster.UniqueAddress
import akka.cluster.ddata.LWWMapKey
import akka.stream.ActorMaterializer
import akka.stream.OverflowStrategy
import akka.stream.javadsl.Source
import akka.stream.scaladsl.Sink
import akka.testkit.TestActorRef
import akka.testkit.TestProbe
import com.neo4j.causalclustering.discovery.CoreServerInfo
import com.neo4j.causalclustering.discovery.DatabaseCoreTopology
import com.neo4j.causalclustering.discovery.ReplicatedRaftMapping
import com.neo4j.causalclustering.discovery.TestCoreDiscoveryMember
import com.neo4j.causalclustering.discovery.TestTopology
import com.neo4j.causalclustering.discovery.akka.BaseAkkaIT
import com.neo4j.causalclustering.discovery.akka.monitoring.ClusterSizeMonitor
import com.neo4j.causalclustering.discovery.akka.monitoring.ReplicatedDataIdentifier
import com.neo4j.causalclustering.discovery.akka.monitoring.ReplicatedDataMonitor
import com.neo4j.causalclustering.identity.IdFactory
import com.neo4j.causalclustering.identity.InMemoryCoreServerIdentity
import com.neo4j.causalclustering.identity.RaftGroupId
import com.neo4j.causalclustering.identity.RaftMemberId
import org.mockito.ArgumentMatchers
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito
import org.mockito.Mockito.verify
import org.neo4j.configuration.Config
import org.neo4j.dbms.identity.ServerId
import org.neo4j.kernel.database.DatabaseId
import org.neo4j.kernel.database.TestDatabaseIdRepository.randomNamedDatabaseId

import scala.collection.JavaConverters.mapAsJavaMapConverter
import scala.collection.JavaConverters.setAsJavaSetConverter

class CoreTopologyActorIT extends BaseAkkaIT("CoreTopologyActorIT") {

  "CoreTopologyActor" when {
    "receiving messages" should {
      "update topology" when {
        "raft id updated" in new Fixture {
          Given("updated raft ID data")
          sendInitialData()
          makeTopologyActorKnowAboutCoreMember()
          val event = new BootstrappedRaftsMessage(Map(raftId -> IdFactory.randomRaftMemberId).asJava)

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
          sendInitialData()
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
          sendInitialData()
          makeTopologyActorKnowAboutCoreMember()
          val members = initialMembers()
          members.add(ClusterViewMessageTest.createMember(1, MemberStatus.Up))
          val clusterView = new ClusterViewMessage(false, members, Collections.emptySet())

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
          val members = initialMembers()
          members.add(ClusterViewMessageTest.createMember(1, MemberStatus.Up))
          val clusterView = new ClusterViewMessage(false, members, Collections.emptySet())
          val nullRaftIdTopology = new DatabaseCoreTopology(databaseId, null, expectedCoreTopology.servers())
          Mockito.when(topologyBuilder.buildCoreTopology(ArgumentMatchers.eq(databaseId), any(), any(), any()))
            .thenReturn(nullRaftIdTopology)
          topologyActorRef ! clusterView
          makeTopologyActorKnowAboutCoreMember(nullRaftIdTopology)

          When("update raft ID")
          val event = new BootstrappedRaftsMessage(Map(raftId -> IdFactory.randomRaftMemberId).asJava)
          Mockito.when(topologyBuilder.buildCoreTopology(ArgumentMatchers.eq(databaseId), any(), any(), any()))
            .thenReturn(expectedCoreTopology)
          topologyActorRef ! event

          Then("update topology")
          awaitExpectedCoreTopology()
        }

        "database core topology updated to empty when database does not exist" in new Fixture {
          Given("metadata updated with default database")
          sendInitialData()
          makeTopologyActorKnowAboutCoreMember()

          val otherDatabaseId = randomNamedDatabaseId().databaseId()
          val otherTopology = new DatabaseCoreTopology(otherDatabaseId, raftId, Map(IdFactory.randomServerId() -> coreServerInfo(42)).asJava)
          val emptyTopology = new DatabaseCoreTopology(databaseId, raftId, Map.empty[ServerId, CoreServerInfo].asJava)

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
          val clusterView = sendInitialData()
          val metadataMessage = newMetadataMessage()

          When("message received")
          topologyActorRef ! metadataMessage

          Then("update bootstrap state")
          val expectedBootstrapState = new BootstrapState(clusterView, metadataMessage,
            myUniqueAddress, config, Map.empty[RaftGroupId,RaftMemberId].asJava)
          bootstrapStateReceiver.receiveOne(defaultWaitTime) should equal(expectedBootstrapState)
        }

        "bootstrapped RaftId->MemberId mappings updated" in new Fixture {
          Given("metadata and bootstrapped raft messages")
          val clusterView = sendInitialData()
          val metadataMessage = newMetadataMessage(databaseId)
          val previouslyBootstrapped = Map(raftId -> IdFactory.randomRaftMemberId).asJava
          val bootstrappedRaftsMessage = new BootstrappedRaftsMessage(previouslyBootstrapped)

          val expectedBootstrapState1 = new BootstrapState(clusterView, metadataMessage,
            myUniqueAddress, config, Map.empty[RaftGroupId,RaftMemberId].asJava)
          val expectedBootstrapState2 = new BootstrapState(clusterView, metadataMessage,
            myUniqueAddress, config, previouslyBootstrapped)

          When("metadata message received")
          topologyActorRef ! metadataMessage

          Then("update bootstrap state with none bootstrapped")
          bootstrapStateReceiver.receiveOne(defaultWaitTime) should equal(expectedBootstrapState1)

          When("bootstrapped rafts message received")
          topologyActorRef ! bootstrappedRaftsMessage

          Then("update bootstrap state with one bootstrapped")
          bootstrapStateReceiver.receiveOne(defaultWaitTime) should equal(expectedBootstrapState2)
        }

        "cluster view updated" in new Fixture {
          Given("metadata and cluster view messages")
          val initialClusterView = sendInitialData()
          val metadataMessage = newMetadataMessage()
          val members = initialMembers()
          members.add(ClusterViewMessageTest.createMember(1, MemberStatus.Up))
          val clusterViewMessage = new ClusterViewMessage(false, members, Collections.emptySet())

          When("messages received")
          topologyActorRef ! metadataMessage
          topologyActorRef ! clusterViewMessage

          Then("update bootstrap state")
          val expectedBootstrapState1 = new BootstrapState(initialClusterView, metadataMessage,
            myUniqueAddress, config, Map.empty[RaftGroupId,RaftMemberId].asJava)
          bootstrapStateReceiver.receiveOne(defaultWaitTime) should equal(expectedBootstrapState1)

          val expectedBootstrapState2 = new BootstrapState(clusterViewMessage, metadataMessage,
            myUniqueAddress, config, Map.empty[RaftGroupId,RaftMemberId].asJava)
          bootstrapStateReceiver.receiveOne(defaultWaitTime) should equal(expectedBootstrapState2)
        }
      }
    }
  }

  trait Fixture {
    val coreTopologyReceiver = TestProbe("CoreTopologyReceiver")
    val bootstrapStateReceiver = TestProbe("BootstrapStateReceiver")
    val mappingReceiver = TestProbe("MappingReceiver")

    val materializer = ActorMaterializer()

    val topologySink = Source.queue[CoreTopologyMessage](1, OverflowStrategy.dropHead)
      .to(Sink.foreach(msg => coreTopologyReceiver.ref ! msg.coreTopology()))
      .run(materializer)

    val bootstrapStateSink = Source.queue[BootstrapState](1, OverflowStrategy.dropHead)
      .to(Sink.foreach(msg => bootstrapStateReceiver.ref ! msg))
      .run(materializer)

    val raftMappingSink = Source.queue[ReplicatedRaftMapping](1, OverflowStrategy.dropHead)
      .to(Sink.foreach(msg => mappingReceiver.ref ! msg))
      .run(materializer)

    val config = {
      val myCoreServerConfig = TestTopology.configFor(coreServerInfo(0))
      val conf = Config.newBuilder().fromConfig(myCoreServerConfig).build()
      conf
    }

    val databaseId = randomNamedDatabaseId().databaseId()
    val raftId = RaftGroupId.from(databaseId)

    val replicatorProbe = TestProbe("replicator")

    val memberDataKey = LWWMapKey[UniqueAddress, CoreServerInfoForServerId](ReplicatedDataIdentifier.METADATA.keyName())
    val raftIdKey = LWWMapKey[String, RaftGroupId](ReplicatedDataIdentifier.RAFT_ID_PUBLISHER.keyName())

    val topologyBuilder = mock[TopologyBuilder]
    val expectedCoreTopology = new DatabaseCoreTopology(databaseId, raftId, Map(
      IdFactory.randomServerId() -> coreServerInfo(0),
      IdFactory.randomServerId() -> coreServerInfo(1)
    ).asJava)
    Mockito.when(topologyBuilder.buildCoreTopology(ArgumentMatchers.eq(databaseId), any(), any(), any()))
      .thenReturn(expectedCoreTopology)

    val readReplicaProbe = TestProbe("readReplicaActor")
    val cluster = mock[Cluster]
    val myAddress = Address("akka", system.name, "myHost", 12)
    val myUniqueAddress = UniqueAddress(myAddress, 42L)
    val identityModule = new InMemoryCoreServerIdentity()
    Mockito.when(cluster.selfAddress).thenReturn(myAddress)
    Mockito.when(cluster.selfUniqueAddress).thenReturn(myUniqueAddress)

    val props = CoreTopologyActor.props(
      topologySink,
      bootstrapStateSink,
      raftMappingSink,
      readReplicaProbe.ref,
      replicatorProbe.ref,
      cluster,
      topologyBuilder,
      config,
      identityModule,
      mock[ReplicatedDataMonitor],
      mock[ClusterSizeMonitor])

    val topologyActorRef = TestActorRef.create[CoreTopologyActor](system, props)
    topologyActorRef.underlyingActor.onMemberUp()

    def awaitExpectedCoreTopology(newCoreTopology: DatabaseCoreTopology = expectedCoreTopology) = {
      awaitAssert(coreTopologyReceiver.receiveOne(defaultWaitTime) should equal(newCoreTopology), max = defaultWaitTime)
      readReplicaProbe.expectMsg(newCoreTopology)
    }

    def makeTopologyActorKnowAboutCoreMember(newCoreTopology: DatabaseCoreTopology = expectedCoreTopology): Unit = {
      val metadataEvent = newMetadataMessage()
      topologyActorRef ! metadataEvent
      awaitExpectedCoreTopology(newCoreTopology)
    }

    def newMetadataMessage(databaseId: DatabaseId = databaseId): MetadataMessage = {
      val myInfo = new CoreServerInfoForServerId(identityModule.serverId(), coreServerInfo(1, databaseId))
      val otherInfo = new CoreServerInfoForServerId(IdFactory.randomServerId(), coreServerInfo(2, databaseId))
      val metadata = Map(
        myUniqueAddress -> myInfo,
        UniqueAddress(Address("protocol", "system"), 1L) -> otherInfo
      ).asJava
      new MetadataMessage(metadata)
    }

    def initialMembers(): util.TreeSet[Member] = {
      val initialMembers = new util.TreeSet[Member](Member.ordering)
      initialMembers.add(ClusterViewMessageTest.createMember(myUniqueAddress, MemberStatus.Up))
      initialMembers
    }

    def coreServerInfo(serverId: Int, databaseId: DatabaseId = databaseId): CoreServerInfo =
      TestTopology.addressesForCore(serverId, false, Set[DatabaseId](databaseId).asJava)

    def sendInitialData(): ClusterViewMessage = {
      val myInfo = new CoreServerInfoForServerId(identityModule.serverId(), coreServerInfo(0, databaseId))
      val metadata = Map(
        myUniqueAddress -> myInfo,
      ).asJava
      topologyActorRef ! new MetadataMessage(metadata)

      val clusterViewMessage = new ClusterViewMessage(false, initialMembers(), Collections.emptySet())
      topologyActorRef ! clusterViewMessage
      awaitAssert(
        verify(topologyBuilder).buildCoreTopology(ArgumentMatchers.eq(databaseId), any(), ArgumentMatchers.eq(clusterViewMessage), any())
      )
      coreTopologyReceiver.receiveOne(defaultWaitTime)
      readReplicaProbe.receiveOne(defaultWaitTime)
      bootstrapStateReceiver.receiveOne(defaultWaitTime)
      clusterViewMessage
    }
  }

}
