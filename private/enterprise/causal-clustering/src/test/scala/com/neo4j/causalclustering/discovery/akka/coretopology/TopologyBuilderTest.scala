/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.coretopology

import akka.actor
import akka.cluster.Cluster
import akka.cluster.ClusterEvent.CurrentClusterState
import akka.cluster.Member
import akka.cluster.MemberStatus
import akka.cluster.UniqueAddress
import akka.cluster.ddata.LWWMap
import com.neo4j.causalclustering.core.consensus.LeaderInfo
import com.neo4j.causalclustering.discovery.TestTopology
import com.neo4j.causalclustering.identity.IdFactory
import com.neo4j.causalclustering.identity.RaftGroupId
import org.junit.runner.RunWith
import org.mockito.Mockito
import org.neo4j.kernel.database.DatabaseId
import org.neo4j.kernel.database.TestDatabaseIdRepository.randomNamedDatabaseId
import org.scalatest.BeforeAndAfterAll
import org.scalatest.Matchers
import org.scalatest.WordSpecLike
import org.scalatest.junit.JUnitRunner
import org.scalatest.mockito.MockitoSugar

import scala.collection.JavaConverters.asScalaSetConverter
import scala.collection.JavaConverters.mapAsJavaMapConverter
import scala.collection.JavaConverters.setAsJavaSetConverter
import scala.collection.immutable.TreeSet
import scala.compat.java8.OptionConverters.RichOptionalGeneric

@RunWith(classOf[JUnitRunner])
class TopologyBuilderTest
  extends WordSpecLike
    with Matchers
    with BeforeAndAfterAll
    with MockitoSugar {

  "TopologyBuilder" should {

    "return an empty topology" when {

      "missing all member metadata" in new Fixture {
        val topology = topologyBuilder().buildCoreTopology(databaseId, raftGroupId, clusterState(3), MetadataMessage.EMPTY)
        topology.servers() shouldBe empty
      }

      "missing all members" in new Fixture {
        val emptyClusterState = ClusterViewMessage.EMPTY
        val topology = topologyBuilder().buildCoreTopology(databaseId, raftGroupId, emptyClusterState, memberMetaData(n = 3))
        topology.servers() shouldBe empty
      }

      "all members unreachable" in new Fixture {
        val topology = topologyBuilder().buildCoreTopology(databaseId, raftGroupId, clusterState(3, 3), memberMetaData(n = 3))
        topology.servers() shouldBe empty
      }

      "no member contains the database" in new Fixture {
        val otherDatabaseId = randomNamedDatabaseId().databaseId()
        val topology = topologyBuilder().buildCoreTopology(otherDatabaseId, raftGroupId, clusterState(3), memberMetaData(n = 3))
        topology.databaseId() shouldBe otherDatabaseId
        topology.servers() shouldBe empty
      }
    }

    "return a topology with members only in both metadata and cluster" when {
      "cluster has more members that metadata" in new Fixture {
        val topology = topologyBuilder().buildCoreTopology(databaseId, raftGroupId, clusterState(4), memberMetaData(n = 3))
        topology.servers() should have size 3
      }

      "metadata has more members than cluster" in new Fixture {
        val topology = topologyBuilder().buildCoreTopology(databaseId, raftGroupId, clusterState(3), memberMetaData(n = 4))
        topology.servers() should have size 3
      }
    }

    "return a topology without unreachable members" in new Fixture {
      val topology = topologyBuilder().buildCoreTopology(databaseId, raftGroupId, clusterState(4, 1), memberMetaData(n = 4))
      topology.servers() should have size 3
    }

    "return a topology with all members from cluster and metadata if have same number and none unreachable" in new Fixture {
      val clusterSize = 7
      val topology = topologyBuilder().buildCoreTopology(databaseId, raftGroupId, clusterState(clusterSize), memberMetaData(n = clusterSize))
      topology.servers() should have size clusterSize
    }

    "correctly associate cluster members and meta data by unique address" in new Fixture {
      val clusterMembers = clusterState(4)
      val metadata = memberMetaData(2, 4)
      val expected = clusterMembers.members.asScala.flatMap(m => metadata.getOpt(m.uniqueAddress).asScala).map(_.serverId)
      val topology = topologyBuilder().buildCoreTopology(databaseId, raftGroupId, clusterMembers, metadata)
      topology.servers().keySet() should contain theSameElementsAs expected
    }

    "equate a DatabaseIdRaw and a DatabaseId" in new Fixture {
      val clusterSize = 3
      val clusterMembers = clusterState(clusterSize)
      val metadata = memberMetaData(n = clusterSize, databaseId = databaseId)
      val topology = topologyBuilder().buildCoreTopology(databaseId, raftGroupId, clusterMembers, metadata)
      topology.servers() should have size clusterSize
    }
  }

  trait Fixture {

    type IdMap = LWWMap[String,RaftGroupId]
    type LeaderMap = LWWMap[String,LeaderInfo]

    implicit val cluster = mock[Cluster]
    val databaseId = randomNamedDatabaseId().databaseId()
    val raftGroupId = RaftGroupId.from(databaseId)

    def topologyBuilder() =
      new TopologyBuilder()

    def clusterState(numMembers: Int, numUnreachable: Int = 0): ClusterViewMessage = {
      require(numMembers >= numUnreachable)
      val members = memberMocks.take(numMembers)
      val unreachableMembers = members.take(numUnreachable)
      val memberSet = TreeSet(members:_*)
      val unreachableMemberSet = Set(unreachableMembers:_*)
      val leader = memberSet.diff(unreachableMemberSet).headOption.map(_.address)
      new ClusterViewMessage( CurrentClusterState(memberSet, unreachableMemberSet, Set.empty, leader) )
    }

    def memberMetaData(from: Int = 0, n: Int, databaseId: DatabaseId = databaseId): MetadataMessage = {
      val coreServerInfoStream = Stream.from(from)
        .map(i => TestTopology.addressesForCore(i, Set[DatabaseId](databaseId).asJava))
        .map(info => new CoreServerInfoForServerId(IdFactory.randomServerId(), info))

      val addressWithInfo = uniqueAddressStreamFrom(from).zip(coreServerInfoStream).take(n)

      val metadataMap = addressWithInfo.foldLeft(Map[UniqueAddress, CoreServerInfoForServerId]()) {
        case (acc: Map[UniqueAddress,CoreServerInfoForServerId], (addr,info)) => acc + (addr -> info)
      }

      new MetadataMessage( metadataMap.asJava )
    }

    private def memberMocks = {
      val memberMocks = Stream.continually(mock[Member])
      memberMocks.zip(uniqueAddressStream).map {
        case (mock, uAddress) =>
          Mockito.when(mock.uniqueAddress).thenReturn(uAddress)
          Mockito.when(mock.address).thenReturn(uAddress.address)
          Mockito.when(mock.status).thenReturn(MemberStatus.up)
          mock
      }
    }

    def uniqueAddressStream = uniqueAddressStreamFrom(0)

    private def uniqueAddressStreamFrom(from: Int) = {
      val address = actor.Address("foo", "bar")
      Stream.from(0).map(i => UniqueAddress(address, i.toLong))
    }
  }

}

