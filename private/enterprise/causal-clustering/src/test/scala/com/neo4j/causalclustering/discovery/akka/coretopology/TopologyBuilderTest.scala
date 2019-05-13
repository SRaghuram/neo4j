/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.coretopology

import java.util.UUID

import akka.actor
import akka.cluster.ClusterEvent.CurrentClusterState
import akka.cluster.ddata.LWWMap
import akka.cluster.{Cluster, Member, MemberStatus, UniqueAddress}
import com.neo4j.causalclustering.core.consensus.LeaderInfo
import com.neo4j.causalclustering.discovery.TestTopology
import com.neo4j.causalclustering.identity.{MemberId, RaftId}
import org.junit.runner.RunWith
import org.mockito.Mockito
import org.neo4j.kernel.database.TestDatabaseIdRepository
import org.neo4j.logging.NullLogProvider
import org.scalatest.junit.JUnitRunner
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import scala.collection.JavaConverters._
import scala.collection.immutable.TreeSet
import scala.compat.java8.OptionConverters._

@RunWith(classOf[JUnitRunner])
class TopologyBuilderTest
  extends WordSpecLike
    with Matchers
    with BeforeAndAfterAll
    with MockitoSugar {

  "TopologyBuilder" should {

    "return an empty topology" when {

      "missing all member metadata" in new Fixture {
        val topology = topologyBuilder().buildCoreTopology(databaseId, raftId, clusterState(3), MetadataMessage.EMPTY)
        topology.members() shouldBe empty
      }

      "missing all members" in new Fixture {
        val emptyClusterState = ClusterViewMessage.EMPTY
        val topology = topologyBuilder().buildCoreTopology(databaseId, raftId, emptyClusterState, memberMetaData(3))
        topology.members() shouldBe empty
      }

      "all members unreachable" in new Fixture {
        val topology = topologyBuilder().buildCoreTopology(databaseId, raftId, clusterState(3, 3), memberMetaData(3))
        topology.members() shouldBe empty
      }

      "no member contains the database" in new Fixture {
        val otherDatabaseId = databaseIdRepository.get("some-other-database")
        val topology = topologyBuilder().buildCoreTopology(otherDatabaseId, raftId, clusterState(3), memberMetaData(3))
        topology.databaseId() shouldBe otherDatabaseId
        topology.members() shouldBe empty
      }
    }

    "return a topology with members only in both metadata and cluster" when {
      "cluster has more members that metadata" in new Fixture {
        val topology = topologyBuilder().buildCoreTopology(databaseId, raftId, clusterState(4), memberMetaData(3))
        topology.members() should have size 3
      }

      "metadata has more members than cluster" in new Fixture {
        val topology = topologyBuilder().buildCoreTopology(databaseId, raftId, clusterState(3), memberMetaData(4))
        topology.members() should have size 3
      }
    }

    "return a topology without unreachable members" in new Fixture {
      val topology = topologyBuilder().buildCoreTopology(databaseId, raftId, clusterState(4, 1), memberMetaData(4))
      topology.members() should have size 3
    }

    "return a topology with all members from cluster and metadata if have same number and none unreachable" in new Fixture {
      val clusterSize = 7
      val topology = topologyBuilder().buildCoreTopology(databaseId, raftId, clusterState(clusterSize), memberMetaData(clusterSize))
      topology.members() should have size clusterSize
    }

    "correctly associate cluster members and meta data by unique address" in new Fixture {
      val clusterMembers = clusterState(4)
      val metadata = memberMetaData(4, 2)
      val expected = clusterMembers.members.asScala.flatMap(m => metadata.getOpt(m.uniqueAddress).asScala ).map(_.memberId)
      val topology = topologyBuilder().buildCoreTopology(databaseId, raftId, clusterMembers, metadata)
      topology.members().keySet() should contain theSameElementsAs expected
    }
  }

  trait Fixture {

    type IdMap = LWWMap[String,RaftId]
    type LeaderMap = LWWMap[String,LeaderInfo]

    val databaseIdRepository = new TestDatabaseIdRepository()
    implicit val cluster = mock[Cluster]
    val databaseId = databaseIdRepository.get("my_database")
    val raftId = new RaftId(UUID.randomUUID)

    def topologyBuilder(self: UniqueAddress = uniqueAddressStream.head) =
      new TopologyBuilder(self, NullLogProvider.getInstance())

    def clusterState(numMembers: Int, numUnreachable: Int = 0): ClusterViewMessage = {
      require(numMembers >= numUnreachable)
      val members = memberMocks.take(numMembers)
      val unreachableMembers = members.take(numUnreachable)
      val memberSet = TreeSet(members:_*)
      val unreachableMemberSet = Set(unreachableMembers:_*)
      val leader = memberSet.diff(unreachableMemberSet).headOption.map(_.address)
      new ClusterViewMessage( CurrentClusterState(memberSet, unreachableMemberSet, Set.empty, leader) )
    }

    def memberMetaData(n: Int, from: Int = 0, refusesToBeLeader: Int => Boolean = _ => false ): MetadataMessage = {
      val coreServerInfoStream = Stream.from(from)
        .map(i => TestTopology.addressesForCore(i, refusesToBeLeader(i), Set(databaseId).asJava))
        .map(info => new CoreServerInfoForMemberId(new MemberId(UUID.randomUUID()), info))

      val addressWithInfo = uniqueAddressStreamFrom(from).zip(coreServerInfoStream).take(n)

      val metadataMap = addressWithInfo.foldLeft(Map[UniqueAddress, CoreServerInfoForMemberId]()) {
        case (acc: Map[UniqueAddress,CoreServerInfoForMemberId], (addr,info)) => acc + (addr -> info)
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

