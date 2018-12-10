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
import com.neo4j.causalclustering.identity.{ClusterId, MemberId}
import org.junit.runner.RunWith
import org.mockito.Mockito
import org.neo4j.kernel.configuration.Config
import org.neo4j.logging.NullLogProvider
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar
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
        val topology= topologyBuilder().buildCoreTopology(clusterId, clusterState(3), MetadataMessage.EMPTY)
        topology.members() shouldBe empty
      }

      "missing all members" in new Fixture {
        val emptyClusterState = ClusterViewMessage.EMPTY
        val topology = topologyBuilder().buildCoreTopology(clusterId, emptyClusterState, memberMetaData(3))
        topology.members() shouldBe empty
      }

      "all members unreachable" in new Fixture {
        val topology = topologyBuilder().buildCoreTopology(clusterId, clusterState(3,3), memberMetaData(3))
        topology.members() shouldBe empty
      }
    }

    "return a topology with members only in both metadata and cluster" when {
      "cluster has more members that metadata" in new Fixture {
        val topology = topologyBuilder().buildCoreTopology(clusterId, clusterState(4), memberMetaData(3))
        topology.members() should have size 3
      }

      "metadata has more members than cluster" in new Fixture {
        val topology = topologyBuilder().buildCoreTopology(clusterId, clusterState(3), memberMetaData(4))
        topology.members() should have size 3
      }
    }

    "return a topology without unreachable members" in new Fixture {
      val topology = topologyBuilder().buildCoreTopology(clusterId, clusterState(4,1), memberMetaData(4))
      topology.members() should have size 3
    }

    "return a topology with all members from cluster and metadata if have same number and none unreachable" in new Fixture {
      val clusterSize = 7
      val topology = topologyBuilder().buildCoreTopology(clusterId, clusterState(clusterSize), memberMetaData(clusterSize))
      topology.members() should have size clusterSize
    }

    "correctly identify canBeBootStrapped" when {

      "none refuse to be leader and this server is the first member" in new Fixture {
        val memberData = memberMetaData(3)
        val topology = topologyBuilder().buildCoreTopology(clusterId, clusterState(3), memberData)
        topology.canBeBootstrapped shouldBe true
      }

      "none refuse to be leader but this server is not first member" in new Fixture {
        val thisServerId = 1
        val thisServerUniqueAddress = uniqueAddressStream.drop(thisServerId).head
        val memberData = memberMetaData(3)
        val topology = topologyBuilder(self = thisServerUniqueAddress).buildCoreTopology(clusterId, clusterState(3), memberData)
        topology.canBeBootstrapped shouldBe false
      }

      "this server is the first non-'refuse to be leader' member" in new Fixture {
        val thisServerId = 2
        val thisServerUniqueAddress = uniqueAddressStream.drop(thisServerId).head
        val whichRefuse: Int => Boolean = id => id < thisServerId
        val memberData = memberMetaData(5, refusesToBeLeader = whichRefuse)
        val topology = topologyBuilder(self = thisServerUniqueAddress).buildCoreTopology(clusterId, clusterState(5), memberData)
        topology.canBeBootstrapped shouldBe true
      }

      "this server refuses to be leader" in new Fixture {
        val thisServerId = 0
        val whichRefuse: Int => Boolean = id => id == thisServerId
        val memberData = memberMetaData(3, refusesToBeLeader = whichRefuse)
        val topology = topologyBuilder().buildCoreTopology(clusterId, clusterState(3), memberData)
        topology.canBeBootstrapped shouldBe false
      }
    }

    "correctly associate cluster members and meta data by unique address" in new Fixture {
      val clusterMembers = clusterState(4)
      val metadata = memberMetaData(4, 2)
      val expected = clusterMembers.members.asScala.flatMap(m => metadata.getOpt(m.uniqueAddress).asScala ).map(_.memberId)
      val topology = topologyBuilder().buildCoreTopology(clusterId, clusterMembers, metadata)
      topology.members().keySet() should contain theSameElementsAs expected
    }
  }

  trait Fixture {

    type IdMap = LWWMap[String,ClusterId]
    type LeaderMap = LWWMap[String,LeaderInfo]

    implicit val cluster = mock[Cluster]
    val databaseName = "default"
    val clusterId = new ClusterId(UUID.randomUUID)
    
    def topologyBuilder(self: UniqueAddress = uniqueAddressStream.head, config: Config = Config.defaults) =
      new TopologyBuilder(config, self, NullLogProvider.getInstance())

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
          .map(i => TestTopology.addressesForCore(i, refusesToBeLeader(i)))
          .map(info => new CoreServerInfoForMemberId(new MemberId(UUID.randomUUID()),info))

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

