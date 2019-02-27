/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.system

import java.util
import java.util.Collections
import java.util.concurrent.TimeUnit

import akka.actor.Address
import akka.cluster.Cluster
import com.neo4j.causalclustering.core.CausalClusteringSettings
import com.neo4j.causalclustering.discovery.akka.BaseAkkaIT
import com.neo4j.causalclustering.discovery.{InitialDiscoveryMembersResolver, NoOpHostnameResolver}
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito
import org.mockito.Mockito.{atLeastOnce, verify}
import org.neo4j.configuration.Config
import org.neo4j.helpers.AdvertisedSocketAddress
import org.neo4j.logging.NullLogProvider

import scala.collection.JavaConverters._
import scala.concurrent.duration.Duration

class ClusterJoiningActorIT extends BaseAkkaIT("ClusterJoining") {

  "ClusterJoiningActor" when {
    "initial message is not rejoin and has no addresses" should {
      "join seed nodes from resolver" in new Fixture {
        actorRef ! JoinMessage.initial(false, Collections.emptyList())

        awaitAssert(verify(cluster).joinSeedNodes(initialDiscoveryMembersAsAddresses.asJava), max = defaultWaitTime)
        awaitNotAssert(verify(cluster, atLeastOnce()).join(any()), max = defaultWaitTime, "Should not have attempted to join an existing node")
      }
      "retry joining seed nodes" in new Fixture {
        actorRef ! JoinMessage.initial(false, Collections.emptyList())

        awaitAssert(verify(cluster, Mockito.atLeast(2)).joinSeedNodes(initialDiscoveryMembersAsAddresses.asJava), max = defaultWaitTime + refresh)
      }
    }
    "initial message is rejoin" when {
      "has own address" should {
        "not connect to self" in new Fixture {
          actorRef ! JoinMessage.initial(true, util.Arrays.asList(self))

          awaitNotAssert(verify(cluster, atLeastOnce()).join(self), max = defaultWaitTime, "Should not join self")
          assertNoJoinToSeedNodes()
        }
      }
      "has addresses" should {
        "attempt to connect in order" in new Fixture {
          val List(address1, address2, address3) = Seq.tabulate(3)(port => Address("akka", system.name, "joinHost", port))

          actorRef ! JoinMessage.initial(true, util.Arrays.asList(address1, address2, address3))

          def assert = {
            val inOrder = Mockito.inOrder(cluster)
            inOrder.verify(cluster).join(address1)
            inOrder.verify(cluster).join(address2)
            inOrder.verify(cluster).join(address3)
          }

          awaitAssert(assert, max = defaultWaitTime + refresh * 3)

          assertNoJoinToSeedNodes()
        }
      }
      "has no addresses" should {
        "attempt to join each node from resolver individually" in new Fixture {
          actorRef ! JoinMessage.initial(true, Collections.emptyList())

          def assert = {
            val inOrder = Mockito.inOrder(cluster)
            initialDiscoveryMembersAsAddresses.foreach(address => inOrder.verify(cluster).join(address))
          }

          awaitAssert(assert, max = defaultWaitTime + refresh * 3)
          assertNoJoinToSeedNodes()
        }
      }
    }
  }

  trait Fixture {
    def assertNoJoinToSeedNodes() = {
      def assertNot = {
        verify(cluster, Mockito.atLeastOnce()).joinSeedNodes(any[util.List[Address]])
      }
      awaitNotAssert(assertNot, max = defaultWaitTime, "Should not join seed nodes")
    }

    val refresh = Duration(1, TimeUnit.SECONDS)
    val cluster = mock[Cluster]

    val List(seed1, seed2, seed3) = Seq.tabulate(3)(port => new AdvertisedSocketAddress("seedHost", port))

    val initialDiscoveryMembers = Seq(seed1, seed2, seed3).mkString(",")
    val initialDiscoveryMembersAsAddresses = Seq(seed1, seed2, seed3)
      .sorted(Ordering.comparatorToOrdering(InitialDiscoveryMembersResolver.advertisedSocketAddressComparator()))
      .map(resolvedAddress => Address("akka", system.name, resolvedAddress.getHostname, resolvedAddress.getPort))

    val config = Config.builder()
      .withSetting(CausalClusteringSettings.initial_discovery_members, initialDiscoveryMembers)
      .withSetting(CausalClusteringSettings.cluster_binding_retry_timeout, refresh.length + "s")
      .build()
    val resolver = NoOpHostnameResolver.resolver(config)

    val self = Address("akka", system.name, "myHost", 1234 )
    Mockito.when(cluster.selfAddress).thenReturn(self)

    val logProvider = NullLogProvider.getInstance()

    val props = ClusterJoiningActor.props(cluster, resolver, config, logProvider)

    val actorRef = system.actorOf(props)
  }

}
