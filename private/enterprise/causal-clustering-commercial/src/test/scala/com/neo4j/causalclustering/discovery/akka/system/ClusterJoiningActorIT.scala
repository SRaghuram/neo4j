/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.system

import java.util
import java.util.Collections
import java.util.concurrent.TimeUnit

import akka.actor.Address
import akka.cluster.Cluster
import com.neo4j.causalclustering.discovery.akka.BaseAkkaIT
import com.neo4j.causalclustering.discovery.akka.system.ClusterJoiningActor.JoinMessage
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito
import org.mockito.Mockito.verify
import org.neo4j.causalclustering.core.CausalClusteringSettings
import org.neo4j.causalclustering.discovery.{InitialDiscoveryMembersResolver, NoOpHostnameResolver}
import org.neo4j.helpers.AdvertisedSocketAddress
import org.neo4j.kernel.configuration.Config
import org.neo4j.logging.NullLogProvider

import scala.collection.JavaConverters._
import scala.concurrent.duration.Duration

class ClusterJoiningActorIT extends BaseAkkaIT("ClusterJoining") {

  "ClusterJoiningActor" when {
    "initial message has no addresses" should {
      "join seed nodes from resolver" in new Fixture {
        actorRef ! JoinMessage.initial(Collections.emptyList())

        awaitAssert(verify(cluster).joinSeedNodes(initialDiscoveryMembersAsAddresses), max = defaultWaitTime)
        awaitNotAssert(verify(cluster).join(any()), max = defaultWaitTime, "Should not have attempted to join an existing node")
      }
      "retry joining seed nodes" in new Fixture {
        actorRef ! JoinMessage.initial(Collections.emptyList())

        val assert = () => verify(cluster, Mockito.atLeast(2)).joinSeedNodes(initialDiscoveryMembersAsAddresses)

        awaitAssert(assert, max = defaultWaitTime + refresh)
      }
    }
    "has own address" should {
      "not connect to self" in new Fixture {
        actorRef ! JoinMessage.initial(util.Arrays.asList(self))

        awaitAssert(verify(cluster).joinSeedNodes(initialDiscoveryMembersAsAddresses), max = defaultWaitTime)
        awaitNotAssert(verify(cluster).join(self), max = defaultWaitTime, "Should not join self")
      }
    }
    "has addresses" should {
      "attempt to connect in order, and then connect to seed nodes" in new Fixture {
        val List(address1, address2, address3) = Seq.tabulate(3)(port => Address("akka", system.name, "joinHost", port))

        actorRef ! JoinMessage.initial(util.Arrays.asList(address1, address2, address3))

        val assert = () => {
          val inOrder = Mockito.inOrder(cluster)
          inOrder.verify(cluster).join(address1)
          inOrder.verify(cluster).join(address2)
          inOrder.verify(cluster).join(address3)
          inOrder.verify(cluster).joinSeedNodes(initialDiscoveryMembersAsAddresses)
        }

        awaitAssert(assert, max = defaultWaitTime + refresh * 3)
      }
    }
  }

  trait Fixture {
    val refresh = Duration(1, TimeUnit.SECONDS)
    val cluster = mock[Cluster]

    val List(seed1, seed2, seed3) = Seq.tabulate(3)(port => new AdvertisedSocketAddress("seedHost", port))

    val initialDiscoveryMembers = Seq(seed1, seed2, seed3).mkString(",")
    val initialDiscoveryMembersAsAddresses = Seq(seed1, seed2, seed3)
      .sorted(Ordering.comparatorToOrdering(InitialDiscoveryMembersResolver.advertisedSocketAddressComparator()))
      .map(resolvedAddress => Address("akka", system.name, resolvedAddress.getHostname, resolvedAddress.getPort))
      .asJava

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
