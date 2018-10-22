/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.readreplicatopology

import java.util.concurrent.TimeUnit
import java.util.{Collections, UUID}

import akka.cluster.client.ClusterClient
import akka.stream.javadsl.Source
import akka.stream.scaladsl.Sink
import akka.stream.{ActorMaterializer, OverflowStrategy}
import akka.testkit.TestProbe
import com.neo4j.causalclustering.discovery.akka.directory.LeaderInfoDirectoryMessage
import com.neo4j.causalclustering.discovery.akka.{BaseAkkaIT, DirectoryUpdateSink, TopologyUpdateSink}
import org.neo4j.causalclustering.core.CausalClusteringSettings
import org.neo4j.causalclustering.core.consensus.LeaderInfo
import org.neo4j.causalclustering.discovery.{CoreTopology, ReadReplicaTopology, TestTopology}
import org.neo4j.causalclustering.identity.{ClusterId, MemberId}
import org.neo4j.kernel.configuration.Config
import org.neo4j.logging.NullLogProvider

import scala.collection.JavaConverters._
import scala.concurrent.duration.Duration

class ClientTopologyActorIT extends BaseAkkaIT("ClientTopologyActorIT") {
  "ClientTopologyActor" when {
    "starting" should {
      "send read replica info to cluster client" in new Fixture {
        val msg = new ReadReplicaRefreshMessage(readReplicaInfo, memberId, clusterClientProbe.ref, topologyActorRef)

        expectMsg(msg)
      }
    }
    "stopping" should {
      "send removal notification to cluster client" in new Fixture {
        When("stop actor")
        system.stop(topologyActorRef)

        Then("send removal")
        val msg = new ReadReplicaRemovalMessage(clusterClientProbe.ref)
        expectMsg(msg)
      }
    }
    "running" should {
      "forward incoming core topologies" in new Fixture {
        Given("new topology")
        val newCoreTopology = new CoreTopology(
          new ClusterId(UUID.randomUUID()),
          false,
          Map(
            new MemberId(UUID.randomUUID()) -> TestTopology.addressesForCore(0, false),
            new MemberId(UUID.randomUUID()) -> TestTopology.addressesForCore(1, false)
          ).asJava
        )

        When("incoming topology")
        topologyActorRef ! newCoreTopology

        Then("topology fed to sink")
        awaitCond(
          actualCoreTopology == newCoreTopology && actualCoreTopology.clusterId() == newCoreTopology.clusterId(),
          max = defaultWaitTime,
          message = s"Expected $newCoreTopology but was $actualCoreTopology"
        )
      }
      "forward incoming read replica topologies" in new Fixture {
        Given("new topology")
        val newRRTopology = new ReadReplicaTopology(
          Map(
            new MemberId(UUID.randomUUID()) -> TestTopology.addressesForReadReplica(0),
            new MemberId(UUID.randomUUID()) -> TestTopology.addressesForReadReplica(1)
          ).asJava
        )

        When("incoming topology")
        topologyActorRef ! newRRTopology

        Then("topology fed to sink")
        awaitCond(
          actualReadReplicaTopology == newRRTopology,
          max = defaultWaitTime,
          message = s"Expected $newRRTopology but was $actualReadReplicaTopology"
        )
      }
      "forward incoming leaders" in new Fixture {
        Given("new leaders")
        val newLeaders = new LeaderInfoDirectoryMessage(
          Map(
            "db1" -> new LeaderInfo(new MemberId(UUID.randomUUID()), 1),
            "db2" -> new LeaderInfo(new MemberId(UUID.randomUUID()), 2)
          ).asJava
        )

        When("incoming topology")
        topologyActorRef ! newLeaders

        Then("topology fed to sink")
        awaitCond(
          actualLeaderPerDb == newLeaders.leaders(),
          max = defaultWaitTime,
          message = s"Expected $newLeaders but was $actualLeaderPerDb"
        )
      }
      "periodically send read replica info" in new Fixture {
        Given("Info to send")
        val msg = new ReadReplicaRefreshMessage(readReplicaInfo, memberId, clusterClientProbe.ref, topologyActorRef)
        val send = new ClusterClient.Publish(ReadReplicaViewActor.READ_REPLICA_TOPIC, msg)

        When("Waiting for a multiple of refresh time")
        val repeatSends = 3
        val waitTime = defaultWaitTime + (refresh * repeatSends)

        Then("Expect at least one message for each refresh time plus one initial message")
        clusterClientProbe.expectMsgAllOf(waitTime, Array.fill(repeatSends + 1)(send): _*)
      }
    }
  }

  trait Fixture {
    var actualCoreTopology = CoreTopology.EMPTY
    var actualReadReplicaTopology = ReadReplicaTopology.EMPTY
    var actualLeaderPerDb = Collections.emptyMap[String, LeaderInfo]

    val updateSink = new TopologyUpdateSink with DirectoryUpdateSink {
      override def onTopologyUpdate(topology: CoreTopology) = actualCoreTopology = topology

      override def onTopologyUpdate(topology: ReadReplicaTopology) = actualReadReplicaTopology = topology

      override def onDbLeaderUpdate(leaderPerDb: java.util.Map[String, LeaderInfo]) = actualLeaderPerDb = leaderPerDb
    }

    val materializer = ActorMaterializer()
    val discoverySink = Source.queue[java.util.Map[String,LeaderInfo]](1, OverflowStrategy.dropHead)
      .to(Sink.foreach(updateSink.onDbLeaderUpdate))
      .run(materializer)
    val coreTopologySink = Source.queue[CoreTopology](1, OverflowStrategy.dropHead)
      .to(Sink.foreach(updateSink.onTopologyUpdate))
      .run(materializer)
    val readReplicaTopologySink = Source.queue[ReadReplicaTopology](1, OverflowStrategy.dropHead)
      .to(Sink.foreach(updateSink.onTopologyUpdate))
      .run(materializer)

    val memberId = new MemberId(UUID.randomUUID())

    val readReplicaInfo = TestTopology.addressesForReadReplica(0)

    val refresh = Duration(1, TimeUnit.SECONDS)

    val config = {
      val conf = Config.defaults()
      val myCoreServerConfig = TestTopology.configFor(readReplicaInfo)
      conf.augment(myCoreServerConfig)
      conf.augment(CausalClusteringSettings.cluster_topology_refresh, s"${refresh.toSeconds}s")
      conf
    }

    val clusterClientProbe = TestProbe()
    val props = ClientTopologyActor.props(
      memberId,
      coreTopologySink,
      readReplicaTopologySink,
      discoverySink,
      clusterClientProbe.ref,
      config,
      NullLogProvider.getInstance()
    )

    val topologyActorRef = system.actorOf(props)

    def expectMsg(msg: Any) = {
      val send = new ClusterClient.Publish(ReadReplicaViewActor.READ_REPLICA_TOPIC, msg)
      clusterClientProbe.fishForSpecificMessage(){ case `send` => }
    }
  }
}
