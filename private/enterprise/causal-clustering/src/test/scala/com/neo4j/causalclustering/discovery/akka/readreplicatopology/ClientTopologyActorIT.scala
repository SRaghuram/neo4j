/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
import com.neo4j.causalclustering.core.CausalClusteringSettings
import com.neo4j.causalclustering.core.consensus.LeaderInfo
import com.neo4j.causalclustering.discovery.akka.directory.LeaderInfoDirectoryMessage
import com.neo4j.causalclustering.discovery.akka.{BaseAkkaIT, DirectoryUpdateSink, TopologyUpdateSink}
import com.neo4j.causalclustering.discovery.{DatabaseCoreTopology, DatabaseReadReplicaTopology, TestDiscoveryMember, TestTopology}
import com.neo4j.causalclustering.identity.{RaftId, MemberId}
import org.neo4j.configuration.Config
import org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME
import org.neo4j.kernel.database.DatabaseId
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
        val newCoreTopology = new DatabaseCoreTopology(
          new DatabaseId(DEFAULT_DATABASE_NAME),
          new RaftId(UUID.randomUUID()),
          Map(
            new MemberId(UUID.randomUUID()) -> TestTopology.addressesForCore(0, false),
            new MemberId(UUID.randomUUID()) -> TestTopology.addressesForCore(1, false)
          ).asJava
        )

        When("incoming topology")
        topologyActorRef ! newCoreTopology

        Then("topology fed to sink")
        awaitCond(
          actualCoreTopology == newCoreTopology && actualCoreTopology.raftId() == newCoreTopology.raftId(),
          max = defaultWaitTime,
          message = s"Expected $newCoreTopology but was $actualCoreTopology"
        )
      }
      "forward incoming read replica topologies" in new Fixture {
        Given("new topology")
        val newRRTopology = new DatabaseReadReplicaTopology(new DatabaseId(DEFAULT_DATABASE_NAME), Map(
                  new MemberId(UUID.randomUUID()) -> TestTopology.addressesForReadReplica(0),
                  new MemberId(UUID.randomUUID()) -> TestTopology.addressesForReadReplica(1)
                ).asJava)

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
        val newLeaders = new LeaderInfoDirectoryMessage(Map(
                    new DatabaseId("db1") -> new LeaderInfo(new MemberId(UUID.randomUUID()), 1),
                    new DatabaseId("db2") -> new LeaderInfo(new MemberId(UUID.randomUUID()), 2)
                  ).asJava)

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
        val send = ClusterClient.Publish(ReadReplicaViewActor.READ_REPLICA_TOPIC, msg)

        When("Waiting for a multiple of refresh time")
        val repeatSends = 3
        val waitTime = defaultWaitTime + (refresh * repeatSends)

        Then("Expect at least one message for each refresh time plus one initial message")
        clusterClientProbe.expectMsgAllOf(waitTime, Array.fill(repeatSends + 1)(send): _*)
      }
    }
  }

  trait Fixture {
    var actualCoreTopology = DatabaseCoreTopology.EMPTY
    var actualReadReplicaTopology = DatabaseReadReplicaTopology.EMPTY
    var actualLeaderPerDb = Collections.emptyMap[DatabaseId, LeaderInfo]

    val updateSink = new TopologyUpdateSink with DirectoryUpdateSink {
      override def onTopologyUpdate(topology: DatabaseCoreTopology) = actualCoreTopology = topology

      override def onTopologyUpdate(topology: DatabaseReadReplicaTopology) = actualReadReplicaTopology = topology

      override def onDbLeaderUpdate(leaderPerDb: java.util.Map[DatabaseId, LeaderInfo]) = actualLeaderPerDb = leaderPerDb
    }

    val materializer = ActorMaterializer()
    val discoverySink = Source.queue[java.util.Map[DatabaseId,LeaderInfo]](1, OverflowStrategy.dropHead)
      .to(Sink.foreach(updateSink.onDbLeaderUpdate))
      .run(materializer)
    val coreTopologySink = Source.queue[DatabaseCoreTopology](1, OverflowStrategy.dropHead)
      .to(Sink.foreach(updateSink.onTopologyUpdate))
      .run(materializer)
    val readReplicaTopologySink = Source.queue[DatabaseReadReplicaTopology](1, OverflowStrategy.dropHead)
      .to(Sink.foreach(updateSink.onTopologyUpdate))
      .run(materializer)

    val memberId = new MemberId(UUID.randomUUID())

    val databaseIds = Set(new DatabaseId("orders"), new DatabaseId("employees"), new DatabaseId("customers")).asJava

    val readReplicaInfo = TestTopology.addressesForReadReplica(0, databaseIds)

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
      new TestDiscoveryMember(memberId, databaseIds),
      coreTopologySink,
      readReplicaTopologySink,
      discoverySink,
      clusterClientProbe.ref,
      config,
      NullLogProvider.getInstance()
    )

    val topologyActorRef = system.actorOf(props)

    def expectMsg(msg: Any) = {
      val send = ClusterClient.Publish(ReadReplicaViewActor.READ_REPLICA_TOPIC, msg)
      clusterClientProbe.fishForSpecificMessage(){ case `send` => }
    }
  }
}
