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
import com.neo4j.causalclustering.discovery.akka.common.{DatabaseStartedMessage, DatabaseStoppedMessage}
import com.neo4j.causalclustering.discovery.akka.database.state.ReplicatedDatabaseState
import com.neo4j.causalclustering.discovery.akka.directory.LeaderInfoDirectoryMessage
import com.neo4j.causalclustering.discovery.akka.{BaseAkkaIT, DatabaseStateUpdateSink, DirectoryUpdateSink, TopologyUpdateSink}
import com.neo4j.causalclustering.discovery.{DatabaseCoreTopology, DatabaseReadReplicaTopology, TestDiscoveryMember, TestTopology}
import com.neo4j.causalclustering.identity.{MemberId, RaftId}
import org.neo4j.configuration.Config
import org.neo4j.dbms.DatabaseState
import org.neo4j.kernel.database.DatabaseId
import org.neo4j.kernel.database.TestDatabaseIdRepository.randomDatabaseId
import org.neo4j.logging.NullLogProvider
import org.neo4j.time.Clocks

import scala.collection.JavaConverters._
import scala.concurrent.duration.Duration

class ClientTopologyActorIT extends BaseAkkaIT("ClientTopologyActorIT") {
  "ClientTopologyActor" when {
    "starting" should {
      "send read replica info to cluster client" in new Fixture {
        val msg = new ReadReplicaRefreshMessage(readReplicaInfo, memberId, clusterClientProbe.ref, topologyActorRef, Map.empty[DatabaseId,DatabaseState].asJava)

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
        val dbId = randomDatabaseId()
        val raftId = RaftId.from(dbId)
        val newCoreTopology = new DatabaseCoreTopology(dbId, raftId, Map(
                                            new MemberId(UUID.randomUUID()) -> TestTopology.addressesForCore(0, false),
                                            new MemberId(UUID.randomUUID()) -> TestTopology.addressesForCore(1, false)
                                          ).asJava)

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
        val newRRTopology = new DatabaseReadReplicaTopology(randomDatabaseId(), Map(
                          new MemberId(UUID.randomUUID()) -> TestTopology.addressesForReadReplica(0),
                          new MemberId(UUID.randomUUID()) -> TestTopology.addressesForReadReplica(1)
                        ).asJava )

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
                    randomDatabaseId() -> new LeaderInfo(new MemberId(UUID.randomUUID()), 1),
                    randomDatabaseId() -> new LeaderInfo(new MemberId(UUID.randomUUID()), 2)
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
        val msg = new ReadReplicaRefreshMessage(readReplicaInfo, memberId, clusterClientProbe.ref, topologyActorRef, Map.empty[DatabaseId,DatabaseState].asJava)
        val send = ClusterClient.Publish(ReadReplicaViewActor.READ_REPLICA_TOPIC, msg)

        And("all databases are stated")
        databaseIds.foreach(id => topologyActorRef ! new DatabaseStartedMessage(id))
        expectRefreshMsgWithDatabases(databaseIds)

        When("Waiting for a multiple of refresh time")
        val repeatSends = 3
        val waitTime = defaultWaitTime + (refresh * repeatSends)

        Then("Expect at least one message for each refresh time plus one initial message")
        clusterClientProbe.expectMsgAllOf(waitTime, Array.fill(repeatSends + 1)(send): _*)
      }
      "handle database started messages" in new Fixture {
        Given("database IDs to start")
        val databaseId1 = randomDatabaseId()
        val databaseId2 = randomDatabaseId()

        When("receive both start messages")
        topologyActorRef ! new DatabaseStartedMessage(databaseId1)
        topologyActorRef ! new DatabaseStartedMessage(databaseId2)

        Then("read replica database changes received")
        expectRefreshMsgWithDatabases(databaseIds + databaseId1)
        expectRefreshMsgWithDatabases(databaseIds + databaseId1 + databaseId2)
      }
      "handle database stopped messages" in new Fixture {
        Given("database IDs to start and stop")
        val databaseId1 = randomDatabaseId()
        val databaseId2 = randomDatabaseId()

        And("both databases started")
        topologyActorRef ! new DatabaseStartedMessage(databaseId1)
        topologyActorRef ! new DatabaseStartedMessage(databaseId2)
        expectRefreshMsgWithDatabases(databaseIds + databaseId1)
        expectRefreshMsgWithDatabases(databaseIds + databaseId1 + databaseId2)

        When("receive both stop messages")
        topologyActorRef ! new DatabaseStoppedMessage(databaseId2)
        topologyActorRef ! new DatabaseStoppedMessage(databaseId1)

        Then("read replica database changes received")
        expectRefreshMsgWithDatabases(databaseIds + databaseId1)
        expectRefreshMsgWithDatabases(databaseIds)
      }
    }
  }

  trait Fixture {
    var actualCoreTopology: DatabaseCoreTopology = _
    var actualReadReplicaTopology: DatabaseReadReplicaTopology = _
    var actualLeaderPerDb = Collections.emptyMap[DatabaseId, LeaderInfo]
    var actualDatabaseStates: ReplicatedDatabaseState = _

    val updateSink = new TopologyUpdateSink with DirectoryUpdateSink with DatabaseStateUpdateSink {
      override def onTopologyUpdate(topology: DatabaseCoreTopology) = actualCoreTopology = topology

      override def onTopologyUpdate(topology: DatabaseReadReplicaTopology) = actualReadReplicaTopology = topology

      override def onDbLeaderUpdate(leaderPerDb: java.util.Map[DatabaseId, LeaderInfo]) = actualLeaderPerDb = leaderPerDb

      override def onDbStateUpdate(databaseState: ReplicatedDatabaseState) = actualDatabaseStates = databaseState
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
    val databaseStateSink = Source.queue[ReplicatedDatabaseState](1, OverflowStrategy.dropHead)
      .to(Sink.foreach(updateSink.onDbStateUpdate))
      .run(materializer)

    val memberId = new MemberId(UUID.randomUUID())

    val databaseIds = Set(randomDatabaseId(), randomDatabaseId(), randomDatabaseId())

    val readReplicaInfo = TestTopology.addressesForReadReplica(0, databaseIds.asJava)

    val refresh = Duration(1, TimeUnit.SECONDS)

    val config = {
      val myCoreServerConfig = TestTopology.configFor(readReplicaInfo)
      val conf = Config.newBuilder()
        .fromConfig(myCoreServerConfig)
        .set(CausalClusteringSettings.cluster_topology_refresh, java.time.Duration.ofSeconds( refresh.toSeconds ) )
        .build()
      conf
    }

    val clusterClientProbe = TestProbe()
    val props = ClientTopologyActor.props(new TestDiscoveryMember(memberId, databaseIds.asJava), coreTopologySink, readReplicaTopologySink, discoverySink,
      databaseStateSink, clusterClientProbe.ref, config, NullLogProvider.getInstance(), Clocks.systemClock() )

    val topologyActorRef = system.actorOf(props)

    def expectMsg(msg: Any): Unit = {
      val send = ClusterClient.Publish(ReadReplicaViewActor.READ_REPLICA_TOPIC, msg)
      clusterClientProbe.fishForSpecificMessage(){ case `send` => }
    }

    def expectRefreshMsgWithDatabases(databaseIds: Set[DatabaseId]): Unit = {
      clusterClientProbe.fishForSpecificMessage(defaultWaitTime) {
        case publish: ClusterClient.Publish if isRefreshMsgWithDatabases(publish.msg, databaseIds) => ()
      }
    }

    def isRefreshMsgWithDatabases(msg: Any, databaseIds: Set[DatabaseId]): Boolean = {
      msg match {
        case refreshMsg: ReadReplicaRefreshMessage => refreshMsg.readReplicaInfo().databaseIds.asScala == databaseIds
        case _ => false
      }
    }
  }
}
