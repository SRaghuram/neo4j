/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.readreplicatopology

import java.util.Collections
import java.util.concurrent.TimeUnit

import akka.cluster.client.ClusterClient
import akka.stream.ActorMaterializer
import akka.stream.OverflowStrategy
import akka.stream.javadsl.Source
import akka.stream.scaladsl.Sink
import akka.testkit.TestProbe
import com.neo4j.causalclustering.core.consensus.LeaderInfo
import com.neo4j.causalclustering.discovery.DatabaseCoreTopology
import com.neo4j.causalclustering.discovery.DatabaseReadReplicaTopology
import com.neo4j.causalclustering.discovery.ReplicatedDatabaseState
import com.neo4j.causalclustering.discovery.ReplicatedRaftMapping
import com.neo4j.causalclustering.discovery.TestTopology
import com.neo4j.causalclustering.discovery.akka.BaseAkkaIT
import com.neo4j.causalclustering.discovery.akka.DatabaseStateUpdateSink
import com.neo4j.causalclustering.discovery.akka.DirectoryUpdateSink
import com.neo4j.causalclustering.discovery.akka.RaftMappingUpdateSink
import com.neo4j.causalclustering.discovery.akka.TopologyUpdateSink
import com.neo4j.causalclustering.discovery.akka.common.DatabaseStartedMessage
import com.neo4j.causalclustering.discovery.akka.common.DatabaseStoppedMessage
import com.neo4j.causalclustering.discovery.akka.directory.LeaderInfoDirectoryMessage
import com.neo4j.causalclustering.discovery.member.TestCoreDiscoveryMember
import com.neo4j.causalclustering.identity.IdFactory
import com.neo4j.causalclustering.identity.InMemoryCoreServerIdentity
import com.neo4j.causalclustering.identity.RaftGroupId
import com.neo4j.configuration.CausalClusteringSettings
import com.neo4j.dbms.EnterpriseDatabaseState
import com.neo4j.dbms.EnterpriseOperatorState
import org.neo4j.configuration.Config
import org.neo4j.dbms.DatabaseState
import org.neo4j.dbms.StubDatabaseStateService
import org.neo4j.kernel.database.DatabaseId
import org.neo4j.kernel.database.NamedDatabaseId
import org.neo4j.kernel.database.TestDatabaseIdRepository.randomNamedDatabaseId
import org.neo4j.logging.NullLogProvider
import org.neo4j.time.Clocks

import scala.collection.JavaConverters.asScalaSetConverter
import scala.collection.JavaConverters.mapAsJavaMapConverter
import scala.collection.JavaConverters.setAsJavaSetConverter
import scala.concurrent.duration.Duration

class ClientTopologyActorIT extends BaseAkkaIT("ClientTopologyActorIT") {
  "ClientTopologyActor" when {
    "starting" should {
      "send read replica info to cluster client" in new Fixture {
        val msg = new ReadReplicaRefreshMessage(readReplicaInfo, serverId, clusterClientProbe.ref, topologyActorRef, Collections.emptyMap())

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
        val dbId = randomNamedDatabaseId().databaseId()
        val raftGroupId = RaftGroupId.from(dbId)
        val newCoreTopology = new DatabaseCoreTopology(dbId, raftGroupId, Map(
                                            IdFactory.randomServerId() -> TestTopology.addressesForCore(0, false),
                                            IdFactory.randomServerId() -> TestTopology.addressesForCore(1, false)
                                          ).asJava)

        When("incoming topology")
        topologyActorRef ! newCoreTopology

        Then("topology fed to sink")
        awaitCond(
          actualCoreTopology == newCoreTopology && actualCoreTopology.raftGroupId() == newCoreTopology.raftGroupId(),
          max = defaultWaitTime,
          message = s"Expected $newCoreTopology but was $actualCoreTopology"
        )
      }
      "forward incoming read replica topologies" in new Fixture {
        Given("new topology")
        val newRRTopology = new DatabaseReadReplicaTopology(randomNamedDatabaseId().databaseId(), Map(
                          IdFactory.randomServerId() -> TestTopology.addressesForReadReplica(0),
                          IdFactory.randomServerId() -> TestTopology.addressesForReadReplica(1)
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
        val newLeaders = new LeaderInfoDirectoryMessage(Map[DatabaseId,LeaderInfo](
                    randomNamedDatabaseId().databaseId() -> new LeaderInfo(IdFactory.randomRaftMemberId(), 1),
                    randomNamedDatabaseId().databaseId() -> new LeaderInfo(IdFactory.randomRaftMemberId(), 2)
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
        val msg = new ReadReplicaRefreshMessage(readReplicaInfo, serverId, clusterClientProbe.ref, topologyActorRef, Collections.emptyMap())
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
        val namedDatabaseId1, namedDatabaseId2 = randomNamedDatabaseId()

        When("receive both start messages")
        topologyActorRef ! new DatabaseStartedMessage(namedDatabaseId1)
        topologyActorRef ! new DatabaseStartedMessage(namedDatabaseId2)

        Then("read replica database changes received")
        expectRefreshMsgWithDatabases(databaseIds + namedDatabaseId1)
        expectRefreshMsgWithDatabases(databaseIds + namedDatabaseId1 + namedDatabaseId2)
      }
      "handle database stopped messages" in new Fixture {
        Given("database IDs to start and stop")
        val namedDatabaseId1, namedDatabaseId2 = randomNamedDatabaseId()

        And("both databases started")
        topologyActorRef ! new DatabaseStartedMessage(namedDatabaseId1)
        topologyActorRef ! new DatabaseStartedMessage(namedDatabaseId2)
        expectRefreshMsgWithDatabases(databaseIds + namedDatabaseId1)
        expectRefreshMsgWithDatabases(databaseIds + namedDatabaseId1 + namedDatabaseId2)

        When("receive both stop messages")
        topologyActorRef ! new DatabaseStoppedMessage(namedDatabaseId2)
        topologyActorRef ! new DatabaseStoppedMessage(namedDatabaseId1)

        Then("read replica database changes received")
        expectRefreshMsgWithDatabases(databaseIds + namedDatabaseId1)
        expectRefreshMsgWithDatabases(databaseIds)
      }
    }
  }

  trait Fixture {
    var actualCoreTopology: DatabaseCoreTopology = _
    var actualReadReplicaTopology: DatabaseReadReplicaTopology = _
    var actualLeaderPerDb = Collections.emptyMap[DatabaseId, LeaderInfo]
    var actualDatabaseStates: ReplicatedDatabaseState = _
    var lastMapping: ReplicatedRaftMapping = _

    val updateSink = new TopologyUpdateSink with DirectoryUpdateSink with DatabaseStateUpdateSink with RaftMappingUpdateSink {
      override def onTopologyUpdate(topology: DatabaseCoreTopology) = actualCoreTopology = topology

      override def onTopologyUpdate(topology: DatabaseReadReplicaTopology) = actualReadReplicaTopology = topology

      override def onDbLeaderUpdate(leaderPerDb: java.util.Map[DatabaseId, LeaderInfo]) = actualLeaderPerDb = leaderPerDb

      override def onDbStateUpdate(databaseState: ReplicatedDatabaseState) = actualDatabaseStates = databaseState

      override def onRaftMappingUpdate(mapping: ReplicatedRaftMapping) = lastMapping = mapping
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
    val raftMappingSink = Source.queue[ReplicatedRaftMapping](1, OverflowStrategy.dropHead)
      .to(Sink.foreach(updateSink.onRaftMappingUpdate))
      .run(materializer)

    val identityModule = new InMemoryCoreServerIdentity()
    val serverId = identityModule.serverId()

    val databaseIds = Set(randomNamedDatabaseId(), randomNamedDatabaseId(), randomNamedDatabaseId())

    val readReplicaInfo = TestTopology.addressesForReadReplica(0, (Set.empty[DatabaseId] ++ databaseIds.map(_.databaseId())).asJava)

    val refresh = Duration(1, TimeUnit.SECONDS)

    val config = {
      val myCoreServerConfig = TestTopology.configFor(readReplicaInfo)
      val conf = Config.newBuilder()
        .fromConfig(myCoreServerConfig)
        .set(CausalClusteringSettings.cluster_topology_refresh, java.time.Duration.ofSeconds( refresh.toSeconds ) )
        .build()
      conf
    }

    val databaseStates = databaseIds.map(id => id -> new EnterpriseDatabaseState(id, EnterpriseOperatorState.STARTED)).toMap[NamedDatabaseId,DatabaseState]
    val databaseStateService = new StubDatabaseStateService(databaseStates.asJava, EnterpriseDatabaseState.unknown _)
    val discoveryMemberSnapshot = TestCoreDiscoveryMember.factory(identityModule,databaseStateService,Map.empty[DatabaseId,LeaderInfo].asJava)
    val clusterClientProbe = TestProbe()
    val props = ClientTopologyActor.props(
      discoveryMemberSnapshot,
      coreTopologySink,
      readReplicaTopologySink,
      discoverySink,
      databaseStateSink,
      raftMappingSink,
      clusterClientProbe.ref,
      config,
      NullLogProvider.getInstance(),
      Clocks.systemClock(),
      identityModule.serverId()
    )

    val topologyActorRef = system.actorOf(props)

    def expectMsg(msg: Any): Unit = {
      val send = ClusterClient.Publish(ReadReplicaViewActor.READ_REPLICA_TOPIC, msg)
      clusterClientProbe.fishForSpecificMessage(){ case `send` => }
    }

    def expectRefreshMsgWithDatabases(namedDatabaseIds: Set[NamedDatabaseId]): Unit = {
      val databaseIds = namedDatabaseIds.map(_.databaseId())
      clusterClientProbe.fishForSpecificMessage(defaultWaitTime) {
        case publish: ClusterClient.Publish if isRefreshMsgWithDatabases(publish.msg, databaseIds) => ()
      }
    }

    def isRefreshMsgWithDatabases(msg: Any, databaseIds: Set[DatabaseId]): Boolean = {
      msg match {
        case refreshMsg: ReadReplicaRefreshMessage => refreshMsg.readReplicaInfo().startedDatabaseIds.asScala == databaseIds
        case _ => false
      }
    }
  }
}
