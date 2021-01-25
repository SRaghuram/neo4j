/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.readreplicatopology

import java.time.Instant

import akka.cluster.client.ClusterClientReceptionist
import akka.stream.ActorMaterializer
import akka.stream.OverflowStrategy
import akka.stream.javadsl.Source
import akka.stream.scaladsl.Sink
import akka.testkit.TestProbe
import com.neo4j.causalclustering.discovery.DatabaseReadReplicaTopology
import com.neo4j.causalclustering.discovery.ReadReplicaInfo
import com.neo4j.causalclustering.discovery.ReplicatedDatabaseState
import com.neo4j.causalclustering.discovery.TestTopology
import com.neo4j.causalclustering.discovery.akka.BaseAkkaIT
import com.neo4j.causalclustering.discovery.akka.database.state.DiscoveryDatabaseState
import com.neo4j.causalclustering.identity.IdFactory
import com.neo4j.dbms.EnterpriseOperatorState.STARTED
import org.neo4j.configuration.Config
import org.neo4j.dbms.identity.ServerId
import org.neo4j.kernel.database.DatabaseId
import org.neo4j.kernel.database.NamedDatabaseId
import org.neo4j.kernel.database.TestDatabaseIdRepository.randomNamedDatabaseId
import org.neo4j.time.Clocks

import scala.collection.JavaConverters.mapAsJavaMapConverter
import scala.collection.JavaConverters.setAsJavaSetConverter

class ReadReplicaTopologyActorIT extends BaseAkkaIT("ReadReplicaTopologyActorIT") {

  "ReadReplicaTopologyActor" when {

    "receiving a read replica view message" should {

      "build topology" in new Fixture {
        Given("read replica view message with 3 entries")
        val namedDatabaseIds = Set(randomNamedDatabaseId(), randomNamedDatabaseId())
        val message = newReadReplicaViewMessage(3, namedDatabaseIds)

        When("send message")
        readReplicaTopologyActor ! message

        Then("topologies for all databases are built")
        val receivedTopology1 = topologyReceiver.expectMsgType[DatabaseReadReplicaTopology]
        val receivedTopology2 = topologyReceiver.expectMsgType[DatabaseReadReplicaTopology]

        Set(receivedTopology1.databaseId(), receivedTopology2.databaseId()) should equal(namedDatabaseIds.map(_.databaseId()))
      }

      "build empty topologies for removed databases" in new Fixture {
        Given("actor with a cached view containing a read replica topology")
        val initialDatabaseId = randomNamedDatabaseId()
        val newDatabaseId = randomNamedDatabaseId()
        readReplicaTopologyActor ! newReadReplicaViewMessage(1, Set(initialDatabaseId))
        topologyReceiver.expectMsgType[DatabaseReadReplicaTopology]

        When("send view message for a different database")
        val message = newReadReplicaViewMessage(1, Set(newDatabaseId))
        readReplicaTopologyActor ! message

        Then("empty topology is built for the previous database")
        val expectedEmptyTopology = new DatabaseReadReplicaTopology(initialDatabaseId.databaseId(), Map.empty[ServerId, ReadReplicaInfo].asJava )
        awaitAssert(topologyReceiver.expectMsg(expectedEmptyTopology), defaultWaitTime)

        And("non-empty topology is built for the new database")
        val expectedNewTopology = message.toReadReplicaTopology(newDatabaseId.databaseId())
        awaitAssert(topologyReceiver.expectMsg(expectedNewTopology), defaultWaitTime)
      }
    }
  }

  trait Fixture {

    val topologyReceiver = TestProbe("ReadReplicaTopologyReceiver")
    val stateReceiver = TestProbe("ReplicatedDatabaseStateReceiver")
    val materializer = ActorMaterializer()

    val readReplicaTopologySink = Source.queue[DatabaseReadReplicaTopology](1, OverflowStrategy.dropHead)
      .to(Sink.foreach(topology => topologyReceiver.ref ! topology))
      .run(materializer)

    val databaseStateSink = Source.queue[ReplicatedDatabaseState](1, OverflowStrategy.dropHead)
      .to(Sink.foreach(state=> stateReceiver.ref ! state))
      .run(materializer)

    val receptionist = ClusterClientReceptionist.get(system)

    val props = ReadReplicaTopologyActor.props(readReplicaTopologySink, databaseStateSink, receptionist, Config.defaults(), Clocks.systemClock())

    val readReplicaTopologyActor = system.actorOf(props)

    def newReadReplicaViewMessage(entriesCount: Int, databaseIds: Set[NamedDatabaseId]): ReadReplicaViewMessage = {
      val clusterClientReadReplicas = (1 to entriesCount).map(id => newReadReplicaViewRecord(id, databaseIds))
        .map(record => (record.topologyClientActorRef().path, record))
        .toMap

      new ReadReplicaViewMessage(clusterClientReadReplicas.asJava)
    }

    def newReadReplicaViewRecord(id: Int, databaseIds: Set[NamedDatabaseId]): ReadReplicaViewRecord = {
      val topologyClient = TestProbe("TopologyClient-" + id).ref
      val readReplicaInfo = TestTopology.addressesForReadReplica(id, (Set.empty[DatabaseId] ++ databaseIds.map(_.databaseId())).asJava)
      val states: Map[DatabaseId, DiscoveryDatabaseState] = databaseIds
        .map(_.databaseId)
        .map(id => (id, new DiscoveryDatabaseState(id, STARTED)) )
        .toMap
      new ReadReplicaViewRecord(readReplicaInfo, topologyClient, IdFactory.randomServerId(), Instant.now(), states.asJava )
    }
  }

}
