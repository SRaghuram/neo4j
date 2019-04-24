/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.readreplicatopology

import java.time.Instant
import java.util.UUID

import akka.cluster.client.ClusterClientReceptionist
import akka.stream.javadsl.Source
import akka.stream.scaladsl.Sink
import akka.stream.{ActorMaterializer, OverflowStrategy}
import akka.testkit.TestProbe
import com.neo4j.causalclustering.discovery.akka.BaseAkkaIT
import com.neo4j.causalclustering.discovery.{DatabaseReadReplicaTopology, ReadReplicaInfo, TestTopology}
import com.neo4j.causalclustering.identity.MemberId
import org.neo4j.configuration.Config
import org.neo4j.kernel.database.DatabaseId
import org.neo4j.logging.NullLogProvider
import org.neo4j.time.Clocks

import scala.collection.JavaConverters._

class ReadReplicaTopologyActorIT extends BaseAkkaIT("ReadReplicaTopologyActorIT") {

  "ReadReplicaTopologyActor" when {

    "receiving a read replica view message" should {

      "build topology" in new Fixture {
        Given("read replica view message with 3 entries")
        val databaseIds = Set(new DatabaseId("orders"), new DatabaseId("customers"))
        val message = newReadReplicaViewMessage(3, databaseIds)

        When("send message")
        readReplicaTopologyActor ! message

        Then("topologies for all databases are built")
        val receivedTopology1 = topologyReceiver.expectMsgType[DatabaseReadReplicaTopology]
        val receivedTopology2 = topologyReceiver.expectMsgType[DatabaseReadReplicaTopology]

        Set(receivedTopology1.databaseId(), receivedTopology2.databaseId()) should equal(databaseIds)
      }

      "build empty topologies for removed databases" in new Fixture {
        Given("actor with a cached view containing a read replica topology")
        val databaseIds = Set(new DatabaseId("employees"))
        readReplicaTopologyActor ! newReadReplicaViewMessage(1, databaseIds)
        topologyReceiver.expectMsgType[DatabaseReadReplicaTopology]

        When("send empty view message")
        readReplicaTopologyActor ! newReadReplicaViewMessage(0, databaseIds)

        Then("empty topology is built")
        val expectedEmptyTopology = new DatabaseReadReplicaTopology(new DatabaseId("employees"), Map.empty[MemberId, ReadReplicaInfo].asJava)
        topologyReceiver.expectMsg(expectedEmptyTopology)
      }
    }
  }

  trait Fixture {

    val topologyReceiver = TestProbe("ReadReplicaTopologyReceiver")
    val materializer = ActorMaterializer()

    val readReplicaTopologySink = Source.queue[DatabaseReadReplicaTopology](1, OverflowStrategy.dropHead)
      .to(Sink.foreach(topology => topologyReceiver.ref ! topology))
      .run(materializer)

    val receptionist = ClusterClientReceptionist.get(system)

    val props = ReadReplicaTopologyActor.props(readReplicaTopologySink,
      receptionist,
      NullLogProvider.getInstance(),
      Config.defaults(),
      Clocks.systemClock())

    val readReplicaTopologyActor = system.actorOf(props)

    def newReadReplicaViewMessage(entriesCount: Int, databaseIds: Set[DatabaseId]): ReadReplicaViewMessage = {
      val clusterClientReadReplicas = (1 to entriesCount).map(id => newReadReplicaViewRecord(id, databaseIds))
        .map(record => (record.topologyClientActorRef(), record))
        .toMap

      new ReadReplicaViewMessage(clusterClientReadReplicas.asJava)
    }

    def newReadReplicaViewRecord(id: Int, databaseIds: Set[DatabaseId]): ReadReplicaViewRecord = {
      val topologyClient = TestProbe("TopologyClient-" + id).ref
      val readReplicaInfo = TestTopology.addressesForReadReplica(id, databaseIds.asJava)
      new ReadReplicaViewRecord(readReplicaInfo, topologyClient, new MemberId(UUID.randomUUID()), Instant.now())
    }
  }

}
