/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.database.state

import java.util.UUID

import akka.cluster.ddata.{LWWMap, LWWMapKey, Replicator}
import akka.stream.javadsl.Source
import akka.stream.scaladsl.Sink
import akka.stream.{ActorMaterializer, OverflowStrategy}
import akka.testkit.TestProbe
import com.neo4j.causalclustering.discovery.ReplicatedDatabaseState
import com.neo4j.causalclustering.discovery.akka.monitoring.ReplicatedDataIdentifier
import com.neo4j.causalclustering.discovery.akka.{BaseAkkaIT, DatabaseStateUpdateSink}
import com.neo4j.causalclustering.identity.MemberId
import com.neo4j.dbms.{EnterpriseDatabaseState, EnterpriseOperatorState}
import org.neo4j.dbms.DatabaseState
import org.neo4j.kernel.database.TestDatabaseIdRepository.randomDatabaseId
import org.neo4j.kernel.database.DatabaseId

import scala.collection.JavaConverters._

class DatabaseStateActorIT extends BaseAkkaIT("DatabaseStateActorIT") {
  "database state actor" should {

    behave like replicatedDataActor(new Fixture())

    "should put to replicator when sending a database state" in new Fixture {
      Given("new database state message")
      val dbId = randomDatabaseId
      val dbState = new EnterpriseDatabaseState(dbId, EnterpriseOperatorState.STARTED)

      When("database state update received")
      replicatedDataActorRef ! dbState

      Then("send database state to replicator")
      val update = expectReplicatorUpdates(replicator, dataKey)
      val ddata = update.modify.apply(Option(LWWMap.create()))
      val dbToMember = new DatabaseToMember(dbId, myself)
      val ddataEntry = ddata.entries(dbToMember)
      ddataEntry.databaseId should equal(dbId)
      ddataEntry.operatorState should equal(EnterpriseOperatorState.STARTED)
    }

    "should remove from replicator when sending a dropped database state" in new Fixture {
      Given("new removed database state message")
      val dbId = randomDatabaseId
      val dbState = new EnterpriseDatabaseState(dbId, EnterpriseOperatorState.DROPPED)

      When("database state update received")
      replicatedDataActorRef ! dbState

      Then("remove database state from replicator")
      val update = expectReplicatorUpdates(replicator, dataKey)
      val ddata = update.modify.apply(Option(LWWMap.create()))
      val dbToMember = new DatabaseToMember(dbId, myself)
      val ddataEntry = ddata.entries.get(dbToMember)
      ddataEntry shouldBe empty
    }

    "should send to update sink and rrTopologyActor for each database state update" in new Fixture {
      Given("new database state message")
      val dbId = randomDatabaseId
      val dbState: DatabaseState = new EnterpriseDatabaseState(dbId, EnterpriseOperatorState.STARTED)
      val dbToMember = new DatabaseToMember(dbId, myself)
      val update = LWWMap.empty.put(cluster, dbToMember, dbState)
      val replicatedDbState = ReplicatedDatabaseState.ofCores(dbId, Map(myself -> dbState).asJava)

      When("database state update received")
      replicatedDataActorRef ! Replicator.Changed(dataKey)(update)

      Then("send message to update sink")
      awaitAssert(actualDatabaseStates should contain (dbId -> replicatedDbState))
      rrTopologyProbe.expectMsg(defaultWaitTime, new AllReplicatedDatabaseStates(List(replicatedDbState).asJava))
    }

  }

  class Fixture extends ReplicatedDataActorFixture[LWWMap[DatabaseToMember,DatabaseState]] {
    val rrTopologyProbe = TestProbe("rrTopology")
    val myself = new MemberId(UUID.randomUUID)
    var actualDatabaseStates = Map.empty[DatabaseId,ReplicatedDatabaseState]

    val updateSink = new DatabaseStateUpdateSink {
      override def onDbStateUpdate(databaseState: ReplicatedDatabaseState) = {
        if ( databaseState.isEmpty )
          actualDatabaseStates -= databaseState.databaseId
        else
          actualDatabaseStates += (databaseState.databaseId -> databaseState)
      }
    }

    val discoverySink = Source.queue[ReplicatedDatabaseState](1, OverflowStrategy.dropHead)
      .to(Sink.foreach(updateSink.onDbStateUpdate))
      .run(ActorMaterializer())

    val replicatedDataActorRef = system.actorOf(DatabaseStateActor.props(
      cluster, replicator.ref, discoverySink, rrTopologyProbe.ref, monitor, myself))
    val dataKey = LWWMapKey.create[DatabaseToMember,DatabaseState](ReplicatedDataIdentifier.DATABASE_STATE.keyName())
    val data: LWWMap[DatabaseToMember, DatabaseState] = LWWMap.empty[DatabaseToMember,DatabaseState]
  }
}
