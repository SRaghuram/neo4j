/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.database.state

import akka.cluster.ddata.LWWMap
import akka.cluster.ddata.LWWMapKey
import akka.cluster.ddata.Replicator
import akka.stream.ActorMaterializer
import akka.stream.OverflowStrategy
import akka.stream.javadsl.Source
import akka.stream.scaladsl.Sink
import akka.testkit.TestProbe
import com.neo4j.causalclustering.core.consensus.LeaderInfo
import com.neo4j.causalclustering.discovery.ReplicatedDatabaseState
import com.neo4j.causalclustering.discovery.TestDiscoveryMember
import com.neo4j.causalclustering.discovery.akka.BaseAkkaIT
import com.neo4j.causalclustering.discovery.akka.DatabaseStateUpdateSink
import com.neo4j.causalclustering.discovery.akka.PublishInitialData
import com.neo4j.causalclustering.discovery.akka.monitoring.ReplicatedDataIdentifier
import com.neo4j.causalclustering.discovery.member.DiscoveryMemberFactory
import com.neo4j.causalclustering.identity.StubClusteringIdentityModule
import com.neo4j.dbms.EnterpriseDatabaseState
import com.neo4j.dbms.EnterpriseOperatorState
import org.neo4j.dbms.DatabaseState
import org.neo4j.kernel.database.DatabaseId
import org.neo4j.kernel.database.NamedDatabaseId
import org.neo4j.kernel.database.TestDatabaseIdRepository.randomDatabaseId
import org.neo4j.kernel.database.TestDatabaseIdRepository.randomNamedDatabaseId

import scala.collection.JavaConverters.mapAsJavaMapConverter
import scala.collection.JavaConverters.seqAsJavaListConverter

class DatabaseStateActorIT extends BaseAkkaIT("DatabaseStateActorIT") {
  "database state actor" should {

    behave like replicatedDataActor(new Fixture())

    "should put to replicator when sending a database state" in new Fixture {
      Given("new database state message")
      val dbId = randomNamedDatabaseId.databaseId
      val dbState = new DiscoveryDatabaseState(dbId, EnterpriseOperatorState.STARTED)

      When("database state update received")
      replicatedDataActorRef ! dbState

      Then("send database state to replicator")
      val update = expectReplicatorUpdates(replicator, dataKey)
      val ddata = update.modify.apply(Option(LWWMap.create()))
      val dbToMember = new DatabaseToMember(dbId, identityModule.memberId())
      val ddataEntry = ddata.entries(dbToMember)
      ddataEntry.databaseId should equal(dbId)
      ddataEntry.operatorState should equal(EnterpriseOperatorState.STARTED)
    }

    "should remove from replicator when sending a dropped database state" in new Fixture {
      Given("new removed database state message")
      val dbId = randomNamedDatabaseId.databaseId
      val dbState = new DiscoveryDatabaseState(dbId, EnterpriseOperatorState.DROPPED)

      When("database state update received")
      replicatedDataActorRef ! dbState

      Then("remove database state from replicator")
      val update = expectReplicatorUpdates(replicator, dataKey)
      val ddata = update.modify.apply(Option(LWWMap.create()))
      val dbToMember = new DatabaseToMember(dbId, identityModule.memberId())
      val ddataEntry = ddata.entries.get(dbToMember)
      ddataEntry shouldBe empty
    }

    "should send to update sink and rrTopologyActor for each database state update" in new Fixture {
      Given("new database state message")
      val dbId = randomDatabaseId
      val dbState = new DiscoveryDatabaseState(dbId, EnterpriseOperatorState.STARTED)
      val dbToMember = new DatabaseToMember(dbId, identityModule.memberId())
      val update = LWWMap.empty.put(cluster, dbToMember, dbState)
      // TODO: change to identitModule.myself() when ReplicatedDatabaseState is updated to use ServerId
      val replicatedDbState = ReplicatedDatabaseState.ofCores(dbId, Map(identityModule.memberId() -> dbState).asJava)

      When("database state update received")
      replicatedDataActorRef ! Replicator.Changed(dataKey)(update)

      Then("send message to update sink")
      awaitAssert(actualDatabaseStates should contain(dbId -> replicatedDbState))
      rrTopologyProbe.expectMsg(defaultWaitTime, new AllReplicatedDatabaseStates(List(replicatedDbState).asJava))
    }

    "should send initial data to replicator when asked" in new Fixture {
      Given("some initial states and a DatabaseStateActor")
      val dbId1 = randomNamedDatabaseId
      val dbId2 = randomNamedDatabaseId
      val states = Map[NamedDatabaseId, DatabaseState](
        dbId1 -> new EnterpriseDatabaseState(dbId1, EnterpriseOperatorState.STARTED),
        dbId2 -> new EnterpriseDatabaseState(dbId2, EnterpriseOperatorState.STOPPED)
      )
      val stateService = databaseStateService(states)

      val snapshotFactory: DiscoveryMemberFactory = TestDiscoveryMember.factory _
      val replicatorProbe = TestProbe("replicatorProbe")
      val actor = system.actorOf(DatabaseStateActor.props(cluster, replicatorProbe.ref, discoverySink, rrTopologyProbe.ref, monitor, identityModule.myself()))

      When("PublishInitialData request received")
      actor ! new PublishInitialData(snapshotFactory.createSnapshot(identityModule, stateService, Map.empty[DatabaseId,LeaderInfo].asJava))

      Then("the initial state should be published")
      val update = expectReplicatorUpdates(replicatorProbe, dataKey)
      val ddata = update.modify(Option(LWWMap.empty))
      ddata.size shouldBe 2
      ddata.entries.map({ case (k,v) => k.databaseId -> v.operatorState }) shouldBe states.map({ case (k,v) => k.databaseId -> v.operatorState })
    }

  }

  class Fixture extends ReplicatedDataActorFixture[LWWMap[DatabaseToMember, DiscoveryDatabaseState]] {
    val rrTopologyProbe = TestProbe("rrTopology")
    val identityModule = new StubClusteringIdentityModule
    var actualDatabaseStates = Map.empty[DatabaseId, ReplicatedDatabaseState]

    val updateSink = new DatabaseStateUpdateSink {
      override def onDbStateUpdate(databaseState: ReplicatedDatabaseState) = {
        if (databaseState.isEmpty) {
          actualDatabaseStates -= databaseState.databaseId
        } else {
          actualDatabaseStates += (databaseState.databaseId -> databaseState)
        }
      }
    }

    val discoverySink = Source.queue[ReplicatedDatabaseState](1, OverflowStrategy.dropHead)
      .to(Sink.foreach(updateSink.onDbStateUpdate))
      .run(ActorMaterializer())

    val replicatedDataActorRef = system.actorOf(
      DatabaseStateActor.props(cluster, replicator.ref, discoverySink, rrTopologyProbe.ref, monitor, identityModule.myself()))
    val dataKey = LWWMapKey.create[DatabaseToMember, DiscoveryDatabaseState](ReplicatedDataIdentifier.DATABASE_STATE.keyName())
    val data: LWWMap[DatabaseToMember, DiscoveryDatabaseState] = LWWMap.empty[DatabaseToMember, DiscoveryDatabaseState]
  }

}
