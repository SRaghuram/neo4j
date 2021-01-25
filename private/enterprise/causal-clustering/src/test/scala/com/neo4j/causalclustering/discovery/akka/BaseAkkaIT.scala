/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka

import java.util.concurrent.Callable
import java.util.concurrent.TimeUnit

import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.BootstrapSetup
import akka.actor.ProviderSelection
import akka.cluster.Cluster
import akka.cluster.ddata.Key
import akka.cluster.ddata.ReplicatedData
import akka.cluster.ddata.Replicator
import akka.testkit.ImplicitSender
import akka.testkit.TestKit
import akka.testkit.TestProbe
import akka.util.Timeout
import com.neo4j.causalclustering.discovery.TestFirstStartupDetector
import com.neo4j.causalclustering.discovery.akka.BaseReplicatedDataActor.MetricsRefresh
import com.neo4j.causalclustering.discovery.akka.coretopology.ClusterViewMessageTest
import com.neo4j.causalclustering.discovery.akka.monitoring.ReplicatedDataIdentifier
import com.neo4j.causalclustering.discovery.akka.monitoring.ReplicatedDataMonitor
import com.neo4j.causalclustering.discovery.akka.system.TypesafeConfigService
import com.neo4j.causalclustering.discovery.akka.system.TypesafeConfigService.ArteryTransport
import com.neo4j.configuration.CausalClusteringSettings
import com.neo4j.dbms.EnterpriseDatabaseState
import com.neo4j.dbms.EnterpriseOperatorState
import org.assertj.core.api.Condition
import org.junit.runner.RunWith
import org.neo4j.configuration.Config
import org.neo4j.configuration.helpers.SocketAddress
import org.neo4j.dbms.DatabaseState
import org.neo4j.dbms.DatabaseStateService
import org.neo4j.dbms.StubDatabaseStateService
import org.neo4j.kernel.database.NamedDatabaseId
import org.neo4j.test.assertion.Assert.assertEventually
import org.scalatest.exceptions.TestFailedException
import org.scalatest.junit.JUnitRunner

import scala.collection.JavaConverters.mapAsJavaMapConverter
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.Duration
import scala.concurrent.duration.FiniteDuration

object BaseAkkaIT {

  type TSConf = com.typesafe.config.Config

  def config: TSConf = {
    val discoveryListenPort = 0
    val config = Config.defaults(CausalClusteringSettings.discovery_listen_address, new SocketAddress( "localhost", discoveryListenPort ) )

    new TypesafeConfigService(ArteryTransport.TCP, new TestFirstStartupDetector(true), config).generate()
  }

  def bootstrapSetup: BootstrapSetup =
    BootstrapSetup().withActorRefProvider(ProviderSelection.cluster()).withConfig(config)
}

/**
  * IT because it opens a real port, so needs a PortAuthority defined that ensures port uniqueness across JVM forks
  */
@RunWith(classOf[JUnitRunner])
abstract class BaseAkkaIT(name: String) extends TestKit(ActorSystem(name, BaseAkkaIT.bootstrapSetup))
    with ImplicitSender
    with NeoSuite {

  val defaultWaitTime = Duration(10, TimeUnit.SECONDS)
  val TRUE_CONDITION = new Condition[Boolean]((value: Boolean) => value, "Should be true.")
  implicit val timeout = Timeout(defaultWaitTime)
  implicit val execContext = ExecutionContext.global

  override protected def beforeAll(): Unit = ClusterViewMessageTest.setMemberConstructor(true)

  override def afterAll {
    ClusterViewMessageTest.setMemberConstructor(false)
    TestKit.shutdownActorSystem(system)
  }

  /**
    * Be as broad as possible in describing state that shouldn't happen.
    */
  def awaitNotCond(p: => Boolean, max: FiniteDuration = defaultWaitTime, message: String) = {
    awaitNot(awaitCond(p,max), message)
  }

  /**
    * Be as broad as possible in describing what shouldn't happen.
    * If using Mockito.verify() ensure that atLeast or atMost is used to avoid erroneously passing tests
    * Without either then there is an implicit exactlyOnce
    */
  def awaitNotAssert[A](a: => A, max: FiniteDuration = defaultWaitTime, message: String) = {
    awaitNot(awaitAssert(a,max), message)
  }

  private def awaitNot(await: => Unit, message: String) = {
    try {
      await
      fail(message)
    }
    catch {
      case _: AssertionError => () // thrown by awaitCond and expected
      case e: TestFailedException => throw e // thrown by fail()
    }
  }

  def expectReplicatorUpdates[A <: ReplicatedData](replicator: TestProbe, key: Key[A]): Replicator.Update[A] = {
    replicator.fishForSpecificMessage(defaultWaitTime) {
      case update: Replicator.Update[A] if update.key == key =>
        update
    }
  }

  def expectNoReplicatorUpdates[A <: ReplicatedData](replicator: TestProbe, key: Key[A]): Unit = {
    val messages = replicator.receiveWhile(defaultWaitTime) { case msg => msg }
    messages.foreach {
      case update: Replicator.Update[A] if update.key == key =>
        fail(s"Received unexpected replicator update: ${update}")
      case _ => //no-op
    }
  }

  def replicatedDataActor[A <: ReplicatedData](newFixture: => ReplicatedDataActorFixture[A]) = {
    "subscribe to replicator" in {
      val fixture = newFixture
      fixture.replicator.fishForSpecificMessage(defaultWaitTime) {
        case Replicator.Subscribe(fixture.dataKey, fixture.replicatedDataActorRef) => ()
      }
    }
    "unsubscribe from replicator" in {
      val fixture = newFixture
      system.stop(fixture.replicatedDataActorRef)
      fixture.replicator.fishForSpecificMessage(defaultWaitTime) {
        case Replicator.Unsubscribe(fixture.dataKey, fixture.replicatedDataActorRef) => ()
      }
    }
    "update metrics on changed data" in {
      val fixture = newFixture
      fixture.replicatedDataActorRef ! Replicator.Changed(fixture.dataKey)(fixture.data)
      assertEventually(fixture.monitor.visSet, TRUE_CONDITION, defaultWaitTime.toMillis, TimeUnit.MILLISECONDS )
      assertEventually(fixture.monitor.invisSet, TRUE_CONDITION, defaultWaitTime.toMillis, TimeUnit.MILLISECONDS )
    }
    "update metrics on tick" in {
      val fixture = newFixture
      fixture.replicatedDataActorRef ! MetricsRefresh.getInstance()
      assertEventually(fixture.monitor.visSet, TRUE_CONDITION, defaultWaitTime.toMillis, TimeUnit.MILLISECONDS )
      assertEventually(fixture.monitor.invisSet, TRUE_CONDITION, defaultWaitTime.toMillis, TimeUnit.MILLISECONDS )
    }
  }

  trait ReplicatedDataActorFixture[A <: ReplicatedData]
  {
    val replicator: TestProbe = TestProbe("replicator")
    val cluster = Cluster.get(system)
    val monitor = new ReplicatedDataMonitor {
      private var hasSetVisible = false
      private var hasSetInvisible = false

      def visSet = new Callable[Boolean] {
        override def call(): Boolean = hasSetVisible
      }
      def invisSet = new Callable[Boolean] {
        override def call(): Boolean = hasSetInvisible
      }

      override def setVisibleDataSize(key: ReplicatedDataIdentifier, size: Int): Unit = hasSetVisible = true
      override def setInvisibleDataSize(key: ReplicatedDataIdentifier, size: Int): Unit = hasSetInvisible = true
    }

    val replicatedDataActorRef: ActorRef
    val dataKey: Key[A]
    val data: A

    def databaseStates(databases: Set[NamedDatabaseId], state: EnterpriseOperatorState): Map[NamedDatabaseId,DatabaseState] =
      databases.map(id => id -> new EnterpriseDatabaseState(id, state)).toMap

    def databaseStateService(startedDatabases: Set[NamedDatabaseId]): DatabaseStateService =
      new StubDatabaseStateService(databaseStates(startedDatabases, EnterpriseOperatorState.STARTED).asJava,
        EnterpriseDatabaseState.unknown _)

    def databaseStateService(databaseStates: Map[NamedDatabaseId,DatabaseState]) =
      new StubDatabaseStateService(databaseStates.asJava, EnterpriseDatabaseState.unknown _)

  }
}
