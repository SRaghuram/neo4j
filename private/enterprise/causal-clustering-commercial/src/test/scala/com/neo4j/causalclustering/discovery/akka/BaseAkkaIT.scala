/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka

import java.util.concurrent.TimeUnit

import akka.actor.{ActorRef, ActorSystem, BootstrapSetup, ProviderSelection}
import akka.cluster.Cluster
import akka.cluster.ddata.{Key, ReplicatedData, Replicator}
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import com.neo4j.causalclustering.discovery.akka.coretopology.ClusterViewMessageTest
import com.neo4j.causalclustering.discovery.akka.system.TypesafeConfigService
import com.neo4j.causalclustering.discovery.akka.system.TypesafeConfigService.ArteryTransport
import org.junit.runner.RunWith
import org.neo4j.causalclustering.discovery.NoOpHostnameResolver
import org.neo4j.kernel.configuration.Config
import org.neo4j.ports.allocation.PortAuthority
import org.scalatest.exceptions.TestFailedException
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar
import org.scalatest.{BeforeAndAfterAll, GivenWhenThen, Matchers, WordSpecLike}

import scala.concurrent.duration.{Duration, FiniteDuration}

object BaseAkkaIT {
  def config: com.typesafe.config.Config = {
    val discoveryListenPort = PortAuthority.allocatePort()
    val config = Config.defaults()
    config.augment("causal_clustering.discovery_listen_address", s":$discoveryListenPort")
    config.augment("causal_clustering.initial_discovery_members", s"localhost:$discoveryListenPort")

    new TypesafeConfigService(new NoOpHostnameResolver(), ArteryTransport.TCP, config).generate()
  }

  def bootstrapSetup: BootstrapSetup = BootstrapSetup().withActorRefProvider(ProviderSelection.cluster()).withConfig(config)
}

/**
  * IT because it opens a real port, so needs a PortAuthority defined that ensures port uniqueness across JVM forks
  */
@RunWith(classOf[JUnitRunner])
abstract class BaseAkkaIT(name: String) extends TestKit(ActorSystem(name, BaseAkkaIT.bootstrapSetup))
    with ImplicitSender
    with WordSpecLike
    with Matchers
    with BeforeAndAfterAll
    with GivenWhenThen
    with MockitoSugar {

  val defaultWaitTime = Duration(3, TimeUnit.SECONDS)

  override protected def beforeAll(): Unit = ClusterViewMessageTest.setMemberConstructor(true)

  override def afterAll {
    ClusterViewMessageTest.setMemberConstructor(false)
    TestKit.shutdownActorSystem(system)
  }

  def awaitNotCond(p: => Boolean, max: FiniteDuration = defaultWaitTime, message: String) = {
    awaitNot(awaitCond(p,max), message)
  }

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

  def expectReplicatorUpdates(replicator: TestProbe, key:Key[_]) = {
    replicator.fishForSpecificMessage(defaultWaitTime) {
      case Replicator.Update(`key`, _, _) => ()
    }
  }

  def replicatedDataActor[A <: ReplicatedData](newFixture: => ReplicatedDataActorFixture[A]) = {
    "subscribe to replicator" in {
      val fixture = newFixture
      import fixture._
      replicator.fishForSpecificMessage(defaultWaitTime) {
        case Replicator.Subscribe(`dataKey`, `replicatedDataActorRef`) => ()
      }
    }
    "unsubscribe from replicator" in {
      val fixture = newFixture
      import fixture._
      system.stop(replicatedDataActorRef)
      replicator.fishForSpecificMessage(defaultWaitTime) {
        case Replicator.Unsubscribe(`dataKey`, `replicatedDataActorRef`) => ()
      }
    }
  }

  trait ReplicatedDataActorFixture[A <: ReplicatedData]
  {
    val replicator: TestProbe = TestProbe("replicator")
    val cluster = Cluster.get(system)

    val replicatedDataActorRef: ActorRef
    val dataKey: Key[A]
  }
}
