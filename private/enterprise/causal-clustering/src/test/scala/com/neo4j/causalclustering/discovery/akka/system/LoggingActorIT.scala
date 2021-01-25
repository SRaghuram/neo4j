/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.system

import akka.actor.ActorSystem
import akka.event.Logging
import akka.testkit.TestKit
import com.neo4j.causalclustering.discovery.TestFirstStartupDetector
import com.neo4j.causalclustering.discovery.akka.NeoSuite
import com.neo4j.causalclustering.discovery.akka.system.TypesafeConfigService.ArteryTransport
import com.neo4j.configuration.CausalClusteringSettings
import org.neo4j.configuration.Config
import org.neo4j.logging.AssertableLogProvider
import org.neo4j.logging.Level
import org.neo4j.logging.LogAssertions.assertThat

class LoggingActorIT extends NeoSuite {

  "LoggingActor receiving logging messages" when {

    "level is WARNING" should {

      "pass warning messages on to Neo logProvider" in new Fixture(Level.WARN) {

        withLogging {
          assertThat(logProvider).doesNotContainMessage("debug test")
          assertThat(logProvider).doesNotContainMessage("info test")
          assertThat(logProvider).containsMessages("warning test")
        }
      }
    }

    "level is INFO" should {

      "pass info and warning messages on to Neo logProvider" in new Fixture(Level.INFO) {

        withLogging {
          println( Thread.currentThread().getName )
          assertThat(logProvider).doesNotContainMessage("debug test")
          assertThat(logProvider).containsMessages("info test", "warning test")
        }
      }
    }

    "level is DEBUG" should {

      "pass all messages on to Neo logProvider" in new Fixture(Level.DEBUG) {

        withLogging {
          assertThat(logProvider).containsMessages("info test", "warning test", "debug test")
        }
      }
    }
  }

  abstract class Fixture(logLevel: Level) {

    val config = Config.defaults(CausalClusteringSettings.middleware_logging_level, logLevel)

    val testSystem = ActorSystem("testSystem", new TypesafeConfigService(ArteryTransport.TCP, new TestFirstStartupDetector(true), config).generate())
    val loggingContext = "LoggingActorIT"
    val logProvider = new AssertableLogProvider(true)
    LoggingFilter.enable(logProvider)
    LoggingActor.enable(testSystem, logProvider)
    val logger = Logging(testSystem, loggingContext)

    def withLogging( assertions: => Unit ): Unit = {
      logger.debug("debug test")
      logger.info("info test")
      logger.warning("warning test")

      assertions
      cleanUp()
    }

    def cleanUp(): Unit = {
      LoggingActor.disable(testSystem)
      TestKit.shutdownActorSystem(testSystem)
    }
  }
}

