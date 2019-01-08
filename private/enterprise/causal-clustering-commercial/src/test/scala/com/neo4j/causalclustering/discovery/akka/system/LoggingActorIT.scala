/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.system



import akka.actor.ActorSystem
import akka.event.Logging
import akka.testkit.TestKit
import com.neo4j.causalclustering.discovery.akka.{BaseAkkaIT, NeoSuite}
import com.neo4j.causalclustering.discovery.akka.system.TypesafeConfigService.ArteryTransport
import org.neo4j.causalclustering.core.CausalClusteringSettings
import org.neo4j.kernel.configuration.Config
import org.neo4j.logging.AssertableLogProvider
import java.util.logging.Level


class LoggingActorIT extends NeoSuite {

  "LoggingActor receiving logging messages" when {

    "level is WARNING" should {

      "pass warning messages on to Neo logProvider" in new Fixture(Level.WARNING) {

        withLogging {
          logProvider.assertNoLogCallContaining("debug test")
          logProvider.assertNoLogCallContaining("info test")
          logProvider.assertContainsLogCallContaining("warning test")
        }
      }
    }

    "level is INFO" should {

      "pass info and warning messages on to Neo logProvider" in new Fixture(Level.INFO) {

        withLogging {
          logProvider.assertNoLogCallContaining("debug test")
          logProvider.assertContainsLogCallContaining("info test")
          logProvider.assertContainsLogCallContaining("warning test")
        }
      }
    }

    "level is DEBUG" should {

      "pass all messages on to Neo logProvider" in new Fixture(Level.ALL) {

        withLogging {
          logProvider.assertContainsLogCallContaining("info test")
          logProvider.assertContainsLogCallContaining("warning test")
          logProvider.assertContainsLogCallContaining("debug test")
        }
      }
    }
  }

  abstract class Fixture(logLevel: Level) {

    val config = Config.defaults
    config.augment(CausalClusteringSettings.disable_middleware_logging, String.valueOf(false))
    config.augment(CausalClusteringSettings.middleware_logging_level, String.valueOf(logLevel.intValue))

    val testSystem = ActorSystem("testSystem", new TypesafeConfigService(ArteryTransport.TCP, config).generate())
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

