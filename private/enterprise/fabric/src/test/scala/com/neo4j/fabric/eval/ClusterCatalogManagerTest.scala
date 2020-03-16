/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.eval

import java.time.Duration
import java.util
import java.util.UUID

import com.neo4j.causalclustering.identity.MemberId
import com.neo4j.fabric.FabricTest
import com.neo4j.fabric.config.FabricConfig
import com.neo4j.fabric.config.FabricConfig.GlobalDriverConfig
import com.neo4j.fabric.config.FabricConfig.Graph
import com.neo4j.fabric.eval.Catalog.ExternalGraph
import com.neo4j.fabric.eval.Catalog.InternalGraph
import com.neo4j.fabric.executor.Location
import org.neo4j.configuration.helpers.NormalizedDatabaseName
import org.neo4j.configuration.helpers.NormalizedGraphName
import org.neo4j.configuration.helpers.SocketAddress
import org.neo4j.cypher.internal.ast.CatalogName
import org.neo4j.kernel.database.DatabaseIdFactory
import org.neo4j.kernel.database.NamedDatabaseId
import org.neo4j.values.storable.Values

import scala.collection.JavaConverters.seqAsJavaListConverter

class ClusterCatalogManagerTest extends FabricTest {

  private val mega0 = new Graph(0L, FabricConfig.RemoteUri.create("bolt://mega:1111"), "neo4j0", new NormalizedGraphName("extA"), null)
  private val mega1 = new Graph(1L, FabricConfig.RemoteUri.create("bolt://mega:2222"), "neo4j1", null, null)
  private val mega2 = new Graph(2L, FabricConfig.RemoteUri.create("bolt://mega:3333"), "neo4j2", new NormalizedGraphName("extB"), null)

  private val config = new FabricConfig(
    true,
    new FabricConfig.Database(new NormalizedDatabaseName("mega"), util.Set.of(mega0, mega1, mega2)),
    util.List.of(), Duration.ZERO, Duration.ZERO,
    new GlobalDriverConfig(Duration.ZERO, Duration.ZERO, 0, null),
    new FabricConfig.DataStream(300, 1000, 50, 10)
  )

  private val intA = DatabaseIdFactory.from("intA", UUID.randomUUID())
  private val intB = DatabaseIdFactory.from("intB", UUID.randomUUID())
  private val mega = DatabaseIdFactory.from("mega", UUID.randomUUID())

  private val internalDbs = Set(intA, intB, mega)

  private val myId = new MemberId(UUID.randomUUID())
  private val remoteId = new MemberId(UUID.randomUUID())
  private val remoteAddress = new SocketAddress("remote", 1234)
  private val remoteAddresses = Map(remoteId -> remoteAddress)

  def createManager(leaderMapping: Map[NamedDatabaseId, MemberId]) = new ClusterCatalogManager(
    databaseLookup = new DatabaseLookup {
      def databaseIds: Set[NamedDatabaseId] = internalDbs
      def databaseId(databaseName: NormalizedDatabaseName): Option[NamedDatabaseId] = internalDbs.find(_.name() == databaseName.name())
    },
    leaderLookup = new LeaderLookup {
      def memberId: MemberId = myId
      def leaderId(databaseId: NamedDatabaseId): Option[MemberId] = leaderMapping.get(databaseId)
      def leaderBoltAddress(databaseId: NamedDatabaseId): Option[SocketAddress] = leaderId(databaseId).flatMap(remoteAddresses.get)
    },
    fabricConfig = config,
  )

  "catalog resolution" in {

    val catalog = createManager(Map.empty).currentCatalog()

    catalog.resolve(CatalogName("mega", "extA"))
      .shouldEqual(external(mega0))

    catalog.resolve(CatalogName("mega", "graph"), Seq(Values.of(1)))
      .shouldEqual(external(mega1))

    catalog.resolve(CatalogName("mega", "extB"))
      .shouldEqual(external(mega2))

    catalog.resolve(CatalogName("intA"))
      .shouldEqual(internal(3, "intA"))

    catalog.resolve(CatalogName("intB"))
      .shouldEqual(internal(4, "intB"))

    catalog.resolve(CatalogName("mega"))
      .shouldEqual(internal(5, "mega"))
  }

  "location resolution" - {

    "when all leaders are local" - {

      val manager = createManager(Map(
        intA -> myId,
        intB -> myId,
        mega -> myId,
      ))

      val catalog = manager.currentCatalog()

      "and writable required" in {

        val writable = true

        manager.locationOf(catalog.resolve(CatalogName("mega", "extA")), writable)
          .shouldEqual(new Location.Remote(0, remoteUri(mega0.getUri), "neo4j0"))

        manager.locationOf(catalog.resolve(CatalogName("mega", "graph"), Seq(Values.of(1))), writable)
          .shouldEqual(new Location.Remote(1, remoteUri(mega1.getUri), "neo4j1"))

        manager.locationOf(catalog.resolve(CatalogName("mega", "extB")), writable)
          .shouldEqual(new Location.Remote(2, remoteUri(mega2.getUri), "neo4j2"))

        manager.locationOf(catalog.resolve(CatalogName("intA")), writable)
          .shouldEqual(new Location.Local(3, "inta"))

        manager.locationOf(catalog.resolve(CatalogName("intB")), writable)
          .shouldEqual(new Location.Local(4, "intb"))

        manager.locationOf(catalog.resolve(CatalogName("mega")), writable)
          .shouldEqual(new Location.Local(5, "mega"))
      }

      "and writable not required" in {

        val writable = false

        manager.locationOf(catalog.resolve(CatalogName("mega", "extA")), writable)
          .shouldEqual(new Location.Remote(0, remoteUri(mega0.getUri), "neo4j0"))

        manager.locationOf(catalog.resolve(CatalogName("mega", "graph"), Seq(Values.of(1))), writable)
          .shouldEqual(new Location.Remote(1, remoteUri(mega1.getUri), "neo4j1"))

        manager.locationOf(catalog.resolve(CatalogName("mega", "extB")), writable)
          .shouldEqual(new Location.Remote(2, remoteUri(mega2.getUri), "neo4j2"))

        manager.locationOf(catalog.resolve(CatalogName("intA")), writable)
          .shouldEqual(new Location.Local(3, "inta"))

        manager.locationOf(catalog.resolve(CatalogName("intB")), writable)
          .shouldEqual(new Location.Local(4, "intb"))

        manager.locationOf(catalog.resolve(CatalogName("mega")), writable)
          .shouldEqual(new Location.Local(5, "mega"))
      }
    }

    "when some leaders are remote" - {

      val manager = createManager(Map(
        intA -> remoteId,
        intB -> remoteId,
        mega -> myId,
      ))
      val catalog = manager.currentCatalog()

      "and writable required" in {

        val writable = true

        manager.locationOf(catalog.resolve(CatalogName("mega", "extA")), writable)
          .shouldEqual(new Location.Remote(0, remoteUri(mega0.getUri), "neo4j0"))

        manager.locationOf(catalog.resolve(CatalogName("mega", "graph"), Seq(Values.of(1))), writable)
          .shouldEqual(new Location.Remote(1, remoteUri(mega1.getUri), "neo4j1"))

        manager.locationOf(catalog.resolve(CatalogName("mega", "extB")), writable)
          .shouldEqual(new Location.Remote(2, remoteUri(mega2.getUri), "neo4j2"))

        manager.locationOf(catalog.resolve(CatalogName("intA")), writable)
          .shouldEqual(new Location.Remote(3, remoteUri("bolt", remoteAddress), "inta"))

        manager.locationOf(catalog.resolve(CatalogName("intB")), writable)
          .shouldEqual(new Location.Remote(4, remoteUri("bolt", remoteAddress), "intb"))

        manager.locationOf(catalog.resolve(CatalogName("mega")), writable)
          .shouldEqual(new Location.Local(5, "mega"))
      }

      "and writable not required" in {

        val writable = false

        manager.locationOf(catalog.resolve(CatalogName("mega", "extA")), writable)
          .shouldEqual(new Location.Remote(0, remoteUri(mega0.getUri), "neo4j0"))

        manager.locationOf(catalog.resolve(CatalogName("mega", "graph"), Seq(Values.of(1))), writable)
          .shouldEqual(new Location.Remote(1, remoteUri(mega1.getUri), "neo4j1"))

        manager.locationOf(catalog.resolve(CatalogName("mega", "extB")), writable)
          .shouldEqual(new Location.Remote(2, remoteUri(mega2.getUri), "neo4j2"))

        manager.locationOf(catalog.resolve(CatalogName("intA")), writable)
          .shouldEqual(new Location.Local(3, "inta"))

        manager.locationOf(catalog.resolve(CatalogName("intB")), writable)
          .shouldEqual(new Location.Local(4, "intb"))

        manager.locationOf(catalog.resolve(CatalogName("mega")), writable)
          .shouldEqual(new Location.Local(5, "mega"))
      }
    }
  }

  private def external(graph: FabricConfig.Graph) = ExternalGraph(graph)
  private def internal(id: Long, name: String) = InternalGraph(id, new NormalizedGraphName(name), new NormalizedDatabaseName(name))

  private def remoteUri(uri: FabricConfig.RemoteUri): Location.RemoteUri = new Location.RemoteUri(uri.getScheme, uri.getAddresses, uri.getQuery)
  private def remoteUri(scheme: String, address: SocketAddress): Location.RemoteUri = new Location.RemoteUri(scheme, Seq(address).asJava, null)
}
