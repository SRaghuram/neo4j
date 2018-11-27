/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance

import java.net.URL

import org.neo4j.cypher.internal.runtime.ResourceMonitor
import org.neo4j.cypher.internal.runtime.interpreted.CSVResource
import org.neo4j.graphdb.{DependencyResolver, GraphDatabaseService}
import org.neo4j.kernel.internal.GraphDatabaseAPI
import org.neo4j.kernel.{GraphDatabaseQueryService, monitoring}
import org.neo4j.cypher.internal.v3_5.util.test_helpers.CypherFunSuite

trait ResourceTracking extends CypherFunSuite {

  var resourceMonitor: TrackingResourceMonitor = _

  def trackResources(graph: GraphDatabaseService): Unit = trackResources(graph.asInstanceOf[GraphDatabaseAPI].getDependencyResolver)

  def trackResources(graph: GraphDatabaseQueryService): Unit = trackResources(graph.getDependencyResolver)

  def trackResources(resolver: DependencyResolver): Unit = {
    val monitors = resolver.resolveDependency(classOf[monitoring.Monitors])
    resourceMonitor = TrackingResourceMonitor()
    monitors.addMonitorListener(resourceMonitor)
  }

  case class TrackingResourceMonitor() extends ResourceMonitor {

    private var traced: Map[URL, Int] = Map()
    private var closed: Map[URL, Int] = Map()

    override def trace(resource: AutoCloseable): Unit =
      resource match {
        case CSVResource(url, _) =>
          val currCount = traced.getOrElse(url, 0)
          traced += url -> (currCount + 1)
        case _ =>
      }

    override def close(resource: AutoCloseable): Unit =
      resource match {
        case CSVResource(url, _) =>
          val currCount = closed.getOrElse(url, 0)
          closed += url -> (currCount + 1)
        case _ =>
      }

    def assertClosedAndClear(expectedNumberOfCSVs: Int): Unit = {
      if (traced.size != closed.size)
        traced.keys should be(closed.keys)
      traced.size should be(expectedNumberOfCSVs)
      for ( (tracedUrl, tracedCount) <- traced ) {
        closed.contains(tracedUrl) should be(true)
        closed(tracedUrl) should be(tracedCount)
      }
      traced = Map()
      closed = Map()
    }
  }
}

