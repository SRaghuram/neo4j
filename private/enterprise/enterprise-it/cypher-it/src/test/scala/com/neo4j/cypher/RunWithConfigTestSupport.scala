/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.cypher

import java.nio.file.Path
import java.util

import com.neo4j.test.TestEnterpriseDatabaseManagementServiceBuilder
import org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME
import org.neo4j.cypher.internal.javacompat.GraphDatabaseCypherService
import org.neo4j.graphdb.config.Setting

import scala.collection.JavaConverters.mapAsJavaMapConverter

trait RunWithConfigTestSupport {
  def runWithConfig(m: (Setting[_], Object)*)(run: GraphDatabaseCypherService => Unit) = {
    val config: util.Map[Setting[_], Object] = m.toMap.asJava
    val storeDir = Path.of("target/test-data/neo4j")
    val managementService = new TestEnterpriseDatabaseManagementServiceBuilder(storeDir)
      .impermanent()
      .setConfig(config)
      .build()
    val graph = managementService.database(DEFAULT_DATABASE_NAME)
    try {
      run(new GraphDatabaseCypherService(graph))
    } finally {
      managementService.shutdown()
    }
  }
}
