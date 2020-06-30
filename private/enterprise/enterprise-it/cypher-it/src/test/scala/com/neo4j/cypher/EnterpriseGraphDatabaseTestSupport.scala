/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.cypher

import java.nio.file.Path

import com.neo4j.configuration.MetricsSettings.metrics_enabled
import com.neo4j.test.TestEnterpriseDatabaseManagementServiceBuilder
import org.neo4j.cypher.GraphDatabaseTestSupport
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.graphdb.config.Setting
import org.neo4j.test.TestDatabaseManagementServiceBuilder

/**
 * Use this trait when you want GraphDatabaseTestSupport functionality with
 * enterprise Databases.
 */
trait EnterpriseGraphDatabaseTestSupport extends GraphDatabaseTestSupport {
  self: CypherFunSuite =>

  override def databaseConfig(): Map[Setting[_], Object] = super.databaseConfig() ++ Map(metrics_enabled -> java.lang.Boolean.FALSE)

  override protected def createDatabaseFactory(databaseRootDir: Path): TestDatabaseManagementServiceBuilder = {
    new TestEnterpriseDatabaseManagementServiceBuilder(databaseRootDir)
  }
}
