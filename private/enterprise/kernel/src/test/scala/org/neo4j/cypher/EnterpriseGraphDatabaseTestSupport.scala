/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher

import org.neo4j.test.{TestEnterpriseGraphDatabaseFactory, TestGraphDatabaseFactory}
import org.neo4j.cypher.internal.v3_5.util.test_helpers.CypherFunSuite

trait EnterpriseGraphDatabaseTestSupport extends GraphDatabaseTestSupport {
  self: CypherFunSuite =>

  override protected def graphDatabaseFactory(): TestGraphDatabaseFactory = {
    new TestEnterpriseGraphDatabaseFactory()
  }
}
