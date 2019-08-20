/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.internal.cypher.acceptance

import java.lang.Boolean.TRUE

import com.neo4j.cypher.CommercialGraphDatabaseTestSupport
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.cypher.ExecutionEngineHelper._
import org.neo4j.cypher.ExecutionEngineFunSuite
import org.neo4j.exceptions.RuntimeUnsupportedException
import org.neo4j.graphdb.InputPosition
import org.neo4j.graphdb.impl.notification.NotificationCode.RUNTIME_UNSUPPORTED
import org.neo4j.graphdb.impl.notification.NotificationDetail

import scala.language.reflectiveCalls

class EnterpriseRuntimeUnsupportedNotificationTest extends ExecutionEngineFunSuite with CommercialGraphDatabaseTestSupport {

  test("Should give specific error message why morsel does not support a query") {
    val result = execute("CYPHER runtime=morsel EXPLAIN RETURN 1 SKIP 1")
    result.notifications should contain(RUNTIME_UNSUPPORTED.notification(InputPosition.empty,
      NotificationDetail.Factory.message("Runtime unsupported", "Morsel does not yet support the plans including `Skip`, use another runtime.")))
  }

  test("can also be configured to fail hard") {
    restartWithConfig(Map(GraphDatabaseSettings.cypher_hints_error -> TRUE))
    eengine = createEngine(graph)

    val exception = intercept[RuntimeUnsupportedException](execute("CYPHER runtime=morsel EXPLAIN RETURN 1 SKIP 1"))
    exception.getMessage should be("Morsel does not yet support the plans including `Skip`, use another runtime.")
  }
}
