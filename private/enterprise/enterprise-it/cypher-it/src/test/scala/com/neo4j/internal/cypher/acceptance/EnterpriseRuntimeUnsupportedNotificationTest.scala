/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.internal.cypher.acceptance

import java.lang.Boolean.TRUE

import com.neo4j.cypher.EnterpriseGraphDatabaseTestSupport
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.cypher.ExecutionEngineFunSuite
import org.neo4j.cypher.ExecutionEngineHelper.createEngine
import org.neo4j.exceptions.RuntimeUnsupportedException
import org.neo4j.graphdb.InputPosition
import org.neo4j.graphdb.impl.notification.NotificationCode.RUNTIME_UNSUPPORTED
import org.neo4j.graphdb.impl.notification.NotificationDetail

class EnterpriseRuntimeUnsupportedNotificationTest extends ExecutionEngineFunSuite with EnterpriseGraphDatabaseTestSupport {

  test("Should give specific error message why pipelined does not support a query") {
    val result = execute("CYPHER runtime=pipelined EXPLAIN MATCH (n) SET n:German")
    result.notifications should contain(RUNTIME_UNSUPPORTED.notification(InputPosition.empty,
      NotificationDetail.Factory.message("Runtime unsupported", "Pipelined does not yet support the plans including `SetLabels`, use another runtime.")))
  }

  test("can also be configured to fail hard") {
    restartWithConfig(Map(GraphDatabaseSettings.cypher_hints_error -> TRUE))
    eengine = createEngine(graph)

    val exception = intercept[RuntimeUnsupportedException](execute("CYPHER runtime=pipelined EXPLAIN MATCH (n) SET n:German"))
    exception.getMessage should be("Pipelined does not yet support the plans including `SetLabels`, use another runtime.")
  }
}
