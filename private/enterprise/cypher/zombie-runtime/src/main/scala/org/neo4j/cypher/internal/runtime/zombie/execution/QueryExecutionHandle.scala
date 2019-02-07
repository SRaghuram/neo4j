/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.zombie.execution

/**
  * A [[QueryExecutionHandle]] represents the ongoing execution of a query by the [[QueryExecutor]].
  */
trait QueryExecutionHandle {

  /**
    * Wait for this QueryExecution to complete.
    *
    * @return An optional error if anything went wrong with the query execution.
    */
  def await(): Option[Throwable]
}
