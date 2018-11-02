/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.compiled

import java.util

import org.neo4j.cypher.internal.executionplan.GeneratedQueryExecution
import org.neo4j.cypher.internal.runtime._
import org.neo4j.cypher.result.QueryResult.QueryResultVisitor
import org.neo4j.cypher.result.RuntimeResult.ConsumptionState
import org.neo4j.cypher.result.{QueryProfile, RuntimeResult}
import org.neo4j.graphdb.ResourceIterator

/**
  * Main class for compiled runtime results.
  */
class CompiledExecutionResult(context: QueryContext,
                              compiledCode: GeneratedQueryExecution,
                              override val queryProfile: QueryProfile)
  extends RuntimeResult {

  private var resultRequested = false

  def executionMode: ExecutionMode = compiledCode.executionMode()

  override def fieldNames(): Array[String] = compiledCode.fieldNames()

  override def accept[EX <: Exception](visitor: QueryResultVisitor[EX]): Unit = {
    compiledCode.accept(visitor)
    resultRequested = true
  }

  override def queryStatistics() = QueryStatistics()

  override def isIterable: Boolean = false

  override def asIterator(): ResourceIterator[util.Map[String, AnyRef]] =
    throw new UnsupportedOperationException("The compiled runtime is not iterable")

  override def consumptionState: RuntimeResult.ConsumptionState =
    if (!resultRequested) ConsumptionState.NOT_STARTED
    else ConsumptionState.EXHAUSTED

  override def close(): Unit = {}
}
