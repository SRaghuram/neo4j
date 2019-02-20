/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.compiled

import java.util

import org.neo4j.cypher.internal.executionplan.GeneratedQueryExecution
import org.neo4j.cypher.internal.runtime._
import org.neo4j.cypher.result.QueryResult.QueryResultVisitor
import org.neo4j.cypher.result.RuntimeResult.ConsumptionState
import org.neo4j.cypher.result.{NaiveQuerySubscription, QueryProfile, QueryResult, RuntimeResult}
import org.neo4j.graphdb.ResourceIterator
import org.neo4j.kernel.impl.query.QuerySubscriber

/**
  * Main class for compiled runtime results.
  */
class CompiledExecutionResult(context: QueryContext,
                              compiledCode: GeneratedQueryExecution,
                              override val queryProfile: QueryProfile,
                              prePopulateResults: Boolean,
                              subscriber: QuerySubscriber)
  extends NaiveQuerySubscription(subscriber) {

  private var resultRequested = false

  override def fieldNames(): Array[String] = compiledCode.fieldNames()

  override def accept[EX <: Exception](visitor: QueryResultVisitor[EX]): Unit = {
    if (prePopulateResults)
      compiledCode.accept(new QueryResultVisitor[EX] {
        override def visit(row: QueryResult.Record): Boolean = {
          val fields = row.fields()
          var i = 0
          while (i < fields.length) {
            ValuePopulation.populate(fields(i))
            i+=1
          }
          visitor.visit(row)
        }
      })
    else
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
