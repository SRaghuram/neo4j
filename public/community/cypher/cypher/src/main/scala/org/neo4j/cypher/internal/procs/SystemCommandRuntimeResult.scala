/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.procs

import java.util

import org.neo4j.cypher.internal.result.InternalExecutionResult
import org.neo4j.cypher.internal.runtime.{QueryContext, QueryStatistics}
import org.neo4j.cypher.result.QueryResult.QueryResultVisitor
import org.neo4j.cypher.result.RuntimeResult.ConsumptionState
import org.neo4j.cypher.result.{OperatorProfile, QueryProfile, QueryResult, RuntimeResult}
import org.neo4j.graphdb.ResourceIterator
import org.neo4j.kernel.impl.query.QuerySubscriber
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values

/**
  * Results, as produced by a system command.
  */
case class SystemCommandRuntimeResult(ctx: QueryContext, subscriber: QuerySubscriber, execution: SystemCommandExecutionResult) extends RuntimeResult {

  override val fieldNames: Array[String] = execution.fieldNames()
  private var resultRequested = false

  // The signature mode is taking care of eagerization

  override val asIterator: ResourceIterator[util.Map[String, AnyRef]] = execution.asIterator

  override def accept[EX <: Exception](visitor: QueryResultVisitor[EX]): Unit = {
    execution.accept(visitor)
  }

  override def queryStatistics(): QueryStatistics = execution.inner.queryStatistics()

  override def isIterable: Boolean = true

  override def consumptionState: RuntimeResult.ConsumptionState =
    if (!resultRequested) ConsumptionState.NOT_STARTED
    else if (asIterator.hasNext) ConsumptionState.HAS_MORE
    else ConsumptionState.EXHAUSTED

  override def close(): Unit = execution.inner.close()

  override def queryProfile(): QueryProfile = SystemCommandProfile(0)

  override def request(numberOfRecords: Long): Unit = execution.inner.request(numberOfRecords)

  override def cancel(): Unit = execution.inner.cancel()

  override def await(): Boolean = execution.inner.await()
}

class SystemCommandExecutionResult(val inner: InternalExecutionResult) {
  def fieldNames(): Array[String] = inner.fieldNames()

  def asIterator: ResourceIterator[util.Map[String, AnyRef]] = inner.javaIterator

  def accept[EX <: Exception](visitor: QueryResultVisitor[EX]): Unit = inner.accept(visitor)
}

class ColumnMappingSystemCommandExecutionResult(context: QueryContext,
                                                inner: InternalExecutionResult,
                                                ignore: Seq[String] = Seq.empty,
                                                valueExtractor: (String, util.Map[String, AnyRef]) => AnyRef = (k, r) => r.get(k))
  extends SystemCommandExecutionResult(inner) {

  self =>

  private val innerFields = inner.fieldNames()
  //private val ignoreIndexes = innerFields.zipWithIndex.filter(v => ignore.contains(v._1)).map(_._2)
  override val fieldNames: Array[String] = innerFields.filter(!ignore.contains(_))

  override def asIterator: ResourceIterator[util.Map[String, AnyRef]] = new ResourceIterator[util.Map[String, AnyRef]] {

    private lazy val innerIterator: ResourceIterator[util.Map[String, AnyRef]] = inner.javaIterator

    override def close(): Unit = innerIterator.close()

    override def hasNext: Boolean = innerIterator.hasNext

    override def next(): util.Map[String, AnyRef] = {
      import scala.collection.JavaConverters._
      mapRecord(innerIterator.next()).asJava
    }
  }

  private def mapRecord(row: util.Map[String, AnyRef]): Map[String, AnyRef] = {
    inner.fieldNames().foldLeft(Map.empty[String, AnyRef]) { (a, k) =>
      if (ignore.contains(k)) a
      else a + (k -> valueExtractor(k, row))
    }
  }

  private def resultAsMap(rowData: Array[AnyValue]): util.Map[String, AnyRef] = {
    val mapData = new util.HashMap[String, AnyRef](rowData.length)
    innerFields.zip(rowData).foreach { entry => mapData.put(entry._1, context.asObject(entry._2)) }
    mapData
  }

  override def accept[EX <: Exception](visitor: QueryResultVisitor[EX]): Unit = {
    inner.accept(new QueryResultVisitor[EX] {
      override def visit(row: QueryResult.Record): Boolean = {
        visitor.visit(() => {
          val mapData = resultAsMap(row.fields())
          fieldNames.map(k => Values.of(valueExtractor(k, mapData))).asInstanceOf[Array[AnyValue]]
        })
      }
    })
  }
}

case class SystemCommandProfile(rowCount: Long) extends QueryProfile with OperatorProfile {

  override def operatorProfile(operatorId: Int): OperatorProfile = this

  override def time(): Long = OperatorProfile.NO_DATA

  override def dbHits(): Long = 1 // for unclear reasons

  override def rows(): Long = rowCount

  override def pageCacheHits(): Long = OperatorProfile.NO_DATA

  override def pageCacheMisses(): Long = OperatorProfile.NO_DATA
}
