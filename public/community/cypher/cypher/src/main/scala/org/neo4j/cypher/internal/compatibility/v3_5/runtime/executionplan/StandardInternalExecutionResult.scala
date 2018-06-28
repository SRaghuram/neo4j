/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.cypher.internal.compatibility.v3_5.runtime.executionplan

import java.io.{PrintWriter, StringWriter}
import java.util

import org.neo4j.cypher.internal.compatibility.v3_5.runtime._
import org.neo4j.cypher.internal.compatibility.v3_5.runtime.helpers.{MapBasedRow, RuntimeTextValueConverter}
import org.neo4j.cypher.internal.compatibility.v3_5.runtime.profiler.PlanDescriptionBuilder
import org.neo4j.cypher.internal.runtime._
import org.neo4j.cypher.internal.runtime.planDescription.InternalPlanDescription
import org.neo4j.cypher.result.QueryResult.QueryResultVisitor
import org.neo4j.cypher.result.{QueryResult, RuntimeResult}
import org.neo4j.graphdb.Result.{ResultRow, ResultVisitor}
import org.neo4j.graphdb.{NotFoundException, Notification, ResourceIterator}
import org.neo4j.values.AnyValue
import org.opencypher.v9_0.util.{ProfilerStatisticsNotReadyException, TaskCloser}

import scala.collection.mutable

class StandardInternalExecutionResult(context: QueryContext,
                                      runtime: RuntimeName,
                                      runtimeResult: RuntimeResult,
                                      taskCloser: TaskCloser,
                                      override val queryType: InternalQueryType,
                                      override val notifications: Set[Notification],
                                      override val executionMode: ExecutionMode,
                                      planDescriptionBuilder: PlanDescriptionBuilder)
  extends InternalExecutionResult {

  self =>

  /*
  ======= RESULT MATERIALIZATION ==========
   */

  private var materializedResult: util.ArrayList[Array[AnyValue]] = _
  private def isMaterialized: Boolean = materializedResult != null
  private def materializeResult(): Unit = {
    materializedResult = new util.ArrayList()
    if (isOpen)
      try {
        runtimeResult.accept(new QueryResultVisitor[Exception] {
          override def visit(row: QueryResult.Record): Boolean = {
            materializedResult.add(row.fields().clone())
            row.release()
            true
          }
        })
      } catch {
        case t: Throwable =>
          throw closeOnError(t)
      }
  }

  private val noRows = runtimeResult.isExhausted // OBS: check before materialization

  /**
    * By policy we materialize the result directly unless it's a read only query.
    */
  if (queryType != READ_ONLY) {
    materializeResult()
  }

  /**
    * ...and if we do not return any rows, we close all resources.
    */
  if (noRows || queryType == WRITE || queryType == SCHEMA_WRITE) {
    close(Success)
  }

  /*
  ======= OPEN / CLOSE ==========
   */

  protected def isOpen: Boolean = !isClosed

  override def isClosed: Boolean = taskCloser.isClosed

  override def close(reason: CloseReason): Unit = {
    taskCloser.close(reason == Success)
  }

  /*
  ======= CONSUME AS ITERATOR ==========
   */

  override def javaIterator: ResourceIterator[util.Map[String, AnyRef]] = inner

  override def javaColumnAs[T](column: String): ResourceIterator[T] =
    new ResourceIterator[T] {
      override def hasNext: Boolean = inner.hasNext
      override def next(): T = extractJavaColumn(column, inner.next()).asInstanceOf[T]
      override def close(): Unit = self.close()
    }

  private def extractJavaColumn(column: String, data: util.Map[String, AnyRef]): AnyRef = {
    val value = data.get(column)
    if (value == null) {
      throw new NotFoundException(s"No column named '$column' was found. Found: ${fieldNames().mkString("(\"", "\", \"", "\")")}")
    }
    value
  }

  protected final lazy val inner: ResourceIterator[util.Map[String, AnyRef]] = {
    if (!isMaterialized && runtimeResult.isIterable)
      runtimeResult.asIterator()
    else {
      if (!isMaterialized)
        materializeResult()
      new MaterializedIterator()
    }
  }

  private class MaterializedIterator() extends ResourceIterator[util.Map[String, AnyRef]] {

    private val inner = materializedResult.iterator()
    private val columns = fieldNames()

    def hasNext: Boolean = inner.hasNext

    def next(): util.Map[String, AnyRef] = {
      val values = inner.next()
      val map = new util.HashMap[String, AnyRef]()
      for (i <- columns.indices) {
        map.put(columns(i), context.asObject(values(i)))
      }
      map
    }

    def remove(): Unit = throw new UnsupportedOperationException("remove")

    def close(): Unit = self.close()
  }

  /*
  ======= CONSUME WITH VISITOR ==========
   */

  protected def accept(body: ResultRow => Unit): Unit = {
    accept(new ResultVisitor[RuntimeException] {
      override def visit(row: ResultRow): Boolean = {
        body(row)
        true
      }
    })
  }

  override def accept[E <: Exception](visitor: ResultVisitor[E]): Unit = {
    accept(new QueryResultVisitor[E] {
      private val names = fieldNames()
      override def visit(record: QueryResult.Record): Boolean = {
        val fields = record.fields()
        val mapData = new mutable.AnyRefMap[String, Any](names.length)
        for (i <- names.indices) {
          mapData.put(names(i), context.asObject(fields(i)))
        }
        visitor.visit(new MapBasedRow(mapData))
      }
    })
  }

  override def accept[E <: Exception](visitor: QueryResultVisitor[E]): Unit = {

    if (isMaterialized) {
      val rowCursor = new MaterializedResultCursor
      while (rowCursor.next()) {
        visitor.visit(rowCursor)
      }
      close(Success)
    } else if (isOpen) {
      runtimeResult.accept(visitor)
      close(Success)
    }
  }

  class MaterializedResultCursor extends QueryResult.Record {
    private var i = -1
    def next(): Boolean = {
      i += 1
      i < materializedResult.size()
    }
    override def fields(): Array[AnyValue] = materializedResult.get(i)
  }

  /*
  ======= DUMP TO STRING ==========
   */

  override def dumpToString(): String = {
    val stringWriter = new StringWriter()
    val writer = new PrintWriter(stringWriter)
    dumpToString(writer)
    writer.close()
    stringWriter.getBuffer.toString
  }

  override def dumpToString(writer: PrintWriter): Unit = {
    val builder = Seq.newBuilder[Map[String, String]]
    val scalaValues = new RuntimeScalaValueConverter(isGraphKernelResultValue)
    val runtimeTextValueConverter = new RuntimeTextValueConverter(scalaValues, context.transactionalContext)

    accept(row => {
      builder += runtimeTextValueConverter.dumpRowToString(fieldNames(), row)
    })

    formatOutput(writer, runtimeResult.fieldNames(), builder.result(), queryStatistics())
  }

  /*
  ======= META DATA ==========
   */

  override def queryStatistics(): QueryStatistics = runtimeResult.queryStatistics()

  override def fieldNames(): Array[String] = runtimeResult.fieldNames()

  override lazy val executionPlanDescription: InternalPlanDescription = {

    if (executionMode == ProfileMode) {
      if (!runtimeResult.isExhausted) {
        taskCloser.close(success = false)
        throw new ProfilerStatisticsNotReadyException()
      }
      planDescriptionBuilder.profile(runtimeResult.queryProfile)
    } else {
      planDescriptionBuilder.explain()
    }

  }
}




