/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.slotted

import org.neo4j.cypher.internal.expressions.ASTCachedProperty
import org.neo4j.cypher.internal.runtime.EntityById
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.ReadableRow
import org.neo4j.cypher.internal.runtime.ResourceLinenumber
import org.neo4j.cypher.internal.runtime.ValuePopulation
import org.neo4j.cypher.internal.runtime.WritableRow
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.result.QueryResult
import org.neo4j.exceptions.InternalException
import org.neo4j.graphdb.NotFoundException
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Value

import scala.collection.mutable

case class ArrayResultExecutionContextFactory(columns: Seq[(String, Expression)]) {
  private val columnExpressionArray = columns.map(_._2).toArray
  private val columnArraySize = columnExpressionArray.length
  private val columnIndexMap = {
    val m = new mutable.OpenHashMap[String, Int](columns.length)
    var index = 0
    columns.foreach {
      case (name, _) => m.put(name, index)
        index += 1
    }
    m
  }

  def newResult(context: CypherRow, state: QueryState, prePopulate: Boolean): ArrayResultRow = {
    val result = allocateExecutionContext

    // Apply the expressions that materializes the result values and fill in the result array
    val resultArray = result.resultArray
    var index = 0

    if (prePopulate)
      while (index < columnArraySize) {
        val value = columnExpressionArray(index).apply(context, state)
        ValuePopulation.populate(value)
        resultArray(index) = value
        index += 1
      }
    else
      while (index < columnArraySize) {
        resultArray(index) = columnExpressionArray(index).apply(context, state)
        index += 1
      }
    result
  }

  //---------------------------------------------------------------------------
  // Instance cache of size 1. Reuses the last created ArrayResultExecutionContext
  private var freeExecutionContextInstance: ArrayResultRow = _

  private def allocateExecutionContext: ArrayResultRow = {
    if (freeExecutionContextInstance != null) {
      val context = freeExecutionContextInstance
      freeExecutionContextInstance = null
      context
    }
    else {
      createNewExecutionContext
    }
  }

  def releaseExecutionContext(executionContext: ArrayResultRow) = {
    freeExecutionContextInstance = executionContext
  }

  private def createNewExecutionContext: ArrayResultRow = {
    val resultArray = new Array[AnyValue](columnArraySize)
    ArrayResultRow(resultArray, columnIndexMap, this)
  }
  //---------------------------------------------------------------------------
}

case class ArrayResultRow(resultArray: Array[AnyValue],
                          columnIndexMap: scala.collection.Map[String, Int],
                          factory: ArrayResultExecutionContextFactory)
  extends CypherRow with QueryResult.Record {

  override def release(): Unit = factory.releaseExecutionContext(this)

  override def fields(): Array[AnyValue] = {
    resultArray
  }

  override def getByName(key: String): AnyValue = {
    columnIndexMap.get(key) match {
      case Some(index) => resultArray(index)
      case _=> throw new NotFoundException(s"Unknown variable `$key`.")
    }
  }


  override def numberOfColumns: Int = columnIndexMap.size

  override def containsName(key: String): Boolean =  columnIndexMap.get(key).isDefined

  //---------------------------------------------------------------------------
  // This is an ExecutionContext by name only and does not support the full API
  // The methods below should never be called on a produced result
  private def fail(): Nothing = throw new InternalException("Tried using a result context as an execution context")

  override def copyAllFrom(input: ReadableRow): Unit = fail()

  override def copyFrom(input: ReadableRow, nLongs: Int, nRefs: Int): Unit = fail()

  override def copyFromOffset(input: ReadableRow, sourceLongOffset: Int, sourceRefOffset: Int, targetLongOffset: Int, targetRefOffset: Int): Unit = fail()

  override def setLongAt(offset: Int, value: Long): Unit = fail()

  override def getLongAt(offset: Int): Long = fail()

  override def setRefAt(offset: Int, value: AnyValue): Unit = fail()

  override def getRefAt(offset: Int): AnyValue = fail()

  override def set(newEntries: Seq[(String, AnyValue)]): Unit = fail()

  override def set(key: String, value: AnyValue): Unit = fail()

  override def set(key1: String, value1: AnyValue, key2: String, value2: AnyValue): Unit = fail()

  override def set(key1: String, value1: AnyValue, key2: String, value2: AnyValue, key3: String, value3: AnyValue): Unit = fail()

  override def mergeWith(other: ReadableRow, entityById: EntityById): Unit = fail()

  override def createClone(): CypherRow = fail()

  override def copyWith(key: String, value: AnyValue): CypherRow = fail()

  override def copyWith(key1: String, value1: AnyValue, key2: String, value2: AnyValue): CypherRow = fail()

  override def copyWith(key1: String, value1: AnyValue, key2: String, value2: AnyValue, key3: String, value3: AnyValue): CypherRow = fail()

  override def copyWith(newEntries: Seq[(String, AnyValue)]): CypherRow = fail()

  override def isNull(key: String): Boolean = fail()

  override def setCachedProperty(key: ASTCachedProperty, value: Value): Unit = fail()

  override def setCachedPropertyAt(offset: Int, value: Value): Unit = fail()

  override def getCachedProperty(key: ASTCachedProperty): Value = fail()

  override def getCachedPropertyAt(offset: Int): Value = fail()

  override def invalidateCachedNodeProperties(node: Long): Unit = fail()

  override def invalidateCachedRelationshipProperties(rel: Long): Unit = fail()

  override def setLinenumber(file: String, line: Long, last: Boolean = false): Unit = fail()

  override def setLinenumber(line: Option[ResourceLinenumber]): Unit = fail()

  override def getLinenumber: Option[ResourceLinenumber] = fail()

  override def estimatedHeapUsage: Long = resultArray.foldLeft(0L)(_ + _.estimatedHeapUsage())
}
