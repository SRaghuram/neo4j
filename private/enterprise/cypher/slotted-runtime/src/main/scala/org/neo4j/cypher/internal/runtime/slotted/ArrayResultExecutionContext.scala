/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.slotted

import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.runtime.{EntityById, ExecutionContext, ResourceLinenumber, ValuePopulation}
import org.neo4j.cypher.internal.v4_0.expressions.ASTCachedProperty
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

  def newResult(context: ExecutionContext, state: QueryState, prePopulate: Boolean): ArrayResultExecutionContext = {
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
  private var freeExecutionContextInstance: ArrayResultExecutionContext = _

  private def allocateExecutionContext: ArrayResultExecutionContext = {
    if (freeExecutionContextInstance != null) {
      val context = freeExecutionContextInstance
      freeExecutionContextInstance = null
      context
    }
    else {
      createNewExecutionContext
    }
  }

  def releaseExecutionContext(executionContext: ArrayResultExecutionContext) = {
    freeExecutionContextInstance = executionContext
  }

  private def createNewExecutionContext: ArrayResultExecutionContext = {
    val resultArray = new Array[AnyValue](columnArraySize)
    ArrayResultExecutionContext(resultArray, columnIndexMap, this)
  }
  //---------------------------------------------------------------------------
}

case class ArrayResultExecutionContext(resultArray: Array[AnyValue],
                                       columnIndexMap: scala.collection.Map[String, Int],
                                       factory: ArrayResultExecutionContextFactory)
  extends ExecutionContext with QueryResult.Record {

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

  override def copyTo(target: ExecutionContext, fromLongOffset: Int, fromRefOffset: Int, toLongOffset: Int, toRefOffset: Int): Unit = fail()

  override def copyFrom(input: ExecutionContext, nLongs: Int, nRefs: Int): Unit = fail()

  override def setLongAt(offset: Int, value: Long): Unit = fail()

  override def getLongAt(offset: Int): Long = fail()

  override def setRefAt(offset: Int, value: AnyValue): Unit = fail()

  override def getRefAt(offset: Int): AnyValue = fail()

  override def set(newEntries: Seq[(String, AnyValue)]): Unit = fail()

  override def set(key: String, value: AnyValue): Unit = fail()

  override def set(key1: String, value1: AnyValue, key2: String, value2: AnyValue): Unit = fail()

  override def set(key1: String, value1: AnyValue, key2: String, value2: AnyValue, key3: String, value3: AnyValue): Unit = fail()

  override def mergeWith(other: ExecutionContext, entityById: EntityById): Unit = fail()

  override def createClone(): ExecutionContext = fail()

  override def copyWith(key: String, value: AnyValue): ExecutionContext = fail()

  override def copyWith(key1: String, value1: AnyValue, key2: String, value2: AnyValue): ExecutionContext = fail()

  override def copyWith(key1: String, value1: AnyValue, key2: String, value2: AnyValue, key3: String, value3: AnyValue): ExecutionContext = fail()

  override def copyWith(newEntries: Seq[(String, AnyValue)]): ExecutionContext = fail()

  override def boundEntities(materializeNode: Long => AnyValue, materializeRelationship: Long => AnyValue): Map[String, AnyValue] = fail()

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
