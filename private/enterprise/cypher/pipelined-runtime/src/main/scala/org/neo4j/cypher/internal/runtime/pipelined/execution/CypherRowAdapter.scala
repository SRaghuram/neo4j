/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.execution

import org.neo4j.cypher.internal.expressions.ASTCachedProperty
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.EntityById
import org.neo4j.cypher.internal.runtime.ReadableRow
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Value

class CypherRowAdapter extends CypherRow {

  override def containsName(name: String): Boolean = ???

  override def numberOfColumns: Int = ???

  override def createClone(): CypherRow = ???

  override def copyWith(key: String, value: AnyValue): CypherRow = ???

  override def copyWith(key1: String,
                        value1: AnyValue,
                        key2: String,
                        value2: AnyValue): CypherRow = ???

  override def copyWith(key1: String,
                        value1: AnyValue,
                        key2: String,
                        value2: AnyValue,
                        key3: String,
                        value3: AnyValue): CypherRow = ???

  override def copyWith(newEntries: Seq[(String, AnyValue)]): CypherRow = ???

  override def isNull(key: String): Boolean = ???

  override def setLongAt(offset: Int, value: Long): Unit = ???

  override def setRefAt(offset: Int, value: AnyValue): Unit = ???

  override def set(newEntries: Seq[(String, AnyValue)]): Unit = ???

  override def set(key: String, value: AnyValue): Unit = ???

  override def set(key1: String,
                   value1: AnyValue,
                   key2: String,
                   value2: AnyValue): Unit = ???

  override def set(key1: String,
                   value1: AnyValue,
                   key2: String,
                   value2: AnyValue,
                   key3: String,
                   value3: AnyValue): Unit = ???

  override def mergeWith(other: ReadableRow,
                         entityById: EntityById,
                         checkNullability: Boolean): Unit = ???

  override def copyAllFrom(input: ReadableRow): Unit = ???

  override def copyFrom(input: ReadableRow, nLongs: Int, nRefs: Int): Unit = ???

  override def copyFromOffset(input: ReadableRow,
                              sourceLongOffset: Int,
                              sourceRefOffset: Int,
                              targetLongOffset: Int,
                              targetRefOffset: Int): Unit = ???

  /**
    * Invalidate all cached node properties for the given node id
    */
  override def invalidateCachedNodeProperties(node: Long): Unit = ???

  /**
    * Invalidate all cached relationship properties for the given relationship id
    */
  override def invalidateCachedRelationshipProperties(rel: Long): Unit = ???

  /**
    * Provides an estimation of the number of bytes currently used by this instance.
    */
  override def estimatedHeapUsage: Long = ???

  override def getLongAt(offset: Int): Long = ???

  override def getRefAt(offset: Int): AnyValue = ???

  override def getByName(name: String): AnyValue = ???

  /**
    * Returns the cached property value
    * or NO_VALUE if the entity does not have the property,
    * or null     if this cached value has been invalidated, or the property value has not been cached.
    */
  override def getCachedProperty(key: ASTCachedProperty): Value = ???

  /**
    * Returns the cached property value
    * or NO_VALUE if the entity does not have the property,
    * or null     if this cached value has been invalidated, or the property value has not been cached.
    */
  override def getCachedPropertyAt(offset: Int): Value = ???

  override def setCachedProperty(key: ASTCachedProperty, value: Value): Unit = ???

  override def setCachedPropertyAt(offset: Int, value: Value): Unit = ???
}
