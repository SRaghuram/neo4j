/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.slotted.helpers

import java.util

import org.neo4j.cypher.internal.logical.plans.CursorProperty
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.planner.spi.TokenContext
import org.neo4j.cypher.internal.runtime.DbAccess
import org.neo4j.internal.kernel.api.TokenRead

import scala.collection.mutable

/**
 * Used for handling multiple property keys at runtime.
 *
 * SlottedPropertyKeys is initiated with some combination of resolved and unresolved property keys and then
 * acts as Set with `accept` returning `true` if the provided key is contained in either the resolved or unresolved
 * set of keys.
 *
 * Usage:
 * {{{
 *   val nodeCursor: NodeCursor = _
 *   val propertyCursor: PropertyCursor = _
 *   read.singleNode(node, nodeCursor)
 *   assert(nodeCursor.next())
 *   nodeCursor.properties(propertyCursor)
 *   while (propertyCursor.next() && slottedPropertyKeys.accept(db, propertyCursor.propertyKey)) {
 *    val offset = slottedPropertyKeys.offset//offset will now be for the "accepted" property key
 *    val value = propertyCursor.propertyValue()
 *    ...
 *   }
 * }}}
 *
 * The point of this class is that we can do one single iteration over the property-chain instead of something like.
 *
 * {{{
 *  if (propertyCursor.seekProperty(p1)) {
 *   ...
 *  }
 *  if (propertyCursor.seekProperty(p2)) {
 *   ...
 *  }
 *  ...
 * }}}
 *
 * @param resolved property tokens that are known at the point of creation
 * @param unresolved property tokens that are unknown at the point of creation.
 */
class SlottedPropertyKeys(val resolved: Seq[(Int, Int)], val unresolved: Seq[(String, Int)]) {
  private var _offset = - 1
  private var _unresolved = unresolved
  private var (resolvedTokens, resolvedOffsets) = resolved.sortBy(_._1).toArray.unzip

  private def resolve(dbAccess: DbAccess): Unit = {
    if (_unresolved.isEmpty) return
    val newResolved = mutable.ArrayBuffer.empty[(Int, Int)]
    val newUnresolved = mutable.ArrayBuffer.empty[(String, Int)]
    _unresolved.foreach {
        case (key, offset) =>
          val token = dbAccess.propertyKey(key)
          if (token != TokenRead.NO_TOKEN) {
            newResolved.append((token, offset))
          } else {
            newUnresolved.append((key, offset))
          }
      }
    if (newResolved.isEmpty) return

    var i = 0
    while (i < resolvedTokens.length) {
      newResolved.append((resolvedTokens(i), resolvedOffsets(i)))
      i += 1
    }

    val (newResolvedTokens, newResolvedOffsets) = newResolved.sortBy(_._1).toArray.unzip
    resolvedTokens = newResolvedTokens
    resolvedOffsets = newResolvedOffsets
    _unresolved = newUnresolved
  }

  def accept(dbAccess: DbAccess, propertyKey: Int): Boolean = {
    resolve(dbAccess)
    val i = util.Arrays.binarySearch(resolvedTokens, propertyKey)
    if (i >= 0) {
      _offset = resolvedOffsets(i)
      true
    } else {
      false
    }
  }
  def offset: Int = _offset
}

object SlottedPropertyKeys {
  def resolve(properties: Seq[CursorProperty],
              slots: SlotConfiguration,
              tokenContext: TokenContext): SlottedPropertyKeys = {

    val resolved = mutable.ArrayBuffer.empty[(Int, Int)]
    val unresolved = mutable.ArrayBuffer.empty[(String, Int)]
    properties.foreach(p => {
      tokenContext.getOptPropertyKeyId(p.propertyKeyName.name) match {
        case Some(token) =>
          resolved.append((token, slots.getCachedPropertyOffsetFor(p.asCachedProperty)))
        case None =>
          unresolved.append((p.propertyKeyName.name, slots.getCachedPropertyOffsetFor(p.asCachedProperty)))
      }
    })
    new SlottedPropertyKeys(resolved, unresolved)
  }
}