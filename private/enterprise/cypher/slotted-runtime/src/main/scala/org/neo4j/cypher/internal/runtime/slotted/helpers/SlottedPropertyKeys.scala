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

class SlottedPropertyKeys(resolved: Seq[(Int, Int)], private var unresolved: Seq[(String, Int)]) {
  private var _offset = - 1
  private var (resolvedTokens, resolvedOffsets) = resolved.sortBy(_._1).toArray.unzip

  private def resolve(dbAccess: DbAccess): Unit = {
    if (unresolved.isEmpty) return
    val newResolved = mutable.ArrayBuffer.empty[(Int, Int)]
    val newUnresolved = mutable.ArrayBuffer.empty[(String, Int)]
    unresolved.foreach {
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
    unresolved = newUnresolved
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