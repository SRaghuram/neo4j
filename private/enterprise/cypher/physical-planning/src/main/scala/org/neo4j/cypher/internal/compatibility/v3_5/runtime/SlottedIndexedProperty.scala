/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.compatibility.v3_5.runtime

import org.neo4j.cypher.internal.v3_5.logical.plans.IndexedProperty

object SlottedIndexedProperty {
  def apply(node: String, property: IndexedProperty, slots: SlotConfiguration): SlottedIndexedProperty = {
    val maybeOffset =
      if (property.shouldGetValue) {
        Some(slots.getCachedNodePropertyOffsetFor(property.asCachedNodeProperty(node)))
      } else {
        None
      }
    SlottedIndexedProperty(property.propertyKeyToken.nameId.id, maybeOffset)
  }
}

case class SlottedIndexedProperty(propertyKeyId: Int, maybeCachedNodePropertySlot: Option[Int]) {
  def getValueFromIndex: Boolean = maybeCachedNodePropertySlot.isDefined
}
