/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel

import org.neo4j.cypher.internal.compatibility.v4_0.runtime.SlotConfiguration
import org.neo4j.values.AnyValue

/*
The lifetime of a Morsel instance is entirely controlled by the Dispatcher. No operator should create Morsels - they
 should only operate on Morsels provided to them
 */
class Morsel(val longs: Array[Long], val refs: Array[AnyValue]) {
  override def toString = s"Morsel[0x${System.identityHashCode(this).toHexString}](longs:${longs.length}, refs:${refs.length})"
}

object Morsel {
  def create(slots: SlotConfiguration, size: Int): Morsel = {
    val longs = new Array[Long](slots.numberOfLongs * size)
    val refs = new Array[AnyValue](slots.numberOfReferences * size)
    new Morsel(longs, refs)
  }
}
