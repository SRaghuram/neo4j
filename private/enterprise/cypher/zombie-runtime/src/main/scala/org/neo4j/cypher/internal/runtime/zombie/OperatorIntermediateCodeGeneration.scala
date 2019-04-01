/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.zombie

import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.runtime.compiled.expressions._
import org.neo4j.cypher.internal.runtime.zombie.OperatorIntermediateCodeGeneration.LocalVariableSlotMapper

import scala.collection.mutable.ArrayBuffer

object OperatorIntermediateCodeGeneration {

  class LocalVariableSlotMapper(slots: SlotConfiguration) {
    val longSlotToLocal = new Array[String](slots.numberOfLongs)
    val refSlotToLocal = new Array[String](slots.numberOfReferences)

    def addLocalForLongSlot(offset: Int): String = {
      val local = s"longSlot$offset"
      longSlotToLocal(offset) = local
      local
    }

    def addLocalForRefSlot(offset: Int): String = {
      val local = s"refSlot$offset"
      refSlotToLocal(offset) = local
      local
    }

    def getLocalForLongSlot(offset: Int): String = longSlotToLocal(offset)

    def getLocalForRefSlot(offset: Int): String = refSlotToLocal(offset)

    def getAllLocalsForLongSlots: Seq[(Int, String)] =
      getAllLocalsFor(longSlotToLocal)

    def getAllLocalsForRefSlots: Seq[(Int, String)] =
      getAllLocalsFor(refSlotToLocal)

    private def getAllLocalsFor(slotToLocal: Array[String]): Seq[(Int, String)] = {
      val locals = new ArrayBuffer[(Int, String)](slotToLocal.length)
      var i = 0
      while (i < slotToLocal.length) {
        val v = slotToLocal(i)
        if (v != null) {
          locals += (i -> v)
        }
        i += 1
      }
      locals
    }
  }
}

class OperatorIntermediateCodeGeneration(slots: SlotConfiguration)
  extends IntermediateCodeGeneration(slots) {

  import IntermediateRepresentation.{assign, block, load}

  val locals: LocalVariableSlotMapper = new LocalVariableSlotMapper(slots)

  override final def getLongAt(offset: Int): IntermediateRepresentation = {
    var local = locals.getLocalForLongSlot(offset)
    if (local == null) {
      local = locals.addLocalForLongSlot(offset)
      assign(local, super.getLongAt(offset))
    }
    load(local)
  }

  override final def getRefAt(offset: Int): IntermediateRepresentation = {
    var local = locals.getLocalForRefSlot(offset)
    if (local == null) {
      local = locals.addLocalForRefSlot(offset)
      assign(local, super.getRefAt(offset))
    }
    load(local)
  }

  override final def setLongAt(offset: Int, value: IntermediateRepresentation): IntermediateRepresentation = {
    var local = locals.getLocalForLongSlot(offset)
    if (local == null) {
      local = locals.addLocalForLongSlot(offset)
    }
    assign(local, value)
  }

  override final def setRefAt(offset: Int, value: IntermediateRepresentation): IntermediateRepresentation = {
    var local = locals.getLocalForRefSlot(offset)
    if (local == null) {
      local = locals.addLocalForRefSlot(offset)
    }
    assign(local, value)
  }

  def writeLocalsToSlots(): IntermediateRepresentation = {
    val writeLongs = locals.getAllLocalsForLongSlots.map { case (offset, local) =>
      super.setLongAt(offset, load(local))
    }
    val writeRefs = locals.getAllLocalsForRefSlots.map { case (offset, local) =>
      super.setRefAt(offset, load(local))
    }
    block(writeLongs ++ writeRefs: _*)
  }
}
