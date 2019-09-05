/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel

import org.neo4j.codegen.api.IntermediateRepresentation
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.physicalplanning.ast.SlottedCachedProperty
import org.neo4j.cypher.internal.runtime.compiled.expressions._
import org.neo4j.cypher.internal.runtime.morsel.OperatorExpressionCompiler.LocalVariableSlotMapper
import org.neo4j.cypher.internal.runtime.morsel.operators.OperatorCodeGenHelperTemplates.INPUT_MORSEL
import org.neo4j.values.storable.Value

import scala.collection.mutable.ArrayBuffer

object OperatorExpressionCompiler {

  class LocalVariableSlotMapper(slots: SlotConfiguration) {
    val longSlotToLocal: Array[String] = new Array[String](slots.numberOfLongs)
    val refSlotToLocal: Array[String] = new Array[String](slots.numberOfReferences)
    val cachedProperties: ArrayBuffer[(Int, String)] = ArrayBuffer.empty[(Int, String)]

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

    def addCachedProperty(offset: Int): String = {
      val refslot = addLocalForRefSlot(offset)
      cachedProperties.append(offset -> refslot)
      refslot
    }

    def getLocalForLongSlot(offset: Int): String = longSlotToLocal(offset)

    def getLocalForRefSlot(offset: Int): String = refSlotToLocal(offset)

    def getAllLocalsForLongSlots: Seq[(Int, String)] =
      getAllLocalsFor(longSlotToLocal)

    def getAllLocalsForRefSlots: Seq[(Int, String)] =
      getAllLocalsFor(refSlotToLocal)

    def getAllLocalsForCachedProperties: Seq[(Int, String)] = cachedProperties

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

class OperatorExpressionCompiler(slots: SlotConfiguration, inputSlotConfiguration: SlotConfiguration, readOnly: Boolean, namer: VariableNamer)
  extends ExpressionCompiler(slots, readOnly, namer) {

  import org.neo4j.codegen.api.IntermediateRepresentation._

  val locals: LocalVariableSlotMapper = new LocalVariableSlotMapper(slots)

  override final def getLongAt(offset: Int): IntermediateRepresentation = {
    var local = locals.getLocalForLongSlot(offset)
    if (local == null) {
      local = locals.addLocalForLongSlot(offset)
      block(
        assign(local, getLongFromExecutionContext(offset, loadField(INPUT_MORSEL))),
        load(local)
      )
    } else {
      load(local)
    }
  }

  final def getLongAtOrElse(offset: Int, orElse: IntermediateRepresentation): IntermediateRepresentation = {
    val local = locals.getLocalForLongSlot(offset)
    if (local == null) orElse else load(local)
  }

  override final def getRefAt(offset: Int): IntermediateRepresentation = {
    var local = locals.getLocalForRefSlot(offset)
    if (local == null) {
      local = locals.addLocalForRefSlot(offset)
      block(
        assign(local, getRefFromExecutionContext(offset, loadField(INPUT_MORSEL))),
        load(local)
      )
    } else {
      load(local)
    }
  }

  final def getRefAtOrElse(offset: Int, orElse: IntermediateRepresentation): IntermediateRepresentation = {
    val local = locals.getLocalForRefSlot(offset)
    if (local == null) orElse else load(local)
  }

  override final def setLongAt(offset: Int, value: IntermediateRepresentation): IntermediateRepresentation = {
    var local = locals.getLocalForLongSlot(offset)
    if (local == null) {
      local = locals.addLocalForLongSlot(offset)
    }
    assign(local, value)
  }

  def hasLongAt(offset: Int): Boolean = locals.getLocalForLongSlot(offset) != null

  def hasRefAt(offset: Int): Boolean = locals.getLocalForRefSlot(offset) != null

  override final def setRefAt(offset: Int, value: IntermediateRepresentation): IntermediateRepresentation = {
    var local = locals.getLocalForRefSlot(offset)
    if (local == null) {
      local = locals.addLocalForRefSlot(offset)
    }
    assign(local, value)
  }

  override def getCachedPropertyAt(property: SlottedCachedProperty, getFromStore: IntermediateRepresentation): IntermediateRepresentation = {
    val offset = property.cachedPropertyOffset
    var local = locals.getLocalForRefSlot(offset)
    val maybeCachedProperty = inputSlotConfiguration.getCachedPropertySlot(property)
    val prepareOps =
      if (local == null && maybeCachedProperty.isDefined) {

        local = locals.addCachedProperty(offset)
        block(
          assign(local, getCachedPropertyFromExecutionContext(maybeCachedProperty.get.offset, loadField(INPUT_MORSEL))),
          condition(isNull(load(local)))(assign(local, getFromStore))
          )
      } else if (local == null) {
        local = locals.addCachedProperty(offset)
        assign(local, getFromStore)
      } else {
        condition(isNull(load(local)))(assign(local, getFromStore))

      }

    block(prepareOps, cast[Value](load(local)))
  }

  override def setCachedPropertyAt(offset: Int,
                                   value: IntermediateRepresentation): IntermediateRepresentation = {
    var local = locals.getLocalForRefSlot(offset)
    if (local == null) {
      local = locals.addCachedProperty(offset)
    }
    assign(local, value)
  }

  def writeLocalsToSlots(): IntermediateRepresentation = {
    val writeLongs = locals.getAllLocalsForLongSlots.map { case (offset, local) =>
      setLongInExecutionContext(offset, load(local))
    }
    val writeRefs = locals.getAllLocalsForRefSlots.map { case (offset, local) =>
      setRefInExecutionContext(offset, load(local))
    }
    block(writeLongs ++ writeRefs: _*)
  }
}
