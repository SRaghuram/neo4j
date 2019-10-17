/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel

import org.neo4j.codegen.api.{CodeGeneration, IntermediateRepresentation}
import org.neo4j.cypher.internal.codegen.CompiledCursorUtils
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.physicalplanning.ast.SlottedCachedProperty
import org.neo4j.cypher.internal.runtime.compiled.expressions._
import org.neo4j.cypher.internal.runtime.morsel.OperatorExpressionCompiler.LocalVariableSlotMapper
import org.neo4j.cypher.internal.runtime.morsel.operators.OperatorCodeGenHelperTemplates
import org.neo4j.cypher.internal.runtime.morsel.operators.OperatorCodeGenHelperTemplates.{INPUT_MORSEL, UNINITIALIZED_LONG_SLOT_VALUE}
import org.neo4j.internal.kernel.api.{NodeCursor, PropertyCursor, Read, RelationshipScanCursor}
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

class OperatorExpressionCompiler(slots: SlotConfiguration,
                                 inputSlotConfiguration: SlotConfiguration,
                                 readOnly: Boolean,
                                 codeGenerationMode: CodeGeneration.CodeGenerationMode,
                                 namer: VariableNamer)
  extends ExpressionCompiler(slots, readOnly, codeGenerationMode, namer) {

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
      block(
        // Even if the local has been seen before in this method, we cannot be sure that the code path which added the initialization code was taken
        // This happens for example when we have a continuation where innerLoop = true
        // It is sub-optimal to check this every time at runtime, so we should come up with a better solution
        // e.g. to do this initialization only once at the entry-point of each nested fused loop
        //      or if possible, save the state when we exit with a continuation, and restore it when we come back
        condition(equal(load(local), UNINITIALIZED_LONG_SLOT_VALUE))(
          // We need to initialize the local from the execution context
          assign(local, getLongFromExecutionContext(offset, loadField(INPUT_MORSEL))),
        ),
        load(local)
      )
    }
  }

  final def getLongAtOrElse(offset: Int, orElse: IntermediateRepresentation): IntermediateRepresentation = {
    val local = locals.getLocalForLongSlot(offset)
    if (local == null) orElse else {
      // Even if the local has been seen before in this method, we cannot be sure that the code path which added the initialization code was taken
      // (See full comment in getLongAt above)
      block(
        condition(equal(load(local), UNINITIALIZED_LONG_SLOT_VALUE))(
          // We need to initialize the local from the execution context
          assign(local, orElse),
        ),
        load(local)
      )
    }
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
      // Even if the local has been seen before in this method, we cannot be sure that the code path which added the initialization code was taken
      // (See full comment in getLongAt above)
      block(
        condition(isNull(load(local)))(
          assign(local, getRefFromExecutionContext(offset, loadField(INPUT_MORSEL))),
        ),
        load(local)
      )
    }
  }

  final def getRefAtOrElse(offset: Int, orElse: IntermediateRepresentation): IntermediateRepresentation = {
    val local = locals.getLocalForRefSlot(offset)
    if (local == null) orElse else {
      // Even if the local has been seen before in this method, we cannot be sure that the code path which added the initialization code was taken
      // (See full comment in getLongAt above)
      block(
        condition(isNull(load(local)))(
          assign(local, orElse),
        ),
        load(local)
      )
    }
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

    def initializeFromStoreIR = assign(local, getFromStore)
    def initializeFromContextIR =
      block(
        assign(local, getCachedPropertyFromExecutionContext(maybeCachedProperty.get.offset, loadField(INPUT_MORSEL))),
        condition(isNull(load(local)))(initializeFromStoreIR)
      )

    val prepareOps =
      if (local == null && maybeCachedProperty.isDefined) {
        local = locals.addCachedProperty(offset)
        initializeFromContextIR
      } else if (local == null) {
        local = locals.addCachedProperty(offset)
        initializeFromStoreIR
      } else {
        // Even if the local has been seen before in this method, we cannot be sure that the code path which added the initialization code was taken
        // (See full comment in getLongAt above)
        condition(isNull(load(local)))(
          if (maybeCachedProperty.isDefined) {
            initializeFromContextIR
          } else {
            initializeFromStoreIR
          }
        )
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

  override protected def isLabelSetOnNode(labelToken: IntermediateRepresentation, offset: Int): IntermediateRepresentation =
    invokeStatic(
      method[CompiledCursorUtils, Boolean, Read, NodeCursor, Long, Int]("nodeHasLabel"),
      loadField(OperatorCodeGenHelperTemplates.DATA_READ),
      ExpressionCompiler.NODE_CURSOR,
      getLongAt(offset),
      labelToken)

  override protected def getNodeProperty(propertyToken: IntermediateRepresentation, offset: Int): IntermediateRepresentation =
    invokeStatic(
          method[CompiledCursorUtils, Value, Read, NodeCursor, Long, PropertyCursor, Int]("nodeGetProperty"),
          loadField(OperatorCodeGenHelperTemplates.DATA_READ),
          ExpressionCompiler.NODE_CURSOR,
          getLongAt(offset),
          ExpressionCompiler.PROPERTY_CURSOR,
          propertyToken)

  override protected def getRelationshipProperty(propertyToken: IntermediateRepresentation, offset: Int): IntermediateRepresentation =
    invokeStatic(
      method[CompiledCursorUtils, Value, Read, RelationshipScanCursor, Long, PropertyCursor, Int]("relationshipGetProperty"),
      loadField(OperatorCodeGenHelperTemplates.DATA_READ),
      ExpressionCompiler.RELATIONSHIP_CURSOR,
      getLongAt(offset),
      ExpressionCompiler.PROPERTY_CURSOR,
      propertyToken)

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
