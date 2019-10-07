/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel

import org.neo4j.codegen.api.IntermediateRepresentation.{assign, block, constant, field, load, loadField, setField}
import org.neo4j.codegen.api.{CodeGeneration, Field, IntermediateRepresentation}
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.physicalplanning.ast.SlottedCachedProperty
import org.neo4j.cypher.internal.runtime.DbAccess
import org.neo4j.cypher.internal.runtime.compiled.expressions._
import org.neo4j.cypher.internal.runtime.morsel.OperatorExpressionCompiler.{LocalVariableSlotMapper, LocalsForSlots, ScopeContinuationState}
import org.neo4j.cypher.internal.runtime.morsel.operators.OperatorCodeGenHelperTemplates
import org.neo4j.cypher.internal.runtime.morsel.operators.OperatorCodeGenHelperTemplates.{INPUT_MORSEL, UNINITIALIZED_LONG_SLOT_VALUE}
import org.neo4j.cypher.operations.CursorUtils
import org.neo4j.internal.kernel.api.{NodeCursor, PropertyCursor, Read, RelationshipScanCursor}
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Value

import scala.collection.mutable.ArrayBuffer

object OperatorExpressionCompiler {

  case class LocalVariableSlotMapper(scopeId: String, slots: SlotConfiguration) {
    val longSlotToLocal: Array[String] = new Array[String](slots.numberOfLongs)
    val refSlotToLocal: Array[String] = new Array[String](slots.numberOfReferences)
    val cachedProperties: Array[String] = new Array[String](slots.numberOfReferences)

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
      cachedProperties(offset) = refslot
      refslot
    }

    def getLocalForLongSlot(offset: Int): String = longSlotToLocal(offset)

    def getLocalForRefSlot(offset: Int): String = refSlotToLocal(offset)

    def getAllLocalsForLongSlots: Seq[(Int, String)] =
      getAllLocalsFor(longSlotToLocal)

    def getAllLocalsForRefSlots: Seq[(Int, String)] =
      getAllLocalsFor(refSlotToLocal)

    def getAllLocalsForCachedProperties: Seq[(Int, String)] = {
      var i = 0
      val locals = new ArrayBuffer[(Int, String)]()
      while (i < cachedProperties.length) {
        val cp = cachedProperties(i)
        if (cp != null) {
          locals += i -> cp
        }
        i += 1
      }
      locals
    }

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

    def merge(other: LocalVariableSlotMapper): Unit = {
      other.getAllLocalsForLongSlots.foreach { case (slot, local) =>
        longSlotToLocal(slot) = local
      }
      other.getAllLocalsForRefSlots.foreach { case (slot, local) =>
        refSlotToLocal(slot) = local
      }
      other.getAllLocalsForCachedProperties.foreach { case(i, key) =>
        cachedProperties(i) = key
      }
    }

    def genScopeContinuationState: ScopeContinuationState = {
      val fields = new ArrayBuffer[Field]()
      val saveOps = new ArrayBuffer[IntermediateRepresentation]()
      val restoreOps = new ArrayBuffer[IntermediateRepresentation]()

      def addField(f: Field, local: String): Unit = {
        fields += f
        saveOps += setField(f, load(local))
        restoreOps += assign(local, loadField(f))
      }

      getAllLocalsForLongSlots.foreach { case (_, local) =>
        addField(field[Long]("saved" + local.capitalize), local)
      }

      getAllLocalsForRefSlots.foreach { case (_, local) =>
        addField(field[AnyValue]("saved" + local.capitalize), local)
      }

      // We use a boolean flag to control if the state is used
      //  - true if it has been saved, and can be restored
      //  - false if it has been restored
      // (this could be made volatile if it needs to be used in places that lacks memory barriers)
      if (fields.nonEmpty) {
        val hasStateField = field[Boolean](scopeId + "HasContinuationState")
        fields += hasStateField

        saveOps += setField(hasStateField, constant(true))
        restoreOps += setField(hasStateField, constant(false))
      }

      ScopeContinuationState(fields, block(saveOps: _*), block(restoreOps: _*))
    }
  }

  case class ScopeContinuationState(fields: Seq[Field], saveStateIR: IntermediateRepresentation, restoreStateIR: IntermediateRepresentation) {
    def isEmpty: Boolean = fields.isEmpty
    def nonEmpty: Boolean = fields.nonEmpty
  }

  case class LocalsForSlots(slots: SlotConfiguration) {
    private val rootScope: LocalVariableSlotMapper = LocalVariableSlotMapper("root", slots)
    private var scopeStack: List[LocalVariableSlotMapper] = rootScope :: Nil

    /**
     * Mark the beginning of a new scope
     *
     * Pushes a new scope to a scope stack.
     * Local slot variables that are adde
     *
     * @param scopeId A string identifier for this scope.
     *                NOTE: This will be used as a prefix to a generated variable name for the continuation state returned by [[endScope]],
     *                so the string has to follow Java variable naming rules
     */
    def beginScope(scopeId: String): Unit = {
      val localVariableSlotMapper = LocalVariableSlotMapper(scopeId, slots)
      scopeStack = localVariableSlotMapper :: scopeStack
    }

    /**
     * End the scope started by the previous call to [[beginScope]]
     *
     * Pops the current scope from the scope state, and generates a [[ScopeContinuationState]] that contains
     * fields for all local slot variables that were added in this scope, along with code to save and restore this state to/from local variables
     *
     * @param mergeIntoParentScope true if locals added in this scope should be merged back into the parent scope, otherwise they will be discarded
     *                             @note The caller is responsible for making sure that the locals are indeed declared and initialized in the parent scope!
     *
     * @return continuationState The generated [[ScopeContinuationState]] for this scope
     */
    def endScope(mergeIntoParentScope: Boolean): ScopeContinuationState = {
      val endedScope = scopeStack.head
      scopeStack = scopeStack.tail
      val continuationState = endedScope.genScopeContinuationState
      if (mergeIntoParentScope) {
        scopeStack.head.merge(endedScope)
      }
      continuationState
    }

    def addLocalForLongSlot(offset: Int): String = {
      scopeStack.head.addLocalForLongSlot(offset)
    }

    def addLocalForRefSlot(offset: Int): String = {
      scopeStack.head.addLocalForRefSlot(offset)
    }

    def addCachedProperty(offset: Int): String = {
      scopeStack.head.addCachedProperty(offset)
    }

    def getLocalForLongSlot(offset: Int): String = {
      var local: String = null
      var scope = scopeStack
      do {
        local = scope.head.getLocalForLongSlot(offset)
        scope = scope.tail
      } while (local == null && scope != Nil)
      local
    }

    def getLocalForRefSlot(offset: Int): String = {
      var local: String = null
      var scope = scopeStack
      do {
        local = scope.head.getLocalForRefSlot(offset)
        scope = scope.tail
      } while (local == null && scope != Nil)
      local
    }

    def getAllLocalsForLongSlots: Seq[(Int, String)] = {
      scopeStack.head.getAllLocalsForLongSlots
    }

    def getAllLocalsForRefSlots: Seq[(Int, String)] = {
      scopeStack.head.getAllLocalsForRefSlots
    }

    def getAllLocalsForCachedProperties: Seq[(Int, String)] = {
      scopeStack.head.getAllLocalsForCachedProperties
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

  private val locals = LocalsForSlots(slots)

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
    if (local == null) orElse else {
      load(local)
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
      load(local)
    }
  }

  final def getRefAtOrElse(offset: Int, orElse: IntermediateRepresentation): IntermediateRepresentation = {
    val local = locals.getLocalForRefSlot(offset)
    if (local == null) orElse else {
      load(local)
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
        // Even if the local has been seen before in this method it could have been reset to null at the end of an iteration
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
      method[CursorUtils, Boolean, Read, NodeCursor, Long, Int]("nodeHasLabel"),
      loadField(OperatorCodeGenHelperTemplates.DATA_READ),
      ExpressionCompiler.NODE_CURSOR,
      getLongAt(offset),
      labelToken)

  override protected def getNodeProperty(propertyToken: IntermediateRepresentation, offset: Int): IntermediateRepresentation =
    invokeStatic(
      method[CursorUtils, Value, Read, NodeCursor, Long, PropertyCursor, Int]("nodeGetProperty"),
      loadField(OperatorCodeGenHelperTemplates.DATA_READ),
      ExpressionCompiler.NODE_CURSOR,
      getLongAt(offset),
      ExpressionCompiler.PROPERTY_CURSOR,
      propertyToken)

  override protected def getRelationshipProperty(propertyToken: IntermediateRepresentation, offset: Int): IntermediateRepresentation =
    invokeStatic(
      method[CursorUtils, Value, Read, RelationshipScanCursor, Long, PropertyCursor, Int]("relationshipGetProperty"),
      loadField(OperatorCodeGenHelperTemplates.DATA_READ),
      ExpressionCompiler.RELATIONSHIP_CURSOR,
      getLongAt(offset),
      ExpressionCompiler.PROPERTY_CURSOR,
      propertyToken)

  override protected def getProperty(key: String,
                                     container: IntermediateRepresentation): IntermediateRepresentation =
    invokeStatic(
      method[CursorUtils, AnyValue, String, AnyValue, Read, DbAccess, NodeCursor, RelationshipScanCursor, PropertyCursor]("propertyGet"),
      constant(key),
      container,
      loadField(OperatorCodeGenHelperTemplates.DATA_READ),
      ExpressionCompiler.DB_ACCESS,
      ExpressionCompiler.NODE_CURSOR,
      ExpressionCompiler.RELATIONSHIP_CURSOR,
      ExpressionCompiler.PROPERTY_CURSOR)

  def writeLocalsToSlots(): IntermediateRepresentation = {
    val writeLongs = locals.getAllLocalsForLongSlots.map { case (offset, local) =>
      setLongInExecutionContext(offset, load(local))
    }
    val writeRefs = locals.getAllLocalsForRefSlots.map { case (offset, local) =>
      setRefInExecutionContext(offset, load(local))
    }
    block(writeLongs ++ writeRefs: _*)
  }

  //===========================================================================
  // Delegates to LocalsForSlots

  def beginScope(scopeId: String): Unit = {
    locals.beginScope(scopeId)
  }

  def endScope(mergeIntoParentScope: Boolean = false): ScopeContinuationState = {
    locals.endScope(mergeIntoParentScope)
  }

  def getAllLocalsForLongSlots: Seq[(Int, String)] = {
    locals.getAllLocalsForLongSlots
  }

  def getAllLocalsForRefSlots: Seq[(Int, String)] = {
    locals.getAllLocalsForRefSlots
  }

  def getAllLocalsForCachedProperties: Seq[(Int, String)] = {
    locals.getAllLocalsForCachedProperties
  }
}
