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
import org.neo4j.cypher.operations.CursorUtils
import org.neo4j.internal.kernel.api.{NodeCursor, PropertyCursor, Read, RelationshipScanCursor}
import org.neo4j.cypher.internal.runtime.ExecutionContext
import org.neo4j.cypher.internal.runtime.compiled.expressions._
import org.neo4j.cypher.internal.runtime.morsel.OperatorExpressionCompiler.{LocalVariableSlotMapper, LocalsForSlots, ScopeContinuationState}
import org.neo4j.cypher.internal.runtime.morsel.operators.OperatorCodeGenHelperTemplates
import org.neo4j.cypher.internal.runtime.morsel.operators.OperatorCodeGenHelperTemplates.{INPUT_MORSEL, OUTPUT_ROW, UNINITIALIZED_LONG_SLOT_VALUE}
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Value

import scala.collection.mutable.ArrayBuffer

object OperatorExpressionCompiler {

  // (`slot offset`, `local variable name`, `is modified`)
  type FOREACH_LOCAL_FUN = (Int, String, Boolean) => Unit

  case class LocalVariableSlotMapper(scopeId: String, slots: SlotConfiguration) {
    private val longSlotToLocal: Array[String] = new Array[String](slots.numberOfLongs)
    private val longSlotToLocalModified: Array[Boolean] = new Array[Boolean](slots.numberOfLongs)
    private val refSlotToLocal: Array[String] = new Array[String](slots.numberOfReferences)
    private val refSlotToLocalModified: Array[Boolean] = new Array[Boolean](slots.numberOfReferences)
    private val cachedProperties: Array[String] = new Array[String](slots.numberOfReferences)

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

    def markModifiedLocalForLongSlot(offset: Int): Unit = {
      longSlotToLocalModified(offset) = true
    }

    def markModifiedLocalForRefSlot(offset: Int): Unit = {
      refSlotToLocalModified(offset) = true
    }

    def getLocalForLongSlot(offset: Int): String = longSlotToLocal(offset)

    def getLocalForRefSlot(offset: Int): String = refSlotToLocal(offset)

    val foreachLocalForLongSlot: (FOREACH_LOCAL_FUN) => Unit = foreachLocalFor(longSlotToLocal, longSlotToLocalModified)
    val foreachLocalForRefSlot: (FOREACH_LOCAL_FUN) => Unit = foreachLocalFor(refSlotToLocal, refSlotToLocalModified)

    private def foreachLocalFor(slotToLocal: Array[String], slotToLocalModified: Array[Boolean])
                               (f: FOREACH_LOCAL_FUN): Unit = {
      var i = 0
      while (i < slotToLocal.length) {
        val name = slotToLocal(i)
        if (name != null) {
          val modified = slotToLocalModified(i)
          f(i, name, modified)
        }
        i += 1
      }
    }

    def foreachCachedProperty(f: (Int, String) => Unit): Unit = {
      var i = 0
      while (i < cachedProperties.length) {
        val cp = cachedProperties(i)
        if (cp != null) {
          f(i, cp)
        }
        i += 1
      }
    }

    def merge(other: LocalVariableSlotMapper): Unit = {
      other.foreachLocalForLongSlot { case (slot, local, modified) =>
        longSlotToLocal(slot) = local
        longSlotToLocalModified(slot) = modified
      }
      other.foreachLocalForRefSlot { case (slot, local, modified) =>
        refSlotToLocal(slot) = local
        refSlotToLocalModified(slot) = modified
      }
      other.foreachCachedProperty { case(i, key) =>
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

      foreachLocalForLongSlot { case (_, local, _) =>
        addField(field[Long]("saved" + local.capitalize), local)
      }

      foreachLocalForRefSlot { case (_, local, _) =>
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
     * Local slot variables that are added from now on will be added to this new scope,
     * until endScope is called with an option to merge them back into the parent scope or not.
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

    def markModifiedLocalForLongSlot(offset: Int): Unit = {
      scopeStack.head.markModifiedLocalForLongSlot(offset)
    }

    def markModifiedLocalForRefSlot(offset: Int): Unit = {
      scopeStack.head.markModifiedLocalForRefSlot(offset)
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

    def foreachLocalForLongSlot(f: FOREACH_LOCAL_FUN): Unit = {
      scopeStack.head.foreachLocalForLongSlot(f)
    }

    def foreachLocalForRefSlot(f: FOREACH_LOCAL_FUN): Unit = {
      scopeStack.head.foreachLocalForRefSlot(f)
    }

    def foreachCachedProperty(f: (Int, String) => Unit): Unit = {
      scopeStack.head.foreachCachedProperty(f)
    }
  }
}

class OperatorExpressionCompiler(slots: SlotConfiguration,
                                 val inputSlotConfiguration: SlotConfiguration,
                                 readOnly: Boolean,
                                 codeGenerationMode: CodeGeneration.CodeGenerationMode,
                                 namer: VariableNamer)
  extends ExpressionCompiler(slots, readOnly, codeGenerationMode, namer) {

  import org.neo4j.codegen.api.IntermediateRepresentation._

  /**
   * Used to track which slots have been loaded into local variables ([[getLongAt]], [[getRefAt]]),
   * and which ones have been modified ([[setLongAt]], [[setRefAt]])
   * and may need to be written to the output context by [[writeLocalsToSlots()]],
   * as well as which properties have been cached ([[getCachedPropertyAt]], [[setCachedPropertyAt]]).
   */
  private val locals = LocalsForSlots(slots)

  /**
   * Used by [[copyFromInput]] to track the argument state
   */
  private var nLongSlotsToCopyFromInput: Int = 0
  private var nRefSlotsToCopyFromInput: Int = 0

  /**
   * Uses a local slot variable if one is already defined, otherwise declares and assigns a new local slot variable
   */
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

  /**
   * Like getLongAt, this uses a local slot variable if one is already defined, otherwise gets the value directly from
   * the input ExecutionContext without declaring a new local slot variable for it.
   * This is useful to avoid creating unnecessary continuation state if only a single use of the value is known to be contained within a local scope.
   */
  final def getLongAtNoSave(offset: Int): IntermediateRepresentation = {
    val local = locals.getLocalForLongSlot(offset)
    if (local == null) {
      getLongFromExecutionContext(offset, loadField(INPUT_MORSEL))
    } else {
      load(local)
    }
  }

  /**
   * Uses a local slot variable if one is already defined, otherwise declares and assigns a new local slot variable
   */
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

  /**
   * Like getRefAt, this uses a local slot variable if one is already defined, otherwise gets the value directly from
   * the input ExecutionContext without declaring a new local slot variable for it.
   * This is useful to avoid creating unnecessary continuation state if only a single use of the value is known to be contained within a local scope.
   */
  final def getRefAtNoSave(offset: Int): IntermediateRepresentation = {
    val local = locals.getLocalForRefSlot(offset)
    if (local == null) {
      getLongFromExecutionContext(offset, loadField(INPUT_MORSEL))
    } else {
      load(local)
    }
  }

  override final def setLongAt(offset: Int, value: IntermediateRepresentation): IntermediateRepresentation = {
    var local = locals.getLocalForLongSlot(offset)
    if (local == null) {
      local = locals.addLocalForLongSlot(offset)
    }
    locals.markModifiedLocalForLongSlot(offset)
    assign(local, value)
  }

  override final def setRefAt(offset: Int, value: IntermediateRepresentation): IntermediateRepresentation = {
    var local = locals.getLocalForRefSlot(offset)
    if (local == null) {
      local = locals.addLocalForRefSlot(offset)
    }
    locals.markModifiedLocalForRefSlot(offset)
    assign(local, value)
  }

  /**
   * Mark the initial range of slots that needs to be copied from the input ExecutionContext.
   * These are usually the argument slots of a pipeline.
   */
  def copyFromInput(nLongs: Int, nRefs: Int): IntermediateRepresentation = {
    // Update the number of slots that we need to copy from the input row to the output row
    if (nLongs > nLongSlotsToCopyFromInput) {
      nLongSlotsToCopyFromInput = nLongs
    }
    if (nRefs > nRefSlotsToCopyFromInput) {
      nRefSlotsToCopyFromInput = nRefs
    }
    // The actual copy will occur later, and only if it is needed, in writeLocalsToSlots()
    noop()
  }

  final def doCopyFromWithExecutionContext(context: IntermediateRepresentation, input: IntermediateRepresentation, nLongs: Int, nRefs: Int): IntermediateRepresentation = {
    invokeSideEffect(context, method[ExecutionContext, Unit, ExecutionContext, Int, Int]("copyFrom"),
      input, constant(nLongs), constant(nRefs)
    )
  }

  // Testing hooks
  protected def didInitializeCachedPropertyFromStore(): Unit = {}
  protected def didInitializeCachedPropertyFromContext(): Unit = {}
  protected def didLoadLocalCachedProperty(): Unit = {}

  override def getCachedPropertyAt(property: SlottedCachedProperty, getFromStore: IntermediateRepresentation): IntermediateRepresentation = {
    val offset = property.cachedPropertyOffset
    var local = locals.getLocalForRefSlot(offset)
    val maybeCachedProperty = inputSlotConfiguration.getCachedPropertySlot(property)

    def initializeFromStoreIR = {
      assign(local, getFromStore)
    }
    def initializeFromContextIR = {
      block(
        assign(local, getCachedPropertyFromExecutionContext(maybeCachedProperty.get.offset, loadField(INPUT_MORSEL))),
        condition(isNull(load(local)))(initializeFromStoreIR)
      )
    }

    val prepareOps =
      if (local == null && maybeCachedProperty.isDefined) {
        didInitializeCachedPropertyFromContext()
        local = locals.addCachedProperty(offset)
        initializeFromContextIR
      } else if (local == null) {
        didInitializeCachedPropertyFromStore()
        local = locals.addCachedProperty(offset)
        initializeFromStoreIR
      } else {
        didLoadLocalCachedProperty()
        // Even if the local has been seen before in this method it may not be in a code path that have been hit at runtime
        // in this loop iteration. The cached property variable is also reset to null at the end of each inner loop iteration.
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
    locals.markModifiedLocalForRefSlot(offset)
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

  /**
   * Write to the output ExecutionContext
   *
   * We write all local slot variables that have been modified within this pipeline,
   * plus an argument prefix range of slots that has been tracked by [[nLongSlotsToCopyFromInput]] and [[nRefSlotsToCopyFromInput]]
   * upon calls to [[copyFromInput]] by operator codegen templates.
   *
   * We can assume that this argument range of m slots can be divided into a prefix range of 0 to n initial arguments from the input context that are not
   * accessed within the pipeline that always needs to be copied to the output context because a pipeline of an outer apply-nesting level may need them later on,
   * and a suffix range of n+1 to m arguments that are being accessed in this pipeline, and thus already declared as locals.
   * However, currently we copy the whole range from the input context, up to the first slot that was modified within this pipeline.
   * If that range is very small, within a threshold, we use individual slot setter methods (e.g. [[ExecutionContext.setLongAt]]),
   * otherwise we use the [[ExecutionContext.copyFrom]] method.
   *
   */
  def writeLocalsToSlots(): IntermediateRepresentation = {
    val writeOps = new ArrayBuffer[IntermediateRepresentation]()
    val writeLongSlotOps = new ArrayBuffer[IntermediateRepresentation]()
    val writeRefSlotOps = new ArrayBuffer[IntermediateRepresentation]()

    val USE_ARRAY_COPY_THRESHOLD = 2

    // First collect all write operations for modified slots
    var firstModifiedLongSlot = Int.MaxValue
    locals.foreachLocalForLongSlot { case (offset, local, modified) =>
      if (modified) {
        if (firstModifiedLongSlot > offset) {
          firstModifiedLongSlot = offset
        }
        writeLongSlotOps += setLongInExecutionContext(offset, load(local))
      }
    }
    var firstModifiedRefSlot = Int.MaxValue
    locals.foreachLocalForRefSlot { case (offset, local, modified) =>
      if (modified) {
        if (firstModifiedRefSlot > offset) {
          firstModifiedRefSlot = offset
        }
        writeRefSlotOps += setRefInExecutionContext(offset, load(local))
      }
    }

    // Now we can compute how many arguments slots that we actually need to copy
    val nLongsToCopy = Math.min(nLongSlotsToCopyFromInput, firstModifiedLongSlot)
    val nRefsToCopy = Math.min(nRefSlotsToCopyFromInput, firstModifiedRefSlot)

    // Prepend the write operations for argument slots
    // Decide if we should use ExecutionContext.copyFrom or just prepend individual set operations for the remaining slots?
    if (nLongsToCopy > USE_ARRAY_COPY_THRESHOLD || nRefsToCopy > USE_ARRAY_COPY_THRESHOLD) {
      // Use the ExecutionContext.copyFrom method (which may use array copy)
      writeOps += doCopyFromWithExecutionContext(OUTPUT_ROW, loadField(INPUT_MORSEL), nLongsToCopy, nRefsToCopy)
      writeOps ++= writeLongSlotOps
      writeOps ++= writeRefSlotOps
    } else {
      // Add ExecutionContext.setLongAt operations for argument slots?
      if (nLongsToCopy > 0) {
        var i = 0
        while (i < nLongsToCopy) {
          val local = locals.getLocalForLongSlot(i)
          val getOp =
            if (local == null)
              getLongFromExecutionContext(i, loadField(INPUT_MORSEL))
            else
              load(local)
          writeOps += setLongInExecutionContext(i, getOp)
          i += 1
        }
      }
      // Add the write operations for the modified long slots
      writeOps ++= writeLongSlotOps

      // Add ExecutionContext.setRefAt operations for argument slots?
      if (nRefsToCopy > 0) {
        var i = 0
        while (i < nRefsToCopy) {
          val local = locals.getLocalForRefSlot(i)
          val getOp =
            if (local == null)
              getRefFromExecutionContext(i, loadField(INPUT_MORSEL))
            else
              load(local)
          setRefInExecutionContext(i, getOp) +=: writeRefSlotOps
          i += 1
        }
      }
      // Add the write operations for the modified ref slots
      writeOps ++= writeRefSlotOps
    }

    block(writeOps: _*)
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
    val all = new ArrayBuffer[(Int, String)]()
    locals.foreachLocalForLongSlot { case (slot, local, _) =>
      all += slot -> local
    }
    all
  }

  def getAllLocalsForRefSlots: Seq[(Int, String)] = {
    val all = new ArrayBuffer[(Int, String)]()
    locals.foreachLocalForRefSlot { case (slot, local, _) =>
      all += slot -> local
    }
    all
  }

  def getAllLocalsForCachedProperties: Seq[(Int, String)] = {
    val all = new ArrayBuffer[(Int, String)]()
    locals.foreachCachedProperty { case (slot, name) =>
      all += slot -> name
    }
    all
  }
}
