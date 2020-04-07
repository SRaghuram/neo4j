/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined


import java.util

import org.neo4j.codegen.api.CodeGeneration
import org.neo4j.codegen.api.Field
import org.neo4j.codegen.api.IntermediateRepresentation
import org.neo4j.codegen.api.IntermediateRepresentation.assign
import org.neo4j.codegen.api.IntermediateRepresentation.block
import org.neo4j.codegen.api.IntermediateRepresentation.cast
import org.neo4j.codegen.api.IntermediateRepresentation.condition
import org.neo4j.codegen.api.IntermediateRepresentation.constant
import org.neo4j.codegen.api.IntermediateRepresentation.declare
import org.neo4j.codegen.api.IntermediateRepresentation.field
import org.neo4j.codegen.api.IntermediateRepresentation.invoke
import org.neo4j.codegen.api.IntermediateRepresentation.invokeSideEffect
import org.neo4j.codegen.api.IntermediateRepresentation.invokeStatic
import org.neo4j.codegen.api.IntermediateRepresentation.isNull
import org.neo4j.codegen.api.IntermediateRepresentation.load
import org.neo4j.codegen.api.IntermediateRepresentation.loadField
import org.neo4j.codegen.api.IntermediateRepresentation.method
import org.neo4j.codegen.api.IntermediateRepresentation.noValue
import org.neo4j.codegen.api.IntermediateRepresentation.noop
import org.neo4j.codegen.api.IntermediateRepresentation.setField
import org.neo4j.codegen.api.IntermediateRepresentation.ternary
import org.neo4j.codegen.api.IntermediateRepresentation.variable
import org.neo4j.codegen.api.LocalVariable
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.physicalplanning.SlottedRewriter
import org.neo4j.cypher.internal.physicalplanning.SlottedRewriter.DEFAULT_NULLABLE
import org.neo4j.cypher.internal.physicalplanning.SlottedRewriter.DEFAULT_OFFSET_IS_FOR_LONG_SLOT
import org.neo4j.cypher.internal.physicalplanning.ast.SlottedCachedProperty
import org.neo4j.cypher.internal.runtime.DbAccess
import org.neo4j.cypher.internal.runtime.ReadableRow
import org.neo4j.cypher.internal.runtime.WritableRow
import org.neo4j.cypher.internal.runtime.compiled.expressions.AbstractExpressionCompiler
import org.neo4j.cypher.internal.runtime.compiled.expressions.CursorRepresentation
import org.neo4j.cypher.internal.runtime.compiled.expressions.ExpressionCompilation
import org.neo4j.cypher.internal.runtime.compiled.expressions.VariableNamer
import org.neo4j.cypher.internal.runtime.pipelined.OperatorExpressionCompiler.LocalsForSlots
import org.neo4j.cypher.internal.runtime.pipelined.OperatorExpressionCompiler.ScopeContinuationState
import org.neo4j.cypher.internal.runtime.pipelined.OperatorExpressionCompiler.ScopeLocalsState
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.DATA_READ
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.INPUT_CURSOR
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.OUTPUT_CURSOR
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.UNINITIALIZED_LONG_SLOT_VALUE
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.UNINITIALIZED_REF_SLOT_VALUE
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.nodeGetProperty
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.nodeHasLabel
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.nodeHasProperty
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.relationshipGetProperty
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.relationshipHasProperty
import org.neo4j.cypher.operations.CursorUtils
import org.neo4j.internal.kernel.api.NodeCursor
import org.neo4j.internal.kernel.api.NodeLabelIndexCursor
import org.neo4j.internal.kernel.api.NodeValueIndexCursor
import org.neo4j.internal.kernel.api.PropertyCursor
import org.neo4j.internal.kernel.api.Read
import org.neo4j.internal.kernel.api.RelationshipScanCursor
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Value

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object OperatorExpressionCompiler {

  // (`slot offset`, `local variable name`, `is modified`)
  type FOREACH_LOCAL_FUN = (Int, String, Boolean) => Unit

  case class LocalVariableSlotMapper(scopeId: String, slots: SlotConfiguration)(
    private val longSlotToLocal: Array[String] = new Array[String](slots.numberOfLongs),
    private val longSlotToLocalModified: Array[Boolean] = new Array[Boolean](slots.numberOfLongs),
    private val longSlotToLocalInitialized: Array[Boolean] = new Array[Boolean](slots.numberOfLongs), // TODO: Change to a single array with flags (modified, initialized)
    private val refSlotToLocal: Array[String] = new Array[String](slots.numberOfReferences),
    private val refSlotToLocalModified: Array[Boolean] = new Array[Boolean](slots.numberOfReferences),
    private val refSlotToLocalInitialized: Array[Boolean] = new Array[Boolean](slots.numberOfReferences), // TODO: Change to a single array with flags (modified, initialized)
    private val cachedProperties: Array[String] = new Array[String](slots.numberOfReferences)
  ) {

    def copy(): LocalVariableSlotMapper = {
      LocalVariableSlotMapper(scopeId, slots)(
        longSlotToLocal = util.Arrays.copyOf(this.longSlotToLocal, this.longSlotToLocal.length),
        longSlotToLocalModified = util.Arrays.copyOf(this.longSlotToLocalModified, this.longSlotToLocalModified.length),
        longSlotToLocalInitialized = util.Arrays.copyOf(this.longSlotToLocalInitialized, this.longSlotToLocalInitialized.length),
        refSlotToLocal = util.Arrays.copyOf(this.refSlotToLocal, this.refSlotToLocal.length),
        refSlotToLocalModified = util.Arrays.copyOf(this.refSlotToLocalModified, this.refSlotToLocalModified.length),
        refSlotToLocalInitialized = util.Arrays.copyOf(this.refSlotToLocalInitialized, this.refSlotToLocalInitialized.length),
        cachedProperties = util.Arrays.copyOf(this.cachedProperties, this.cachedProperties.length)
      )
    }

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

    def markInitializedLocalForLongSlot(offset: Int): Unit = {
      longSlotToLocalInitialized(offset) = true
    }

    def markInitializedLocalForRefSlot(offset: Int): Unit = {
      refSlotToLocalInitialized(offset) = true
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
        longSlotToLocalInitialized(slot) = true // If we merge a scope, we assume that initialization is taken care of, and we should not automatically generate code to initialize from input context
      }
      other.foreachLocalForRefSlot { case (slot, local, modified) =>
        refSlotToLocal(slot) = local
        refSlotToLocalModified(slot) = modified
        refSlotToLocalInitialized(slot) = true // If we merge a scope, we assume that initialization is taken care of, and we should not automatically generate code to initialize from input context
      }
      other.foreachCachedProperty { case(i, key) =>
        cachedProperties(i) = key
      }
    }

    def genScopeLocalsState(codeGen: AbstractExpressionCompiler, inputContext: IntermediateRepresentation): ScopeLocalsState = {
      val locals = new ArrayBuffer[LocalVariable]()

      foreachLocalForLongSlot { case (slot, name, modified) =>
        val initialized = longSlotToLocalInitialized(slot)
        val initValueIR =
          if (initialized) {
            // This value will be overwritten within this scope or a child scope
            UNINITIALIZED_LONG_SLOT_VALUE
          } else {
            // Load from input context
            codeGen.getLongFromExecutionContext(slot, inputContext)
          }
        locals += variable[Long](name, initValueIR)
      }

      foreachLocalForRefSlot { case (slot, name, modified) =>
        val initialized = refSlotToLocalInitialized(slot)
        val initValueIR =
          if (initialized) {
            // This value will be overwritten within this scope or a child scope
            UNINITIALIZED_REF_SLOT_VALUE
          } else {
            // Load from input context
            codeGen.getRefFromExecutionContext(slot, inputContext)
          }
        locals += variable[AnyValue](name, initValueIR)
      }

      val declarations = new ArrayBuffer[IntermediateRepresentation]()
      val assignments = new ArrayBuffer[IntermediateRepresentation]()
      locals.foreach { lv =>
        declarations += declare(lv.typ, lv.name)
        assignments += assign(lv.name, lv.value)
      }

      ScopeLocalsState(locals, declarations, assignments)
    }

    def genScopeContinuationState(codeGen: AbstractExpressionCompiler, inputContext: IntermediateRepresentation): ScopeContinuationState = {
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

      val scopeLocalsState = genScopeLocalsState(codeGen, inputContext)

      ScopeContinuationState(fields, block(saveOps: _*), block(restoreOps: _*), scopeLocalsState.declarations, scopeLocalsState.assignments)
    }
  }

  case class ScopeLocalsState(locals: Seq[LocalVariable], declarations: Seq[IntermediateRepresentation], assignments: Seq[IntermediateRepresentation]) {
    def isEmpty: Boolean = locals.isEmpty
    def nonsEmpty: Boolean = locals.nonEmpty
  }

  case class ScopeContinuationState(fields: Seq[Field],
                                    saveStateIR: IntermediateRepresentation,
                                    restoreStateIR: IntermediateRepresentation,
                                    declarations: Seq[IntermediateRepresentation],
                                    assignments: Seq[IntermediateRepresentation]) {
    def isEmpty: Boolean = fields.isEmpty
    def nonEmpty: Boolean = fields.nonEmpty
  }

  case class LocalsForSlots(operatorExpressionCompiler: OperatorExpressionCompiler) {
    private val slots = operatorExpressionCompiler.slots
    private val rootScope: LocalVariableSlotMapper = LocalVariableSlotMapper("root", slots)()
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
      val localVariableSlotMapper = LocalVariableSlotMapper(scopeId, slots)()
      scopeStack = localVariableSlotMapper :: scopeStack
    }

    /**
     * End the scope started by the previous call to [[beginScope]]
     *
     * Pops the current scope from the scope state, and generates a [[ScopeLocalsState]] that contains
     * all the local slot variables that were added in this scope, along with separate declaration and assignment code to load their values from the input context
     *
     * @param mergeIntoParentScope true if locals added in this scope should be merged back into the parent scope, otherwise they will be discarded
     *                             @note The caller is responsible for making sure that the locals are indeed declared and initialized in the parent scope!
     *
     * @return continuationState The generated [[ScopeLocalsState]] for this scope
     */
    def endScope(mergeIntoParentScope: Boolean): ScopeLocalsState = {
      endScope[ScopeLocalsState](_.genScopeLocalsState(operatorExpressionCompiler, INPUT_CURSOR), mergeIntoParentScope)
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
    def endInitializationScope(mergeIntoParentScope: Boolean): ScopeContinuationState = {
      endScope[ScopeContinuationState](_.genScopeContinuationState(operatorExpressionCompiler, INPUT_CURSOR), mergeIntoParentScope)
    }

    private def endScope[T](genState: LocalVariableSlotMapper => T, mergeIntoParentScope: Boolean): T = {
      val endedScope = scopeStack.head
      scopeStack = scopeStack.tail
      val state = genState(endedScope)
      if (mergeIntoParentScope) {
        scopeStack.head.merge(endedScope)
      }
      state
    }

    /**
     * Return a new scope which is the result of merging all the scopes currently on the scope stack.
     * The returned scope is a copy and the original scopes on the scope stack are not affected.
     */
    def mergeAllScopesCopy(): LocalVariableSlotMapper = {
      var s = scopeStack
      val scope = s.head.copy()
      while (s.tail != Nil) {
        s = s.tail
        scope.merge(s.head)
      }
      scope
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

    def markInitializedLocalForLongSlot(offset: Int): Unit = {
      scopeStack.head.markInitializedLocalForLongSlot(offset)
    }

    def markInitializedLocalForRefSlot(offset: Int): Unit = {
      scopeStack.head.markInitializedLocalForRefSlot(offset)
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
  extends AbstractExpressionCompiler(slots, readOnly, codeGenerationMode, namer) {


  /**
   * Used to track which slots have been loaded into local variables ([[getLongAt]], [[getRefAt]]),
   * and which ones have been modified ([[setLongAt]], [[setRefAt]])
   * and may need to be written to the output context by [[writeLocalsToSlots()]],
   * as well as which properties have been cached ([[getCachedPropertyAt]], [[setCachedPropertyAt]]).
   */
  protected val locals: LocalsForSlots = LocalsForSlots(this)

  /**
   * Used by [[copyFromInput]] to track the argument state
   */
  private var nLongSlotsToCopyFromInput: Int = 0
  private var nRefSlotsToCopyFromInput: Int = 0

  /**
   * Used for giving direct access to cursors from expressions.
   *
   * For example for a query `MATCH (a)` you can register the cursor used for
   * traversing for a so that following expressions such as `hasLabel` etc
   * can use the cursor directly instead of using a separate cursor an postion it
   * on the correct node.
   */
  private val cursors = mutable.Map.empty[String, CursorRepresentation]

  /**
   * Registers a cursor that points at the entity with the given name
   * @param name the name of the variable that the cursor is traversing
   * @param cursor the representation for accessing the cursor
   */
  def registerCursor(name: String, cursor: CursorRepresentation): Unit = {
    cursors.update(name, cursor)
  }

  override def cursorFor(name: String): Option[CursorRepresentation] = cursors.get(name)

  /**
   * Uses a local slot variable if one is already defined, otherwise declares and assigns a new local slot variable
   */
  override final def getLongAt(offset: Int): IntermediateRepresentation = {
    var local = locals.getLocalForLongSlot(offset)
    if (local == null) {
      local = locals.addLocalForLongSlot(offset)
    }
    load(local)
  }

  /**
   * Uses a local slot variable if one is already defined, otherwise declares and assigns a new local slot variable
   */
  override final def getRefAt(offset: Int): IntermediateRepresentation = {
    var local = locals.getLocalForRefSlot(offset)
    if (local == null) {
      local = locals.addLocalForRefSlot(offset)
    }
    load(local)
  }

  override final def setLongAt(offset: Int, value: IntermediateRepresentation): IntermediateRepresentation = {
    var local = locals.getLocalForLongSlot(offset)
    if (local == null) {
      local = locals.addLocalForLongSlot(offset)
      // We set this slot before reading it, so we can mark it as not needing initialization from input context
      locals.markInitializedLocalForLongSlot(offset)
    }
    locals.markModifiedLocalForLongSlot(offset)
    assign(local, value)
  }

  override final def setRefAt(offset: Int, value: IntermediateRepresentation): IntermediateRepresentation = {
    var local = locals.getLocalForRefSlot(offset)
    if (local == null) {
      local = locals.addLocalForRefSlot(offset)
      // We set this slot before reading it, so we can mark it as not needing initialization from input context
      locals.markInitializedLocalForRefSlot(offset)
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

  final def doCopyFromWithWritableRow(context: IntermediateRepresentation, input: IntermediateRepresentation, nLongs: Int, nRefs: Int): IntermediateRepresentation = {
    invokeSideEffect(context, method[WritableRow, Unit, ReadableRow, Int, Int]("copyFrom"),
      input, constant(nLongs), constant(nRefs)
    )
  }

  // Testing hooks
  protected def didInitializeCachedPropertyFromStore(): Unit = {}
  protected def didInitializeCachedPropertyFromContext(): Unit = {}
  protected def didLoadLocalCachedProperty(): Unit = {}

  /**
   * Extension point of caching of properties needs to be modified.
   */
  abstract class PropertyCacher(property: SlottedCachedProperty, getFromStore: IntermediateRepresentation) {
    protected val offset: Int = property.cachedPropertyOffset
    protected var local: String = locals.getLocalForRefSlot(offset)

    final def initializeFromStore: IntermediateRepresentation = {
      assign(local, getFromStore)
    }

    def assignLocalVariables: IntermediateRepresentation
    def initializeIfLocalDoesNotExist: IntermediateRepresentation
    def initializeIfLocalExists: IntermediateRepresentation

    def getCachedProperty: IntermediateRepresentation = {
      // Mark the corresponding refslot as initialized in this scope, to prevent us from generating an additional load from input context
      locals.markInitializedLocalForRefSlot(offset)

      val prepareOps = if (local == null) {
        local = locals.addCachedProperty(offset)
        initializeIfLocalDoesNotExist
      } else {
        didLoadLocalCachedProperty()
        // Even if the local has been seen before in this method it may not be in a code path that have been hit at runtime
        // in this loop iteration. The cached property variable is also reset to null at the end of each inner loop iteration.
        condition(isNull(load(local)))(
          initializeIfLocalExists
        )
      }

      locals.markModifiedLocalForRefSlot(offset)
      block(assignLocalVariables, prepareOps, cast[Value](load(local)))
    }
  }

  /**
   * The default behavior for cached properties. Override this to modify behavior of [[getCachedPropertyAt]]
   */
  def getPropertyCacherAt(property: SlottedCachedProperty, getFromStore: IntermediateRepresentation): PropertyCacher =
    new PropertyCacher(property, getFromStore) {
      private val maybeCachedPropertyOffset = inputSlotConfiguration.getCachedPropertySlot(property).map(_.offset)

      override def assignLocalVariables: IntermediateRepresentation = noop()

      private def initializeFromContextOrStore: IntermediateRepresentation = {
        block(
          assign(local, getCachedPropertyFromExecutionContext(maybeCachedPropertyOffset.get, INPUT_CURSOR)),
          condition(isNull(load(local)))(initializeFromStore)
        )
      }

      override def initializeIfLocalDoesNotExist: IntermediateRepresentation =
        if (maybeCachedPropertyOffset.isDefined) {
          didInitializeCachedPropertyFromContext()
          local = locals.addCachedProperty(offset)
          initializeFromContextOrStore
        } else {
          didInitializeCachedPropertyFromStore()
          local = locals.addCachedProperty(offset)
          initializeFromStore
        }

      override def initializeIfLocalExists: IntermediateRepresentation =
        if (maybeCachedPropertyOffset.isDefined) {
          initializeFromContextOrStore
        } else {
          initializeFromStore
        }
    }

  /**
   * Get _and_ cache property into a local variable for its predetermined refslot.
   * If this is the first time this cached property is accessed and no local variable exists in this scope,
   * the value will be retrieved from either 1) the input context if it exist there or else 2) from the store.
   *
   * Even if a local variable exists, a runtime check is also generated together with code that retrieves the value (in the same order as above),
   * if the local variable is uninitialized (null).
   * This is needed because the planner does not determine a single definition point for cached properties at compile time,
   * but rather defers to the runtime to do this on first access.
   */
  override final def getCachedPropertyAt(property: SlottedCachedProperty, getFromStore: IntermediateRepresentation): IntermediateRepresentation = {
    getPropertyCacherAt(property, getFromStore).getCachedProperty
  }

  override def setCachedPropertyAt(offset: Int,
                                   value: IntermediateRepresentation): IntermediateRepresentation = {
    var local = locals.getLocalForRefSlot(offset)
    if (local == null) {
      local = locals.addCachedProperty(offset)
      // We set this slot before reading it, so we can mark it as not needing initialization from input context
      locals.markInitializedLocalForRefSlot(offset)
    }
    locals.markModifiedLocalForRefSlot(offset)
    assign(local, value)
  }

  override protected def isLabelSetOnNode(labelToken: IntermediateRepresentation,
                                          offset: Int): IntermediateRepresentation = {
    slots.nameOfSlot(offset, DEFAULT_OFFSET_IS_FOR_LONG_SLOT).flatMap(cursorFor) match {
      case Some(cursor) => cursor.hasLabel(labelToken)
      case None => nodeHasLabel(getNodeIdAt(offset, DEFAULT_OFFSET_IS_FOR_LONG_SLOT, DEFAULT_NULLABLE), labelToken)
    }
  }

  override protected def getNodeProperty(propertyToken: IntermediateRepresentation,
                                         offset: Int,
                                         offsetIsForLongSlot: Boolean,
                                         nullable: Boolean): IntermediateRepresentation = {
    slots.nameOfSlot(offset, offsetIsForLongSlot).flatMap(cursorFor) match {
      case Some(cursor) => cursor.getProperty(propertyToken)
      case None => nodeGetProperty(getNodeIdAt(offset, offsetIsForLongSlot, nullable), propertyToken)
    }
  }

  override protected def hasNodeProperty(propertyToken: IntermediateRepresentation, offset: Int): IntermediateRepresentation = {
    slots.nameOfSlot(offset, DEFAULT_OFFSET_IS_FOR_LONG_SLOT).flatMap(cursorFor) match {
      case Some(cursor) => cursor.hasProperty(propertyToken)
      case None => nodeHasProperty(getNodeIdAt(offset, DEFAULT_OFFSET_IS_FOR_LONG_SLOT, DEFAULT_NULLABLE), propertyToken)
    }
  }

  override protected def getRelationshipProperty(propertyToken: IntermediateRepresentation,
                                                 offset: Int,
                                                 offsetIsForLongSlot: Boolean,
                                                 nullable: Boolean): IntermediateRepresentation = {
    slots.nameOfSlot(offset, offsetIsForLongSlot).flatMap(cursorFor) match {
      case Some(cursor) => cursor.getProperty(propertyToken)
      case None => relationshipGetProperty(getRelationshipIdAt(offset, offsetIsForLongSlot, nullable), propertyToken)
    }
  }

  override protected def hasRelationshipProperty(propertyToken: IntermediateRepresentation, offset: Int): IntermediateRepresentation = {
    slots.nameOfSlot(offset, DEFAULT_OFFSET_IS_FOR_LONG_SLOT).flatMap(cursorFor) match {
      case Some(cursor) => cursor.hasProperty(propertyToken)
      case None => relationshipHasProperty(getRelationshipIdAt(offset, DEFAULT_OFFSET_IS_FOR_LONG_SLOT, DEFAULT_NULLABLE), propertyToken)
    }
  }

  override protected def getProperty(key: String,
                                     container: IntermediateRepresentation): IntermediateRepresentation =
    invokeStatic(
      method[CursorUtils, AnyValue, String, AnyValue, Read, DbAccess, NodeCursor, RelationshipScanCursor, PropertyCursor]("propertyGet"),
      constant(key),
      container,
      loadField(DATA_READ),
      ExpressionCompilation.DB_ACCESS,
      ExpressionCompilation.NODE_CURSOR,
      ExpressionCompilation.RELATIONSHIP_CURSOR,
      ExpressionCompilation.PROPERTY_CURSOR)

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
   * If that range is very small, within a threshold, we use individual slot setter methods (e.g. [[WritableRow.setLongAt]]),
   * otherwise we use the [[WritableRow.copyFrom]] method.
   *
   */
  def writeLocalsToSlots(): IntermediateRepresentation = {
    val writeOps = new ArrayBuffer[IntermediateRepresentation]()
    val writeLongSlotOps = new ArrayBuffer[IntermediateRepresentation]()
    val writeRefSlotOps = new ArrayBuffer[IntermediateRepresentation]()

    val USE_ARRAY_COPY_THRESHOLD = 2

    // Merge all scopes (into a copy, without modifying the original scope stack)
    val mergedLocals = locals.mergeAllScopesCopy()

    // Collect all write operations for modified slots
    var firstModifiedLongSlot = Int.MaxValue
    mergedLocals.foreachLocalForLongSlot { case (offset, local, modified) =>
      if (modified) {
        if (firstModifiedLongSlot > offset) {
          firstModifiedLongSlot = offset
        }
        writeLongSlotOps += setLongInExecutionContext(offset, load(local))
      }
    }
    var firstModifiedRefSlot = Int.MaxValue
    mergedLocals.foreachLocalForRefSlot { case (offset, local, modified) =>
      if (modified) {
        if (firstModifiedRefSlot > offset) {
          firstModifiedRefSlot = offset
        }
        writeRefSlotOps += setRefInExecutionContext(offset, load(local))
      }
    }

    // Now we can compute how many arguments slots we actually need to copy
    val nLongsToCopy = Math.min(nLongSlotsToCopyFromInput, firstModifiedLongSlot)
    val nRefsToCopy = Math.min(nRefSlotsToCopyFromInput, firstModifiedRefSlot)

    // Prepend the write operations for argument slots
    // Decide if we should use WritableRow.copyFrom or just prepend individual set operations for the remaining slots?
    if (nLongsToCopy > USE_ARRAY_COPY_THRESHOLD || nRefsToCopy > USE_ARRAY_COPY_THRESHOLD) {
      // Use the WritableRow.copyFrom method (which may use array copy)
      writeOps += doCopyFromWithWritableRow(OUTPUT_CURSOR, INPUT_CURSOR, nLongsToCopy, nRefsToCopy)
      writeOps ++= writeLongSlotOps
      writeOps ++= writeRefSlotOps
    } else {
      // Add ExecutionContext.setLongAt operations for argument slots?
      if (nLongsToCopy > 0) {
        var i = 0
        while (i < nLongsToCopy) {
          val local = mergedLocals.getLocalForLongSlot(i)
          val getOp =
            if (local == null)
              getLongFromExecutionContext(i, INPUT_CURSOR)
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
          val local = mergedLocals.getLocalForRefSlot(i)
          val getOp =
            if (local == null)
              getRefFromExecutionContext(i, INPUT_CURSOR)
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

  def endInitializationScope(mergeIntoParentScope: Boolean = true): ScopeContinuationState = {
    locals.endInitializationScope(mergeIntoParentScope)
  }

  def endScope(mergeIntoParentScope: Boolean = false): ScopeLocalsState = {
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

abstract class BaseCursorRepresentation extends CursorRepresentation {
  override def reference: IntermediateRepresentation = fail()
  override def hasLabel(labelToken: IntermediateRepresentation): IntermediateRepresentation = fail()
  override def relationshipType: IntermediateRepresentation = fail()
  override def getProperty(propertyToken: IntermediateRepresentation): IntermediateRepresentation = fail()
  override def hasProperty(propertyToken: IntermediateRepresentation): IntermediateRepresentation = fail()
  private def fail() = throw new IllegalStateException(s"illegal usage of cursor: $this")
}

case class NodeCursorRepresentation(target: IntermediateRepresentation) extends BaseCursorRepresentation {
  override def reference: IntermediateRepresentation = {
    invoke(target, method[NodeCursor, Long]("nodeReference"))
  }

  override def hasLabel(labelToken: IntermediateRepresentation): IntermediateRepresentation =
    invoke(target, method[NodeCursor, Boolean, Int]("hasLabel"), labelToken)

  override def getProperty(propertyToken: IntermediateRepresentation): IntermediateRepresentation = {
    block(
      invokeSideEffect(target, method[NodeCursor, Unit, PropertyCursor]("properties"), ExpressionCompilation.PROPERTY_CURSOR),
      ternary(invoke(ExpressionCompilation.PROPERTY_CURSOR, method[PropertyCursor, Boolean, Int]("seekProperty"), propertyToken),
        invoke( ExpressionCompilation.PROPERTY_CURSOR, method[PropertyCursor, Value]("propertyValue")),
        noValue)
    )
  }

  override def hasProperty(propertyToken: IntermediateRepresentation): IntermediateRepresentation = {
    block(
      invokeSideEffect(target, method[NodeCursor, Unit, PropertyCursor]("properties"), ExpressionCompilation.PROPERTY_CURSOR),
      invoke(ExpressionCompilation.PROPERTY_CURSOR, method[PropertyCursor, Boolean, Int]("seekProperty"), propertyToken)
    )
  }
}

case class NodeLabelCursorRepresentation(target: IntermediateRepresentation) extends BaseCursorRepresentation {
  override def reference: IntermediateRepresentation = {
    invoke(target, method[NodeLabelIndexCursor, Long]("nodeReference"))
  }

  override def hasLabel(labelToken: IntermediateRepresentation): IntermediateRepresentation = {
    nodeHasLabel(reference, labelToken)
  }

  override def getProperty(propertyToken: IntermediateRepresentation): IntermediateRepresentation =
    OperatorCodeGenHelperTemplates.nodeGetProperty(reference, propertyToken)

  override def hasProperty(propertyToken: IntermediateRepresentation): IntermediateRepresentation =
    OperatorCodeGenHelperTemplates.nodeHasProperty(reference, propertyToken)
}

case class NodeIndexCursorRepresentation(target: IntermediateRepresentation) extends BaseCursorRepresentation {
  override def reference: IntermediateRepresentation = {
    invoke(target, method[NodeValueIndexCursor, Long]("nodeReference"))
  }

  override def hasLabel(labelToken: IntermediateRepresentation): IntermediateRepresentation = {
    nodeHasLabel(reference, labelToken)
  }

  override def getProperty(propertyToken: IntermediateRepresentation): IntermediateRepresentation =
    OperatorCodeGenHelperTemplates.nodeGetProperty(reference, propertyToken)

  override def hasProperty(propertyToken: IntermediateRepresentation): IntermediateRepresentation =
    OperatorCodeGenHelperTemplates.nodeHasProperty(reference, propertyToken)
}

case class RelationshipCursorRepresentation(target: IntermediateRepresentation) extends BaseCursorRepresentation {

  override def reference: IntermediateRepresentation = {
    invoke(target, method[RelationshipTraversalCursor, Long]("relationshipReference"))
  }

  override def relationshipType: IntermediateRepresentation = {
    invoke(target, method[RelationshipTraversalCursor, Int]("type"))
  }

  override def getProperty(propertyToken: IntermediateRepresentation): IntermediateRepresentation = {
    block(
      invokeSideEffect(target, method[RelationshipTraversalCursor, Unit, PropertyCursor]("properties"),
        ExpressionCompilation.PROPERTY_CURSOR),
      ternary(invoke(ExpressionCompilation.PROPERTY_CURSOR, method[PropertyCursor, Boolean, Int]("seekProperty"), propertyToken),
        invoke( ExpressionCompilation.PROPERTY_CURSOR, method[PropertyCursor, Value]("propertyValue")),
        noValue)
    )
  }

  override def hasProperty(propertyToken: IntermediateRepresentation): IntermediateRepresentation = {
    block(
      invokeSideEffect(target, method[RelationshipTraversalCursor, Unit, PropertyCursor]("properties"),
        ExpressionCompilation.PROPERTY_CURSOR),
      invoke(ExpressionCompilation.PROPERTY_CURSOR, method[PropertyCursor, Boolean, Int]("seekProperty"), propertyToken)
    )
  }
}


