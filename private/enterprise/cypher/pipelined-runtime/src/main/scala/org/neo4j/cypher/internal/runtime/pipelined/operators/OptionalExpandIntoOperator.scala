/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.operators

import org.neo4j.codegen.api.Field
import org.neo4j.codegen.api.IntermediateRepresentation
import org.neo4j.codegen.api.IntermediateRepresentation.and
import org.neo4j.codegen.api.IntermediateRepresentation.assign
import org.neo4j.codegen.api.IntermediateRepresentation.block
import org.neo4j.codegen.api.IntermediateRepresentation.condition
import org.neo4j.codegen.api.IntermediateRepresentation.constant
import org.neo4j.codegen.api.IntermediateRepresentation.declareAndAssign
import org.neo4j.codegen.api.IntermediateRepresentation.equal
import org.neo4j.codegen.api.IntermediateRepresentation.field
import org.neo4j.codegen.api.IntermediateRepresentation.getStatic
import org.neo4j.codegen.api.IntermediateRepresentation.ifElse
import org.neo4j.codegen.api.IntermediateRepresentation.load
import org.neo4j.codegen.api.IntermediateRepresentation.loadField
import org.neo4j.codegen.api.IntermediateRepresentation.loop
import org.neo4j.codegen.api.IntermediateRepresentation.noop
import org.neo4j.codegen.api.IntermediateRepresentation.not
import org.neo4j.codegen.api.IntermediateRepresentation.notEqual
import org.neo4j.codegen.api.IntermediateRepresentation.or
import org.neo4j.codegen.api.IntermediateRepresentation.setField
import org.neo4j.codegen.api.IntermediateRepresentation.trueValue
import org.neo4j.codegen.api.IntermediateRepresentation.typeRefOf
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.physicalplanning.Slot
import org.neo4j.cypher.internal.runtime.NoMemoryTracker
import org.neo4j.cypher.internal.runtime.QueryMemoryTracker
import org.neo4j.cypher.internal.runtime.ReadWriteRow
import org.neo4j.cypher.internal.runtime.compiled.expressions.ExpressionCompiler.nullCheckIfRequired
import org.neo4j.cypher.internal.runtime.compiled.expressions.IntermediateExpression
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.runtime.interpreted.pipes.RelationshipTypes
import org.neo4j.cypher.internal.runtime.pipelined.ArgumentStateMapCreator
import org.neo4j.cypher.internal.runtime.pipelined.OperatorExpressionCompiler
import org.neo4j.cypher.internal.runtime.pipelined.execution.Morsel
import org.neo4j.cypher.internal.runtime.pipelined.execution.MorselFullCursor
import org.neo4j.cypher.internal.runtime.pipelined.execution.MorselWriteCursor
import org.neo4j.cypher.internal.runtime.pipelined.execution.PipelinedQueryState
import org.neo4j.cypher.internal.runtime.pipelined.execution.QueryResources
import org.neo4j.cypher.internal.runtime.pipelined.operators.ExpandAllOperatorTaskTemplate.getNodeIdFromSlot
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.cursorNext
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.profileRow
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentStateMaps
import org.neo4j.cypher.internal.runtime.pipelined.state.MorselParallelizer
import org.neo4j.cypher.internal.runtime.pipelined.state.StateFactory
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.runtime.slotted.helpers.NullChecker.entityIsNull
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.internal.kernel.api.helpers.CachingExpandInto
import org.neo4j.internal.kernel.api.helpers.RelationshipSelectionCursor
import org.neo4j.values.storable.Values

class OptionalExpandIntoOperator(val workIdentity: WorkIdentity,
                                 fromSlot: Slot,
                                 relOffset: Int,
                                 toSlot: Slot,
                                 dir: SemanticDirection,
                                 types: RelationshipTypes,
                                 maybeExpression: Option[Expression]) extends StreamingOperator {

  private var memoryTracker: QueryMemoryTracker = NoMemoryTracker

  override def toString: String = "OptionalExpandInto"

  override protected def nextTasks(state: PipelinedQueryState,
                                   inputMorsel: MorselParallelizer,
                                   parallelism: Int,
                                   resources: QueryResources,
                                   argumentStateMaps: ArgumentStateMaps): IndexedSeq[ContinuableOperatorTaskWithMorsel] = {
    maybeExpression match {
      case None => IndexedSeq(new OptionalExpandIntoTask(inputMorsel.nextCopy, memoryTracker))
      case Some(expression) => IndexedSeq(new FilteringOptionalExpandIntoTask(inputMorsel.nextCopy, expression))
    }
  }

  override def createState(argumentStateCreator: ArgumentStateMapCreator,
                           stateFactory: StateFactory,
                           state: PipelinedQueryState,
                           resources: QueryResources): OperatorState = {
    this.memoryTracker = stateFactory.memoryTracker
    this
  }

  class OptionalExpandIntoTask(inputMorsel: Morsel, memoryTracker: QueryMemoryTracker)
    extends ExpandIntoTask(inputMorsel,
                           workIdentity,
                           fromSlot,
                           relOffset,
                           toSlot,
                           dir,
                           types,
                           memoryTracker) {

    override def toString: String = "OptionalExpandIntoTask"

    protected var hasWritten = false

    protected def setUp(state: PipelinedQueryState,
                        resources: QueryResources): Unit = {

    }

    protected override def initializeInnerLoop(state: PipelinedQueryState, resources: QueryResources, initExecutionContext: ReadWriteRow): Boolean = {
      if (expandInto == null) {
        expandInto = new CachingExpandInto(state.queryContext.transactionalContext.dataRead, kernelDirection(dir), memoryTracker, workIdentity.workId.x)
      }
      val fromNode = getFromNodeFunction.applyAsLong(inputCursor)
      val toNode = getToNodeFunction.applyAsLong(inputCursor)
      hasWritten = false
      if (entityIsNull(fromNode) || entityIsNull(toNode)) {
        relationships = RelationshipSelectionCursor.EMPTY
      } else {
        setUp(state, resources)
        setupCursors(state.queryContext, resources, fromNode, toNode)
      }
      true
    }

    override protected def innerLoop(outputRow: MorselFullCursor, state: PipelinedQueryState): Unit = {

      while (outputRow.onValidRow && relationships.next()) {
        hasWritten = writeRow(outputRow,
          relationships.relationshipReference())
      }
      if (outputRow.onValidRow && !hasWritten) {
        writeNullRow(outputRow)
        hasWritten = true
      }
    }

    private def writeNullRow(outputRow: MorselWriteCursor): Unit = {
      outputRow.copyFrom(inputCursor)
      outputRow.setLongAt(relOffset, -1)
      outputRow.next()
    }

    protected def writeRow(outputRow: MorselFullCursor, relId: Long): Boolean = {
      outputRow.copyFrom(inputCursor)
      outputRow.setLongAt(relOffset, relId)
      outputRow.next()
      true
    }
  }

  class FilteringOptionalExpandIntoTask(inputMorsel: Morsel,
                                        predicate: Expression)
    extends OptionalExpandIntoTask(inputMorsel, memoryTracker) {

    private var expressionState: QueryState = _

    override protected def setUp(state: PipelinedQueryState,
                                 resources: QueryResources): Unit = {
      expressionState = state.queryStateForExpressionEvaluation(resources)
    }

    override protected def writeRow(outputRow: MorselFullCursor,
                                    relId: Long): Boolean = {

      //preemptively write the row, if predicate fails it will be overwritten
      outputRow.copyFrom(inputCursor)
      outputRow.setLongAt(relOffset, relId)
      if (predicate.apply(outputRow, expressionState) eq Values.TRUE) {
        outputRow.next()
        true
      } else {
        false
      }
    }
  }
}

class OptionalExpandIntoOperatorTaskTemplate(inner: OperatorTaskTemplate,
                                             id: Id,
                                             innermost: DelegateOperatorTaskTemplate,
                                             isHead: Boolean,
                                             fromName: String,
                                             fromSlot: Slot,
                                             relName: String,
                                             relOffset: Int,
                                             toSlot: Slot,
                                             dir: SemanticDirection,
                                             types: Array[Int],
                                             missingTypes: Array[String],
                                             generatePredicate: Option[() => IntermediateExpression])
                                            (codeGen: OperatorExpressionCompiler)
  extends ExpandIntoOperatorTaskTemplate(inner,
    id,
    innermost,
    isHead,
    fromSlot,
    relName,
    relOffset,
    toSlot,
    dir,
    types,
    missingTypes)(codeGen) {


  private val hasWritten = field[Boolean](codeGen.namer.nextVariableName())
  private lazy val predicate: Option[IntermediateExpression] = generatePredicate.map(_())

  override def scopeId: String = "optionalExpandAll" + id.x
  override def genMoreFields: Seq[Field] = super.genMoreFields :+ hasWritten
  override def genExpressions: Seq[IntermediateExpression] = super.genExpressions ++ predicate

  /**
   * {{{
   *    val fromNode = [get from slot]
   *    val toNode = [get from slot]
   *    this.hasWritten = false
   *    if (fromNode != -1L && toNode != -1L) ) {
   *      [setUpCursors]
   *    } else {
   *       this.relationships = RelationshipSelectionCursor.EMPTY
   *    }
   *    true
   * }}}
   */
  override protected def genInitializeInnerLoop: IntermediateRepresentation = {
    val fromNode = codeGen.namer.nextVariableName() + "fromNode"
    val toNode = codeGen.namer.nextVariableName() + "toNode"
    block(
      setField(canContinue, constant(false)),
      declareAndAssign(typeRefOf[Long], fromNode, getNodeIdFromSlot(fromSlot, codeGen)),
      declareAndAssign(typeRefOf[Long], toNode, getNodeIdFromSlot(toSlot, codeGen)),
      setField(hasWritten, constant(false)),
      ifElse(and(notEqual(load(fromNode), constant(-1L)), notEqual(load(toNode), constant(-1L)))) {
        block(
          setUpCursors(fromNode,toNode),
          setField(canContinue, cursorNext[RelationshipSelectionCursor](loadField(relationshipsField))),
        )
      }{//else
        setField(relationshipsField,
          getStatic[RelationshipSelectionCursor, RelationshipSelectionCursor]("EMPTY"))
      },
      constant(true))
  }

  /**
   *{{{
   *   val shouldWriteRow = false
   *   while (!this.hasWritten || (hasDemand && this.canContinue) ) {
   *     if (!this.hasWritten && !this.canContinue) {
   *       rel = -1L
   *       node = -1L
   *       shouldWriteRow = true
   *     } else {
   *       [rel = getRel]
   *       [node = getNode]
   *       shouldWriteRow = [evaluate predicate]
   *     }
   *     if (shouldWriteRow) {
   *        <<< inner.genOperate() >>>
   *       this.hasWritten = true
   *      }
   *     this.canContinue = this.canContinue && relationship.next()
   *   }
   *}}}
   */
  override protected def genInnerLoop: IntermediateRepresentation = {
    def doIfPredicateOrElse(onPredicate: => IntermediateRepresentation)(orElse: => IntermediateRepresentation): IntermediateRepresentation =
      if (generatePredicate.isEmpty) orElse else onPredicate
    def doIfPredicate(ir: => IntermediateRepresentation): IntermediateRepresentation = doIfPredicateOrElse(ir)(noop())
    def innerBlock: IntermediateRepresentation = block(
      setField(hasWritten, constant(true)),
      profileRow(id),
      inner.genOperateWithExpressions,
    )

    val shouldWriteRow = codeGen.namer.nextVariableName()
    block(
      doIfPredicate(declareAndAssign(typeRefOf[Boolean], shouldWriteRow, constant(false))),
      loop(or(not(loadField(hasWritten)), and(innermost.predicate, loadField(canContinue))))(
        block(
          ifElse(and(not(loadField(hasWritten)), not(loadField(canContinue))))(
            block(
              writeRow(constant(-1L)),
              doIfPredicate(assign(shouldWriteRow, constant(true)))))
          (/*else*/
            block(
              writeRow(getRelationship),
              doIfPredicate(assign(shouldWriteRow, predicate.map(p => equal(nullCheckIfRequired(p), trueValue)).getOrElse(constant(true))))
            )),
          doIfPredicateOrElse(condition(load(shouldWriteRow))(innerBlock))(innerBlock),
          doIfInnerCantContinue(
            setField(canContinue, and(loadField(canContinue),
              cursorNext[RelationshipSelectionCursor](loadField(relationshipsField))))),
          endInnerLoop
        )))
  }
}









