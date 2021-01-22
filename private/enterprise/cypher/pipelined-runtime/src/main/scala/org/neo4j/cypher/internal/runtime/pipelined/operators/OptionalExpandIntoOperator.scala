/*
 * Copyright (c) "Neo4j"
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
import org.neo4j.codegen.api.IntermediateRepresentation.ifElse
import org.neo4j.codegen.api.IntermediateRepresentation.invokeStatic
import org.neo4j.codegen.api.IntermediateRepresentation.load
import org.neo4j.codegen.api.IntermediateRepresentation.loadField
import org.neo4j.codegen.api.IntermediateRepresentation.loop
import org.neo4j.codegen.api.IntermediateRepresentation.method
import org.neo4j.codegen.api.IntermediateRepresentation.noop
import org.neo4j.codegen.api.IntermediateRepresentation.not
import org.neo4j.codegen.api.IntermediateRepresentation.notEqual
import org.neo4j.codegen.api.IntermediateRepresentation.or
import org.neo4j.codegen.api.IntermediateRepresentation.setField
import org.neo4j.codegen.api.IntermediateRepresentation.trueValue
import org.neo4j.codegen.api.IntermediateRepresentation.typeRefOf
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.physicalplanning.Slot
import org.neo4j.cypher.internal.runtime.ReadWriteRow
import org.neo4j.cypher.internal.runtime.compiled.expressions.ExpressionCompilation.nullCheckIfRequired
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
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.DATA_READ
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.conditionallyProfileRow
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.cursorNext
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.getNodeIdFromSlot
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentStateMaps
import org.neo4j.cypher.internal.runtime.pipelined.state.Collections.singletonIndexedSeq
import org.neo4j.cypher.internal.runtime.pipelined.state.MorselParallelizer
import org.neo4j.cypher.internal.runtime.pipelined.state.StateFactory
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.runtime.slotted.helpers.NullChecker.entityIsNull
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.internal.kernel.api.Read
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor
import org.neo4j.internal.kernel.api.helpers.CachingExpandInto
import org.neo4j.kernel.impl.newapi.Cursors
import org.neo4j.kernel.impl.newapi.Cursors.emptyTraversalCursor
import org.neo4j.memory.MemoryTracker
import org.neo4j.values.storable.Values

class OptionalExpandIntoOperator(val workIdentity: WorkIdentity,
                                 fromSlot: Slot,
                                 relOffset: Int,
                                 toSlot: Slot,
                                 dir: SemanticDirection,
                                 types: RelationshipTypes,
                                 maybeExpression: Option[Expression]) (val id: Id = Id.INVALID_ID) extends StreamingOperator {

  override def toString: String = "OptionalExpandInto"

  override def createState(argumentStateCreator: ArgumentStateMapCreator,
                           stateFactory: StateFactory,
                           state: PipelinedQueryState,
                           resources: QueryResources): OperatorState =  {
    new MemoryTrackingOperatorState(this, id.x, stateFactory)
  }

  override protected def nextTasks(state: PipelinedQueryState,
                                   inputMorsel: MorselParallelizer,
                                   parallelism: Int,
                                   resources: QueryResources,
                                   argumentStateMaps: ArgumentStateMaps): IndexedSeq[ContinuableOperatorTaskWithMorsel] = {
    maybeExpression match {
      case None => singletonIndexedSeq(new OptionalExpandIntoTask(inputMorsel.nextCopy, resources.memoryTracker, id))
      case Some(expression) => singletonIndexedSeq(new FilteringOptionalExpandIntoTask(inputMorsel.nextCopy, expression, resources.memoryTracker, id))
    }
  }

  class OptionalExpandIntoTask(inputMorsel: Morsel, memoryTracker: MemoryTracker, id: Id)
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

    protected override def initializeInnerLoop(state: PipelinedQueryState, resources: QueryResources, initExecutionContext: ReadWriteRow): Boolean = {
      if (expandInto == null) {
        expandInto = new CachingExpandInto(state.queryContext.transactionalContext.dataRead, kernelDirection(dir), memoryTracker)
      }
      val fromNode = getFromNodeFunction.applyAsLong(inputCursor)
      val toNode = getToNodeFunction.applyAsLong(inputCursor)
      hasWritten = false
      if (entityIsNull(fromNode) || entityIsNull(toNode)) {
        relationships = emptyTraversalCursor(state.query.transactionalContext.dataRead)
      } else {
        setupCursors(state.queryContext, resources, fromNode, toNode)
      }
      true
    }

    override protected def innerLoop(outputRow: MorselFullCursor, state: PipelinedQueryState): Unit = {

      while (outputRow.onValidRow && relationships.next()) {
        hasWritten = writeRow(outputRow,
          relationships.relationshipReference()
        ) || hasWritten
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
                                        predicate: Expression,
                                        memoryTracker: MemoryTracker,
                                        id: Id)
    extends OptionalExpandIntoTask(inputMorsel, memoryTracker, id) {

    private var expressionState: QueryState = _

    override protected def enterOperate(state: PipelinedQueryState,
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
   *       this.relationships = Cursors.emptyTraversalCursor(read)
   *    }
   *    true
   * }}}
   */
  override protected def genInitializeInnerLoop: IntermediateRepresentation = {
    val fromNode = codeGen.namer.nextVariableName() + "fromNode"
    val toNode = codeGen.namer.nextVariableName() + "toNode"
    block(
      setField(canContinue, constant(false)),
      declareAndAssign(fromNode, getNodeIdFromSlot(fromSlot, codeGen)),
      declareAndAssign(toNode, getNodeIdFromSlot(toSlot, codeGen)),
      setField(hasWritten, constant(false)),
      ifElse(and(notEqual(load[Long](fromNode), constant(-1L)), notEqual(load[Long](toNode), constant(-1L)))) {
        block(
          setUpCursors(fromNode,toNode),
          setField(canContinue, cursorNext[RelationshipTraversalCursor](loadField(relationshipsField)))
        )
      }{//else
        setField(relationshipsField,
          invokeStatic(method[Cursors, RelationshipTraversalCursor, Read]("emptyTraversalCursor"), loadField(DATA_READ)))
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
   *       relationship = Cursors.emptyTraversalCursor()
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
      inner.genOperateWithExpressions,
      conditionallyProfileRow(innerCannotContinue, id, doProfile)
    )

    val shouldWriteRow = codeGen.namer.nextVariableName()
    block(
      doIfPredicate(declareAndAssign(typeRefOf[Boolean], shouldWriteRow, constant(false))),
      loop(or(not(loadField(hasWritten)), and(innermost.predicate, loadField(canContinue))))(
        block(
          ifElse(and(not(loadField(hasWritten)), not(loadField(canContinue))))(
            block(
              writeRow(constant(-1L)),
              setField(relationshipsField,
                invokeStatic(method[Cursors, RelationshipTraversalCursor, Read]("emptyTraversalCursor"), loadField(DATA_READ))),
              doIfPredicate(assign(shouldWriteRow, constant(true)))))
          (/*else*/
            block(
              writeRow(getRelationship),
              doIfPredicate(assign(shouldWriteRow, predicate.map(p => equal(nullCheckIfRequired(p), trueValue)).getOrElse(constant(true))))
            )),
          doIfPredicateOrElse(condition(load[Boolean](shouldWriteRow))(innerBlock))(innerBlock),
          doIfPredicateOrElse(
            //NOTE: it is important here that if the predicate failed, we always need to advance the cursor
            //      if not execution might hang if innerCanContinue is true.
            ifElse(not(load[Boolean](shouldWriteRow))){
              setField(canContinue, cursorNext[RelationshipTraversalCursor](loadField(relationshipsField)))
            } {
              doIfInnerCantContinue(
                innermost.setUnlessPastLimit(canContinue,
                  and(loadField(canContinue), cursorNext[RelationshipTraversalCursor](loadField(relationshipsField)))))
            }
          )(
            doIfInnerCantContinue(
              innermost.setUnlessPastLimit(canContinue,
                and(loadField(canContinue), cursorNext[RelationshipTraversalCursor](loadField(relationshipsField)))))
          ),
          endInnerLoop
        )))
  }
}









