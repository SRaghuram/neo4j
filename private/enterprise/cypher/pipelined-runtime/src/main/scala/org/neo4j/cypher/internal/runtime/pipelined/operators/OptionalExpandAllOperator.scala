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
import org.neo4j.cypher.internal.runtime.ExecutionContext
import org.neo4j.cypher.internal.runtime.NoMemoryTracker
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.compiled.expressions.ExpressionCompiler.nullCheckIfRequired
import org.neo4j.cypher.internal.runtime.compiled.expressions.IntermediateExpression
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.RelationshipTypes
import org.neo4j.cypher.internal.runtime.pipelined.OperatorExpressionCompiler
import org.neo4j.cypher.internal.runtime.pipelined.execution.CursorPools
import org.neo4j.cypher.internal.runtime.pipelined.execution.MorselExecutionContext
import org.neo4j.cypher.internal.runtime.pipelined.execution.QueryResources
import org.neo4j.cypher.internal.runtime.pipelined.execution.QueryState
import org.neo4j.cypher.internal.runtime.pipelined.operators.ExpandAllOperatorTaskTemplate.getNodeIdFromSlot
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.cursorNext
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.profileRow
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentStateMaps
import org.neo4j.cypher.internal.runtime.pipelined.state.MorselParallelizer
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.runtime.slotted.SlottedQueryState
import org.neo4j.cypher.internal.runtime.slotted.helpers.NullChecker.entityIsNull
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.internal.kernel.api.IndexReadSession
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor
import org.neo4j.values.storable.Values

class OptionalExpandAllOperator(val workIdentity: WorkIdentity,
                                fromSlot: Slot,
                                relOffset: Int,
                                toOffset: Int,
                                dir: SemanticDirection,
                                types: RelationshipTypes,
                                maybeExpression: Option[Expression]) extends StreamingOperator {



  override def toString: String = "OptionalExpandAll"

  override protected def nextTasks(queryContext: QueryContext,
                                   state: QueryState,
                                   inputMorsel: MorselParallelizer,
                                   parallelism: Int,
                                   resources: QueryResources,
                                   argumentStateMaps: ArgumentStateMaps): IndexedSeq[ContinuableOperatorTaskWithMorsel] = {
    maybeExpression match {
      case None => IndexedSeq(new OptionalExpandAllTask(inputMorsel.nextCopy))
      case Some(expression) => IndexedSeq(new FilteringOptionalExpandAllTask(inputMorsel.nextCopy, expression))
    }
  }

  class OptionalExpandAllTask(inputMorsel: MorselExecutionContext) extends ExpandAllTask(inputMorsel,
    workIdentity,
    fromSlot,
    relOffset,
    toOffset,
    dir,
    types) {

    override def toString: String = "OptionalExpandAllTask"

    protected var hasWritten = false

    protected def setUp(context: QueryContext,
                        state: QueryState,
                        resources: QueryResources): Unit = {

    }

    protected override def initializeInnerLoop(context: QueryContext,
                                               state: QueryState,
                                               resources: QueryResources,
                                               initExecutionContext: ExecutionContext): Boolean = {
      val fromNode = getFromNodeFunction.applyAsLong(inputMorsel)
      hasWritten = false
      if (entityIsNull(fromNode)) {
        relationships = RelationshipTraversalCursor.EMPTY
      } else {
        setUp(context, state, resources)
        val pools: CursorPools = resources.cursorPools
        nodeCursor = pools.nodeCursorPool.allocateAndTrace()
        relationships = getRelationshipsCursor(context, pools, fromNode, dir, types.types(context))
      }
      true
    }

    override protected def innerLoop(outputRow: MorselExecutionContext,
                                     context: QueryContext,
                                     state: QueryState): Unit = {

      while (outputRow.isValidRow && relationships.next()) {
        hasWritten = writeRow(outputRow,
          relationships.relationshipReference(),
          relationships.otherNodeReference())
      }
      if (outputRow.isValidRow && !hasWritten) {
        writeNullRow(outputRow)
        hasWritten = true
      }
    }

    private def writeNullRow(outputRow: MorselExecutionContext): Unit = {
      outputRow.copyFrom(inputMorsel)
      outputRow.setLongAt(relOffset, -1)
      outputRow.setLongAt(toOffset, -1)
      outputRow.moveToNextRow()
    }

    protected def writeRow(outputRow: MorselExecutionContext, relId: Long, otherSide: Long): Boolean = {
      outputRow.copyFrom(inputMorsel)
      outputRow.setLongAt(relOffset, relId)
      outputRow.setLongAt(toOffset, otherSide)
      outputRow.moveToNextRow()
      true
    }
  }

  class FilteringOptionalExpandAllTask(inputMorsel: MorselExecutionContext,
                                       predicate: Expression)
    extends OptionalExpandAllTask(inputMorsel: MorselExecutionContext) {

    private var expressionState: SlottedQueryState = _

    override protected def setUp(context: QueryContext,
                                 state: QueryState,
                                 resources: QueryResources): Unit = {
      expressionState = new SlottedQueryState(context,
        resources = null,
        params = state.params,
        resources.expressionCursors,
        Array.empty[IndexReadSession],
        resources.expressionVariables(state.nExpressionSlots),
        state.subscriber,
        NoMemoryTracker)
    }

    override protected def writeRow(outputRow: MorselExecutionContext,
                                    relId: Long, otherSide: Long): Boolean = {

      //preemptively write the row, if predicate fails it will be overwritten
      outputRow.copyFrom(inputMorsel)
      outputRow.setLongAt(relOffset, relId)
      outputRow.setLongAt(toOffset, otherSide)
      if (predicate.apply(outputRow, expressionState) eq Values.TRUE) {
        outputRow.moveToNextRow()
        true
      } else {
        false
      }
    }
  }
}

class OptionalExpandAllOperatorTaskTemplate(inner: OperatorTaskTemplate,
                                            id: Id,
                                            innermost: DelegateOperatorTaskTemplate,
                                            isHead: Boolean,
                                            fromName: String,
                                            fromSlot: Slot,
                                            relName: String,
                                            relOffset: Int,
                                            toOffset: Int,
                                            dir: SemanticDirection,
                                            types: Array[Int],
                                            missingTypes: Array[String],
                                            generatePredicate: Option[() => IntermediateExpression])
                                           (codeGen: OperatorExpressionCompiler)
  extends ExpandAllOperatorTaskTemplate(inner,
    id,
    innermost,
    isHead,
    fromName,
    fromSlot,
    relName,
    relOffset,
    toOffset,
    dir,
    types,
    missingTypes)(codeGen) {


  private val hasWritten = field[Boolean](codeGen.namer.nextVariableName())
  private lazy val predicate: Option[IntermediateExpression] = generatePredicate.map(_())

  override final def scopeId: String = "optionalExpandAll" + id.x
  override def genMoreFields: Seq[Field] = super.genMoreFields :+ hasWritten
  override def genExpressions: Seq[IntermediateExpression] = super.genExpressions ++ predicate

  /**
   * {{{
   *    val fromNode = [get from slot]
   *    this.hasWritten = false
   *    if (fromNode != -1L) {
   *      [setUpCursors]
   *    }
   *    true
   * }}}
   */
  override protected def genInitializeInnerLoop: IntermediateRepresentation = {
    val fromNode = codeGen.namer.nextVariableName("fromNode")
    block(
      declareAndAssign(typeRefOf[Long], fromNode, getNodeIdFromSlot(fromSlot, codeGen)),
      setField(hasWritten, constant(false)),
      ifElse(notEqual(load(fromNode), constant(-1L))) {
        block(
          setUpCursors(fromNode),
          setField(canContinue, cursorNext[RelationshipTraversalCursor](loadField(relationshipsField))),
        )
      }{//else
        setField(relationshipsField,
          getStatic[RelationshipTraversalCursor, RelationshipTraversalCursor]("EMPTY"))
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
              writeRow(constant(-1L), constant(-1L)),
              doIfPredicate(assign(shouldWriteRow, constant(true)))))
          (/*else*/
            block(
              writeRow(getRelationship, getOtherNode),
              doIfPredicate(assign(shouldWriteRow, predicate.map(p => equal(nullCheckIfRequired(p), trueValue)).getOrElse(constant(true))))
            )),
          doIfPredicateOrElse(condition(load(shouldWriteRow))(innerBlock))(innerBlock),
          doIfInnerCantContinue(
            setField(canContinue, and(loadField(canContinue),
              cursorNext[RelationshipTraversalCursor](loadField(relationshipsField))))),
          endInnerLoop
        )))
  }
}
