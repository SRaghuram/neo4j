/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.operators

import org.neo4j.codegen.api.IntermediateRepresentation._
import org.neo4j.codegen.api.{Field, IntermediateRepresentation}
import org.neo4j.cypher.internal.physicalplanning.Slot
import org.neo4j.cypher.internal.runtime.compiled.expressions.ExpressionCompiler.nullCheckIfRequired
import org.neo4j.cypher.internal.runtime.compiled.expressions.IntermediateExpression
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.RelationshipTypes
import org.neo4j.cypher.internal.runtime.pipelined.OperatorExpressionCompiler
import org.neo4j.cypher.internal.runtime.pipelined.execution.{MorselExecutionContext, QueryResources, QueryState}
import org.neo4j.cypher.internal.runtime.pipelined.operators.ExpandAllOperatorTaskTemplate.getNodeIdFromSlot
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentStateMaps
import org.neo4j.cypher.internal.runtime.pipelined.state.MorselParallelizer
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.runtime.slotted.SlottedQueryState
import org.neo4j.cypher.internal.runtime.slotted.helpers.NullChecker.entityIsNull
import org.neo4j.cypher.internal.runtime.{ExecutionContext, NoMemoryTracker, QueryContext}
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.internal.kernel.api._
import org.neo4j.internal.kernel.api.helpers.{CachingExpandInto, RelationshipSelectionCursor}
import org.neo4j.values.storable.Values

class OptionalExpandIntoOperator(val workIdentity: WorkIdentity,
                                fromSlot: Slot,
                                relOffset: Int,
                                toSlot: Slot,
                                dir: SemanticDirection,
                                types: RelationshipTypes,
                                maybeExpression: Option[Expression]) extends StreamingOperator {



  override def toString: String = "OptionalExpandInto"

  override protected def nextTasks(queryContext: QueryContext,
                                   state: QueryState,
                                   inputMorsel: MorselParallelizer,
                                   parallelism: Int,
                                   resources: QueryResources,
                                   argumentStateMaps: ArgumentStateMaps): IndexedSeq[ContinuableOperatorTaskWithMorsel] = {
    maybeExpression match {
      case None => IndexedSeq(new OptionalExpandIntoTask(inputMorsel.nextCopy))
      case Some(expression) => IndexedSeq(new FilteringOptionalExpandIntoTask(inputMorsel.nextCopy, expression))
    }
  }

  class OptionalExpandIntoTask(inputMorsel: MorselExecutionContext) extends ExpandIntoTask(inputMorsel,
                                                                                           workIdentity,
                                                                                           fromSlot,
                                                                                           relOffset,
                                                                                           toSlot,
                                                                                           dir,
                                                                                           types) {

    override def toString: String = "OptionalExpandIntoTask"

    protected var hasWritten = false

    protected def setUp(context: QueryContext,
                        state: QueryState,
                        resources: QueryResources): Unit = {

    }

    protected override def initializeInnerLoop(context: QueryContext,
                                               state: QueryState,
                                               resources: QueryResources,
                                               initExecutionContext: ExecutionContext): Boolean = {
      if (expandInto == null) {
        expandInto = new CachingExpandInto(context.transactionalContext.dataRead,
                                           kernelDirection(dir))
      }
      val fromNode = getFromNodeFunction.applyAsLong(inputMorsel)
      val toNode = getToNodeFunction.applyAsLong(inputMorsel)
      hasWritten = false
      if (entityIsNull(fromNode) || entityIsNull(toNode)) {
        relationships = RelationshipSelectionCursor.EMPTY
      } else {
        setUp(context, state, resources)
        setupCursors(context, resources, fromNode, toNode)
      }
      true
    }

    override protected def innerLoop(outputRow: MorselExecutionContext,
                                     context: QueryContext,
                                     state: QueryState): Unit = {

      while (outputRow.isValidRow && relationships.next()) {
        hasWritten = writeRow(outputRow,
                              relationships.relationshipReference())
      }
      if (outputRow.isValidRow && !hasWritten) {
        writeNullRow(outputRow)
        hasWritten = true
      }
    }

    private def writeNullRow(outputRow: MorselExecutionContext): Unit = {
      outputRow.copyFrom(inputMorsel)
      outputRow.setLongAt(relOffset, -1)
      outputRow.moveToNextRow()
    }

    protected def writeRow(outputRow: MorselExecutionContext, relId: Long): Boolean = {
      outputRow.copyFrom(inputMorsel)
      outputRow.setLongAt(relOffset, relId)
      outputRow.moveToNextRow()
      true
    }
  }

  class FilteringOptionalExpandIntoTask(inputMorsel: MorselExecutionContext,
                                       predicate: Expression)
    extends OptionalExpandIntoTask(inputMorsel: MorselExecutionContext) {

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
                                    relId: Long): Boolean = {

      //preemptively write the row, if predicate fails it will be overwritten
      outputRow.copyFrom(inputMorsel)
      outputRow.setLongAt(relOffset, relId)
      if (predicate.apply(outputRow, expressionState) eq Values.TRUE) {
        outputRow.moveToNextRow()
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

  import OperatorCodeGenHelperTemplates._

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









