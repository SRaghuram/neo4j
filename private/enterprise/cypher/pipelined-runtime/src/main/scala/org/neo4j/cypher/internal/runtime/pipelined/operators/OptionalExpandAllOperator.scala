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
import org.neo4j.cypher.internal.runtime.pipelined.OperatorExpressionCompiler
import org.neo4j.cypher.internal.runtime.pipelined.execution.CursorPools
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
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.runtime.slotted.helpers.NullChecker.entityIsNull
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.internal.kernel.api.Read
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor
import org.neo4j.kernel.impl.newapi.Cursors
import org.neo4j.kernel.impl.newapi.Cursors.emptyTraversalCursor
import org.neo4j.values.storable.Values

class OptionalExpandAllOperator(val workIdentity: WorkIdentity,
                                fromSlot: Slot,
                                relOffset: Int,
                                toOffset: Int,
                                dir: SemanticDirection,
                                types: RelationshipTypes,
                                maybeExpression: Option[Expression]) extends StreamingOperator {



  override def toString: String = "OptionalExpandAll"

  override protected def nextTasks(state: PipelinedQueryState,
                                   inputMorsel: MorselParallelizer,
                                   parallelism: Int,
                                   resources: QueryResources,
                                   argumentStateMaps: ArgumentStateMaps): IndexedSeq[ContinuableOperatorTaskWithMorsel] = {
    maybeExpression match {
      case None => singletonIndexedSeq(new OptionalExpandAllTask(inputMorsel.nextCopy))
      case Some(expression) => singletonIndexedSeq(new FilteringOptionalExpandAllTask(inputMorsel.nextCopy, expression))
    }
  }

  class OptionalExpandAllTask(inputMorsel: Morsel) extends ExpandAllTask(inputMorsel,
    workIdentity,
    fromSlot,
    relOffset,
    toOffset,
    dir,
    types) {

    override def toString: String = "OptionalExpandAllTask"

    protected var hasWritten = false

    protected override def initializeInnerLoop(state: PipelinedQueryState, resources: QueryResources, initExecutionContext: ReadWriteRow): Boolean = {
      val fromNode = getFromNodeFunction.applyAsLong(inputCursor)
      hasWritten = false
      if (entityIsNull(fromNode)) {
        relationships = emptyTraversalCursor(state.query.transactionalContext.dataRead)
      } else {
        val pools: CursorPools = resources.cursorPools
        nodeCursor = pools.nodeCursorPool.allocateAndTrace()
        relationships = getRelationshipsCursor(state.queryContext, pools, fromNode, dir, types.types(state.queryContext))
      }
      true
    }

    override protected def innerLoop(outputRow: MorselFullCursor, state: PipelinedQueryState): Unit = {
      while (outputRow.onValidRow && relationships.next()) {
        hasWritten = writeRow(outputRow,
          relationships.relationshipReference(),
          relationships.otherNodeReference()
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
      outputRow.setLongAt(toOffset, -1)
      outputRow.next()
    }

    protected def writeRow(outputRow: MorselFullCursor, relId: Long, otherSide: Long): Boolean = {
      outputRow.copyFrom(inputCursor)
      outputRow.setLongAt(relOffset, relId)
      outputRow.setLongAt(toOffset, otherSide)
      outputRow.next()
      true
    }
  }

  class FilteringOptionalExpandAllTask(inputMorsel: Morsel,
                                       predicate: Expression)
    extends OptionalExpandAllTask(inputMorsel: Morsel) {

    private var expressionState: QueryState = _

    override protected def enterOperate(state: PipelinedQueryState,
                                        resources: QueryResources): Unit = {
      expressionState = state.queryStateForExpressionEvaluation(resources)
    }

    override protected def writeRow(outputRow: MorselFullCursor,
                                    relId: Long, otherSide: Long): Boolean = {
      outputRow.copyFrom(inputCursor)
      outputRow.setLongAt(relOffset, relId)
      outputRow.setLongAt(toOffset, otherSide)
      if (predicate.apply(outputRow, expressionState) eq Values.TRUE) {
        outputRow.next()
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

  private val hasWritten = field[Boolean](codeGen.namer.nextVariableName("hasWritten"))
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
   *      this.canContinue = relationship.next
   *    }
   *    true
   * }}}
   */
  override protected def genInitializeInnerLoop: IntermediateRepresentation = {
    val fromNode = codeGen.namer.nextVariableName("fromNode")
    block(
      declareAndAssign(typeRefOf[Long], fromNode, getNodeIdFromSlot(fromSlot, codeGen)),
      setField(hasWritten, constant(false)),
      ifElse(notEqual(load[Long](fromNode), constant(-1L))) {
        block(
          setUpCursors(fromNode, canBeNull = true),
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

    val shouldWriteRow = codeGen.namer.nextVariableName("shouldWrite")
    block(
      doIfPredicate(declareAndAssign(typeRefOf[Boolean], shouldWriteRow, constant(false))),
      loop(or(not(loadField(hasWritten)), and(innermost.predicate, loadField(canContinue))))(
        block(
          ifElse(and(not(loadField(hasWritten)), not(loadField(canContinue))))(
            block(
              writeRow(constant(-1L), constant(-1L)),
              setField(relationshipsField,
                invokeStatic(method[Cursors, RelationshipTraversalCursor, Read]("emptyTraversalCursor"), loadField(DATA_READ))),
              doIfPredicate(assign(shouldWriteRow, constant(true)))))
          (/*else*/
            block(
              writeRow(getRelationship, getOtherNode),
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
