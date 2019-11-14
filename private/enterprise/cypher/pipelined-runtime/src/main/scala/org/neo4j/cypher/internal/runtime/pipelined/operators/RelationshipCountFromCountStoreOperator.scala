/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.operators

import org.neo4j.codegen.api.IntermediateRepresentation._
import org.neo4j.codegen.api.{Field, IntermediateRepresentation, LocalVariable}
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.profiling.OperatorProfileEvent
import org.neo4j.cypher.internal.runtime.compiled.expressions.IntermediateExpression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.{LazyLabel, RelationshipTypes}
import org.neo4j.cypher.internal.runtime.pipelined.OperatorExpressionCompiler
import org.neo4j.cypher.internal.runtime.pipelined.execution.{MorselExecutionContext, QueryResources, QueryState}
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentStateMaps
import org.neo4j.cypher.internal.runtime.pipelined.state.MorselParallelizer
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.runtime.{DbAccess, ExecutionContext, QueryContext}
import org.neo4j.cypher.internal.v4_0.util.NameId
import org.neo4j.cypher.internal.v4_0.util.attribution.Id
import org.neo4j.values.storable.{LongValue, Values}

class RelationshipCountFromCountStoreOperator(val workIdentity: WorkIdentity,
                                              offset: Int,
                                              startLabel: Option[LazyLabel],
                                              relationshipTypes: RelationshipTypes,
                                              endLabel: Option[LazyLabel],
                                              argumentSize: SlotConfiguration.Size) extends StreamingOperator {

  override protected def nextTasks(queryContext: QueryContext,
                                   state: QueryState,
                                   inputMorsel: MorselParallelizer,
                                   parallelism: Int,
                                   resources: QueryResources,
                                   argumentStateMaps: ArgumentStateMaps): IndexedSeq[ContinuableOperatorTaskWithMorsel] = {
    IndexedSeq(new RelationshipFromCountStoreTask(inputMorsel.nextCopy))
  }

  sealed trait RelId {
    def id: Int
  }

  case object Unknown extends RelId {
    override def id: Int = throw new UnsupportedOperationException
  }

  case object Wildcard extends RelId {
    override def id: Int = NameId.WILDCARD
  }

  case class Known(override val id: Int) extends RelId

  class RelationshipFromCountStoreTask(val inputMorsel: MorselExecutionContext) extends InputLoopTask {

    override def toString: String = "RelationshipFromCountStoreTask"

    private var hasNext = false
    private var executionEvent: OperatorProfileEvent = OperatorProfileEvent.NONE

    override def workIdentity: WorkIdentity = RelationshipCountFromCountStoreOperator.this.workIdentity

    override protected def initializeInnerLoop(context: QueryContext,
                                               state: QueryState,
                                               resources: QueryResources,
                                               initExecutionContext: ExecutionContext): Boolean = {
      hasNext = true
      true
    }

    override protected def innerLoop(outputRow: MorselExecutionContext, context: QueryContext, state: QueryState): Unit = {
      if (hasNext) {
        val startLabelId = getLabelId(startLabel, context)
        val endLabelId = getLabelId(endLabel, context)

        val count = if (startLabelId == Unknown || endLabelId == Unknown)
          0
        else
          countOneDirection(context, startLabelId.id, endLabelId.id)

        outputRow.copyFrom(inputMorsel, argumentSize.nLongs, argumentSize.nReferences)
        outputRow.setRefAt(offset, Values.longValue(count))
        outputRow.moveToNextRow()
        hasNext = false
      }
    }

    private def getLabelId(lazyLabel: Option[LazyLabel], context: QueryContext): RelId = lazyLabel match {
      case Some(label) =>
        val id = label.getId(context)
        if (id == LazyLabel.UNKNOWN)
          Unknown
        else
          Known(id)
      case _ => Wildcard
    }

    private def countOneDirection(context: QueryContext, startLabelId: Int, endLabelId: Int): Long = {
      val relationshipTypeIds: Array[Int] = relationshipTypes.types(context)
      if (relationshipTypeIds == null) {
        executionEvent.dbHit()
        context.relationshipCountByCountStore(startLabelId, NameId.WILDCARD, endLabelId)
      } else {
        var i = 0
        var count = 0L
        while (i < relationshipTypeIds.length) {
          executionEvent.dbHit()
          count += context.relationshipCountByCountStore(startLabelId, relationshipTypeIds(i), endLabelId)
          i += 1
        }
        count
      }
    }

    override def setExecutionEvent(event: OperatorProfileEvent): Unit = {
      this.executionEvent = event
    }

    override protected def closeInnerLoop(resources: QueryResources): Unit = {
      // nothing to do here
    }
  }
}

class RelationshipCountFromCountStoreOperatorTemplate(override val inner: OperatorTaskTemplate,
                                                      id: Id,
                                                      innermost: DelegateOperatorTaskTemplate,
                                                      offset: Int,
                                                      startLabel: Option[Either[String,Int]],
                                                      relationshipTypes: Seq[Either[String, Int]],
                                                      endLabel: Option[Either[String,Int]],
                                                      argumentSize: SlotConfiguration.Size)
                                                     (codeGen: OperatorExpressionCompiler)
  extends InputLoopTaskTemplate(inner, id, innermost, codeGen) {
  import OperatorCodeGenHelperTemplates._

  private val startLabelField = startLabel.collect {
    case Left(labelName) => labelName -> field[Int](codeGen.namer.variableName(labelName), NO_TOKEN)
  }.toMap
  private val relTypesFields = relationshipTypes.collect {
    case Left(typeName) => typeName -> field[Int](codeGen.namer.nextVariableName(), NO_TOKEN)
  }.toMap

  private val endLabelField = endLabel.collect {
    case Left(labelName) => labelName -> field[Int](codeGen.namer.variableName(labelName), NO_TOKEN)
  }.toMap

  override def genMoreFields: Seq[Field] =
    startLabelField.values.toSeq ++ relTypesFields.values ++ endLabelField.values

  override def genLocalVariables: Seq[LocalVariable] = Seq(CURSOR_POOL_V)

  override def genExpressions: Seq[IntermediateExpression] = Seq.empty

  override protected def genInitializeInnerLoop: IntermediateRepresentation = {
    val startLabelOps = block(startLabelField.map {
      case (name, field) => condition(equal(loadField(field), NO_TOKEN))(setField(field, nodeLabelId(name)))
    }.toSeq: _*)

    val endLabelOps = block(endLabelField.map {
      case (name, field) => condition(equal(loadField(field), NO_TOKEN))(setField(field, nodeLabelId(name)))
    }.toSeq: _*)

    val relTypesOps = block(relTypesFields.toSeq.map{
      case (name, field) => condition(equal(loadField(field), NO_TOKEN))(setField(field, relationshipTypeId(name)))
    }: _*)

    block(
      startLabelOps,
      endLabelOps,
      relTypesOps,
      setField(canContinue, constant(true)),
      constant(true))
  }

  /**
    *{{{
    *  setRefAt(offset, Values.longValue( this.start == -1 || this.end == -1 ? 0L : dbAccess.relationshipCountByCountStore(this.start, 42, this.end)  + ....))
    *  << inner.genOperate >>
    *  this.canContinue = false
    *}}}
    */
  override protected def genInnerLoop: IntermediateRepresentation = {

    //If label tokens are known statically use it otherwise look up and store in fields, or use wildcard
    val startOps = startLabel match {
      case Some(Left(name)) => loadField(startLabelField(name))
      case Some(Right(token)) => constant(token)
      case None => constant(-1)
    }
    val endOps = endLabel match {
      case Some(Left(name)) => loadField(endLabelField(name))
      case Some(Right(token)) => constant(token)
      case None => constant(-1)
    }

    //if no relationshipTypes we do a wildcard lookup otherwise we add the contribution from each type
    val countOps =
      if (relationshipTypes.isEmpty)
        invoke(DB_ACCESS, method[DbAccess, Long, Int, Int, Int]("relationshipCountByCountStore"), startOps,
               constant(-1), endOps)
      else {
        relationshipTypes.map {
          case Left(name) =>
            ternary(equal(loadField(relTypesFields(name)), constant(-1)), constant(0L),
                    invoke(DB_ACCESS, method[DbAccess, Long, Int, Int, Int]("relationshipCountByCountStore"), startOps,
                           loadField(relTypesFields(name)), endOps))
          case Right(token) =>
            invoke(DB_ACCESS, method[DbAccess, Long, Int, Int, Int]("relationshipCountByCountStore"), startOps,
                   constant(token), endOps)
        }.reduceLeft(add)
      }

    //if we are accessing start or end labels from fields they might be undefined in which case we must check for -1 and
    //if so the return 0
    val condition: IntermediateRepresentation => IntermediateRepresentation = (startLabel, endLabel) match {
      case (Some(Left(start)), Some(Left(end))) =>
        ternary(or(equal(loadField(startLabelField(start)), constant(-1)), equal(loadField(endLabelField(end)), constant(-1))), constant(0L), _)
      case (Some(Left(start)), _) =>
        ternary(equal(loadField(startLabelField(start)), constant(-1)), constant(0L), _)
      case (_, Some(Left(end))) =>
        ternary(equal(loadField(endLabelField(end)), constant(-1)), constant(0L), _)
      case _ => ops => ops
    }

    val dbHits =
      if (relationshipTypes.size <= 1) invoke(loadField(executionEventField), TRACE_DB_HIT)
      else invoke(loadField(executionEventField), TRACE_DB_HITS, constant(relationshipTypes.size))

    block(
      codeGen.copyFromInput(argumentSize.nLongs, argumentSize.nReferences),
      codeGen.setRefAt(offset, invokeStatic(method[Values, LongValue, Long]("longValue"), condition(countOps))),
      dbHits,
      profileRow(id),
      inner.genOperateWithExpressions,
      setField(canContinue, constant(false))
      )
  }

  override protected def genCloseInnerLoop: IntermediateRepresentation = noop()

  override def genSetExecutionEvent(event: IntermediateRepresentation): IntermediateRepresentation = noop()
}

