/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.physicalplanning

import org.neo4j.cypher.internal.compiler.planner.CantCompileQueryException
import org.neo4j.cypher.internal.physicalplanning.PhysicalPlanningAttributes.SlotConfigurations
import org.neo4j.cypher.internal.physicalplanning.PipelineId.NO_PIPELINE
import org.neo4j.cypher.internal.logical.plans
import org.neo4j.cypher.internal.logical.plans._
import org.neo4j.cypher.internal.v4_0.util.attribution.Id

import scala.collection.mutable.ArrayBuffer

// TODO: refactor this file into a proper builder pattern, where the result of
//  building is immutable definitions, and mutability is visible inside the builder only

case class BufferId(x: Int) extends AnyVal
case class PipelineId(x: Int) extends AnyVal

object PipelineId {
  val NO_PIPELINE: PipelineId = PipelineId(-1)
}

/**
  * A buffer between two pipelines.
  */
class BufferDefinition(val id: BufferId,
                       val producingPipelineId: PipelineId) {
  // We need multiple reducers because a buffer might need to
  // reference count for multiple downstream reduce operators,
  // at potentially different argument depths
  val reducers = new ArrayBuffer[Id]
}

/**
  * A buffer between the LHS and RHS of an Apply.
  */
class ApplyBufferDefinition(id: BufferId,
                            producingPipelineId: PipelineId,
                            applyPlanId: Id,
                            val argumentSlotOffset: Int) extends BufferDefinition(id, producingPipelineId) {
  val reducersForThisApply = new ArrayBuffer[Id]
}

/**
  * This buffer groups data by argument row and sits between a pre-reduce and a reduce operator.
  */
class ArgumentStateBufferDefinition(id: BufferId,
                                    producingPipelineId: PipelineId,
                                    val reducingPlanId: Id) extends BufferDefinition(id, producingPipelineId)

class Pipeline(val id: PipelineId,
               val headPlan: LogicalPlan) {
  var inputBuffer: BufferDefinition = _
  var outputBuffer: BufferDefinition = _
  val middlePlans = new ArrayBuffer[LogicalPlan]
  var produceResults: Option[ProduceResult] = None
  var serial: Boolean = false
}

class StateDefinition(val physicalPlan: PhysicalPlan) {
  val buffers = new ArrayBuffer[BufferDefinition]

  def newBuffer(producingPipelineId: PipelineId): BufferDefinition = {
    val x = buffers.size
    val buffer = new BufferDefinition(BufferId(x), producingPipelineId)
    buffers += buffer
    buffer
  }

  def newArgumentBuffer(producingPipelineId: PipelineId,
                        applyPlanId: Id,
                        argumentSlotOffset: Int): ApplyBufferDefinition = {
    val x = buffers.size
    val buffer = new ApplyBufferDefinition(BufferId(x), producingPipelineId, applyPlanId, argumentSlotOffset)
    buffers += buffer
    buffer
  }

  def newArgumentStateBuffer(producingPipelineId: PipelineId,
                             reducingPlanId: Id): ArgumentStateBufferDefinition = {
    val x = buffers.size
    val buffer = new ArgumentStateBufferDefinition(BufferId(x), producingPipelineId, reducingPlanId)
    buffers += buffer
    buffer
  }

  var initBuffer: ApplyBufferDefinition = _
}

object PipelineBuilder {
  val NO_PRODUCING_PIPELINE: Int = -1
}

class PipelineBuilder(breakingPolicy: PipelineBreakingPolicy,
                      stateDefinition: StateDefinition,
                      slotConfigurations: SlotConfigurations)
  extends TreeBuilder2[Pipeline, ApplyBufferDefinition] {

  val pipelines = new ArrayBuffer[Pipeline]

  private def newPipeline(plan: LogicalPlan) = {
    val pipeline = new Pipeline(PipelineId(pipelines.size), plan)
    pipelines += pipeline
    pipeline
  }

  private def outputToBuffer(pipeline: Pipeline): BufferDefinition = {
    val output = stateDefinition.newBuffer(pipeline.id)
    pipeline.outputBuffer = output
    output
  }

  private def outputToArgumentBuffer(pipeline: Pipeline, applyPlanId: Id, argumentSlotOffset: Int): ApplyBufferDefinition = {
    val output = stateDefinition.newArgumentBuffer(pipeline.id, applyPlanId, argumentSlotOffset)
    pipeline.outputBuffer = output
    output
  }

  private def outputToArgumentStateBuffer(pipeline: Pipeline, reducingPlanId: Id): ArgumentStateBufferDefinition = {
    val output = stateDefinition.newArgumentStateBuffer(pipeline.id, reducingPlanId)
    pipeline.outputBuffer = output
    output
  }

  override protected def initialArgument(leftLeaf: LogicalPlan): ApplyBufferDefinition = {
    val initialArgumentSlotOffset = slotConfigurations(leftLeaf.id).getArgumentLongOffsetFor(Id.INVALID_ID)
    stateDefinition.initBuffer = stateDefinition.newArgumentBuffer(NO_PIPELINE, Id.INVALID_ID, initialArgumentSlotOffset)
    stateDefinition.initBuffer
  }

  override protected def onLeaf(plan: LogicalPlan,
                                argument: ApplyBufferDefinition): Pipeline = {

    if (breakingPolicy.breakOn(plan)) {
      val pipeline = newPipeline(plan)
      pipeline.inputBuffer = argument
      pipeline
    } else
      throw new UnsupportedOperationException("not implemented")
  }

  override protected def onOneChildPlan(plan: LogicalPlan,
                                        source: Pipeline,
                                        argument: ApplyBufferDefinition): Pipeline = {
    plan match {
      case produceResult: ProduceResult =>
        if (breakingPolicy.breakOn(plan)) {
          val pipeline = newPipeline(plan)
          pipeline.inputBuffer = outputToBuffer(source)
          pipeline.serial = true
          pipeline
        } else {
          source.produceResults = Some(produceResult)
          source.serial = true
          source
        }

      case _: Sort =>
        if (breakingPolicy.breakOn(plan)) {
          source.middlePlans += plan

          val pipeline = newPipeline(plan)
          val argumentStateBuffer = outputToArgumentStateBuffer(source, plan.id)
          pipeline.inputBuffer = argumentStateBuffer
          markReducerInUpstreamBuffers(argumentStateBuffer, argument, plan.id)
          pipeline
        } else throw new UnsupportedOperationException("not implemented")

      case _: Expand |
           _: PruningVarExpand |
           _: VarExpand |
           _: OptionalExpand |
           _: FindShortestPaths |
           _: UnwindCollection |
           _: VarExpand =>
        if (breakingPolicy.breakOn(plan)) {
          val pipeline = newPipeline(plan)
          pipeline.inputBuffer = outputToBuffer(source)
          pipeline
        } else {
          source.middlePlans += plan
          source
        }

      case _ =>
        source.middlePlans += plan
        source
    }
  }

  override protected def onTwoChildPlanComingFromLeft(plan: LogicalPlan,
                                                      lhs: Pipeline,
                                                      argument: ApplyBufferDefinition): ApplyBufferDefinition =
  {
    plan match {
      case _: plans.Apply =>
        val argumentSlotOffset = slotConfigurations(plan.id).getArgumentLongOffsetFor(plan.id)
        outputToArgumentBuffer(lhs, plan.id, argumentSlotOffset)

      case _ =>
        argument
    }
  }

  override protected def onTwoChildPlanComingFromRight(plan: LogicalPlan, lhs: Pipeline, rhs: Pipeline): Pipeline = {

    plan match {
      case _: plans.Apply =>
        rhs

      case p =>
        throw new CantCompileQueryException(s"$plan not supported in morsel runtime")
    }
  }

  // HELPERS

  /*
    * Plan:
    *             ProduceResults
    *               |
    *             Apply
    *             /  \
    *           LHS  Sort
    *                |
    *                Expand
    *                |
    *                ...
    *                |
    *                Scan
    *
    * Pipelines:
    *  -LHS->  ApplyBuffer  -Scan->  Buffer -...->  Buffer  -Presort->  ArgumentStateMapBuffer  -Sort,ProduceResults->
    *             ^                  |--------------------|
    *             |                                  ^
    *   reducersForThisApply += reducePlanId        reducers += reducePlanId
    *
    * Mark `reducePlanId` as a reducer in all buffers between `argumentStateBuffer` and `applyBuffer`. This has
    * to done so that reference counting of inflight work will work correctly, so that `argumentStateBuffer` knows
    * when each argument is complete.
    */
  private def markReducerInUpstreamBuffers(argumentStateBuffer: ArgumentStateBufferDefinition,
                                           applyBuffer: ApplyBufferDefinition,
                                           reducePlanId: Id): Unit = {
    var b = pipelines(argumentStateBuffer.producingPipelineId.x).inputBuffer
    while (b != applyBuffer) {
      b.reducers += reducePlanId
      b = pipelines(b.producingPipelineId.x).inputBuffer
    }
    applyBuffer.reducersForThisApply += reducePlanId
  }
}
