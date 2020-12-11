/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.physicalplanning

import org.eclipse.collections.api.map.primitive.MutableObjectIntMap
import org.eclipse.collections.impl.map.mutable.primitive.ObjectIntHashMap
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.IsNull
import org.neo4j.cypher.internal.expressions.Not
import org.neo4j.cypher.internal.expressions.Ors
import org.neo4j.cypher.internal.logical.plans
import org.neo4j.cypher.internal.logical.plans.Aggregation
import org.neo4j.cypher.internal.logical.plans.Anti
import org.neo4j.cypher.internal.logical.plans.CartesianProduct
import org.neo4j.cypher.internal.logical.plans.Distinct
import org.neo4j.cypher.internal.logical.plans.LeftOuterHashJoin
import org.neo4j.cypher.internal.logical.plans.Limit
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.LogicalPlans
import org.neo4j.cypher.internal.logical.plans.NodeHashJoin
import org.neo4j.cypher.internal.logical.plans.Optional
import org.neo4j.cypher.internal.logical.plans.OrderedAggregation
import org.neo4j.cypher.internal.logical.plans.OrderedDistinct
import org.neo4j.cypher.internal.logical.plans.PartialSort
import org.neo4j.cypher.internal.logical.plans.PartialTop
import org.neo4j.cypher.internal.logical.plans.PreserveOrder
import org.neo4j.cypher.internal.logical.plans.ProduceResult
import org.neo4j.cypher.internal.logical.plans.RightOuterHashJoin
import org.neo4j.cypher.internal.logical.plans.Skip
import org.neo4j.cypher.internal.logical.plans.Sort
import org.neo4j.cypher.internal.logical.plans.Top
import org.neo4j.cypher.internal.logical.plans.TriadicBuild
import org.neo4j.cypher.internal.logical.plans.TriadicFilter
import org.neo4j.cypher.internal.logical.plans.ValueHashJoin
import org.neo4j.cypher.internal.physicalplanning.PhysicalPlanningAttributes.ApplyPlans
import org.neo4j.cypher.internal.physicalplanning.PhysicalPlanningAttributes.ArgumentSizes
import org.neo4j.cypher.internal.physicalplanning.PhysicalPlanningAttributes.SlotConfigurations
import org.neo4j.cypher.internal.physicalplanning.PipelineDefinition.FusedHead
import org.neo4j.cypher.internal.physicalplanning.PipelineDefinition.HeadPlan
import org.neo4j.cypher.internal.physicalplanning.PipelineDefinition.InterpretedHead
import org.neo4j.cypher.internal.physicalplanning.PipelineId.NO_PIPELINE
import org.neo4j.cypher.internal.physicalplanning.PipelineTreeBuilder.ApplyBufferDefiner
import org.neo4j.cypher.internal.physicalplanning.PipelineTreeBuilder.ArgumentStateBufferDefiner
import org.neo4j.cypher.internal.physicalplanning.PipelineTreeBuilder.ArgumentStreamMorselBufferDefiner
import org.neo4j.cypher.internal.physicalplanning.PipelineTreeBuilder.AttachBufferDefiner
import org.neo4j.cypher.internal.physicalplanning.PipelineTreeBuilder.BufferDefiner
import org.neo4j.cypher.internal.physicalplanning.PipelineTreeBuilder.ConditionalBufferDefiner
import org.neo4j.cypher.internal.physicalplanning.PipelineTreeBuilder.DelegateBufferDefiner
import org.neo4j.cypher.internal.physicalplanning.PipelineTreeBuilder.ExecutionStateDefiner
import org.neo4j.cypher.internal.physicalplanning.PipelineTreeBuilder.LHSAccumulatingRHSStreamingBufferDefiner
import org.neo4j.cypher.internal.physicalplanning.PipelineTreeBuilder.MorselBufferDefiner
import org.neo4j.cypher.internal.physicalplanning.PipelineTreeBuilder.PipelineDefiner
import org.neo4j.cypher.internal.physicalplanning.PipelineTreeBuilder.UnionBufferDefiner
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration.SlotWithKeyAndAliases
import org.neo4j.cypher.internal.physicalplanning.ast.IsPrimitiveNull
import org.neo4j.cypher.internal.physicalplanning.ast.ReferenceFromSlot
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.LeveragedOrders
import org.neo4j.cypher.internal.runtime.interpreted.commands
import org.neo4j.cypher.internal.runtime.interpreted.commands.convert.ExpressionConverters
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.attribution.Id

import scala.annotation.tailrec
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

sealed trait HeadPlanBuilder {
  def id: Id = plan.id
  protected def plan: LogicalPlan
}

case class InterpretedHeadBuilder(plan: LogicalPlan) extends HeadPlanBuilder
case class FusedHeadBuilder(operatorFuser: OperatorFuser) extends HeadPlanBuilder {
  override protected def plan: LogicalPlan = operatorFuser.fusedPlans.head
}

/**
 * Collection of mutable builder classes that are modified by [[PipelineTreeBuilder]] and finally
 * converted to an [[ExecutionGraphDefinition]] by the [[ExecutionGraphDefiner]].
 */
object PipelineTreeBuilder {
  /**
   * Builder for [[PipelineDefinition]]
   */
  class PipelineDefiner(val id: PipelineId,
                        val operatorFuserFactory: OperatorFuserFactory,
                        val inputBuffer: BufferDefiner,
                        val headLogicalPlan: LogicalPlan) {
    var lhs: PipelineId = NO_PIPELINE
    var rhs: PipelineId = NO_PIPELINE
    private var outputDefinition: OutputDefinition = NoOutput
    /**
     * The list of fused plans contains all fusable plans starting from the headPlan and
     * continuing with as many fusable consecutive middlePlans as possible.
     * If a plan is in `fusedPlans`. it will not be in `middlePlans` and vice versa.
     */
    val headPlan: HeadPlanBuilder = {
      val operatorFuser = operatorFuserFactory.newOperatorFuser(headLogicalPlan.id)
      if (operatorFuser.fuseIn(headLogicalPlan))
        FusedHeadBuilder(operatorFuser)
      else
        InterpretedHeadBuilder(headLogicalPlan)
    }

    val middlePlans = new ArrayBuffer[LogicalPlan]
    var serial: Boolean = false
    var workCanceller: Option[ArgumentStateMapId] = None

    def fuse(plan: LogicalPlan): Boolean = {
      headPlan match {
        case FusedHeadBuilder(operatorFuser) if middlePlans.isEmpty && outputDefinition == NoOutput =>
          operatorFuser.fuseIn(plan)
        case _ => false
      }
    }

    /**
     * This can only be used on plans that are determined to be non-breaking
     */
    def fuseOrInterpretMiddlePlan(middlePlan: LogicalPlan): Unit = {
      if (!fuse(middlePlan)) {
        middlePlans += middlePlan
      }
    }

    def fuse(output: OutputDefinition): Boolean = {
      headPlan match {
        case FusedHeadBuilder(operatorFuser) if middlePlans.isEmpty =>
          operatorFuser.fuseIn(output)
        case _ => false
      }
    }

    def fuseOrInterpretOutput(output: OutputDefinition): Unit = {
      if (!fuse(output)) {
        outputDefinition = output
      }
    }

    private def headPlanResult: HeadPlan =
      headPlan match {
        case f: FusedHeadBuilder =>
          if (f.operatorFuser.fusedPlans.size > 1)
            FusedHead(f.operatorFuser.fusedPlans)
          else
            InterpretedHead(f.operatorFuser.fusedPlans.head)
        case InterpretedHeadBuilder(plan) => InterpretedHead(plan)
      }

    def pipelineTemplates: IndexedSeq[_] =
      headPlan match {
        case FusedHeadBuilder(operatorFuser) => operatorFuser.templates
        case InterpretedHeadBuilder(_) => IndexedSeq.empty
      }

    def result: PipelineDefinition = PipelineDefinition(id, lhs, rhs, headPlanResult, inputBuffer.result, outputDefinition, middlePlans, serial, workCanceller)
  }

  abstract class BufferDefiner(val id: BufferId, val memoryTrackingOperatorId: Id, val bufferConfiguration: SlotConfiguration) {
    val downstreamReducers: mutable.ArrayBuilder[ArgumentStateMapId] = mutable.ArrayBuilder.make[ArgumentStateMapId]()

    // Map from the ArgumentStates of downstream states on the RHS to the initial count they should be initialized with
    private val downstreamWorkCancellersMap: MutableObjectIntMap[ArgumentStateMapId] = new ObjectIntHashMap[ArgumentStateMapId]()
    def registerDownstreamWorkCanceller(downstreamWorkCancellerASMID: ArgumentStateMapId): Unit = {
      downstreamWorkCancellersMap.addToValue(downstreamWorkCancellerASMID, 1)
    }

    def result: BufferDefinition = {
      val downstreamWorkCancellers = mutable.ArrayBuilder.make[Initialization[ArgumentStateMapId]]()
      downstreamWorkCancellers.sizeHint(downstreamWorkCancellersMap.size())
      downstreamWorkCancellersMap.forEachKeyValue( (argStateId, initialCount) => downstreamWorkCancellers += Initialization(argStateId, initialCount) )

      BufferDefinition(id,
                       memoryTrackingOperatorId,
                       new ReadOnlyArray(downstreamReducers.result()),
                       new ReadOnlyArray(downstreamWorkCancellers.result()),
                       variant
                     )(bufferConfiguration)
    }

    protected def variant: BufferVariant
  }

  class MorselBufferDefiner(id: BufferId,
                            memoryTrackingOperatorId: Id,
                            val producingPipelineId: PipelineId,
                            bufferConfiguration: SlotConfiguration) extends BufferDefiner(id, memoryTrackingOperatorId, bufferConfiguration) {
    override protected def variant: BufferVariant = RegularBufferVariant
  }

  class UnionBufferDefiner(id: BufferId,
                           memoryTrackingOperatorId: Id,
                           val lhsProducingPipelineId: PipelineId,
                           val rhsProducingPipelineId: PipelineId,
                           bufferConfiguration: SlotConfiguration) extends BufferDefiner(id, memoryTrackingOperatorId, bufferConfiguration) {
    override protected def variant: BufferVariant = RegularBufferVariant
  }

  class ArgumentStreamMorselBufferDefiner(id: BufferId,
                                          memoryTrackingOperatorId: Id,
                                          val producingPipelineId: PipelineId,
                                          val argumentStateMapId: ArgumentStateMapId,
                                          val argumentSlotOffset: Int,
                                          bufferConfiguration: SlotConfiguration,
                                          val bufferVariantType: ArgumentStreamBufferVariantType) extends BufferDefiner(id, memoryTrackingOperatorId, bufferConfiguration) {
    override protected def variant: BufferVariant = ArgumentStreamBufferVariant(argumentStateMapId, bufferVariantType)
  }

  class DelegateBufferDefiner(id: BufferId,
                              operatorId: Id,
                              val applyBuffer: ApplyBufferDefiner,
                              bufferConfiguration: SlotConfiguration) extends BufferDefiner(id, operatorId, bufferConfiguration) {
    override protected def variant: BufferVariant = RegularBufferVariant
  }

  class ConditionalBufferDefiner(val onFalseBuffer: BufferDefiner,
                                 predicate: commands.expressions.Expression,
                                 id: BufferId,
                                 memoryTrackingOperatorId: Id,
                                 bufferSlotConfiguration: SlotConfiguration) extends  BufferDefiner(id, memoryTrackingOperatorId, bufferSlotConfiguration) {
    var onTrueBuffer: BufferDefiner = _

    override protected def variant: BufferVariant = ConditionalBufferVariant(onTrueBuffer.result, onFalseBuffer.result, predicate)
  }

  class ApplyBufferDefiner(id: BufferId,
                           memoryTrackingOperatorId: Id,
                           producingPipelineId: PipelineId,
                           val argumentSlotOffset: Int,
                           bufferSlotConfiguration: SlotConfiguration
                          ) extends MorselBufferDefiner(id, memoryTrackingOperatorId, producingPipelineId, bufferSlotConfiguration) {
    val downstreamStatesOnRHS: mutable.ArrayBuilder[ArgumentStateMapId] = mutable.ArrayBuilder.make[ArgumentStateMapId]()

    // Map from the ArgumentStates of reducers on the RHS to the initial count they should be initialized with
    private val reducersOnRHSMap: MutableObjectIntMap[ArgumentStateDefinition] = new ObjectIntHashMap[ArgumentStateDefinition]()
    private var _conditionalApplySink: ConditionalBufferDefiner = _
    def registerReducerOnRHS(reducer: ArgumentStateDefinition): Unit = {
      reducersOnRHSMap.addToValue(reducer, 1)
    }

    val delegates: mutable.ArrayBuilder[BufferId] = mutable.ArrayBuilder.make[BufferId]()

    override protected def variant: BufferVariant = {
      val reducersOnRHS = mutable.ArrayBuilder.make[Initialization[ArgumentStateMapId]]()
      reducersOnRHS.sizeHint(reducersOnRHSMap.size())
      reducersOnRHSMap.forEachKeyValue( (argStateBuild, initialCount) => reducersOnRHS += Initialization(argStateBuild.id, initialCount) )

      // Argument state maps IDs are given in upstream -> downstream order.
      // By ordering the reducers by ID descending, we get them in downstream -> upstream order.
      val reducersOnRHSReversed = reducersOnRHS.result().sortBy(_.receiver.x * -1)

      ApplyBufferVariant(argumentSlotOffset,
                         new ReadOnlyArray(reducersOnRHSReversed),
                         new ReadOnlyArray(downstreamStatesOnRHS.result()),
                         new ReadOnlyArray(delegates.result()))
    }

    def setConditionalApplyBuffer(conditionalApplyBuffer: ConditionalBufferDefiner): Unit = {
      this._conditionalApplySink = conditionalApplyBuffer
    }
    def conditionalApplySink: Option[ConditionalBufferDefiner] = Option(_conditionalApplySink)
  }

  class AttachBufferDefiner(id: BufferId,
                            memoryTrackingOperatorId: Id,
                            inputSlotConfiguration: SlotConfiguration,
                            val outputSlotConfiguration: SlotConfiguration,
                            val argumentSlotOffset: Int,
                            val argumentSize: SlotConfiguration.Size
                           ) extends BufferDefiner(id, memoryTrackingOperatorId, inputSlotConfiguration) {

    var applyBuffer: ApplyBufferDefiner = _

    override protected def variant: BufferVariant = AttachBufferVariant(applyBuffer.result, outputSlotConfiguration, argumentSlotOffset, argumentSize)
  }

  class ArgumentStateBufferDefiner(id: BufferId,
                                   memoryTrackingOperatorId: Id,
                                   producingPipelineId: PipelineId,
                                   val argumentStateMapId: ArgumentStateMapId,
                                   bufferSlotConfiguration: SlotConfiguration) extends MorselBufferDefiner(id, memoryTrackingOperatorId, producingPipelineId, bufferSlotConfiguration) {
    override protected def variant: BufferVariant = ArgumentStateBufferVariant(argumentStateMapId)
  }

  class LHSAccumulatingBufferDefiner(id: BufferId,
                                     memoryTrackingOperatorId: Id,
                                     val lhsArgumentStateMapId: ArgumentStateMapId,
                                     rhsArgumentStateMapId: ArgumentStateMapId,
                                     val producingPipelineId: PipelineId,
                                     bufferSlotConfiguration: SlotConfiguration,
                                     joinVariant: JoinVariant) extends BufferDefiner(id, memoryTrackingOperatorId, bufferSlotConfiguration) {
    override protected def variant: BufferVariant = LHSAccumulatingBufferVariant(lhsArgumentStateMapId, rhsArgumentStateMapId, joinVariant)
  }

  class RHSStreamingBufferDefiner(id: BufferId,
                                  memoryTrackingOperatorId: Id,
                                  lhsArgumentStateMapId: ArgumentStateMapId,
                                  val rhsArgumentStateMapId: ArgumentStateMapId,
                                  val producingPipelineId: PipelineId,
                                  bufferSlotConfiguration: SlotConfiguration,
                                  joinVariant: JoinVariant) extends BufferDefiner(id, memoryTrackingOperatorId, bufferSlotConfiguration) {
    override protected def variant: BufferVariant = RHSStreamingBufferVariant(lhsArgumentStateMapId, rhsArgumentStateMapId, joinVariant)
  }

  class LHSAccumulatingRHSStreamingBufferDefiner(id: BufferId,
                                                 memoryTrackingOperatorId: Id,
                                                 val lhsSink: LHSAccumulatingBufferDefiner,
                                                 val rhsSink: RHSStreamingBufferDefiner,
                                                 val lhsArgumentStateMapId: ArgumentStateMapId,
                                                 val rhsArgumentStateMapId: ArgumentStateMapId,
                                                 bufferSlotConfiguration: SlotConfiguration,
                                                 joinVariant: JoinVariant) extends BufferDefiner(id, memoryTrackingOperatorId, bufferSlotConfiguration) {
    override protected def variant: BufferVariant =
      LHSAccumulatingRHSStreamingBufferVariant(lhsSink.result, rhsSink.result, lhsArgumentStateMapId, rhsArgumentStateMapId, joinVariant)
  }

  /**
   * Builder for [[ExecutionGraphDefinition]]
   */
  class ExecutionStateDefiner(val physicalPlan: PhysicalPlan) {
    val buffers = new ArrayBuffer[BufferDefiner]
    val argumentStateMaps = new ArrayBuffer[ArgumentStateDefinition]
    var initBuffer: ApplyBufferDefiner = _

    def newArgumentStateMap(planId: Id, argumentSlotOffset: Int): ArgumentStateDefinition = {
      val x = argumentStateMaps.size
      val asm = ArgumentStateDefinition(ArgumentStateMapId(x), planId, argumentSlotOffset)
      argumentStateMaps += asm
      asm
    }

    def newBuffer(producingPipelineId: PipelineId,
                  memoryTrackingOperatorId: Id,
                  bufferSlotConfiguration: SlotConfiguration): MorselBufferDefiner = {
      val x = buffers.size
      val buffer = new MorselBufferDefiner(BufferId(x), memoryTrackingOperatorId, producingPipelineId, bufferSlotConfiguration)
      buffers += buffer
      buffer
    }

    def newUnionBuffer(lhsProducingPipelineId: PipelineId,
                       rhsProducingPipelineId: PipelineId,
                       memoryTrackingOperatorId: Id,
                       bufferSlotConfiguration: SlotConfiguration): UnionBufferDefiner = {
      val x = buffers.size
      val buffer = new UnionBufferDefiner(BufferId(x), memoryTrackingOperatorId, lhsProducingPipelineId, rhsProducingPipelineId, bufferSlotConfiguration)
      buffers += buffer
      buffer
    }

    def newArgumentStreamBuffer(producingPipelineId: PipelineId,
                                memoryTrackingOperatorId: Id,
                                argumentStateMapId: ArgumentStateMapId,
                                argumentSlotOffset: Int,
                                bufferSlotConfiguration: SlotConfiguration,
                                bufferVariantType: ArgumentStreamBufferVariantType): ArgumentStreamMorselBufferDefiner = {
      val x = buffers.size
      val buffer = new ArgumentStreamMorselBufferDefiner(BufferId(x),
                                                         memoryTrackingOperatorId,
                                                         producingPipelineId,
                                                         argumentStateMapId,
                                                         argumentSlotOffset,
                                                         bufferSlotConfiguration,
                                                         bufferVariantType)
      buffers += buffer
      buffer
    }

    def newDelegateBuffer(applyBufferDefinition: ApplyBufferDefiner,
                          memoryTrackingOperatorId: Id,
                          bufferSlotConfiguration: SlotConfiguration): DelegateBufferDefiner = {
      val x = buffers.size
      val buffer = new DelegateBufferDefiner(BufferId(x), memoryTrackingOperatorId, applyBufferDefinition, bufferSlotConfiguration)
      buffers += buffer
      buffer
    }

    def newApplyBuffer(producingPipelineId: PipelineId,
                       memoryTrackingOperatorId: Id,
                       argumentSlotOffset: Int,
                       bufferSlotConfiguration: SlotConfiguration): ApplyBufferDefiner = {
      val x = buffers.size
      val buffer = new ApplyBufferDefiner(BufferId(x), memoryTrackingOperatorId, producingPipelineId, argumentSlotOffset, bufferSlotConfiguration)
      buffers += buffer
      buffer
    }

    def newConditionalSink(onFalseBuffer: MorselBufferDefiner,
                           expression: commands.expressions.Expression,
                           memoryTrackingOperatorId: Id,
                           bufferSlotConfiguration: SlotConfiguration): ConditionalBufferDefiner = {
      val x = buffers.size
      val buffer = new ConditionalBufferDefiner(onFalseBuffer, expression, BufferId(x), memoryTrackingOperatorId, bufferSlotConfiguration)
      buffers += buffer
      buffer
    }

    def newAttachBuffer(memoryTrackingOperatorId: Id,
                        inputSlotConfiguration: SlotConfiguration,
                        postAttachSlotConfiguration: SlotConfiguration,
                        outerArgumentSlotOffset: Int,
                        outerArgumentSize: SlotConfiguration.Size): AttachBufferDefiner = {

      val x = buffers.size
      val buffer = new AttachBufferDefiner(BufferId(x),
                                           memoryTrackingOperatorId,
                                           inputSlotConfiguration,
                                           postAttachSlotConfiguration,
                                           outerArgumentSlotOffset,
                                           outerArgumentSize)
      buffers += buffer
      buffer
    }

    def newArgumentStateBuffer(producingPipelineId: PipelineId,
                               memoryTrackingOperatorId: Id,
                               argumentStateMapId: ArgumentStateMapId,
                               bufferSlotConfiguration: SlotConfiguration): ArgumentStateBufferDefiner = {
      val x = buffers.size
      val buffer = new ArgumentStateBufferDefiner(BufferId(x), memoryTrackingOperatorId, producingPipelineId, argumentStateMapId, bufferSlotConfiguration)
      buffers += buffer
      buffer
    }

    def newLhsAccumulatingRhsStreamingBuffer(lhsProducingPipelineId: PipelineId,
                                             rhsProducingPipelineId: PipelineId,
                                             memoryTrackingOperatorId: Id,
                                             lhsArgumentStateMapId: ArgumentStateMapId,
                                             rhsArgumentStateMapId: ArgumentStateMapId,
                                             bufferSlotConfiguration: SlotConfiguration,
                                             joinVariant: JoinVariant): LHSAccumulatingRHSStreamingBufferDefiner = {
      val lhsAccId = BufferId(buffers.size)
      val lhsAcc = new LHSAccumulatingBufferDefiner(lhsAccId,
                                                    memoryTrackingOperatorId,
                                                    lhsArgumentStateMapId,
                                                    rhsArgumentStateMapId,
                                                    lhsProducingPipelineId,
                                                    bufferSlotConfiguration = null, // left null because buffer is never used as source
                                                    joinVariant)
      buffers += lhsAcc

      val rhsAccId = BufferId(buffers.size)
      val rhsAcc = new RHSStreamingBufferDefiner(rhsAccId,
                                                 memoryTrackingOperatorId,
                                                 lhsArgumentStateMapId,
                                                 rhsArgumentStateMapId,
                                                 rhsProducingPipelineId,
                                                 bufferSlotConfiguration = null, // left null because buffer is never used as source
                                                 joinVariant)
      buffers += rhsAcc

      val x = buffers.size
      val buffer = new LHSAccumulatingRHSStreamingBufferDefiner(BufferId(x),
                                                                memoryTrackingOperatorId,
                                                                lhsAcc,
                                                                rhsAcc,
                                                                lhsArgumentStateMapId,
                                                                rhsArgumentStateMapId,
                                                                bufferSlotConfiguration,
                                                                joinVariant)
      buffers += buffer
      buffer
    }
  }
}

/**
 * Fills an [[ExecutionStateDefiner]] and and array of [[PipelineDefiner]]s.
 * Final conversion to [[ExecutionGraphDefinition]] is done by [[ExecutionGraphDefiner]].
 */
class PipelineTreeBuilder(breakingPolicy: PipelineBreakingPolicy,
                          operatorFuserFactory: OperatorFuserFactory,
                          stateDefiner: ExecutionStateDefiner,
                          slotConfigurations: SlotConfigurations,
                          argumentSizes: ArgumentSizes,
                          applyPlans: ApplyPlans,
                          expressionConverters: ExpressionConverters,
                          leveragedOrders: LeveragedOrders)
  extends TreeBuilder[PipelineDefiner, ApplyBufferDefiner] {

  private[physicalplanning] val pipelines = new ArrayBuffer[PipelineDefiner]
  private[physicalplanning] val applyRhsPlans = new mutable.HashMap[Int, Int]()

  private def newPipeline(plan: LogicalPlan, inputBuffer: BufferDefiner): PipelineDefiner = {
    val pipeline = new PipelineDefiner(PipelineId(pipelines.size), operatorFuserFactory, inputBuffer, plan)
    pipelines += pipeline
    pipeline
  }

  private def outputToBuffer(pipeline: PipelineDefiner, nextPipelineHeadPlan: LogicalPlan): MorselBufferDefiner = {
    val output = stateDefiner.newBuffer(pipeline.id, nextPipelineHeadPlan.id, slotConfigurations(pipeline.headPlan.id))
    pipeline.fuseOrInterpretOutput(MorselBufferOutput(output.id, nextPipelineHeadPlan.id))
    output
  }

  private def outputToUnionBuffer(lhs: PipelineDefiner, rhs: PipelineDefiner, nextPipelineHeadPlan: Id): UnionBufferDefiner = {
    // When fusing, buffer configuration is used to determine which slots are not modified by the pipeline and should be copied from input.
    val bufferSlotConfiguration = SlotConfiguration.empty // For Union we want it to be empty because:
                                                          // a) Union operator logic already copies everything we need.
                                                          // b) Buffer stores morsels from both LHS and RHS with different slot configs, so
                                                          //    there isn't a single non-empty config that would match them both for all cases.
    val output = stateDefiner.newUnionBuffer(lhs.id, rhs.id, nextPipelineHeadPlan, bufferSlotConfiguration)
    lhs.fuseOrInterpretOutput(MorselBufferOutput(output.id, nextPipelineHeadPlan))
    rhs.fuseOrInterpretOutput(MorselBufferOutput(output.id, nextPipelineHeadPlan))
    output
  }

  private def outputToApplyBuffer(pipeline: PipelineDefiner, argumentSlotOffset: Int, nextPipelineHeadPlan: LogicalPlan): ApplyBufferDefiner = {
    val output = stateDefiner.newApplyBuffer(pipeline.id, nextPipelineHeadPlan.id, argumentSlotOffset, slotConfigurations(pipeline.headPlan.id))
    pipeline.fuseOrInterpretOutput(MorselBufferOutput(output.id, nextPipelineHeadPlan.id))
    output
  }

  private def outputToConditionalSink(predicate: Expression,
                                      pipeline: PipelineDefiner,
                                      argumentSlotOffset: Int,
                                      currentPlan: LogicalPlan): ApplyBufferDefiner = {
    /*
     * The buffers will be set up as follows:
     *   - when the predicate is `true` we will write directly to output
     *   - when the predicate is `false` we will write to an `ApplyBuffer` that is later connected to output
     *
     *                         .--onTrue------------------------------------.
     *                        /                                              \
     *   -LHS->  ConditionalSink                                           output --TOP-->
     *                        \                                              /
     *                         `-onFalse-> ApplyBuffer ->  ...    --RHS-----`
     *
     */
    val applyBuffer: ApplyBufferDefiner = stateDefiner.newApplyBuffer(pipeline.id,
                                                                      currentPlan.id,
                                                                      argumentSlotOffset,
                                                                      slotConfigurations(pipeline.headPlan.id))
    val conditionalSink = stateDefiner.newConditionalSink(applyBuffer,
                                                          expressionConverters.toCommandExpression(currentPlan.id, predicate),
                                                          currentPlan.id,
                                                          slotConfigurations(pipeline.headPlan.id))
    pipeline.fuseOrInterpretOutput(MorselBufferOutput(conditionalSink.id, currentPlan.id))
    applyBuffer.setConditionalApplyBuffer(conditionalSink)
    applyBuffer
  }

  private def outputToAttachApplyBuffer(pipeline: PipelineDefiner,
                                        attachingPlanId: Id,
                                        argumentSlotOffset: Int,
                                        postAttachSlotConfiguration: SlotConfiguration,
                                        outerArgumentSlotOffset: Int,
                                        outerArgumentSize: SlotConfiguration.Size): AttachBufferDefiner = {

    val output = stateDefiner.newAttachBuffer(
      attachingPlanId,
      slotConfigurations(pipeline.headPlan.id),
      postAttachSlotConfiguration,
      outerArgumentSlotOffset,
      outerArgumentSize)

    output.applyBuffer = stateDefiner.newApplyBuffer(pipeline.id, attachingPlanId, argumentSlotOffset, postAttachSlotConfiguration)
    pipeline.fuseOrInterpretOutput(MorselBufferOutput(output.id, attachingPlanId))
    output
  }

  private def outputToArgumentStateBuffer(pipeline: PipelineDefiner, plan: LogicalPlan, applyBuffer: ApplyBufferDefiner, argumentSlotOffset: Int): ArgumentStateBufferDefiner = {
    val asm = stateDefiner.newArgumentStateMap(plan.id, argumentSlotOffset)
    val output = stateDefiner.newArgumentStateBuffer(pipeline.id, plan.id, asm.id, slotConfigurations(pipeline.headPlan.id))
    pipeline.fuseOrInterpretOutput(ReduceOutput(output.id, asm.id, plan))
    markReducerInUpstreamBuffers(pipeline.inputBuffer, applyBuffer, asm)
    output
  }

  private def outputToArgumentStreamBuffer(pipeline: PipelineDefiner,
                                           plan: LogicalPlan,
                                           applyBuffer: ApplyBufferDefiner,
                                           argumentSlotOffset: Int): ArgumentStreamMorselBufferDefiner = {
    val asm = stateDefiner.newArgumentStateMap(plan.id, argumentSlotOffset)
    val output = stateDefiner.newArgumentStreamBuffer(pipeline.id, plan.id, asm.id, argumentSlotOffset, slotConfigurations(pipeline.headPlan.id), ArgumentStreamType)
    pipeline.fuseOrInterpretOutput(MorselArgumentStateBufferOutput(output.id, argumentSlotOffset, plan.id))
    markReducerInUpstreamBuffers(pipeline.inputBuffer, applyBuffer, asm)
    output
  }

  private def outputToAntiPipelinedBuffer(pipeline: PipelineDefiner,
                                          plan: LogicalPlan,
                                          applyBuffer: ApplyBufferDefiner,
                                          argumentSlotOffset: Int): ArgumentStreamMorselBufferDefiner = {
    val asm = stateDefiner.newArgumentStateMap(plan.id, argumentSlotOffset)
    val output = stateDefiner.newArgumentStreamBuffer(pipeline.id, plan.id, asm.id, argumentSlotOffset, slotConfigurations(pipeline.headPlan.id), AntiType)
    pipeline.fuseOrInterpretOutput(MorselArgumentStateBufferOutput(output.id, argumentSlotOffset, plan.id))
    markReducerInUpstreamBuffers(pipeline.inputBuffer, applyBuffer, asm)
    output
  }

  private def outputToLhsAccumulatingRhsStreamingBuffer(maybeLhs: Option[PipelineDefiner],
                                                        rhs: PipelineDefiner,
                                                        planId: Id,
                                                        applyBuffer: ApplyBufferDefiner,
                                                        argumentSlotOffset: Int,
                                                        joinVariant: JoinVariant): LHSAccumulatingRHSStreamingBufferDefiner = {
    val lhsAsm = stateDefiner.newArgumentStateMap(planId, argumentSlotOffset)
    val rhsAsm = stateDefiner.newArgumentStateMap(planId, argumentSlotOffset)
    val lhsId = maybeLhs.map(_.id).getOrElse(PipelineId.NO_PIPELINE)
    val output = stateDefiner.newLhsAccumulatingRhsStreamingBuffer(lhsId, rhs.id, planId, lhsAsm.id, rhsAsm.id, slotConfigurations(rhs.headPlan.id), joinVariant)
    maybeLhs match {
      case Some(lhs) =>
        lhs.fuseOrInterpretOutput(MorselArgumentStateBufferOutput(output.lhsSink.id, argumentSlotOffset, planId))
        markReducerInUpstreamBuffers(lhs.inputBuffer, applyBuffer, lhsAsm)
      case None =>
        // The LHS argument state map has to be initiated by the apply even if there is no pipeline
        // connecting them. Otherwise the LhsAccumulatingRhsStreamingBuffer can never output anything.
        applyBuffer.registerReducerOnRHS(lhsAsm)
    }
    rhs.fuseOrInterpretOutput(MorselArgumentStateBufferOutput(output.rhsSink.id, argumentSlotOffset, planId))
    markReducerInUpstreamBuffers(rhs.inputBuffer, applyBuffer, rhsAsm)
    output
  }

  override protected def validatePlan(plan: LogicalPlan): Unit = breakingPolicy.breakOn(plan, applyPlans(plan.id))

  override protected def initialArgument(leftLeaf: LogicalPlan): ApplyBufferDefiner = {
    val initialArgumentSlotOffset = slotConfigurations(leftLeaf.id).getArgumentLongOffsetFor(Id.INVALID_ID)
    stateDefiner.initBuffer = stateDefiner.newApplyBuffer(NO_PIPELINE, leftLeaf.id, initialArgumentSlotOffset, SlotAllocation.INITIAL_SLOT_CONFIGURATION)
    stateDefiner.initBuffer
  }

  override protected def onLeaf(plan: LogicalPlan,
                                argument: ApplyBufferDefiner): PipelineDefiner = {
    if (breakingPolicy.breakOn(plan, applyPlans(plan.id))) {
      val delegate = stateDefiner.newDelegateBuffer(argument, plan.id, argument.bufferConfiguration)
      argument.delegates += delegate.id
      val pipeline = newPipeline(plan, delegate)
      pipeline.lhs = argument.producingPipelineId
      pipeline
    } else {
      throw new UnsupportedOperationException(s"Not breaking on ${plan.getClass.getSimpleName} is not supported.")
    }
  }

  override protected def onOneChildPlan(plan: LogicalPlan,
                                        source: PipelineDefiner,
                                        argument: ApplyBufferDefiner): PipelineDefiner = {

    plan match {
      case produceResult: ProduceResult =>
        if (breakingPolicy.breakOn(plan, applyPlans(plan.id))) {
          val buffer = outputToBuffer(source, plan)
          val pipeline = newPipeline(plan, buffer)
          pipeline.lhs = source.id
          pipeline.serial = true
          pipeline
        } else {
          source.fuseOrInterpretOutput(ProduceResultOutput(produceResult))
          source.serial = true
          source
        }

      case _: Sort |
           _: Aggregation |
           _: Top =>
        if (breakingPolicy.breakOn(plan, applyPlans(plan.id))) {
          val argumentStateBuffer = outputToArgumentStateBuffer(source, plan, argument, argument.argumentSlotOffset)
          val pipeline = newPipeline(plan, argumentStateBuffer)
          pipeline.lhs = source.id
          pipeline
        } else {
          throw new UnsupportedOperationException(s"Not breaking on ${plan.getClass.getSimpleName} is not supported.")
        }

      case _: PartialTop =>
        if (!breakingPolicy.breakOn(plan, applyPlans(plan.id))) {
          throw new UnsupportedOperationException(s"Not breaking on ${plan.getClass.getSimpleName} is not supported.")
        }
        val buffer = outputToArgumentStreamBuffer(source, plan, argument, argument.argumentSlotOffset)
        val pipeline = newPipeline(plan, buffer)
        pipeline.lhs = source.id

        val workCancellerAsmId = stateDefiner.newArgumentStateMap(plan.id, argument.argumentSlotOffset).id
        //if we are a toplevel PartialTop we want to propagate the limit upstream
        val onPipeline: PipelineDefiner => Unit = createPropagateTopLevelLimitCallback(argument, workCancellerAsmId)
        //we want to propagate starting with source and then all upstream pipelines
        onPipeline(pipeline)
        markCancellerInUpstreamBuffers(pipeline.inputBuffer, argument, workCancellerAsmId, onPipeline)
        pipeline

      case triadicBuild: TriadicBuild =>
        if (!breakingPolicy.breakOn(plan, applyPlans(plan.id)))
          throw new UnsupportedOperationException(s"Not breaking on ${plan.getClass.getSimpleName} is not supported.")

        val triadicStateAsm = stateDefiner.newArgumentStateMap(triadicBuild.triadicSelectionId.value, argument.argumentSlotOffset)
        argument.downstreamStatesOnRHS += triadicStateAsm.id

        val buffer = outputToArgumentStreamBuffer(source, plan, argument, argument.argumentSlotOffset)
        val pipeline = newPipeline(plan, buffer)
        pipeline.lhs = source.id
        pipeline

      case _: TriadicFilter =>
        if (!breakingPolicy.breakOn(plan, applyPlans(plan.id)))
          throw new UnsupportedOperationException(s"Not breaking on ${plan.getClass.getSimpleName} is not supported.")

        val buffer = outputToArgumentStreamBuffer(source, plan, argument, argument.argumentSlotOffset)
        val pipeline = newPipeline(plan, buffer)
        pipeline.lhs = source.id
        pipeline

      case _: Optional |
           _: OrderedAggregation  |
           _: PartialSort |
           _: PreserveOrder =>
        if (breakingPolicy.breakOn(plan, applyPlans(plan.id))) {
          val buffer = outputToArgumentStreamBuffer(source, plan, argument, argument.argumentSlotOffset)
          val pipeline = newPipeline(plan, buffer)
          pipeline.lhs = source.id
          pipeline
        } else {
          throw new UnsupportedOperationException(s"Not breaking on ${plan.getClass.getSimpleName} is not supported.")
        }

      case _: Anti =>
        if (breakingPolicy.breakOn(plan, applyPlans(plan.id))) {
          val antiPipelinedBuffer = outputToAntiPipelinedBuffer(source, plan, argument, argument.argumentSlotOffset)
          val pipeline = newPipeline(plan, antiPipelinedBuffer)
          pipeline.lhs = source.id
          pipeline
        } else {
          throw new UnsupportedOperationException(s"Not breaking on ${plan.getClass.getSimpleName} is not supported.")
        }

      case _: Limit =>
        if (breakingPolicy.breakOn(plan, applyPlans(plan.id))) {
          throw new UnsupportedOperationException(s"Breaking on ${plan.getClass.getSimpleName} is not supported.")
        } else {
          val asm = stateDefiner.newArgumentStateMap(plan.id, argument.argumentSlotOffset)
          //if we are a toplevel limit we want to propagate the limit upstream
          val onPipeline: PipelineDefiner => Unit = createPropagateTopLevelLimitCallback(argument, asm.id)
          //we want to propagate starting with source and then all upstream pipelines
          onPipeline(source)
          markCancellerInUpstreamBuffers(source.inputBuffer, argument, asm.id, onPipeline)
          source.fuseOrInterpretMiddlePlan(plan)
          source
        }

      case _: Distinct | _: OrderedDistinct | _: Skip =>
        if (breakingPolicy.breakOn(plan, applyPlans(plan.id))) {
          throw new UnsupportedOperationException(s"Breaking on ${plan.getClass.getSimpleName} is not supported.")
        } else {
          val asm = stateDefiner.newArgumentStateMap(plan.id, argument.argumentSlotOffset)
          argument.downstreamStatesOnRHS += asm.id
          source.fuseOrInterpretMiddlePlan(plan)
          source
        }

      case _ =>
        val isBreaking = breakingPolicy.breakOn(plan, applyPlans(plan.id))
        if (isBreaking) {
          val buffer = outputToBuffer(source, plan)
          val pipeline = newPipeline(plan, buffer)
          pipeline.lhs = source.id
          pipeline
        } else if (source.fuse(plan)) {
          source
        } else {
          source.middlePlans += plan
          source
        }
    }
  }

  override protected def onTwoChildPlanComingFromLeft(plan: LogicalPlan,
                                                      lhs: PipelineDefiner,
                                                      argument: ApplyBufferDefiner): ApplyBufferDefiner =
  {
    plan match {
      case _: plans.Apply =>
        val argumentSlotOffset = slotConfigurations(plan.id).getArgumentLongOffsetFor(plan.id)
        outputToApplyBuffer(lhs, argumentSlotOffset, plan)

      case plans.ConditionalApply(_, _, items) =>
        val argumentSlotOffset = slotConfigurations(plan.id).getArgumentLongOffsetFor(plan.id)
        val slots = slotConfigurations.get(plan.id)
        val vars = items.collect {
          case i => slots(i) match {
              case LongSlot(offset, _, _) => IsPrimitiveNull(offset)
              case RefSlot(offset, _, _) => IsNull(ReferenceFromSlot(offset, i))(InputPosition.NONE)
            }
        }
        val expression = if (vars.size == 1) vars.head else Ors(vars)(InputPosition.NONE)
        outputToConditionalSink(expression, lhs, argumentSlotOffset, plan)

      case plans.AntiConditionalApply(_, _, items) =>
        val argumentSlotOffset = slotConfigurations(plan.id).getArgumentLongOffsetFor(plan.id)
        val slots = slotConfigurations.get(plan.id)
        val vars = items.collect {
          case i => slots(i) match {
            case LongSlot(offset, _, _) => Not(IsPrimitiveNull(offset))(InputPosition.NONE)
            case RefSlot(offset, _, _) => Not(IsNull(ReferenceFromSlot(offset, i))(InputPosition.NONE))(InputPosition.NONE)
          }
        }
        val expression = if (vars.size == 1) vars.head else Ors(vars)(InputPosition.NONE)
        outputToConditionalSink(expression, lhs, argumentSlotOffset, plan)

      case p: plans.AbstractSelectOrSemiApply =>
        val argumentSlotOffset = slotConfigurations(plan.id).getArgumentLongOffsetFor(plan.id)
        outputToConditionalSink(p.expression, lhs, argumentSlotOffset, plan)

      case _: plans.CartesianProduct if leveragedOrders.get(plan.id) =>
        val argumentSlotOffset = slotConfigurations(plan.id).getArgumentLongOffsetFor(plan.id)
        outputToApplyBuffer(lhs, argumentSlotOffset, plan)

      case _: plans.CartesianProduct =>
        val rhsSlots = slotConfigurations(plan.rhs.get.id)
        val argumentSlotOffset = rhsSlots.getArgumentLongOffsetFor(plan.id)
        val outerArgumentSize = argumentSizes.get(LogicalPlans.leftLeaf(plan).id)
        val outerArgumentSlotOffset = argument.argumentSlotOffset

        outputToAttachApplyBuffer(lhs, plan.id, argumentSlotOffset, rhsSlots, outerArgumentSlotOffset, outerArgumentSize).applyBuffer

      case _ =>
        argument
    }
  }

  override protected def onTwoChildPlanComingFromRight(plan: LogicalPlan,
                                                       lhs: PipelineDefiner,
                                                       rhs: PipelineDefiner,
                                                       argument: ApplyBufferDefiner): PipelineDefiner = {

    plan match {
      case apply: plans.Apply =>
        val applyRhsPlan =
          if (rhs.middlePlans.isEmpty) {
            rhs.headPlan match {
              case InterpretedHeadBuilder(plan) => plan
              case FusedHeadBuilder(fuser) => fuser.fusedPlans.last
            }
          } else {
            rhs.middlePlans.last
          }
        applyRhsPlans(apply.id.x) = applyRhsPlan.id.x
        rhs

      case _: plans.ConditionalApply |
           _: plans.AntiConditionalApply |
           _: plans.SelectOrSemiApply |
           _: plans.SelectOrAntiSemiApply =>
        /*
         * This part will connect the rhs with the output buffer
         *
         *                         .--onTrue------------------------------------.
         *                        /                                              \
         *   -LHS->  ConditionalSink                                           output --TOP-->
         *                        \                                              /
         *                         `-onFalse-> ApplyBuffer ->  ...    --RHS-----`
         *
         */
        val sink = argument.conditionalApplySink.getOrElse(throw new IllegalStateException("must have a conditional sink at this point"))
        val rhsSlotConfiguration = slotConfigurations(rhs.headPlan.id)
        verifyLhsPrefixOfRhs(slotConfigurations(lhs.headPlan.id), rhsSlotConfiguration)
        val output = stateDefiner.newBuffer(rhs.id, plan.id, rhsSlotConfiguration)
        sink.onTrueBuffer = output
        rhs.fuseOrInterpretOutput(MorselBufferOutput(output.id, plan.id))
        val pipeline: PipelineDefiner = newPipeline(plan, output)
        //this is weird, but used for getting the scheduler to do the right thing
        pipeline.lhs = rhs.id
        pipeline

      case _: CartesianProduct =>
        if (breakingPolicy.breakOn(plan, applyPlans(plan.id))) {
          val buffer = outputToLhsAccumulatingRhsStreamingBuffer(None, rhs, plan.id, argument, argument.argumentSlotOffset, CartesianProductVariant)
          val pipeline = newPipeline(plan, buffer)
          pipeline.lhs = lhs.id
          pipeline.rhs = rhs.id
          pipeline
        } else {
          throw new UnsupportedOperationException(s"Not breaking on ${plan.getClass.getSimpleName} is not supported.")
        }

      case _: plans.NodeHashJoin | _: plans.ValueHashJoin | _:plans.RightOuterHashJoin | _:plans.LeftOuterHashJoin =>
        if (breakingPolicy.breakOn(plan, applyPlans(plan.id))) {
          val joinVariant = plan match {
            case _: NodeHashJoin => InnerVariant
            case _: ValueHashJoin => InnerVariant
            case _: RightOuterHashJoin => RightOuterVariant
            case _: LeftOuterHashJoin => LeftOuterVariant
          }
          val buffer = outputToLhsAccumulatingRhsStreamingBuffer(Some(lhs), rhs, plan.id, argument, argument.argumentSlotOffset, joinVariant)
          val pipeline = newPipeline(plan, buffer)
          pipeline.lhs = lhs.id
          pipeline.rhs = rhs.id
          pipeline
        } else {
          throw new UnsupportedOperationException(s"Not breaking on ${plan.getClass.getSimpleName} is not supported.")
        }

      case _: plans.Union =>
        if (breakingPolicy.breakOn(plan, applyPlans(plan.id))) {
          val buffer = outputToUnionBuffer(lhs, rhs, plan.id)
          val pipeline = newPipeline(plan, buffer)
          pipeline.lhs = lhs.id
          pipeline.rhs = rhs.id
          pipeline
        } else {
          throw new UnsupportedOperationException(s"Not breaking on ${plan.getClass.getSimpleName} is not supported.")
        }
    }
  }

  // HELPERS
  //Check so that lhs slot configuration is a prefix of the rhs slot configuration
  private def verifyLhsPrefixOfRhs(lhsSlotConfiguration: SlotConfiguration,
                                   rhsSlotConfiguration: SlotConfiguration): Unit = {
    lhsSlotConfiguration.foreachSlotAndAliases {
      case SlotWithKeyAndAliases(k, s, as) =>
        k match {
          case SlotConfiguration.VariableSlotKey(name) =>
            require(rhsSlotConfiguration.get(name).contains(s))
            val rhsAliases = rhsSlotConfiguration.getAliasesFor(name)
            as.foreach(a => require(rhsAliases.contains(a)))
          case SlotConfiguration.CachedPropertySlotKey(property) =>
            require(rhsSlotConfiguration.getCachedPropertySlot(property).contains(s))
          case SlotConfiguration.ApplyPlanSlotKey(applyPlanId) =>
            require(rhsSlotConfiguration.getArgumentSlot(applyPlanId).contains(s))
        }
    }
  }

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
      *   reducersOnRHS += argumentStateDefinition     reducers += argumentStateDefinition.id
      *
      * Mark `argumentStateMapId` as a reducer in all buffers between `buffer` and `applyBuffer`. This has
      * to be done so that reference counting of inflight work will work correctly, so that `buffer` knows
      * when each argument is complete.
      */
  private def markReducerInUpstreamBuffers(buffer: BufferDefiner,
                                           applyBuffer: ApplyBufferDefiner,
                                           argumentStateDefinition: ArgumentStateDefinition): Unit = {
    val downstreamReduceASMID = argumentStateDefinition.id
    traverseBuffers(buffer,
      applyBuffer,
      inputBuffer => inputBuffer.downstreamReducers += downstreamReduceASMID,
      lHSAccumulatingRHSStreamingBufferDefinition => {
        lHSAccumulatingRHSStreamingBufferDefinition.lhsSink.downstreamReducers += downstreamReduceASMID
        lHSAccumulatingRHSStreamingBufferDefinition.rhsSink.downstreamReducers += downstreamReduceASMID
        lHSAccumulatingRHSStreamingBufferDefinition.downstreamReducers += downstreamReduceASMID
      },
      delegateBuffer => {
        val b = delegateBuffer.applyBuffer
        b.downstreamReducers += downstreamReduceASMID
        delegateBuffer.downstreamReducers += downstreamReduceASMID
      },
      lastDelegateBuffer => {
        val b = lastDelegateBuffer.applyBuffer
        b.registerReducerOnRHS(argumentStateDefinition)
        lastDelegateBuffer.downstreamReducers += downstreamReduceASMID
      },
      _ => {}
    )
  }

  private def markCancellerInUpstreamBuffers(buffer: BufferDefiner,
                                             applyBuffer: ApplyBufferDefiner,
                                             workCancellerASMID: ArgumentStateMapId,
                                             onPipeline: PipelineDefiner => Unit): Unit = {
    traverseBuffers(buffer,
      applyBuffer,
      inputBuffer => inputBuffer.registerDownstreamWorkCanceller(workCancellerASMID),
      lHSAccumulatingRHSStreamingBufferDefinition => {
        lHSAccumulatingRHSStreamingBufferDefinition.lhsSink.registerDownstreamWorkCanceller(workCancellerASMID)
        lHSAccumulatingRHSStreamingBufferDefinition.rhsSink.registerDownstreamWorkCanceller(workCancellerASMID)
        lHSAccumulatingRHSStreamingBufferDefinition.registerDownstreamWorkCanceller(workCancellerASMID)
      },
      delegateBuffer => delegateBuffer.registerDownstreamWorkCanceller(workCancellerASMID),
      lastDelegateBuffer => {
        val b = lastDelegateBuffer.applyBuffer
        b.registerDownstreamWorkCanceller(workCancellerASMID)
        lastDelegateBuffer.registerDownstreamWorkCanceller(workCancellerASMID)
      },
      onPipeline
    )
  }

  /**
   * This traverses buffers in a breadth first manner, from the given buffer towards the input, stopping at applyBuffer.
   *
   * @param buffer                              start the traversal here
   * @param applyBuffer                         end the traversal here
   * @param onInputBuffer                       called for every input buffer
   * @param onDelegateBuffer                    called for every delegate buffer except the last one belonging to applyBuffer
   * @param onLHSAccumulatingRHSStreamingBuffer called for every LHSAccumulatingRHSStreamingBufferDefinition
   * @param onLastDelegate                      called for the last delegate belonging to applyBuffer
   * @param onPipeline                          called for each pipeline
   */
  private def traverseBuffers(buffer: BufferDefiner,
                              applyBuffer: ApplyBufferDefiner,
                              onInputBuffer: BufferDefiner => Unit,
                              onLHSAccumulatingRHSStreamingBuffer: LHSAccumulatingRHSStreamingBufferDefiner => Unit,
                              onDelegateBuffer: DelegateBufferDefiner => Unit,
                              onLastDelegate: DelegateBufferDefiner => Unit,
                              onPipeline: PipelineDefiner => Unit): Unit = {
    @tailrec
    def bfs(buffers: Seq[BufferDefiner]): Unit = {
      val upstreams = new ArrayBuffer[BufferDefiner]()

      def getPipeline(id: Int): PipelineDefiner = {
        val p = pipelines(id)
        onPipeline(p)
        p
      }

      buffers.foreach {
        case d: DelegateBufferDefiner if d.applyBuffer == applyBuffer =>
          onLastDelegate(d)
        case _: AttachBufferDefiner =>
          throw new IllegalStateException("Nothing should have an attach buffer as immediate input, it should have a delegate buffer instead.")
        case _: ApplyBufferDefiner =>
          throw new IllegalStateException("Nothing should have an apply buffer as immediate input, it should have a delegate buffer instead.")
        case _: ConditionalBufferDefiner =>
          throw new IllegalStateException("Nothing should have a conditional apply buffer as immediate input, it should have a delegate buffer instead.")
        case b: LHSAccumulatingRHSStreamingBufferDefiner =>
          onLHSAccumulatingRHSStreamingBuffer(b)
          // We only add the RHS since the LHS is not streaming through the join
          // Therefore it needs to finish before the join can even start
          upstreams += getPipeline(b.rhsSink.producingPipelineId.x).inputBuffer
        case d: DelegateBufferDefiner =>
          onDelegateBuffer(d)
          upstreams += getPipeline(d.applyBuffer.producingPipelineId.x).inputBuffer
        case b: MorselBufferDefiner =>
          onInputBuffer(b)
          upstreams += getPipeline(b.producingPipelineId.x).inputBuffer
        case b: UnionBufferDefiner =>
          onInputBuffer(b)
          upstreams += getPipeline(b.lhsProducingPipelineId.x).inputBuffer
          upstreams += getPipeline(b.rhsProducingPipelineId.x).inputBuffer
        case b: ArgumentStreamMorselBufferDefiner =>
          onInputBuffer(b)
          upstreams += getPipeline(b.producingPipelineId.x).inputBuffer
      }

      if (upstreams.nonEmpty) {
        bfs(upstreams)
      }
    }

    val buffers = Seq(buffer)
    bfs(buffers)
  }

  private def createPropagateTopLevelLimitCallback(argument: ApplyBufferDefiner, workCancellerAsmId: ArgumentStateMapId): PipelineDefiner => Unit =
    if (argument.argumentSlotOffset == TopLevelArgument.SLOT_OFFSET) {
      p => {
        if (p.workCanceller.isEmpty) {
          p.workCanceller = Some(workCancellerAsmId)
        }
      }
    } else {
      _ => ()
    }
}
