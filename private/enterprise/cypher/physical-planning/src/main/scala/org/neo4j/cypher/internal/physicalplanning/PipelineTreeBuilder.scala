/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.physicalplanning

import org.eclipse.collections.api.map.primitive.MutableObjectIntMap
import org.eclipse.collections.impl.map.mutable.primitive.ObjectIntHashMap
import org.neo4j.cypher.internal.logical.plans
import org.neo4j.cypher.internal.logical.plans.Aggregation
import org.neo4j.cypher.internal.logical.plans.Anti
import org.neo4j.cypher.internal.logical.plans.CartesianProduct
import org.neo4j.cypher.internal.logical.plans.Distinct
import org.neo4j.cypher.internal.logical.plans.Limit
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.LogicalPlans
import org.neo4j.cypher.internal.logical.plans.Optional
import org.neo4j.cypher.internal.logical.plans.OrderedDistinct
import org.neo4j.cypher.internal.logical.plans.ProduceResult
import org.neo4j.cypher.internal.logical.plans.Skip
import org.neo4j.cypher.internal.logical.plans.Sort
import org.neo4j.cypher.internal.logical.plans.Top
import org.neo4j.cypher.internal.physicalplanning.PhysicalPlanningAttributes.ApplyPlans
import org.neo4j.cypher.internal.physicalplanning.PhysicalPlanningAttributes.ArgumentSizes
import org.neo4j.cypher.internal.physicalplanning.PhysicalPlanningAttributes.SlotConfigurations
import org.neo4j.cypher.internal.physicalplanning.PipelineId.NO_PIPELINE
import org.neo4j.cypher.internal.physicalplanning.PipelineTreeBuilder.ApplyBufferDefiner
import org.neo4j.cypher.internal.physicalplanning.PipelineTreeBuilder.ArgumentStateBufferDefiner
import org.neo4j.cypher.internal.physicalplanning.PipelineTreeBuilder.AttachBufferDefiner
import org.neo4j.cypher.internal.physicalplanning.PipelineTreeBuilder.BufferDefiner
import org.neo4j.cypher.internal.physicalplanning.PipelineTreeBuilder.DelegateBufferDefiner
import org.neo4j.cypher.internal.physicalplanning.PipelineTreeBuilder.ExecutionStateDefiner
import org.neo4j.cypher.internal.physicalplanning.PipelineTreeBuilder.LHSAccumulatingRHSStreamingBufferDefiner
import org.neo4j.cypher.internal.physicalplanning.PipelineTreeBuilder.MorselBufferDefiner
import org.neo4j.cypher.internal.physicalplanning.PipelineTreeBuilder.OptionalMorselBufferDefiner
import org.neo4j.cypher.internal.physicalplanning.PipelineTreeBuilder.PipelineDefiner
import org.neo4j.cypher.internal.physicalplanning.PipelineTreeBuilder.UnionBufferDefiner
import org.neo4j.cypher.internal.util.attribution.Id

import scala.annotation.tailrec
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

sealed trait HeadPlan {
  def id: Id
}
case class InterpretedHead(plan: LogicalPlan) extends HeadPlan { def id: Id = plan.id }
case class FusedHead(operatorFuser: OperatorFuser) extends HeadPlan { def id: Id = operatorFuser.fusedPlans.head.id }

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
    val headPlan: HeadPlan = {
      val operatorFuser = operatorFuserFactory.newOperatorFuser(headLogicalPlan.id, inputBuffer.bufferConfiguration)
      if (operatorFuser.fuseIn(headLogicalPlan))
        FusedHead(operatorFuser)
      else
        InterpretedHead(headLogicalPlan)
    }
    val middlePlans = new ArrayBuffer[LogicalPlan]
    var serial: Boolean = false
    var workCanceller: Option[ArgumentStateMapId] = None

    def fuse(plan: LogicalPlan, isBreaking: Boolean): Boolean = {
      headPlan match {
        case FusedHead(operatorFuser) if middlePlans.isEmpty && outputDefinition == NoOutput =>
          operatorFuser.fuseIn(plan)
        case _ => false
      }
    }

    def fuseOrInterpret(plan: LogicalPlan, isBreaking: Boolean): Unit = {
      if (!fuse(plan, isBreaking)) {
        middlePlans += plan
      }
    }

    def fuse(output: OutputDefinition): Boolean = {
      headPlan match {
        case FusedHead(operatorFuser) if middlePlans.isEmpty =>
          operatorFuser.fuseIn(output)
        case _ => false
      }
    }

    def fuseOrInterpret(output: OutputDefinition): Unit = {
      if (!fuse(output)) {
        outputDefinition = output
      }
    }

    private def headPlanResult: HeadPlan =
      headPlan match {
        case f: FusedHead =>
          if (f.operatorFuser.fusedPlans.size > 1)
            f
          else
            InterpretedHead(f.operatorFuser.fusedPlans.head)
        case i: InterpretedHead => i
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
                       downstreamReducers.result(),
                       downstreamWorkCancellers.result(),
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

  class OptionalMorselBufferDefiner(id: BufferId,
                                    memoryTrackingOperatorId: Id,
                                    val producingPipelineId: PipelineId,
                                    val argumentStateMapId: ArgumentStateMapId,
                                    val argumentSlotOffset: Int,
                                    bufferConfiguration: SlotConfiguration,
                                    val optionalBufferVariantType: OptionalBufferVariantType) extends BufferDefiner(id, memoryTrackingOperatorId, bufferConfiguration) {
    override protected def variant: BufferVariant = OptionalBufferVariant(argumentStateMapId, optionalBufferVariantType)
  }

  class DelegateBufferDefiner(id: BufferId,
                              operatorId: Id,
                              val applyBuffer: ApplyBufferDefiner,
                              bufferConfiguration: SlotConfiguration) extends BufferDefiner(id, operatorId, bufferConfiguration) {
    override protected def variant: BufferVariant = RegularBufferVariant
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
                         reducersOnRHSReversed,
                         downstreamStatesOnRHS.result(),
                         delegates.result())
    }
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
                                     val argumentStateMapId: ArgumentStateMapId,
                                     val producingPipelineId: PipelineId,
                                     bufferSlotConfiguration: SlotConfiguration) extends BufferDefiner(id, memoryTrackingOperatorId, bufferSlotConfiguration) {
    override protected def variant: BufferVariant = LHSAccumulatingBufferVariant(argumentStateMapId)
  }

  class RHSStreamingBufferDefiner(id: BufferId,
                                  memoryTrackingOperatorId: Id,
                                  val argumentStateMapId: ArgumentStateMapId,
                                  val producingPipelineId: PipelineId,
                                  bufferSlotConfiguration: SlotConfiguration) extends BufferDefiner(id, memoryTrackingOperatorId, bufferSlotConfiguration) {
    override protected def variant: BufferVariant = RHSStreamingBufferVariant(argumentStateMapId)
  }

  class LHSAccumulatingRHSStreamingBufferDefiner(id: BufferId,
                                                 memoryTrackingOperatorId: Id,
                                                 val lhsSink: LHSAccumulatingBufferDefiner,
                                                 val rhsSink: RHSStreamingBufferDefiner,
                                                 val lhsArgumentStateMapId: ArgumentStateMapId,
                                                 val rhsArgumentStateMapId: ArgumentStateMapId,
                                                 bufferSlotConfiguration: SlotConfiguration) extends BufferDefiner(id, memoryTrackingOperatorId, bufferSlotConfiguration) {
    override protected def variant: BufferVariant =
      LHSAccumulatingRHSStreamingBufferVariant(lhsSink.result, rhsSink.result, lhsArgumentStateMapId, rhsArgumentStateMapId)
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

    def newOptionalBuffer(producingPipelineId: PipelineId,
                          memoryTrackingOperatorId: Id,
                          argumentStateMapId: ArgumentStateMapId,
                          argumentSlotOffset: Int,
                          bufferSlotConfiguration: SlotConfiguration,
                          optionalBufferVariantType: OptionalBufferVariantType): OptionalMorselBufferDefiner = {
      val x = buffers.size
      val buffer = new OptionalMorselBufferDefiner(BufferId(x),
                                                   memoryTrackingOperatorId,
                                                   producingPipelineId,
                                                   argumentStateMapId,
                                                   argumentSlotOffset,
                                                   bufferSlotConfiguration,
                                                   optionalBufferVariantType)
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

    def newAttachBuffer(producingPipelineId: PipelineId,
                        memoryTrackingOperatorId: Id,
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
                                             bufferSlotConfiguration: SlotConfiguration): LHSAccumulatingRHSStreamingBufferDefiner = {
      val lhsAccId = BufferId(buffers.size)
      val lhsAcc = new LHSAccumulatingBufferDefiner(lhsAccId,
                                                    memoryTrackingOperatorId,
                                                    lhsArgumentStateMapId,
                                                    lhsProducingPipelineId,
                                                    bufferSlotConfiguration = null) // left null because buffer is never used as source
      buffers += lhsAcc

      val rhsAccId = BufferId(buffers.size)
      val rhsAcc = new RHSStreamingBufferDefiner(rhsAccId,
                                                 memoryTrackingOperatorId,
                                                 rhsArgumentStateMapId,
                                                 rhsProducingPipelineId,
                                                 bufferSlotConfiguration = null) // left null because buffer is never used as source
      buffers += rhsAcc

      val x = buffers.size
      val buffer = new LHSAccumulatingRHSStreamingBufferDefiner(BufferId(x),
                                                                memoryTrackingOperatorId,
                                                                lhsAcc,
                                                                rhsAcc,
                                                                lhsArgumentStateMapId,
                                                                rhsArgumentStateMapId,
                                                                bufferSlotConfiguration)
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
                          applyPlans: ApplyPlans)
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
    pipeline.fuseOrInterpret(MorselBufferOutput(output.id, nextPipelineHeadPlan.id))
    output
  }

  private def outputToUnionBuffer(lhs: PipelineDefiner, rhs: PipelineDefiner, nextPipelineHeadPlan: Id): UnionBufferDefiner = {
    // The Buffer doesn't really have a slot configuration, since it can store Morsels from both LHS and RHS with different slot configs.
    // Does it work to use the configuration of the Union plan?
    val output = stateDefiner.newUnionBuffer(lhs.id, rhs.id, nextPipelineHeadPlan, slotConfigurations(nextPipelineHeadPlan))
    lhs.fuseOrInterpret(MorselBufferOutput(output.id, nextPipelineHeadPlan))
    rhs.fuseOrInterpret(MorselBufferOutput(output.id, nextPipelineHeadPlan))
    output
  }

  private def outputToApplyBuffer(pipeline: PipelineDefiner, argumentSlotOffset: Int, nextPipelineHeadPlan: LogicalPlan): ApplyBufferDefiner = {
    val output = stateDefiner.newApplyBuffer(pipeline.id, nextPipelineHeadPlan.id, argumentSlotOffset, slotConfigurations(pipeline.headPlan.id))
    pipeline.fuseOrInterpret(MorselBufferOutput(output.id, nextPipelineHeadPlan.id))
    output
  }

  private def outputToAttachApplyBuffer(pipeline: PipelineDefiner,
                                        attachingPlanId: Id,
                                        argumentSlotOffset: Int,
                                        postAttachSlotConfiguration: SlotConfiguration,
                                        outerArgumentSlotOffset: Int,
                                        outerArgumentSize: SlotConfiguration.Size): AttachBufferDefiner = {

    val output = stateDefiner.newAttachBuffer(pipeline.id,
      attachingPlanId,
      slotConfigurations(pipeline.headPlan.id),
      postAttachSlotConfiguration,
      outerArgumentSlotOffset,
      outerArgumentSize)

    output.applyBuffer = stateDefiner.newApplyBuffer(pipeline.id, attachingPlanId, argumentSlotOffset, postAttachSlotConfiguration)
    pipeline.fuseOrInterpret(MorselBufferOutput(output.id, attachingPlanId))
    output
  }

  private def outputToArgumentStateBuffer(pipeline: PipelineDefiner, plan: LogicalPlan, applyBuffer: ApplyBufferDefiner, argumentSlotOffset: Int): ArgumentStateBufferDefiner = {
    val asm = stateDefiner.newArgumentStateMap(plan.id, argumentSlotOffset)
    val output = stateDefiner.newArgumentStateBuffer(pipeline.id, plan.id, asm.id, slotConfigurations(pipeline.headPlan.id))
    pipeline.fuseOrInterpret(ReduceOutput(output.id, plan))
    markReducerInUpstreamBuffers(pipeline.inputBuffer, applyBuffer, asm)
    output
  }

  private def outputToOptionalPipelinedBuffer(pipeline: PipelineDefiner,
                                              plan: LogicalPlan,
                                              applyBuffer: ApplyBufferDefiner,
                                              argumentSlotOffset: Int): OptionalMorselBufferDefiner = {
    val asm = stateDefiner.newArgumentStateMap(plan.id, argumentSlotOffset)
    val output = stateDefiner.newOptionalBuffer(pipeline.id, plan.id, asm.id, argumentSlotOffset, slotConfigurations(pipeline.headPlan.id), OptionalType)
    pipeline.fuseOrInterpret(MorselArgumentStateBufferOutput(output.id, argumentSlotOffset, plan.id))
    markReducerInUpstreamBuffers(pipeline.inputBuffer, applyBuffer, asm)
    output
  }

  private def outputToAntiPipelinedBuffer(pipeline: PipelineDefiner,
                                          plan: LogicalPlan,
                                          applyBuffer: ApplyBufferDefiner,
                                          argumentSlotOffset: Int): OptionalMorselBufferDefiner = {
    val asm = stateDefiner.newArgumentStateMap(plan.id, argumentSlotOffset)
    val output = stateDefiner.newOptionalBuffer(pipeline.id, plan.id, asm.id, argumentSlotOffset, slotConfigurations(pipeline.headPlan.id), AntiType)
    pipeline.fuseOrInterpret(MorselArgumentStateBufferOutput(output.id, argumentSlotOffset, plan.id))
    markReducerInUpstreamBuffers(pipeline.inputBuffer, applyBuffer, asm)
    output
  }

  private def outputToLhsAccumulatingRhsStreamingBuffer(maybeLhs: Option[PipelineDefiner],
                                                        rhs: PipelineDefiner,
                                                        planId: Id,
                                                        applyBuffer: ApplyBufferDefiner,
                                                        argumentSlotOffset: Int): LHSAccumulatingRHSStreamingBufferDefiner = {
    val lhsAsm = stateDefiner.newArgumentStateMap(planId, argumentSlotOffset)
    val rhsAsm = stateDefiner.newArgumentStateMap(planId, argumentSlotOffset)
    val lhsId = maybeLhs.map(_.id).getOrElse(PipelineId.NO_PIPELINE)
    val output = stateDefiner.newLhsAccumulatingRhsStreamingBuffer(lhsId, rhs.id, planId, lhsAsm.id, rhsAsm.id, slotConfigurations(rhs.headPlan.id))
    maybeLhs match {
      case Some(lhs) =>
        lhs.fuseOrInterpret(MorselArgumentStateBufferOutput(output.lhsSink.id, argumentSlotOffset, planId))
        markReducerInUpstreamBuffers(lhs.inputBuffer, applyBuffer, lhsAsm)
      case None =>
        // The LHS argument state map has to be initiated by the apply even if there is no pipeline
        // connecting them. Otherwise the LhsAccumulatingRhsStreamingBuffer can never output anything.
        applyBuffer.registerReducerOnRHS(lhsAsm)
    }
    rhs.fuseOrInterpret(MorselArgumentStateBufferOutput(output.rhsSink.id, argumentSlotOffset, planId))
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
          source.fuseOrInterpret(ProduceResultOutput(produceResult))
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

      case _: Optional =>
        if (breakingPolicy.breakOn(plan, applyPlans(plan.id))) {
          val optionalPipelinedBuffer = outputToOptionalPipelinedBuffer(source, plan, argument, argument.argumentSlotOffset)
          val pipeline = newPipeline(plan, optionalPipelinedBuffer)
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
        val asm = stateDefiner.newArgumentStateMap(plan.id, argument.argumentSlotOffset)
        //if we are a toplevel limit we want to to propagate the limit upstream
        val onPipeline: PipelineDefiner => Unit =
          if(argument.argumentSlotOffset == TopLevelArgument.SLOT_OFFSET) p => {
            if (p.workCanceller.isEmpty) {
              p.workCanceller = Some(asm.id)
            }
          }
          else _ => {}
        //we want to propagate starting with source and then all upstream pipelines
        onPipeline(source)
        markCancellerInUpstreamBuffers(source.inputBuffer, argument, asm.id, onPipeline)
        source.fuseOrInterpret(plan, isBreaking = true)
        source

      case _: Distinct | _: OrderedDistinct | _: Skip =>
        val asm = stateDefiner.newArgumentStateMap(plan.id, argument.argumentSlotOffset)
        argument.downstreamStatesOnRHS += asm.id
        source.fuseOrInterpret(plan, breakingPolicy.breakOn(plan, applyPlans(plan.id)))
        source

      case _ =>
        val isBreaking = breakingPolicy.breakOn(plan, applyPlans(plan.id))
        if (source.fuse(plan, isBreaking)) {
          source
        } else {
          if (isBreaking) {
            val buffer = outputToBuffer(source, plan)
            val pipeline = newPipeline(plan, buffer)
            pipeline.lhs = source.id
            pipeline
          } else {
            source.middlePlans += plan
            source
          }
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
              case InterpretedHead(plan) => plan
              case FusedHead(fuser) => fuser.fusedPlans.last
            }
          } else {
            rhs.middlePlans.last
          }
        applyRhsPlans(apply.id.x) = applyRhsPlan.id.x
        rhs

      case _: CartesianProduct =>
        if (breakingPolicy.breakOn(plan, applyPlans(plan.id))) {
          val buffer = outputToLhsAccumulatingRhsStreamingBuffer(None, rhs, plan.id, argument, argument.argumentSlotOffset)
          val pipeline = newPipeline(plan, buffer)
          pipeline.lhs = lhs.id
          pipeline.rhs = rhs.id
          pipeline
        } else {
          throw new UnsupportedOperationException(s"Not breaking on ${plan.getClass.getSimpleName} is not supported.")
        }

      case _: plans.NodeHashJoin =>
        if (breakingPolicy.breakOn(plan, applyPlans(plan.id))) {
          val buffer = outputToLhsAccumulatingRhsStreamingBuffer(Some(lhs), rhs, plan.id, argument, argument.argumentSlotOffset)
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
        case b: OptionalMorselBufferDefiner =>
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
}
