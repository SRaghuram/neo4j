/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.physicalplanning

import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.ProduceResult
import org.neo4j.cypher.internal.physicalplanning
import org.neo4j.cypher.internal.physicalplanning.ExecutionGraphDefinition.NO_ARGUMENT_STATE_MAPS
import org.neo4j.cypher.internal.physicalplanning.ExecutionGraphDefinition.NO_ARGUMENT_STATE_MAP_INITIALIZATIONS
import org.neo4j.cypher.internal.physicalplanning.ExecutionGraphDefinition.NO_BUFFERS
import org.neo4j.cypher.internal.util.attribution.Id
import org.scalatest.matchers.MatchResult
import org.scalatest.matchers.Matcher
import org.scalatest.words.MatcherWords

import scala.collection.mutable

object ExecutionGraphDefinitionMatcher {

  /**
   * @return a new [[ExecutionGraphDefinitionMatcher]]
   */
  def newGraph: ExecutionGraphDefinitionMatcher = new ExecutionGraphDefinitionMatcher()

  /**
   * Start defining a sequence of buffers and pipelines with this method.
   *
   *
   * The first time you specify a buffer, pipeline, reducer or canceller with a particular ID,
   * you have to supply also the default arguments. This applies to all functions that are reachable from here.
   *
   */
  def start(m: ExecutionGraphDefinitionMatcher) = new m.StartSequence()

  /**
   * Used for the syntax
   * ```
   * ... should plan {
   *
   * }
   * ```
   *
   */
  def plan(m: ExecutionGraphDefinitionMatcher): ExecutionGraphDefinitionMatcher = m
}

/**
 * Matcher for ExecutionGraphDefinitions that lets one build the expected graph
 * through a sequence of method calls that describe the connections between buffers and pipelines.
 */
class ExecutionGraphDefinitionMatcher() extends Matcher[ExecutionGraphDefinition] {

  private case class MatchablePipeline(id: PipelineId,
                                       plans: Seq[Class[_ <: LogicalPlan]],
                                       fusedPlans: Seq[Class[_ <: LogicalPlan]],
                                       inputBuffer: BufferId,
                                       outputDefinition: MatchableOutputDefinition,
                                       serial: Boolean,
                                       workLimiter: Option[ArgumentStateMapId])

  private sealed trait MatchableOutputDefinition

  private case object ProduceResultOutput extends MatchableOutputDefinition

  private case class MorselBufferOutput(id: BufferId) extends MatchableOutputDefinition

  private case class MorselArgumentStateBufferOutput(id: BufferId, argumentSlotOffset: Int) extends MatchableOutputDefinition

  private case class ReduceOutput(bufferId: BufferId, argumentStateMapId: ArgumentStateMapId) extends MatchableOutputDefinition

  private case object NoOutput extends MatchableOutputDefinition

  private val buffers = mutable.Map[Int, BufferDefinition]()
  private val pipelines = mutable.Map[Int, MatchablePipeline]()
  private val argumentStates = mutable.Map[Int, ArgumentStateDefinition]()

  private def out(outputDefinition: OutputDefinition): MatchableOutputDefinition = {
    outputDefinition match {
      case physicalplanning.ProduceResultOutput(_) => ProduceResultOutput
      case physicalplanning.MorselBufferOutput(id, _) => MorselBufferOutput(id)
      case physicalplanning.MorselArgumentStateBufferOutput(id, argumentSlotOffset, _) => MorselArgumentStateBufferOutput(id, argumentSlotOffset)
      case physicalplanning.ReduceOutput(bufferId, argumentStateMapId, _) => ReduceOutput(bufferId, argumentStateMapId)
      case physicalplanning.NoOutput => NoOutput
    }
  }

  private def produceResult(outputDefinition: OutputDefinition): Option[ProduceResult] = {
    outputDefinition match {
      case physicalplanning.ProduceResultOutput(plan) => Some(plan)
      case _ => None
    }
  }

  private def registerArgumentState(id: Int, planId: Int, argumentSlotOffset: Int): ArgumentStateDefinition = {
    argumentStates.getOrElseUpdate(id, ArgumentStateDefinition(ArgumentStateMapId(id), Id(planId), argumentSlotOffset))
  }

  class StartSequence {

    private val UNKNOWN_ARG_SLOT_OFFSET = -2

    def applyBuffer(id: Int, argumentSlotOffset: Int = UNKNOWN_ARG_SLOT_OFFSET, planId: Int = -1): ApplyBufferSequence = {
      val bd = buffers.getOrElseUpdate(id, BufferDefinition(BufferId(id),
        Id(planId),
        NO_ARGUMENT_STATE_MAPS,
        NO_ARGUMENT_STATE_MAP_INITIALIZATIONS,
        ApplyBufferVariant(
          if (argumentSlotOffset == UNKNOWN_ARG_SLOT_OFFSET)
            throw new IllegalArgumentException("You have to specify the argumentSlotOffset for new apply buffers")
          else argumentSlotOffset,
          NO_ARGUMENT_STATE_MAP_INITIALIZATIONS,
          NO_ARGUMENT_STATE_MAPS,
          NO_BUFFERS))(SlotConfiguration.empty))
      new ApplyBufferSequence(bd)
    }

    def morselBuffer(id: Int): MorselBufferSequence = {
      // This buffer must have been specified before
      new MorselBufferSequence(buffers(id))
    }
  }

  abstract class BufferSequence(bufferDefinition: BufferDefinition) {
    def reducer(id: Int, planId: Int = -1, argumentSlotOffset: Int = -1): ExecutionGraphDefinitionMatcher = {
      val asd = registerArgumentState(id, planId, argumentSlotOffset)
      buffers(bufferDefinition.id.x) = bufferDefinition.withReducers(bufferDefinition.reducers :+ asd.id)
      ExecutionGraphDefinitionMatcher.this
    }

    def canceller(id: Int, planId: Int = -1, argumentSlotOffset: Int = -1, initialCount: Int = 1): ExecutionGraphDefinitionMatcher = {
      val asd = registerArgumentState(id, planId, argumentSlotOffset)
      buffers(bufferDefinition.id.x) = bufferDefinition.withWorkCancellers(bufferDefinition.workCancellers :+ Initialization(asd.id, initialCount))
      ExecutionGraphDefinitionMatcher.this
    }
  }

  class ApplyBufferSequence(bufferDefinition: BufferDefinition) extends BufferSequence(bufferDefinition) {

    private val variant = bufferDefinition.variant.asInstanceOf[ApplyBufferVariant]

    def delegateToMorselBuffer(id: Int, planId: Int = -1): MorselBufferSequence = {
      val bd = buffers.getOrElseUpdate(id,
        BufferDefinition(
          BufferId(id),
          Id(planId),
          NO_ARGUMENT_STATE_MAPS,
          NO_ARGUMENT_STATE_MAP_INITIALIZATIONS,
          RegularBufferVariant)(SlotConfiguration.empty))
      val updatedApplyBuffer = bufferDefinition.copy(variant = variant.copy(delegates = variant.delegates :+ bd.id))(SlotConfiguration.empty)
      buffers(bufferDefinition.id.x) = updatedApplyBuffer

      updateAttachAndConditionalBuffers(updatedApplyBuffer)

      new MorselBufferSequence(bd)
    }

    def reducerOnRHS(id: Int, planId: Int = -1, argumentSlotOffset: Int = -1, initialCount: Int = 1): ExecutionGraphDefinitionMatcher = {
      val asd = registerArgumentState(id, planId, argumentSlotOffset)
      val updatedApplyBuffer = bufferDefinition.copy(variant =
        variant.copy(reducersOnRHSReversed = Initialization(asd.id, initialCount) +: variant.reducersOnRHSReversed)
      )(SlotConfiguration.empty)
      buffers(bufferDefinition.id.x) = updatedApplyBuffer

      updateAttachAndConditionalBuffers(updatedApplyBuffer)

      ExecutionGraphDefinitionMatcher.this
    }

    def downstreamStateOnRHS(id: Int, planId: Int = -1, argumentSlotOffset: Int = -1): ExecutionGraphDefinitionMatcher = {
      val asd = registerArgumentState(id, planId, argumentSlotOffset)
      val updatedApplyBuffer = bufferDefinition.copy(variant =
        variant.copy(downstreamStatesOnRHS = variant.downstreamStatesOnRHS :+ asd.id)
      )(SlotConfiguration.empty)
      buffers(bufferDefinition.id.x) = updatedApplyBuffer

      updateAttachAndConditionalBuffers(updatedApplyBuffer)

      ExecutionGraphDefinitionMatcher.this
    }

    private def updateAttachAndConditionalBuffers(updatedApplyBuffer: BufferDefinition): Unit = {
      buffers.transform {
        case (_, bd@BufferDefinition(_, _, _, _, atv@AttachBufferVariant(`bufferDefinition`, _, _, _))) =>
          bd.copy(variant = atv.copy(applyBuffer = updatedApplyBuffer))(SlotConfiguration.empty)
        case (_, bd@BufferDefinition(_, _, _, _, cbv@ConditionalBufferVariant(_, `bufferDefinition`, _))) =>
          bd.copy(variant = cbv.copy(onFalse = updatedApplyBuffer))(SlotConfiguration.empty)

        case (_, x) => x
      }
    }
  }

  class AttachBufferSequence(bufferDefinition: BufferDefinition) extends BufferSequence(bufferDefinition) {

    private val variant = bufferDefinition.variant.asInstanceOf[AttachBufferVariant]

    def lhsJoinSinkForAttach(lhsSinkId: Int, lhsAsmId: Int, planId: Int, rhsAsmId: Int): AttachBufferSequence = {
      /*
      Create LHS sink of join buffer, but do not set as output.
      Attach buffer only needs the ASM of LHS Sink.
       */
      buffers.getOrElseUpdate(lhsSinkId,
        BufferDefinition(
          BufferId(lhsSinkId),
          Id(planId),
          NO_ARGUMENT_STATE_MAPS,
          NO_ARGUMENT_STATE_MAP_INITIALIZATIONS,
          variant = LHSAccumulatingBufferVariant(ArgumentStateMapId(lhsAsmId), ArgumentStateMapId(rhsAsmId)),
        )(SlotConfiguration.empty))
      this
    }

    def delegateToApplyBuffer(id: Int, argumentSlotOffset: Int = -1, planId: Int = -1): ApplyBufferSequence = {
      val bd = buffers.getOrElseUpdate(id,
        BufferDefinition(
          BufferId(id),
          Id(planId),
          NO_ARGUMENT_STATE_MAPS,
          NO_ARGUMENT_STATE_MAP_INITIALIZATIONS,
          ApplyBufferVariant(argumentSlotOffset, NO_ARGUMENT_STATE_MAP_INITIALIZATIONS, NO_ARGUMENT_STATE_MAPS, NO_BUFFERS))(SlotConfiguration.empty))

      buffers(bufferDefinition.id.x) = bufferDefinition.copy(variant = variant.copy(applyBuffer = bd))(SlotConfiguration.empty)

      new ApplyBufferSequence(bd)
    }
  }

  abstract class BufferBeforePipelineSequence(bufferDefinition: BufferDefinition) extends BufferSequence(bufferDefinition) {
    def pipeline(id: Int,
                 plans: Seq[Class[_ <: LogicalPlan]] = Seq.empty,
                 fusedPlans: Seq[Class[_ <: LogicalPlan]] = Seq.empty,
                 serial: Boolean = false,
                 workLimiter: Option[ArgumentStateMapId] = None): PipelineSequence = {
      val mp = pipelines.getOrElseUpdate(id, MatchablePipeline(PipelineId(id), plans, fusedPlans, bufferDefinition.id, null, serial, workLimiter))
      new PipelineSequence(mp)
    }

  }

  class MorselBufferSequence(bufferDefinition: BufferDefinition) extends BufferBeforePipelineSequence(bufferDefinition)

  class ArgumentStateBufferSequence(bufferDefinition: BufferDefinition) extends BufferBeforePipelineSequence(bufferDefinition)

  class ArgumentStreamBufferSequence(bufferDefinition: BufferDefinition) extends BufferBeforePipelineSequence(bufferDefinition)

  class AntiBufferSequence(bufferDefinition: BufferDefinition) extends BufferBeforePipelineSequence(bufferDefinition)

  class LhsJoinBufferSequence(bufferDefinition: BufferDefinition) extends BufferBeforePipelineSequence(bufferDefinition)

  class JoinBufferSequence(bufferDefinition: BufferDefinition) extends BufferBeforePipelineSequence(bufferDefinition)

  class PipelineSequence(matchablePipeline: MatchablePipeline) {
    def morselBuffer(id: Int, planId: Int = -1) : MorselBufferSequence = {
      val bd = buffers.getOrElseUpdate(id,
        BufferDefinition(
          BufferId(id),
          Id(planId),
          NO_ARGUMENT_STATE_MAPS,
          NO_ARGUMENT_STATE_MAP_INITIALIZATIONS,
          RegularBufferVariant)(SlotConfiguration.empty))
      val out = MorselBufferOutput(BufferId(id))
      pipelines(matchablePipeline.id.x) = matchablePipeline.copy(outputDefinition = out)
      new MorselBufferSequence(bd)
    }

    def argumentStateBuffer(id: Int, asmId: Int = -1, planId: Int = -1, fusedOutput: Boolean = false): ArgumentStateBufferSequence = {
      val bd = buffers.getOrElseUpdate(id,
        BufferDefinition(
          BufferId(id),
          Id(planId),
          NO_ARGUMENT_STATE_MAPS,
          NO_ARGUMENT_STATE_MAP_INITIALIZATIONS,
          ArgumentStateBufferVariant(ArgumentStateMapId(asmId)))(SlotConfiguration.empty))
      pipelines(matchablePipeline.id.x) =
        matchablePipeline.copy(outputDefinition =
          if (fusedOutput) NoOutput
          else ReduceOutput(BufferId(id), ArgumentStateMapId(asmId)))
      new ArgumentStateBufferSequence(bd)
    }

    def argumentStreamBuffer(id: Int, argumentSlotOffset: Int, asmId: Int = -1, planId: Int = -1): ArgumentStreamBufferSequence = {
      val bd = buffers.getOrElseUpdate(id,
        BufferDefinition(
          BufferId(id),
          Id(planId),
          NO_ARGUMENT_STATE_MAPS,
          NO_ARGUMENT_STATE_MAP_INITIALIZATIONS,
          ArgumentStreamBufferVariant(ArgumentStateMapId(asmId), ArgumentStreamType))(SlotConfiguration.empty))
      val out = MorselArgumentStateBufferOutput(BufferId(id),argumentSlotOffset)
      pipelines(matchablePipeline.id.x) = matchablePipeline.copy(outputDefinition = out)
      new ArgumentStreamBufferSequence(bd)
    }

    def antiBuffer(id: Int, argumentSlotOffset: Int, asmId: Int = -1, planId: Int = -1): AntiBufferSequence = {
      val bd = buffers.getOrElseUpdate(id,
        BufferDefinition(
          BufferId(id),
          Id(planId),
          NO_ARGUMENT_STATE_MAPS,
          NO_ARGUMENT_STATE_MAP_INITIALIZATIONS,
          ArgumentStreamBufferVariant(ArgumentStateMapId(asmId), AntiType))(SlotConfiguration.empty))
      val out = MorselArgumentStateBufferOutput(BufferId(id),argumentSlotOffset)
      pipelines(matchablePipeline.id.x) = matchablePipeline.copy(outputDefinition = out)
      new AntiBufferSequence(bd)
    }

    def attachBuffer(id: Int, argumentSlotOffset: Int = -1, planId: Int = -1, slots: SlotConfiguration = SlotConfiguration.empty) : AttachBufferSequence = {
      val bd = buffers.getOrElseUpdate(id,
        BufferDefinition(
          BufferId(id),
          Id(planId),
          NO_ARGUMENT_STATE_MAPS,
          NO_ARGUMENT_STATE_MAP_INITIALIZATIONS,
          AttachBufferVariant(null, slots, argumentSlotOffset, SlotConfiguration.Size.zero)
        )(SlotConfiguration.empty)
      )
      val out = MorselBufferOutput(BufferId(id))
      pipelines(matchablePipeline.id.x) = matchablePipeline.copy(outputDefinition = out)
      new AttachBufferSequence(bd)
    }

    def applyBuffer(id: Int, argumentSlotOffset: Int = -1, planId: Int = -1): ApplyBufferSequence = {
      val bd = buffers.getOrElseUpdate(id,
        BufferDefinition(
          BufferId(id),
          Id(planId),
          NO_ARGUMENT_STATE_MAPS,
          NO_ARGUMENT_STATE_MAP_INITIALIZATIONS,
          ApplyBufferVariant(argumentSlotOffset, NO_ARGUMENT_STATE_MAP_INITIALIZATIONS, NO_ARGUMENT_STATE_MAPS, NO_BUFFERS))(SlotConfiguration.empty))
      val out = MorselBufferOutput(BufferId(id))
      pipelines(matchablePipeline.id.x) = matchablePipeline.copy(outputDefinition = out)
      new ApplyBufferSequence(bd)
    }

    def conditionalSink(conditionalId: Int, onTrueId: Int, onFalseId: Int, argumentSlotOffset: Int = -1, planId: Int = -1): ApplyBufferSequence = {
      val onTrue = buffers.getOrElseUpdate(onTrueId,
        BufferDefinition(
          BufferId(onTrueId),
          Id(planId),
          NO_ARGUMENT_STATE_MAPS,
          NO_ARGUMENT_STATE_MAP_INITIALIZATIONS,
          RegularBufferVariant)(SlotConfiguration.empty))
      val apply = applyBuffer(onFalseId, argumentSlotOffset, planId)
      val bd = buffers.getOrElseUpdate(conditionalId,
        BufferDefinition(
          BufferId(conditionalId),
          Id(planId),
          NO_ARGUMENT_STATE_MAPS,
          NO_ARGUMENT_STATE_MAP_INITIALIZATIONS,
          ConditionalBufferVariant(onTrue, buffers(onFalseId), null))(SlotConfiguration.empty))
      val out = MorselBufferOutput(BufferId(conditionalId))
      pipelines(matchablePipeline.id.x) = matchablePipeline.copy(outputDefinition = out)
      apply
    }

    def leftOfJoinBuffer(id: Int, argumentSlotOffset: Int, asmId: Int, planId: Int, rhsAsmId: Int): LhsJoinBufferSequence = {
      val out = MorselArgumentStateBufferOutput(BufferId(id), argumentSlotOffset)
      pipelines(matchablePipeline.id.x) = matchablePipeline.copy(outputDefinition = out)
      val lhsSink: BufferDefinition = buffers.getOrElseUpdate(id,
        BufferDefinition(
          BufferId(id),
          Id(planId),
          NO_ARGUMENT_STATE_MAPS,
          NO_ARGUMENT_STATE_MAP_INITIALIZATIONS,
          variant = LHSAccumulatingBufferVariant(ArgumentStateMapId(asmId), ArgumentStateMapId(rhsAsmId)),
        )(SlotConfiguration.empty))
      new LhsJoinBufferSequence(lhsSink)
    }

    def rightOfJoinBuffer(lhsId: Int, rhsId: Int, sourceId: Int, argumentSlotOffset: Int, lhsAsmId: Int, rhsAsmId: Int, planId: Int): JoinBufferSequence = {
      val out = MorselArgumentStateBufferOutput(BufferId(rhsId), argumentSlotOffset)
      pipelines(matchablePipeline.id.x) = matchablePipeline.copy(outputDefinition = out)
      val lhsSink: BufferDefinition = buffers.getOrElse(lhsId, throw new IllegalStateException(s"LHS Sink '$lhsId' should have already been created"))
      val rhsSink: BufferDefinition = buffers.getOrElseUpdate(rhsId,
        BufferDefinition(
          BufferId(rhsId),
          Id(planId),
          NO_ARGUMENT_STATE_MAPS,
          NO_ARGUMENT_STATE_MAP_INITIALIZATIONS,
          variant = RHSStreamingBufferVariant(ArgumentStateMapId(rhsAsmId), ArgumentStateMapId(lhsAsmId)),
        )(SlotConfiguration.empty))
      val bd: BufferDefinition = buffers.getOrElseUpdate(sourceId,
                                       BufferDefinition(
                                         BufferId(sourceId),
                                         Id(planId),
                                         NO_ARGUMENT_STATE_MAPS,
                                         NO_ARGUMENT_STATE_MAP_INITIALIZATIONS,
                                         LHSAccumulatingRHSStreamingBufferVariant(
                                           lhsSink,
                                           rhsSink,
            lhsSink.variant.asInstanceOf[LHSAccumulatingBufferVariant].argumentStateMapId,
            rhsSink.variant.asInstanceOf[RHSStreamingBufferVariant].argumentStateMapId)
                                       )(SlotConfiguration.empty))
      new JoinBufferSequence(bd)
    }

    def end: ExecutionGraphDefinitionMatcher = {
      pipelines(matchablePipeline.id.x) = matchablePipeline.copy(outputDefinition = ProduceResultOutput)
      ExecutionGraphDefinitionMatcher.this
    }

    def endFused: ExecutionGraphDefinitionMatcher = {
      pipelines(matchablePipeline.id.x) = matchablePipeline.copy(outputDefinition = NoOutput)
      ExecutionGraphDefinitionMatcher.this
    }
  }

  override def apply(graph: ExecutionGraphDefinition): MatchResult = {
    val expectedBuffers = buffers.values.toSeq.sortBy(_.id.x)
    val gotBuffers = graph.buffers.toSeq
    if (expectedBuffers != gotBuffers) {
      return MatcherWords.equal(expectedBuffers).matcher[Seq[BufferDefinition]].apply(gotBuffers)
    }

    val expectedASMs = argumentStates.values.toSeq.sortBy(_.id.x)
    val gotASMs = graph.argumentStateMaps.toSeq
    if (expectedASMs != gotASMs) {
      return MatcherWords.equal(expectedASMs).matcher[Seq[ArgumentStateDefinition]].apply(gotASMs)
    }

    val expectedPipelines = pipelines.values.toSeq.sortBy(_.id.x)
    val gotPipelines = graph.pipelines.map { got =>
      val (interpretedHead, fusedHead) =
        got.headPlan match {
          case FusedHead(fuser) => (Nil, fuser.fusedPlans)
          case InterpretedHead(plan) => (Seq(plan), Nil)
        }
      MatchablePipeline(got.id,
        (interpretedHead ++ got.middlePlans ++ produceResult(got.outputDefinition)).map(_.getClass),
        fusedHead.map(_.getClass),
        got.inputBuffer.id,
        out(got.outputDefinition),
        got.serial,
        got.workLimiter)

    }
    if (gotPipelines != expectedPipelines) {
      return MatcherWords.equal(expectedPipelines).matcher[Seq[MatchablePipeline]].apply(gotPipelines)
    }

    MatchResult(matches = true, "", "")
  }
}
