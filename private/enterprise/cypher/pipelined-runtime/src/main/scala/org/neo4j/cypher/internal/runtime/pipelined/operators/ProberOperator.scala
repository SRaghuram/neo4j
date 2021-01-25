/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.operators

import org.neo4j.codegen.api.Field
import org.neo4j.codegen.api.InstanceField
import org.neo4j.codegen.api.IntermediateRepresentation
import org.neo4j.codegen.api.IntermediateRepresentation.block
import org.neo4j.codegen.api.IntermediateRepresentation.field
import org.neo4j.codegen.api.IntermediateRepresentation.getStatic
import org.neo4j.codegen.api.IntermediateRepresentation.invoke
import org.neo4j.codegen.api.IntermediateRepresentation.invokeStatic
import org.neo4j.codegen.api.IntermediateRepresentation.loadField
import org.neo4j.codegen.api.IntermediateRepresentation.method
import org.neo4j.codegen.api.IntermediateRepresentation.staticConstant
import org.neo4j.codegen.api.LocalVariable
import org.neo4j.codegen.api.StaticField
import org.neo4j.cypher.internal.logical.plans.Prober
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.runtime.compiled.expressions.IntermediateExpression
import org.neo4j.cypher.internal.runtime.pipelined.OperatorExpressionCompiler
import org.neo4j.cypher.internal.runtime.pipelined.execution.Morsel
import org.neo4j.cypher.internal.runtime.pipelined.execution.PipelinedQueryState
import org.neo4j.cypher.internal.runtime.pipelined.execution.QueryResources
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.runtime.slotted.SlottedRow
import org.neo4j.cypher.internal.util.attribution.Id

/**
 * Operator that sends each row to an observing probe. Intended for test-use only.
 */
class ProberOperator(val workIdentity: WorkIdentity, probe: Prober.Probe) extends StatelessOperator {

  override def operate(morsel: Morsel,
                       state: PipelinedQueryState,
                       resources: QueryResources): Unit = {
    val cursor = morsel.readCursor()
    while (cursor.next()) {
      probe.onRow(cursor)
    }
  }
}

// TODO: Refine Probe so that you can specify specifically which slots you want to see. Then you may not have to create a row
class ProberOperatorTemplate(val inner: OperatorTaskTemplate,
                             override val id: Id,
                             probe: Prober.Probe)
                            (protected val codeGen: OperatorExpressionCompiler) extends OperatorTaskTemplate {
  private val probeField: StaticField = staticConstant[Prober.Probe](codeGen.namer.nextVariableName("probe"), probe)
  private val slotConfigField: StaticField = staticConstant[SlotConfiguration](codeGen.namer.nextVariableName("slotConfig"), codeGen.slots)

  private val probeRowField: InstanceField =
    field[SlottedRow](codeGen.namer.nextVariableName("probeRow"), createProbeRow) // Reuse a single row

  private def createProbeRow: IntermediateRepresentation = invokeStatic(method[SlottedRow, SlottedRow, SlotConfiguration]("apply"), getStatic(slotConfigField))
  private def getProbeRow: IntermediateRepresentation = loadField(probeRowField)

  override def genInit: IntermediateRepresentation = {
    inner.genInit
  }

  override def genOperate: IntermediateRepresentation = {
    // Capture all locals and inputs in the probe row
    val probeCompiler = codeGen.probeCompiler(getProbeRow)
    val writeProbeRow = probeCompiler.writeLocalsToSlots()

    block(
      writeProbeRow,
      invoke(getStatic(probeField), method[Prober.Probe, Unit, AnyRef]("onRow"), getProbeRow),
      inner.genOperateWithExpressions
    )
  }

  override def genExpressions: Seq[IntermediateExpression] = Seq.empty

  override def genLocalVariables: Seq[LocalVariable] = Seq.empty

  override def genFields: Seq[Field] = Seq(probeField, slotConfigField, probeRowField)

  override def genCanContinue: Option[IntermediateRepresentation] = inner.genCanContinue

  override def genCloseCursors: IntermediateRepresentation = inner.genCloseCursors

  override def genSetExecutionEvent(event: IntermediateRepresentation): IntermediateRepresentation = inner.genSetExecutionEvent(event)

  override protected def isHead: Boolean = false
}

