/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.operators

import org.neo4j.codegen.api.Field
import org.neo4j.codegen.api.InstanceField
import org.neo4j.codegen.api.IntermediateRepresentation
import org.neo4j.codegen.api.IntermediateRepresentation.and
import org.neo4j.codegen.api.IntermediateRepresentation.block
import org.neo4j.codegen.api.IntermediateRepresentation.condition
import org.neo4j.codegen.api.IntermediateRepresentation.constant
import org.neo4j.codegen.api.IntermediateRepresentation.declareAndAssign
import org.neo4j.codegen.api.IntermediateRepresentation.field
import org.neo4j.codegen.api.IntermediateRepresentation.ifElse
import org.neo4j.codegen.api.IntermediateRepresentation.invoke
import org.neo4j.codegen.api.IntermediateRepresentation.invokeSideEffect
import org.neo4j.codegen.api.IntermediateRepresentation.invokeStatic
import org.neo4j.codegen.api.IntermediateRepresentation.isNotNull
import org.neo4j.codegen.api.IntermediateRepresentation.load
import org.neo4j.codegen.api.IntermediateRepresentation.loadField
import org.neo4j.codegen.api.IntermediateRepresentation.loop
import org.neo4j.codegen.api.IntermediateRepresentation.method
import org.neo4j.codegen.api.IntermediateRepresentation.not
import org.neo4j.codegen.api.IntermediateRepresentation.setField
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.runtime.pipelined.OperatorExpressionCompiler
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.conditionallyProfileRow
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.cursorNext
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.profilingCursorNext
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.singleRelationship
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.internal.kernel.api.KernelReadTracer
import org.neo4j.internal.kernel.api.RelationshipScanCursor
import org.neo4j.internal.kernel.api.RelationshipValueIndexCursor
import org.neo4j.util.Preconditions

abstract class BaseRelationshipIndexTaskTemplate(inner: OperatorTaskTemplate,
                                                 id: Id,
                                                 val innermost: DelegateOperatorTaskTemplate,
                                                 val relOffset: Int,
                                                 val startOffset: Int,
                                                 val endOffset: Int,
                                                 val argumentSize: SlotConfiguration.Size,
                                                 codeGen: OperatorExpressionCompiler)
  extends InputLoopTaskTemplate(inner, id, innermost, codeGen) {
  protected val relIndexCursorField: InstanceField = field[RelationshipValueIndexCursor](codeGen.namer.nextVariableName("valueCursor"))
  protected val relScanCursorField: InstanceField = field[RelationshipScanCursor](codeGen.namer.nextVariableName("scanCursor"))

  protected def cacheProperties: IntermediateRepresentation

  override protected def genInnerLoop: IntermediateRepresentation = {
    val localRelVar = codeGen.namer.nextVariableName("rel")

    /**
     * {{{
     *   while (hasDemand && this.canContinue) {
     *     ...
     *     setLongAt(relOffset, relIndexCursor.relationshipReference())
     *     [set scan cursor to point at relationshipReference]
     *     setLongAt(startOffset, relScanCursor.sourceNodeReference())
     *     setLongAt(endOffset, relScanCursor.targetNodeReference())
     *     setCachedPropertyAt(relOffset, [getPropertyValue])
     *     << inner.genOperate >>
     *     this.canContinue = this.relIndexCursor.next()
     *   }
     * }}}
     */
    loop(and(innermost.predicate, loadField(canContinue)))(
      block(
        codeGen.copyFromInput(argumentSize.nLongs, argumentSize.nReferences),
        declareAndAssign(localRelVar, invoke(loadField(relIndexCursorField), method[RelationshipValueIndexCursor, Long]("relationshipReference"))),
        codeGen.setLongAt(relOffset, load[Long](localRelVar)),
        singleRelationship(load[Long](localRelVar), loadField(relScanCursorField)),
        invokeStatic(method[Preconditions, Unit, Boolean, String]("checkState"),
          cursorNext[RelationshipScanCursor](loadField(relScanCursorField)), constant("Missing relationship")),
        codeGen.setLongAt(startOffset, invoke(loadField(relScanCursorField), method[RelationshipScanCursor, Long]("sourceNodeReference"))),
        codeGen.setLongAt(endOffset, invoke(loadField(relScanCursorField), method[RelationshipScanCursor, Long]("targetNodeReference"))),
        cacheProperties,
        inner.genOperateWithExpressions,
        doIfInnerCantContinue(innermost.setUnlessPastLimit(canContinue, profilingCursorNext[RelationshipValueIndexCursor](loadField(relIndexCursorField), id, doProfile, codeGen.namer))),
        endInnerLoop
      )
    )
  }

  override def genMoreFields: Seq[Field] = Seq(relIndexCursorField, relScanCursorField)

  override def genSetExecutionEvent(event: IntermediateRepresentation): IntermediateRepresentation =
    block(
      condition(isNotNull(loadField(relIndexCursorField)))(block(
        invokeSideEffect(loadField(relIndexCursorField), method[RelationshipValueIndexCursor, Unit, KernelReadTracer]("setTracer"), event),
        invokeSideEffect(loadField(relScanCursorField), method[RelationshipScanCursor, Unit, KernelReadTracer]("setTracer"), event))
      ),
      inner.genSetExecutionEvent(event)
    )
}

trait UndirectedRelationshipIndexTask {
  self: BaseRelationshipIndexTaskTemplate =>

  protected val forwardDirection: Field = field[Boolean](codeGen.namer.nextVariableName(), constant(true))
  override protected def genInnerLoop: IntermediateRepresentation = {
    val localRelVar = codeGen.namer.nextVariableName("rel")
    /**
     * {{{
     *   while (hasDemand && this.canContinue) {
     *     ...
     *     if (forwardDirection) {
     *        setLongAt(relOffset, relIndexCursor.relationshipReference())
     *        [set scan cursor to point at relationshipReference]
     *        setLongAt(startOffset, relScanCursor.sourceNodeReference())
     *        setLongAt(endOffset, relScanCursor.targetNodeReference())
     *     } else {
     *        setLongAt(startOffset, relScanCursor.targetNodeReference())
     *        setLongAt(endOffset, relScanCursor.sourceNodeReference())
     *     }
     *     setCachedPropertyAt(relOffset, [getPropertyValue])
     *     << inner.genOperate >>
     *     forwardDirection = !forwardDirection
     *     this.canContinue = !forwardDirection || this.relIndexCursor.next()
     *   }
     * }}}
     */
    loop(and(innermost.predicate, loadField(canContinue)))(
      block(
        codeGen.copyFromInput(argumentSize.nLongs, argumentSize.nReferences),
        declareAndAssign(localRelVar, invoke(loadField(relIndexCursorField), method[RelationshipValueIndexCursor, Long]("relationshipReference"))),
        codeGen.setLongAt(relOffset, load[Long](localRelVar)),
        ifElse(loadField(forwardDirection)) {
          block(
            singleRelationship(load[Long](localRelVar), loadField(relScanCursorField)),
            invokeStatic(method[Preconditions, Unit, Boolean, String]("checkState"),
              cursorNext[RelationshipScanCursor](loadField(relScanCursorField)), constant("Missing relationship")),
            codeGen.setLongAt(startOffset, invoke(loadField(relScanCursorField), method[RelationshipScanCursor, Long]("sourceNodeReference"))),
            codeGen.setLongAt(endOffset, invoke(loadField(relScanCursorField), method[RelationshipScanCursor, Long]("targetNodeReference")))
          )
        } {//else
          block(
            codeGen.setLongAt(startOffset, invoke(loadField(relScanCursorField), method[RelationshipScanCursor, Long]("targetNodeReference"))),
            codeGen.setLongAt(endOffset, invoke(loadField(relScanCursorField), method[RelationshipScanCursor, Long]("sourceNodeReference")))
          )
        },
        cacheProperties,
        inner.genOperateWithExpressions,
        doIfInnerCantContinue(
          block(
            setField(forwardDirection, not(loadField(forwardDirection))),
            condition(loadField(forwardDirection)) {
              innermost.setUnlessPastLimit(canContinue, profilingCursorNext[RelationshipValueIndexCursor](loadField(relIndexCursorField), id, doProfile, codeGen.namer))
            },
            conditionallyProfileRow(not(loadField(forwardDirection)), id, doProfile)
          )
        ),
        endInnerLoop
      )
    )
  }
}
