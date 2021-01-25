/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.operators

import org.neo4j.codegen.api.Field
import org.neo4j.codegen.api.IntermediateRepresentation
import org.neo4j.codegen.api.IntermediateRepresentation.arrayOf
import org.neo4j.codegen.api.IntermediateRepresentation.block
import org.neo4j.codegen.api.IntermediateRepresentation.cast
import org.neo4j.codegen.api.IntermediateRepresentation.condition
import org.neo4j.codegen.api.IntermediateRepresentation.constant
import org.neo4j.codegen.api.IntermediateRepresentation.constructor
import org.neo4j.codegen.api.IntermediateRepresentation.declareAndAssign
import org.neo4j.codegen.api.IntermediateRepresentation.fail
import org.neo4j.codegen.api.IntermediateRepresentation.ifElse
import org.neo4j.codegen.api.IntermediateRepresentation.instanceOf
import org.neo4j.codegen.api.IntermediateRepresentation.invoke
import org.neo4j.codegen.api.IntermediateRepresentation.invokeStatic
import org.neo4j.codegen.api.IntermediateRepresentation.load
import org.neo4j.codegen.api.IntermediateRepresentation.loadField
import org.neo4j.codegen.api.IntermediateRepresentation.method
import org.neo4j.codegen.api.IntermediateRepresentation.newInstance
import org.neo4j.codegen.api.IntermediateRepresentation.noValue
import org.neo4j.codegen.api.IntermediateRepresentation.notEqual
import org.neo4j.codegen.api.IntermediateRepresentation.typeRefOf
import org.neo4j.codegen.api.LocalVariable
import org.neo4j.cypher.internal.runtime.compiled.expressions.ExpressionCompilation.nullCheckIfRequired
import org.neo4j.cypher.internal.runtime.compiled.expressions.IntermediateExpression
import org.neo4j.cypher.internal.runtime.pipelined.MutableQueryStatistics
import org.neo4j.cypher.internal.runtime.pipelined.OperatorExpressionCompiler
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.DATA_WRITE
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.LOCKS
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.QUERY_STATS_TRACKER
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.QUERY_STATS_TRACKER_V
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.TOKEN
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.conditionallyProfileRow
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.dbHit
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.exceptions.InvalidArgumentException
import org.neo4j.internal.kernel.api.Locks
import org.neo4j.internal.kernel.api.Token
import org.neo4j.internal.kernel.api.Write
import org.neo4j.values.AnyValue
import org.neo4j.values.virtual.VirtualNodeValue
import org.neo4j.values.virtual.VirtualRelationshipValue

object SetPropertyOperatorTemplate {
  def setProperty(isNode: Boolean,
                  id: IntermediateRepresentation,
                  propertyKey: String,
                  propValue: IntermediateRepresentation,
                  codeGen: OperatorExpressionCompiler,
                  needsExclusiveLock: Boolean): IntermediateRepresentation = {
    val setFunction = if (isNode) "setNodeProperty" else "setRelationshipProperty"
    val setPropertyBlock = invokeStatic(
      method[SetOperator, Unit, Long, String, AnyValue, Token, Write, MutableQueryStatistics](setFunction),
      id,
      constant(propertyKey),
      propValue,
      loadField(TOKEN),
      loadField(DATA_WRITE),
      QUERY_STATS_TRACKER,
    )

    if (needsExclusiveLock) {
      val errorVar = codeGen.namer.nextVariableName("errorN")
      val acquireLockFunction = if (isNode) "acquireExclusiveNodeLock" else "acquireExclusiveRelationshipLock"
      val releaseLockFunction = if (isNode) "releaseExclusiveNodeLock" else "releaseExclusiveRelationshipLock"

      block(
        invoke(loadField(LOCKS), method[Locks, Unit, Array[Long]](acquireLockFunction), arrayOf[Long](id)),
        IntermediateRepresentation.tryCatch[Exception](errorVar)
          (block(
            setPropertyBlock,
            invoke(loadField(LOCKS), method[Locks, Unit, Array[Long]](releaseLockFunction), arrayOf[Long](id)),
          ))
          (block(
            invoke(loadField(LOCKS), method[Locks, Unit, Array[Long]](releaseLockFunction), arrayOf[Long](id)),
            fail(load[Exception](errorVar))
          ))
      )
    } else {
      setPropertyBlock
    }
  }
}

class SetPropertyOperatorTemplate(override val inner: OperatorTaskTemplate,
                                  override val id: Id,
                                  entity: () => IntermediateExpression,
                                  key: String,
                                  value: () => IntermediateExpression)(protected val codeGen: OperatorExpressionCompiler) extends OperatorTaskTemplate {

  private var entityValue: IntermediateExpression = _
  private var relationshipProperty: IntermediateExpression = _
  private var nodeProperty: IntermediateExpression = _

  override def genInit: IntermediateRepresentation = {
    inner.genInit
  }

  override def genOperate: IntermediateRepresentation = {
    if (entityValue == null) {
      entityValue = entity()
    }
    if (relationshipProperty == null) {
      // We need two separate IntermediateExpressions to make sure the property value is evaluated
      // at the correct place. If the "value()" contains a "oneTime"-expression, it would otherwise
      // end up with only evaluating the expression in the first condition.
      // Note: we can't evaluate the expression before the condition, since we need to lock the node/relationship
      // before evaluating the expression.
      relationshipProperty = value()
      nodeProperty = value()
    }

    val entityValueVar = codeGen.namer.nextVariableName("entity")
    val relProp = nullCheckIfRequired(relationshipProperty)
    val nodeProp = nullCheckIfRequired(nodeProperty)

    block(
      declareAndAssign(typeRefOf[AnyValue], entityValueVar, nullCheckIfRequired(entityValue)),
      ifElse(instanceOf[VirtualNodeValue](load[Long](entityValueVar)))(
        SetPropertyOperatorTemplate.setProperty(
          isNode = true,
          invoke(cast[VirtualNodeValue](load[Long](entityValueVar)), method[VirtualNodeValue, Long]("id")),
          key,
          nodeProp,
          codeGen,
          needsExclusiveLock = false
        )
      )(ifElse(instanceOf[VirtualRelationshipValue](load[Long](entityValueVar)))
      (
        SetPropertyOperatorTemplate.setProperty(
          isNode = false,
          invoke(cast[VirtualRelationshipValue](load[Long](entityValueVar)), method[VirtualRelationshipValue, Long]("id")),
          key,
          relProp,
          codeGen,
          needsExclusiveLock = false
        )
      )(
        block(
          condition(notEqual(load[Long](entityValueVar), noValue))(
            fail(newInstance(constructor[InvalidArgumentException, String], constant("Expected to set property on a node or a relationship.")))
          )
        )
      )),
      dbHit(loadField(executionEventField)),
      inner.genOperateWithExpressions,
      conditionallyProfileRow(innerCannotContinue, id, doProfile),
    )
  }

  override def genSetExecutionEvent(event: IntermediateRepresentation): IntermediateRepresentation =
    inner.genSetExecutionEvent(event)

  override def genExpressions: Seq[IntermediateExpression] = Seq(entityValue, relationshipProperty, nodeProperty)

  override def genLocalVariables: Seq[LocalVariable] = Seq(QUERY_STATS_TRACKER_V)

  override def genFields: Seq[Field] = Seq(DATA_WRITE, TOKEN, LOCKS)

  override def genCanContinue: Option[IntermediateRepresentation] = inner.genCanContinue

  override def genCloseCursors: IntermediateRepresentation = inner.genCloseCursors

  override protected def isHead: Boolean = false
}