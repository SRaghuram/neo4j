/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
import org.neo4j.cypher.internal.runtime.compiled.expressions.ExpressionCompilation.NODE_CURSOR
import org.neo4j.cypher.internal.runtime.compiled.expressions.ExpressionCompilation.PROPERTY_CURSOR
import org.neo4j.cypher.internal.runtime.compiled.expressions.ExpressionCompilation.RELATIONSHIP_CURSOR
import org.neo4j.cypher.internal.runtime.compiled.expressions.ExpressionCompilation.nullCheckIfRequired
import org.neo4j.cypher.internal.runtime.compiled.expressions.ExpressionCompilation.vNODE_CURSOR
import org.neo4j.cypher.internal.runtime.compiled.expressions.ExpressionCompilation.vPROPERTY_CURSOR
import org.neo4j.cypher.internal.runtime.compiled.expressions.ExpressionCompilation.vRELATIONSHIP_CURSOR
import org.neo4j.cypher.internal.runtime.compiled.expressions.IntermediateExpression
import org.neo4j.cypher.internal.runtime.compiled.expressions.IntermediateExpression.EMPTY
import org.neo4j.cypher.internal.runtime.pipelined.MutableQueryStatistics
import org.neo4j.cypher.internal.runtime.pipelined.OperatorExpressionCompiler
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.DATA_READ
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
import org.neo4j.internal.kernel.api.NodeCursor
import org.neo4j.internal.kernel.api.PropertyCursor
import org.neo4j.internal.kernel.api.Token
import org.neo4j.internal.kernel.api.Write
import org.neo4j.values.AnyValue
import org.neo4j.values.virtual.VirtualNodeValue
import org.neo4j.values.virtual.VirtualRelationshipValue

class SetPropertiesFromMapOperatorTemplate(override val inner: OperatorTaskTemplate,
                                           override val id: Id,
                                           entity: () => IntermediateExpression, // n, r
                                           propertiesExpression: () => IntermediateExpression, // prop -> {"prop":1}
                                           removeOtherProps: Boolean)(protected val codeGen: OperatorExpressionCompiler) extends OperatorTaskTemplate {

  private var entityValue: IntermediateExpression = _
  private var nodePropertiesMap: IntermediateExpression = _ // {"prop": 1}
  private var relationshipPropertiesMap: IntermediateExpression = _ // {"prop": 1}
  //private val propertyCursorField = field[PropertyCursor](codeGen.namer.nextVariableName("propertyCursor"))


  override def genInit: IntermediateRepresentation = {
    inner.genInit
  }

  override def genOperate: IntermediateRepresentation = {
    if (entityValue == null) {
      entityValue = entity()
      nodePropertiesMap = propertiesExpression()
      relationshipPropertiesMap = propertiesExpression()
    }

    val entityValueVar = codeGen.namer.nextVariableName("entity")

    val nodeProps = nullCheckIfRequired(nodePropertiesMap)
    val relProps = nullCheckIfRequired(relationshipPropertiesMap)

    block(
      declareAndAssign(typeRefOf[AnyValue], entityValueVar, nullCheckIfRequired(entityValue)),
      ifElse(instanceOf[VirtualNodeValue](load(entityValueVar)))(
        setPropertyFromMap(
          isNode = true,
          invoke(cast[VirtualNodeValue](load(entityValueVar)), method[VirtualNodeValue, Long]("id")),
          nodeProps,
          removeOtherProps
        )
      )(ifElse(instanceOf[VirtualRelationshipValue](load(entityValueVar)))
      (
        setPropertyFromMap(
          isNode = false,
          invoke(cast[VirtualRelationshipValue](load(entityValueVar)), method[VirtualRelationshipValue, Long]("id")),
          relProps,
          removeOtherProps
        )
      )(
        block(
          condition(notEqual(load(entityValueVar), noValue))(
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

  override def genExpressions: Seq[IntermediateExpression] =
    Seq(entityValue, nodePropertiesMap, relationshipPropertiesMap) :+ EMPTY.withVariable(vNODE_CURSOR, vRELATIONSHIP_CURSOR, vPROPERTY_CURSOR)

  override def genLocalVariables: Seq[LocalVariable] = Seq(QUERY_STATS_TRACKER_V)

  override def genFields: Seq[Field] = Seq(DATA_WRITE, TOKEN, LOCKS)

  override def genCanContinue: Option[IntermediateRepresentation] = inner.genCanContinue

  override def genCloseCursors: IntermediateRepresentation = inner.genCloseCursors

  override protected def isHead: Boolean = false

  private def setPropertyFromMap(isNode: Boolean, id: IntermediateRepresentation, propValueMap: IntermediateRepresentation, removeOtherProps : Boolean): IntermediateRepresentation = {
    val errorVar = codeGen.namer.nextVariableName("errorN")
    val acquireLockFunction = if (isNode) "acquireExclusiveNodeLock" else "acquireExclusiveRelationshipLock"
    val releaseLockFunction = if (isNode) "releaseExclusiveNodeLock" else "releaseExclusiveRelationshipLock"
    val setFunction = if (isNode) "setNodePropertiesFromMap" else "setRelationshipPropertiesFromMap"

    block(
      invoke(loadField(LOCKS), method[Locks, Unit, Array[Long]](acquireLockFunction), arrayOf[Long](id)),
      IntermediateRepresentation.tryCatch[Exception](errorVar)
        (block(
          invokeStatic(
            method[SetOperator, Unit, Long, AnyValue, Boolean, Token, Write, MutableQueryStatistics, NodeCursor, PropertyCursor](setFunction),
            id,
            propValueMap,
            constant(removeOtherProps),
            loadField(TOKEN),
            loadField(DATA_WRITE),
            loadField(DATA_READ),
            QUERY_STATS_TRACKER,
            NODE_CURSOR,
            RELATIONSHIP_CURSOR,
            PROPERTY_CURSOR,
          ),
          invoke(loadField(LOCKS), method[Locks, Unit, Array[Long]](releaseLockFunction), arrayOf[Long](id)),
        ))
        (block(
          invoke(loadField(LOCKS), method[Locks, Unit, Array[Long]](releaseLockFunction), arrayOf[Long](id)),
          fail(load(errorVar))
        ))
    )
  }
}