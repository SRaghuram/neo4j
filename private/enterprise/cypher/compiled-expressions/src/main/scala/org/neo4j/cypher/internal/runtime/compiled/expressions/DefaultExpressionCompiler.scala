/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.compiled.expressions

import org.neo4j.codegen.api.CodeGeneration
import org.neo4j.codegen.api.IntermediateRepresentation
import org.neo4j.codegen.api.IntermediateRepresentation.assign
import org.neo4j.codegen.api.IntermediateRepresentation.block
import org.neo4j.codegen.api.IntermediateRepresentation.condition
import org.neo4j.codegen.api.IntermediateRepresentation.constant
import org.neo4j.codegen.api.IntermediateRepresentation.declareAndAssign
import org.neo4j.codegen.api.IntermediateRepresentation.invoke
import org.neo4j.codegen.api.IntermediateRepresentation.invokeStatic
import org.neo4j.codegen.api.IntermediateRepresentation.isNull
import org.neo4j.codegen.api.IntermediateRepresentation.load
import org.neo4j.codegen.api.IntermediateRepresentation.method
import org.neo4j.codegen.api.IntermediateRepresentation.typeRefOf
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.physicalplanning.SlottedRewriter.DEFAULT_NULLABLE
import org.neo4j.cypher.internal.physicalplanning.SlottedRewriter.DEFAULT_OFFSET_IS_FOR_LONG_SLOT
import org.neo4j.cypher.internal.physicalplanning.ast.SlottedCachedProperty
import org.neo4j.cypher.internal.runtime.DbAccess
import org.neo4j.cypher.internal.runtime.compiled.expressions.AbstractExpressionCompiler.NODE_PROPERTY
import org.neo4j.cypher.internal.runtime.compiled.expressions.AbstractExpressionCompiler.RELATIONSHIP_PROPERTY
import org.neo4j.cypher.internal.runtime.compiled.expressions.ExpressionCompilation.DB_ACCESS
import org.neo4j.cypher.internal.runtime.compiled.expressions.ExpressionCompilation.NODE_CURSOR
import org.neo4j.cypher.internal.runtime.compiled.expressions.ExpressionCompilation.PROPERTY_CURSOR
import org.neo4j.cypher.internal.runtime.compiled.expressions.ExpressionCompilation.RELATIONSHIP_CURSOR
import org.neo4j.cypher.operations.CypherFunctions
import org.neo4j.internal.kernel.api.NodeCursor
import org.neo4j.internal.kernel.api.PropertyCursor
import org.neo4j.internal.kernel.api.RelationshipScanCursor
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Value

class DefaultExpressionCompiler(slots: SlotConfiguration,
                                readOnly: Boolean,
                                codeGenerationMode: CodeGeneration.CodeGenerationMode,
                                namer: VariableNamer
                               ) extends AbstractExpressionCompiler(slots, readOnly, codeGenerationMode, namer) {

  override protected def getLongAt(offset: Int): IntermediateRepresentation = getLongFromExecutionContext(offset)

  override protected def getRefAt(offset: Int): IntermediateRepresentation = getRefFromExecutionContext(offset)

  override protected def setRefAt(offset: Int,
                                  value: IntermediateRepresentation): IntermediateRepresentation =
    setRefInExecutionContext(offset, value)

  override protected def setLongAt(offset: Int,
                                   value: IntermediateRepresentation): IntermediateRepresentation =
    setLongInExecutionContext(offset, value)

  override protected def setCachedPropertyAt(offset: Int,
                                             value: IntermediateRepresentation): IntermediateRepresentation =
    setCachedPropertyInExecutionContext(offset, value)

  override protected def getCachedPropertyAt(property: SlottedCachedProperty, getFromStore: IntermediateRepresentation): IntermediateRepresentation = {
    val variableName = namer.nextVariableName()
    block(
      declareAndAssign(typeRefOf[Value], variableName, getCachedPropertyFromExecutionContext(property.cachedPropertyOffset)),
      condition(isNull(load(variableName)))(
        block(
          assign(variableName, getFromStore),
          setCachedPropertyAt(property.cachedPropertyOffset, load(variableName)))
      ),
      load(variableName))
  }

  override protected def isLabelSetOnNode(labelToken: IntermediateRepresentation,
                                          offset: Int): IntermediateRepresentation =
    invoke(DB_ACCESS,
      method[DbAccess, Boolean, Int, Long, NodeCursor]("isLabelSetOnNode"),
      labelToken,
      getNodeIdAt(offset, DEFAULT_OFFSET_IS_FOR_LONG_SLOT, DEFAULT_NULLABLE),
      NODE_CURSOR)

  override protected def getNodeProperty(propertyToken: IntermediateRepresentation,
                                         offset: Int,
                                         offsetIsForLongSlot: Boolean,
                                         nullable: Boolean): IntermediateRepresentation =
    invoke(DB_ACCESS, NODE_PROPERTY,
      getNodeIdAt(offset, offsetIsForLongSlot, nullable), propertyToken, NODE_CURSOR, PROPERTY_CURSOR, constant(true))

  override protected def hasNodeProperty(propertyToken: IntermediateRepresentation, offset: Int): IntermediateRepresentation =
    invoke(DB_ACCESS, method[DbAccess, Boolean, Long, Int, NodeCursor, PropertyCursor]("nodeHasProperty"),
      getNodeIdAt(offset, DEFAULT_OFFSET_IS_FOR_LONG_SLOT, DEFAULT_NULLABLE), propertyToken, NODE_CURSOR, PROPERTY_CURSOR)

  override protected def getRelationshipProperty(propertyToken: IntermediateRepresentation,
                                                 offset: Int,
                                                 offsetIsForLongSlot: Boolean,
                                                 nullable: Boolean): IntermediateRepresentation =
    invoke(DB_ACCESS, RELATIONSHIP_PROPERTY,
      getRelationshipIdAt(offset, offsetIsForLongSlot, nullable), propertyToken, RELATIONSHIP_CURSOR, PROPERTY_CURSOR, constant(true))

  override protected def hasRelationshipProperty(propertyToken: IntermediateRepresentation, offset: Int): IntermediateRepresentation =
    invoke(DB_ACCESS, method[DbAccess, Boolean, Long, Int, RelationshipScanCursor, PropertyCursor]("relationshipHasProperty"),
      getRelationshipIdAt(offset, DEFAULT_OFFSET_IS_FOR_LONG_SLOT, DEFAULT_NULLABLE), propertyToken, RELATIONSHIP_CURSOR, PROPERTY_CURSOR)

  override protected def getProperty(key: String, container: IntermediateRepresentation): IntermediateRepresentation =
    invokeStatic(
      method[CypherFunctions, AnyValue, String, AnyValue, DbAccess, NodeCursor, RelationshipScanCursor, PropertyCursor]("propertyGet"),
      constant(key),
      container,
      DB_ACCESS,
      NODE_CURSOR,
      RELATIONSHIP_CURSOR,
      PROPERTY_CURSOR)
}
