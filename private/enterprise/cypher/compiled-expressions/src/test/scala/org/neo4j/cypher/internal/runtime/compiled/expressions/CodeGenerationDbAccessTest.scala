/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.compiled.expressions

import org.mockito.Mockito.when
import org.neo4j.cypher.internal.compatibility.v4_0.runtime.SlotConfiguration
import org.neo4j.cypher.internal.compatibility.v4_0.runtime.ast._
import org.neo4j.cypher.internal.runtime.{DbAccess, ExpressionCursors}
import org.neo4j.cypher.internal.runtime.interpreted.ExecutionContext
import org.neo4j.internal.kernel.api.NodeCursor
import org.neo4j.values.storable.Values
import org.neo4j.values.storable.Values.{NO_VALUE, stringValue}
import org.neo4j.values.virtual.VirtualValues.EMPTY_MAP
import org.neo4j.values.virtual.{NodeValue, RelationshipValue}
import org.opencypher.v9_0.ast.AstConstructionTestSupport
import org.opencypher.v9_0.expressions.{Expression, HasLabels, LabelName, SemanticDirection}
import org.opencypher.v9_0.util.test_helpers.CypherFunSuite

class CodeGenerationDbAccessTest extends CypherFunSuite with AstConstructionTestSupport {

  test("node property access") {
    // Given
    val expression = NodeProperty(nodeOffset, property, "prop")(null)

    // When
    val compiled = compile(expression)

    // Then
    compiled.evaluate(ctx, dbAccess, EMPTY_MAP, cursors) should equal(stringValue("hello"))
  }

  test("late node property access") {
    compile(NodePropertyLate(nodeOffset, "prop", "prop")(null)).evaluate(ctx, dbAccess, EMPTY_MAP, cursors) should
      equal(stringValue("hello"))
    compile(NodePropertyLate(nodeOffset, "notThere", "prop")(null)).evaluate(ctx, dbAccess, EMPTY_MAP, cursors) should
      equal(NO_VALUE)
  }

  test("relationship property access") {
    // Given
    val expression = RelationshipProperty(relOffset, property, "prop")(null)

    // When
    val compiled = compile(expression)

    // Then
    compiled.evaluate(ctx, dbAccess, EMPTY_MAP, cursors) should equal(stringValue("hello"))
  }

  test("late relationship property access") {
    compile(RelationshipPropertyLate(relOffset, "prop", "prop")(null)).evaluate(ctx, dbAccess, EMPTY_MAP, cursors) should
      equal(stringValue("hello"))
    compile(RelationshipPropertyLate(relOffset, "notThere", "prop")(null)).evaluate(ctx, dbAccess, EMPTY_MAP, cursors) should
      equal(NO_VALUE)
  }


  test("late cached node property access from tx state") {
    // Given
    val expression = CachedNodePropertyLate(nodeOffset, "txStateProp", cachedPropertyOffset)

    // When
    val compiled = compile(expression)

    // Then
    compiled.evaluate(ctx, dbAccess, EMPTY_MAP, cursors) should equal(stringValue("hello from tx state"))
  }

  test("late cached node property access") {
    // Given
    val expression = CachedNodePropertyLate(nodeOffset, "cachedProp", cachedPropertyOffset)

    // When
    val compiled = compile(expression)

    // Then
    compiled.evaluate(ctx, dbAccess, EMPTY_MAP, cursors) should equal(stringValue("hello from cache"))
  }

  test("getDegree without type") {
    compile(GetDegreePrimitive(nodeOffset, None, SemanticDirection.OUTGOING)).evaluate(ctx, dbAccess, EMPTY_MAP, cursors) should
      equal(Values.longValue(3))
    compile(GetDegreePrimitive(nodeOffset, None, SemanticDirection.INCOMING)).evaluate(ctx, dbAccess, EMPTY_MAP, cursors) should
      equal(Values.longValue(2))
    compile(GetDegreePrimitive(nodeOffset, None, SemanticDirection.BOTH)).evaluate(ctx, dbAccess, EMPTY_MAP, cursors) should
      equal(Values.longValue(5))
  }

  test("getDegree with type") {
    compile(GetDegreePrimitive(nodeOffset, Some(relType), SemanticDirection.OUTGOING))
          .evaluate(ctx, dbAccess, EMPTY_MAP, cursors) should
      equal(Values.longValue(2))
    compile(GetDegreePrimitive(nodeOffset, Some(relType), SemanticDirection.INCOMING))
          .evaluate(ctx, dbAccess, EMPTY_MAP, cursors) should
      equal(Values.longValue(1))
    compile(GetDegreePrimitive(nodeOffset, Some(relType), SemanticDirection.BOTH))
          .evaluate(ctx, dbAccess, EMPTY_MAP, cursors) should
      equal(Values.longValue(3))
  }

  test("NodePropertyExists") {
    compile(NodePropertyExists(nodeOffset, property, "prop")(null)).evaluate(ctx, dbAccess, EMPTY_MAP, cursors) should
      equal(Values.TRUE)
    compile(NodePropertyExists(nodeOffset, nonExistingProperty, "otherProp")(null))
          .evaluate(ctx, dbAccess, EMPTY_MAP, cursors) should
      equal(Values.FALSE)
  }

  test("NodePropertyExistsLate") {
    compile(NodePropertyExistsLate(nodeOffset, "prop", "prop")(null)).evaluate(ctx, dbAccess, EMPTY_MAP, cursors) should
      equal(Values.TRUE)
    compile(NodePropertyExistsLate(nodeOffset, "otherProp", "otherProp")(null))
          .evaluate(ctx, dbAccess, EMPTY_MAP, cursors) should
      equal(Values.FALSE)
  }

  test("RelationshipPropertyExists") {
    compile(RelationshipPropertyExists(relOffset, property, "prop")(null)).evaluate(ctx, dbAccess, EMPTY_MAP, cursors) should
      equal(Values.TRUE)
    compile(RelationshipPropertyExists(relOffset, nonExistingProperty, "otherProp")(null))
          .evaluate(ctx, dbAccess, EMPTY_MAP, cursors) should
      equal(Values.FALSE)
  }

  test("RelationshipPropertyExistsLate") {
    compile(RelationshipPropertyExistsLate(relOffset, "prop", "prop")(null)).evaluate(ctx, dbAccess, EMPTY_MAP, cursors) should
      equal(Values.TRUE)
    compile(RelationshipPropertyExistsLate(relOffset, "otherProp", "otherProp")(null))
          .evaluate(ctx, dbAccess, EMPTY_MAP, cursors) should
      equal(Values.FALSE)
  }

  test("NodeFromSlot") {
    // Given
    val expression = NodeFromSlot(nodeOffset, "foo")

    // When
    val compiled = compile(expression)

    // Then
    compiled.evaluate(ctx, dbAccess, EMPTY_MAP, cursors) should equal(nodeValue)
  }

  test("RelationshipFromSlot") {
    // Given
    val expression = RelationshipFromSlot(relOffset, "foo")

    // When
    val compiled = compile(expression)

    // Then
    compiled.evaluate(ctx, dbAccess, EMPTY_MAP, cursors) should equal(relationshipValue)
  }

  test("HasLabels") {
    compile(checkLabels("L1")).evaluate(ctx, dbAccess, EMPTY_MAP, cursors) should equal(Values.TRUE)
    compile(checkLabels("L1", "L2")).evaluate(ctx, dbAccess, EMPTY_MAP, cursors) should equal(Values.TRUE)
    compile(checkLabels("L1", "L3")).evaluate(ctx, dbAccess, EMPTY_MAP, cursors) should equal(Values.FALSE)
    compile(checkLabels("L2", "L3")).evaluate(ctx, dbAccess, EMPTY_MAP, cursors) should equal(Values.FALSE)
    compile(checkLabels("L1", "L2", "L3")).evaluate(ctx, dbAccess, EMPTY_MAP, cursors) should equal(Values.FALSE)
  }

  private def checkLabels(labels: String*) =
    HasLabels(NodeFromSlot(nodeOffset, "foo"), labels.toSeq.map(l => LabelName(l)(pos)))(pos)

  private def compile(e: Expression) =
    CodeGeneration.compileExpression(new IntermediateCodeGeneration(SlotConfiguration.empty).compileExpression(e).getOrElse(fail()))

  private val node = 11
  private val nodeValue = mock[NodeValue]
  private val relationshipValue = mock[RelationshipValue]
  private val relationship = 12
  private val relType = "R"
  private val relTypeId = 56
  private val property = 1337
  private val label1 = 100
  private val label2 = 101
  private val label3 = 102
  private val nonExistingProperty = 1338
  private val cachedProperty = 1339
  private val txStateProperty = 1340
  private val nodeOffset = 42
  private val relOffset = 43
  private val cachedPropertyOffset = 44
  private val ctx = mock[ExecutionContext]
  private val dbAccess = mock[DbAccess]
  private val nodeCursor = mock[NodeCursor]
  private val cursors = mock[ExpressionCursors]
  when(cursors.nodeCursor).thenReturn(nodeCursor)

  when(ctx.getLongAt(nodeOffset)).thenReturn(node)
  when(ctx.getLongAt(relOffset)).thenReturn(relationship)
  when(ctx.getCachedPropertyAt(cachedPropertyOffset)).thenReturn(stringValue("hello from cache"))
  when(dbAccess.nodeProperty(node, property)).thenReturn(stringValue("hello"))
  when(dbAccess.propertyKey("prop")).thenReturn(property)
  when(dbAccess.propertyKey("notThere")).thenReturn(nonExistingProperty)
  when(dbAccess.propertyKey("cachedProp")).thenReturn(cachedProperty)
  when(dbAccess.propertyKey("txStateProp")).thenReturn(txStateProperty)
  when(dbAccess.relationshipProperty(relationship, property)).thenReturn(stringValue("hello"))
  when(dbAccess.relationshipProperty(relationship, nonExistingProperty)).thenReturn(NO_VALUE)
  when(dbAccess.nodeProperty(node, property)).thenReturn(stringValue("hello"))
  when(dbAccess.nodeProperty(node, nonExistingProperty)).thenReturn(NO_VALUE)
  when(dbAccess.nodeHasProperty(node, property)).thenReturn(true)
  when(dbAccess.nodeHasProperty(node, nonExistingProperty)).thenReturn(false)
  when(dbAccess.relationshipHasProperty(relationship, property)).thenReturn(true)
  when(dbAccess.relationshipHasProperty(relationship, nonExistingProperty)).thenReturn(false)
  when(dbAccess.nodeGetOutgoingDegree(node)).thenReturn(3)
  when(dbAccess.nodeGetIncomingDegree(node)).thenReturn(2)
  when(dbAccess.nodeGetTotalDegree(node)).thenReturn(5)
  when(dbAccess.nodeGetOutgoingDegree(node, relTypeId, nodeCursor)).thenReturn(2)
  when(dbAccess.nodeGetIncomingDegree(node, relTypeId, nodeCursor)).thenReturn(1)
  when(dbAccess.nodeGetTotalDegree(node, relTypeId, nodeCursor)).thenReturn(3)
  when(nodeValue.id()).thenReturn(node)
  when(dbAccess.nodeById(node)).thenReturn(nodeValue)
  when(dbAccess.relationshipById(relationship)).thenReturn(relationshipValue)
  when(dbAccess.relationshipType(relType)).thenReturn(relTypeId)
  when(dbAccess.nodeLabel("L1")).thenReturn(label1)
  when(dbAccess.nodeLabel("L2")).thenReturn(label2)
  when(dbAccess.nodeLabel("L3")).thenReturn(label3)
  when(dbAccess.isLabelSetOnNode(label1, node, nodeCursor)).thenReturn(true)
  when(dbAccess.isLabelSetOnNode(label2, node, nodeCursor)).thenReturn(true)
  when(dbAccess.isLabelSetOnNode(label3, node, nodeCursor)).thenReturn(false)
  when(dbAccess.getTxStateNodePropertyOrNull(node, txStateProperty)).thenReturn(stringValue("hello from tx state"))
  when(dbAccess.getTxStateNodePropertyOrNull(node, cachedProperty)).thenReturn(null)

}
