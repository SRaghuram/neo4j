/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.compiled

import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport2
import org.neo4j.cypher.internal.compiler.planner.logical.PlanMatchHelp
import org.neo4j.cypher.internal.logical.plans._
import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.expressions.{NODE_TYPE, CachedProperty, LabelToken, Property, PropertyKeyName, Variable}
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.util.{InputPosition, LabelId}

class removeCachedPropertiesTest extends CypherFunSuite with LogicalPlanningTestSupport2 with PlanMatchHelp {

  type IndexOperator = GetValueFromIndexBehavior => IndexLeafPlan

  private val indexSeek: IndexOperator = getValue => IndexSeek("n:Awesome(prop = 42)", getValue)
  private val uniqueIndexSeek: IndexOperator = getValue => NodeUniqueIndexSeek(
    "n",
    LabelToken("Awesome", LabelId(0)),
    Seq(indexedProperty("prop", 0, getValue)),
    SingleQueryExpression(literalInt(42)),
    Set.empty,
    IndexOrderNone)
  private val indexContainsScan: IndexOperator = getValue => IndexSeek("n:Awesome(prop CONTAINS 'foo')", getValue)
  private val indexEndsWithScan: IndexOperator = getValue => IndexSeek("n:Awesome(prop ENDS WITH 'foo')", getValue)
  private val indexScan: IndexOperator = getValue => IndexSeek("n:Awesome(prop)", getValue)

  private val expectedProjections = Map("n.prop" -> prop("n", "prop"))

  private val indexOperators = Seq(indexSeek, uniqueIndexSeek, indexContainsScan, indexEndsWithScan, indexScan)

  for(indexOperator <- indexOperators) {

    val doNotGetValues = indexOperator(DoNotGetValue)
    val getValues = indexOperator(GetValue)
    val operatorName = getValues.getClass.getSimpleName

    test(s"should introduce projection for $operatorName with index properties") {
      val emptyTable = SemanticTable()

      val (newPlan, newTable) = removeCachedProperties(getValues, emptyTable)
      newPlan should equal(Projection(doNotGetValues, expectedProjections)(idGen))
      // We have to use the exact var in the plan so that the input position is the same
      val varInNewPlan = newPlan.asInstanceOf[Projection].projectExpressions("n.prop").asInstanceOf[Property].map.asInstanceOf[Variable]
      newTable.isNode(varInNewPlan) should be(true)
    }

    test(s"should not introduce projection for $operatorName without index properties") {
      val updater = removeCachedProperties
      val emptyTable = SemanticTable()

      val (newPlan, newTable) = updater(doNotGetValues, emptyTable)
      newPlan should equal(doNotGetValues)
      newTable should equal(emptyTable)
    }

  }

  test("should remove CachedProperties") {
    val pos0 = InputPosition(0, 0, 0)
    val updater = removeCachedProperties
    val emptyTable = SemanticTable()
    val plan = Projection(Argument(Set("n")), Map("np" -> CachedProperty("n", Variable("n")(pos), PropertyKeyName("p")(pos0.bumped()), NODE_TYPE)(pos0.bumped().bumped())))

    val (newPlan, newTable) = updater(plan, emptyTable)
    newPlan should equal(Projection(Argument(Set("n")), Map("np" -> Property(Variable("n")(pos), PropertyKeyName("p")(pos0.bumped()))(pos0.bumped().bumped()))))
    newTable should equal(emptyTable)
  }

}
