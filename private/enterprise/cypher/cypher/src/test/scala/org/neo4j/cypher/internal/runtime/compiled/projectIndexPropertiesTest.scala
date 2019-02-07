/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.compiled

import org.neo4j.cypher.internal.compiler.v4_0.planner.LogicalPlanningTestSupport2
import org.neo4j.cypher.internal.compiler.v4_0.planner.logical.PlanMatchHelp
import org.neo4j.cypher.internal.v4_0.logical.plans._
import org.neo4j.cypher.internal.v4_0.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.v4_0.expressions.{LabelToken, Property, Variable}
import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.v4_0.util.LabelId

class projectIndexPropertiesTest extends CypherFunSuite with LogicalPlanningTestSupport2 with PlanMatchHelp {

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
      val updater = projectIndexProperties
      val emptyTable = SemanticTable()

      val (newPlan, newTable) = updater(getValues, emptyTable)
      newPlan should equal(Projection(doNotGetValues, expectedProjections)(idGen))
      // We have to use the exact var in the plan so that the input position is the same
      val varInNewPlan = newPlan.asInstanceOf[Projection].projectExpressions("n.prop").asInstanceOf[Property].map.asInstanceOf[Variable]
      newTable.isNode(varInNewPlan) should be(true)
    }

    test(s"should not introduce projection for $operatorName without index properties") {
      val updater = projectIndexProperties
      val emptyTable = SemanticTable()

      val (newPlan, _) = updater(doNotGetValues, emptyTable)
      newPlan should equal(doNotGetValues)
    }

  }

}
