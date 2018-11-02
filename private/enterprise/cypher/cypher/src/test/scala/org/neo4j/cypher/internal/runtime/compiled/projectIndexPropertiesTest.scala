/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.compiled

import org.neo4j.cypher.internal.compiler.v3_5.planner.LogicalPlanningTestSupport2
import org.neo4j.cypher.internal.v3_5.logical.plans._
import org.opencypher.v9_0.ast.semantics.SemanticTable
import org.opencypher.v9_0.expressions._
import org.opencypher.v9_0.util.test_helpers.CypherFunSuite
import org.opencypher.v9_0.util.{LabelId, PropertyKeyId}

class projectIndexPropertiesTest extends CypherFunSuite with LogicalPlanningTestSupport2 {

  type IndexOperator = GetValueFromIndexBehavior => IndexLeafPlan

  val indexSeek: IndexOperator = getValue => IndexSeek("n:Awesome(prop = 42)", getValue)
  val uniqueIndexSeek: IndexOperator = getValue => NodeUniqueIndexSeek(
    "n",
    LabelToken("Awesome", LabelId(0)),
    Seq(IndexedProperty(PropertyKeyToken(PropertyKeyName("prop") _, PropertyKeyId(0)), getValue)),
    SingleQueryExpression(SignedDecimalIntegerLiteral("42") _),
    Set.empty,
    IndexOrderNone)
  val indexContainsScan: IndexOperator = getValue => IndexSeek("n:Awesome(prop CONTAINS 'foo')", getValue)
  val indexEndsWithScan: IndexOperator = getValue => IndexSeek("n:Awesome(prop ENDS WITH 'foo')", getValue)
  val indexScan: IndexOperator = getValue => IndexSeek("n:Awesome(prop)", getValue)

  val expectedProjections = Map("n.prop" -> prop("n", "prop"))

  val indexOperators = Seq(indexSeek, uniqueIndexSeek, indexContainsScan, indexEndsWithScan, indexScan)

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

      val (newPlan, newTable) = updater(doNotGetValues, emptyTable)
      newPlan should equal(doNotGetValues)
    }

  }

}
