package org.neo4j.cypher.internal.physicalplanning

import org.neo4j.cypher.internal.physicalplanning.ast.ParameterFromSlot
import org.neo4j.cypher.internal.v4_0.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.v4_0.logical.plans.{AllNodesScan, ProduceResult, Selection}
import org.neo4j.cypher.internal.v4_0.util.attribution.SequentialIdGen
import org.neo4j.cypher.internal.v4_0.util.symbols
import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherFunSuite

class SlottedParametersTest extends CypherFunSuite with AstConstructionTestSupport {
  implicit val idGen: SequentialIdGen = new SequentialIdGen()

  test("should rewrite plan") {
    //given
    val allNodes = AllNodesScan("x", Set.empty)
    val predicate = greaterThan(add(parameter("a", symbols.CTAny), parameter("b", symbols.CTAny)), literalInt(42))
    val produceResult = ProduceResult(Selection(Seq(predicate), allNodes), Seq("x"))


    //when
    val (newPlan, mapping) = slottedParameters(produceResult)

    //then
    val newPredicate = greaterThan(add(ParameterFromSlot(0, "a", symbols.CTAny), ParameterFromSlot( 1, "b", symbols.CTAny)), literalInt(42))
    mapping should equal(Map("a" -> 0, "b" -> 1))
    newPlan should equal(ProduceResult(Selection(Seq(newPredicate), allNodes), Seq("x")))
  }

}
