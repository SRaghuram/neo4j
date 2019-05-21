/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.ExecutionEngineFunSuite
import org.scalatest.matchers.{MatchResult, Matcher}

class DebugToStringTest extends ExecutionEngineFunSuite {

  /**
    * This tests an internal feature that is not supported or critical for end users. Still nice to see that it works
    * and what the expected outputs are.
    */

  test("ast-tostring works") {
    val textResult = graph.execute(queryWithOutputOf("ast")).resultAsString()

    textResult should beLikeString(
      """+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
        || col                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
        |+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
        || "Query(None,SingleQuery(List(Match(false,Pattern(List(EveryPath(RelationshipChain(NodePattern(Some(Variable(a)),List(),None,None),RelationshipPattern(Some(Variable(  UNNAMED10)),List(RelTypeName(T)),None,None,OUTGOING,false,None),NodePattern(Some(Variable(b)),List(),None,None))))),List(),None), Return(false,ReturnItems(false,Vector(AliasedReturnItem(Variable(a),Variable(a)), AliasedReturnItem(Variable(b),Variable(b)))),None,None,None,Set()))))" |
        |+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
        |1 row
        |""".stripMargin)
  }

  test("logicalplan-tostring works") {
    val textResult = graph.execute(queryWithOutputOf("logicalPlan")).resultAsString()

    textResult should beLikeString(
      """+-----------------------------------------------------------------------------------+
        || col                                                                               |
        |+-----------------------------------------------------------------------------------+
        || "ProduceResult(Vector(a, b)) {"                                                   |
        || "  LHS -> Expand(a, OUTGOING, List(RelTypeName(T)), b,   UNNAMED10, ExpandAll) {" |
        || "    LHS -> AllNodesScan(a, Set()) {}"                                            |
        || "  }"                                                                             |
        || "}"                                                                               |
        |+-----------------------------------------------------------------------------------+
        |5 rows
        |""".stripMargin)
  }

  test("queryGraph-tostring works") {
    val textResult = graph.execute(queryWithOutputOf("queryGraph")).resultAsString()
    val expected = """+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
                      || col                                                                                                                                                                                                                                                                                                             |
                      |+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
                      || "UnionQuery(List(RegularPlannerQuery(QueryGraph {Nodes: ['a', 'b'], Rels: ['(a)--[  UNNAMED10:T]->-(b)']},InterestingOrder(RequiredOrderCandidate(List()),List()),RegularQueryProjection(Map(a -> Variable(a), b -> Variable(b)),QueryPagination(None,None),Selections(Set())),None)),false,Vector(a, b),None)" |
                      |+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
                      |1 row
                      |""".stripMargin
    textResult should beLikeString(expected)
  }

  test("semanticState-tostring works") {
    val textResult = graph.execute(queryWithOutputOf("semanticState")).resultAsString()

    // There's no good toString for SemanticState, which makes it difficult to test the output in a good way
    textResult should include("ScopeLocation")
  }

  test("cost reporting") {
    val stringResult = graph.execute("CYPHER debug=dumpCosts MATCH (a:A) RETURN *").resultAsString()

        stringResult should beLikeString(
          """+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
            || # | planId | planText                                                            | planCost                                                           | cost   | est cardinality | winner |
            |+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
            || 1 | 0      | ""                                                                  | ""                                                                 | 10.0   | 10.0            | "WON"  |
            || 1 | <null> | "NodeByLabelScan(a, LabelName(A), Set()) {}"                        | "NodeByLabelScan costs Cost(10.0) cardinality Cardinality(10.0)"   | <null> | <null>          | <null> |
            || 1 | 2      | ""                                                                  | ""                                                                 | 22.0   | 10.0            | "LOST" |
            || 1 | <null> | "Selection(Ands(Set(HasLabels(Variable(a),List(LabelName(A)))))) {" | "Selection costs Cost(22.0) cardinality Cardinality(10.0)"         | <null> | <null>          | <null> |
            || 1 | <null> | "  LHS -> AllNodesScan(a, Set()) {}"                                | "  AllNodesScan costs Cost(12.0) cardinality Cardinality(10.0)"    | <null> | <null>          | <null> |
            || 1 | <null> | "}"                                                                 | ""                                                                 | <null> | <null>          | <null> |
            || 1 | 0      | ""                                                                  | ""                                                                 | 0.0    | 1.0             | "LOST" |
            || 1 | <null> | "*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-"                      | "*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-"                     | <null> | <null>          | <null> |
            || 0 | 2      | ""                                                                  | ""                                                                 | 22.0   | 10.0            | "WON"  |
            || 0 | <null> | "Selection(Ands(Set(HasLabels(Variable(a),List(LabelName(A)))))) {" | "Selection costs Cost(22.0) cardinality Cardinality(10.0)"         | <null> | <null>          | <null> |
            || 0 | <null> | "  LHS -> AllNodesScan(a, Set()) {}"                                | "  AllNodesScan costs Cost(12.0) cardinality Cardinality(10.0)"    | <null> | <null>          | <null> |
            || 0 | <null> | "}"                                                                 | ""                                                                 | <null> | <null>          | <null> |
            || 0 | 4      | ""                                                                  | ""                                                                 | 77.0   | 10.0            | "LOST" |
            || 0 | <null> | "NodeHashJoin(Set(a)) {"                                            | "NodeHashJoin costs Cost(77.0) cardinality Cardinality(10.0)"      | <null> | <null>          | <null> |
            || 0 | <null> | "  LHS -> AllNodesScan(a, Set()) {}"                                | "  AllNodesScan costs Cost(12.0) cardinality Cardinality(10.0)"    | <null> | <null>          | <null> |
            || 0 | <null> | "  RHS -> NodeByLabelScan(a, LabelName(A), Set()) {}"               | "  NodeByLabelScan costs Cost(10.0) cardinality Cardinality(10.0)" | <null> | <null>          | <null> |
            || 0 | <null> | "}"                                                                 | ""                                                                 | <null> | <null>          | <null> |
            || 0 | 0      | ""                                                                  | ""                                                                 | 0.0    | 1.0             | "LOST" |
            || 0 | <null> | "*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-"                      | "*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-"                     | <null> | <null>          | <null> |
            |+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
            |19 rows
            |""".stripMargin)
  }

  test("cost of non-trivial cartesians") {
    val stringResult = graph.execute("CYPHER debug=dumpCosts MATCH (a), (b) WHERE a.p = 1 AND b.p = 2 RETURN *").resultAsString()

    stringResult should beLikeString(
      """+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
        || # | planId | planText                                                                                                                         | planCost                                                                        | cost   | est cardinality    | winner |
        |+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
        || 0 | 4      | ""                                                                                                                               | ""                                                                              | 44.0   | 1.0000000000000002 | "WON"  |
        || 0 | <null> | "CartesianProduct() {"                                                                                                           | "CartesianProduct costs Cost(44.0) cardinality Cardinality(1.0000000000000002)" | <null> | <null>             | <null> |
        || 0 | <null> | "  LHS -> Selection(Ands(Set(In(Property(Variable(a),PropertyKeyName(p)),ListLiteral(List(Parameter(  AUTOINT0,Integer))))))) {" | "  Selection costs Cost(22.0) cardinality Cardinality(1.0)"                     | <null> | <null>             | <null> |
        || 0 | <null> | "    LHS -> AllNodesScan(a, Set()) {}"                                                                                           | "    AllNodesScan costs Cost(12.0) cardinality Cardinality(10.0)"               | <null> | <null>             | <null> |
        || 0 | <null> | "  }"                                                                                                                            | ""                                                                              | <null> | <null>             | <null> |
        || 0 | <null> | "  RHS -> Selection(Ands(Set(In(Property(Variable(b),PropertyKeyName(p)),ListLiteral(List(Parameter(  AUTOINT1,Integer))))))) {" | "  Selection costs Cost(22.0) cardinality Cardinality(1.0)"                     | <null> | <null>             | <null> |
        || 0 | <null> | "    LHS -> AllNodesScan(b, Set()) {}"                                                                                           | "    AllNodesScan costs Cost(12.0) cardinality Cardinality(10.0)"               | <null> | <null>             | <null> |
        || 0 | <null> | "  }"                                                                                                                            | ""                                                                              | <null> | <null>             | <null> |
        || 0 | <null> | "}"                                                                                                                              | ""                                                                              | <null> | <null>             | <null> |
        || 0 | 5      | ""                                                                                                                               | ""                                                                              | 44.0   | 1.0000000000000002 | "LOST" |
        || 0 | <null> | "CartesianProduct() {"                                                                                                           | "CartesianProduct costs Cost(44.0) cardinality Cardinality(1.0000000000000002)" | <null> | <null>             | <null> |
        || 0 | <null> | "  LHS -> Selection(Ands(Set(In(Property(Variable(b),PropertyKeyName(p)),ListLiteral(List(Parameter(  AUTOINT1,Integer))))))) {" | "  Selection costs Cost(22.0) cardinality Cardinality(1.0)"                     | <null> | <null>             | <null> |
        || 0 | <null> | "    LHS -> AllNodesScan(b, Set()) {}"                                                                                           | "    AllNodesScan costs Cost(12.0) cardinality Cardinality(10.0)"               | <null> | <null>             | <null> |
        || 0 | <null> | "  }"                                                                                                                            | ""                                                                              | <null> | <null>             | <null> |
        || 0 | <null> | "  RHS -> Selection(Ands(Set(In(Property(Variable(a),PropertyKeyName(p)),ListLiteral(List(Parameter(  AUTOINT0,Integer))))))) {" | "  Selection costs Cost(22.0) cardinality Cardinality(1.0)"                     | <null> | <null>             | <null> |
        || 0 | <null> | "    LHS -> AllNodesScan(a, Set()) {}"                                                                                           | "    AllNodesScan costs Cost(12.0) cardinality Cardinality(10.0)"               | <null> | <null>             | <null> |
        || 0 | <null> | "  }"                                                                                                                            | ""                                                                              | <null> | <null>             | <null> |
        || 0 | <null> | "}"                                                                                                                              | ""                                                                              | <null> | <null>             | <null> |
        || 0 | 0      | ""                                                                                                                               | ""                                                                              | 0.0    | 1.0                | "LOST" |
        || 0 | <null> | "*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-"                                                                                   | "*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-"                                  | <null> | <null>             | <null> |
        |+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
        |20 rows
        |""".stripMargin)
  }

  private def queryWithOutputOf(s: String) = s"CYPHER debug=tostring debug=$s MATCH (a)-[:T]->(b) RETURN *"

  class IsLikeString(expected: String) extends Matcher[String] {
    private def stripOsLineEndings(string: String): String =
      string.replace("\r\n", "\n")

    def apply(text: String) = MatchResult(
      stripOsLineEndings(text).contains(stripOsLineEndings(expected)),
      s"expected $text to contain the substring $expected",
      s"expected $text to not contain the substring $expected")
  }

  def beLikeString[T](expected: String) = new IsLikeString(expected)
}
