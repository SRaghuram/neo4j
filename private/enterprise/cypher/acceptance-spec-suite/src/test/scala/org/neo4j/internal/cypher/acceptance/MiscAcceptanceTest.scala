/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance

import java.util.concurrent.TimeUnit

import org.neo4j.cypher.ExecutionEngineFunSuite
import org.neo4j.internal.cypher.acceptance.comparisonsupport.{Configs, CypherComparisonSupport}

class MiscAcceptanceTest extends ExecutionEngineFunSuite with CypherComparisonSupport {

  // This test verifies a bugfix in slotted runtime
  test("should be able to compare integers") {
    val query = """
      UNWIND range(0, 1) AS i
      UNWIND range(0, 1) AS j
      WITH i, j
      WHERE i <> j
      RETURN i, j"""

    val result = executeWith(Configs.InterpretedAndSlottedAndMorsel, query)
    result.toList should equal(List(Map("j" -> 1, "i" -> 0), Map("j" -> 0, "i" -> 1)))
  }

  test("should be able to compare booleans") {
    val query = "WITH true AS t, false AS f RETURN t<f, t>f"

    val result = executeWith(Configs.InterpretedAndSlottedAndMorsel, query)
    result.toList should equal(List(Map("t<f" -> false, "t>f" -> true)))
  }

  test("order by after projection") {
    val query =
      """
        |UNWIND [ 1,2 ] as x
        |UNWIND [ 3,4 ] as y
        |RETURN x AS y, y as y3
        |ORDER BY y
      """.stripMargin

    val result = executeWith(Configs.All, query)
    result.toList should equal(List(Map("y" -> 1, "y3" -> 3), Map("y" -> 1, "y3" -> 4), Map("y" -> 2, "y3" -> 3), Map("y" -> 2, "y3" -> 4)))
  }

  test("should be able to use long values for LIMIT in interpreted runtime") {
    val a = createNode()
    val b = createNode()

    val limit: Long = Int.MaxValue + 1l
    // If we would use Ints for storing the limit, then we would end up with "limit 0"
    // thus, if we actually return the two nodes, then it proves that we used a long
    val query = "MATCH (n) RETURN n LIMIT " + limit
    val result = executeWith(Configs.All, query)
    result.toList should equal(List(Map("n" -> a), Map("n" -> b)))
  }

  test("should not explode on complex pattern comprehension projection in write query") {

    val query = """UNWIND [{children : [
                  |            {_type : "browseNodeId", _text : "20" },
                  |            {_type : "childNodes", _text : "21" }
                  |        ]},
                  |       {children : [
                  |            {_type : "browseNodeId", _text : "30" },
                  |            {_type : "childNodes", _text : "31" }
                  |        ]}] AS row
                  |
                  |WITH   head([child IN row.children WHERE child._type = "browseNodeId"])._text as nodeId,
                  |       head([child IN row.children WHERE child._type = "childNodes"]) as childElement
                  |
                  |MERGE  (parent:Category { id: toInteger(nodeId) })
                  |
                  |RETURN *""".stripMargin

    val result = graph.execute(query)
    result.resultAsString() // should not explode
  }

  test("optional match with distinct should take labels into account") {

    val setupQuery =
      """
        |CREATE (s1: StartLabel1), (s2: StartLabel2)
        |CREATE (s1)-[:REL]->(:MidLabel1)-[:REL]->(:End)
        |CREATE (s1)-[:REL]->(:MidLabel2)-[:REL]->(:End)
        |CREATE (s2)-[:REL]->(:MidLabel1)-[:REL]->(:End)
        |CREATE (s2)-[:REL]->(:MidLabel2)-[:REL]->(:End)
      """.stripMargin

    executeSingle(setupQuery)

    val countQuery =
      """
        |OPTIONAL MATCH (:StartLabel1)-->(:MidLabel1)-->(end:End)
        |RETURN COUNT(end) as count
      """.stripMargin

    val countResult = executeWith(Configs.Optional, countQuery)

    countResult.toList should be(List(Map("count" -> 1)))

    val countDistinctQuery =
      """
        |OPTIONAL MATCH (:StartLabel1)-->(:MidLabel1)-->(end:End)
        |RETURN COUNT(DISTINCT end) as count
      """.stripMargin

    val countDistinctResult = executeWith(Configs.InterpretedAndSlotted, countDistinctQuery)

    countDistinctResult.toList should be(List(Map("count" -> 1)))
  }

  test("should be able to plan customer query using outer join and alias (ZenDesk ticket #6628)") {
    graph.inTx {
      executeSingle("CREATE INDEX ON :L0(p0)")
      executeSingle("CREATE INDEX ON :L1(p1)")
      executeSingle("CREATE INDEX ON :L2(p2,p3)")
    }

    graph.inTx {
      graph.schema.awaitIndexesOnline(10, TimeUnit.SECONDS)
    }

    // The query was run through IdAnonymizer
    val query =
      s"""
         |MATCH (var0:L0 {p0: $$param0})
         |USING INDEX var0:L0(p0)
         |MATCH (var1:L1 {p1: $$param1})
         |  WHERE (var0)-[:UNKNOWN0]->(:UNKNOWN1)-[:UNKNOWN2]->(var1) OR (var0)-[:UNKNOWN3]-(:UNKNOWN4)-[:UNKNOWN5]-(var1)
         |MATCH (var0)-[:UNKNOWN0]->(var2:UNKNOWN6)
         |WITH COLLECT(var2) AS var2, var0
         |MATCH (var1:L1 {p1: $$param1})-[:UNKNOWN7]-(var3:UNKNOWN8)
         |MATCH (var3:UNKNOWN8)-[:UNKNOWN9]->(var4:UNKNOWN10)
         |MATCH (var5:UNKNOWN6)<-[:UNKNOWN5]-(var6:UNKNOWN1)-[:UNKNOWN2]-(var1)
         |  WHERE var5 IN var2
         |WITH var5 AS var2, var1, var6, var3, var4, var0
         |OPTIONAL MATCH (var1)-[:UNKNOWN11]->(var7:UNKNOWN12)
         |OPTIONAL MATCH (var1)-[:UNKNOWN13]->(var8:UNKNOWN14)
         |OPTIONAL MATCH (var1)-[:UNKNOWN5]->(var9:UNKNOWN15)
         |OPTIONAL MATCH (var1)-[:UNKNOWN16]->(var10:L1)
         |OPTIONAL MATCH (var1)-[:UNKNOWN17]->(var11:UNKNOWN18)
         |OPTIONAL MATCH (var11)-[:UNKNOWN5]->(var12:UNKNOWN19)
         |OPTIONAL MATCH (var1)-[:UNKNOWN20]->(var13:UNKNOWN21)
         |WITH var2, var1, var6, var3, var4, var7, var8, var9, var10, var0, var11, var12, var13
         |MATCH (var1)-[:UNKNOWN22]-(var14:UNKNOWN23)
         |MATCH (var1)-[:UNKNOWN24]-(var15:UNKNOWN25)
         |MATCH (var14)-[var16:UNKNOWN26]->(var15)
         |MATCH (var1)-[:UNKNOWN27]-(var17:UNKNOWN28)
         |MATCH (var1)-[:UNKNOWN29]-(var18:UNKNOWN30)
         |MATCH (var2)-[:UNKNOWN29]->(var19:UNKNOWN31)
         |MATCH (var6)<-[:UNKNOWN0]-(var20:L0)
         |OPTIONAL MATCH (var1)-[:UNKNOWN32]->(var21:UNKNOWN33)
         |OPTIONAL MATCH (var21)-[:UNKNOWN34]->(var22:UNKNOWN23)
         |OPTIONAL MATCH (var21)-[:UNKNOWN35]->(var23:UNKNOWN25)
         |OPTIONAL MATCH (var21)-[:UNKNOWN36]->(var24:UNKNOWN37)-[:UNKNOWN38]->(var25:UNKNOWN39)
         |OPTIONAL MATCH (var1:L1)-[:UNKNOWN7]->(var3:UNKNOWN8)-[:UNKNOWN40]->(var26:UNKNOWN41)
         |OPTIONAL MATCH (var18)-[:UNKNOWN42]->(var27:UNKNOWN19)
         |OPTIONAL MATCH (var6)-[:UNKNOWN43]->(var28:UNKNOWN44)
         |OPTIONAL MATCH (var29:UNKNOWN45)-[:UNKNOWN5]->(var1)
         |OPTIONAL MATCH (var30:L0)-[:UNKNOWN3]-(var29)-[:UNKNOWN46]-(var31:UNKNOWN47)
         |OPTIONAL MATCH (var32:UNKNOWN4)-[:UNKNOWN5]->(var1)
         |OPTIONAL MATCH (var33:L0)-[:UNKNOWN3]-(var32)-[:UNKNOWN46]->(var34:UNKNOWN47)
         |OPTIONAL MATCH (var1)-[:UNKNOWN2]-(var6)-[:UNKNOWN48]-(var35:L2)
         |OPTIONAL MATCH (var36:L0)-[:UNKNOWN48]-(var35)-[:UNKNOWN46]->(var37:UNKNOWN47)
         |OPTIONAL MATCH (var6)<-[:UNKNOWN49]-(:L2 {p2: "string[1]", p3: "string[1]"})<-[:UNKNOWN48]-(var38:UNKNOWN50 {UNKNOWN51: "string[6]"})
         |USING JOIN ON var6 // <- ###################### This is needed to reproduce the problematic plan that failed in slot allocation ####################
         |OPTIONAL MATCH (var1)-[:UNKNOWN52]->(var39)
         |OPTIONAL MATCH (var1)-[:UNKNOWN53]->(var40)
         |OPTIONAL MATCH (var0)-[var41:UNKNOWN54]->(var1)
         |RETURN var1, var6{.*, UNKNOWN55: var2.p0, UNKNOWN56: collect(DISTINCT var20)},
         |var28.p0 AS var42, var3 AS var43, var26 AS var26, var2.p0 AS var44, var2.p1 AS var45, var14, var15, var19, var4, var16.UNKNOWN57 AS var46,
         |var17.UNKNOWN58 AS var47, var18, var27, var39, var40, var41, var11, var12, var13, var7,
         |collect(var21{.*, UNKNOWN59: var22, UNKNOWN60: var23}) AS var48, collect(var25) AS var49,
         |case when count(var38) > 0 then "string[8]" else "string[8]" end AS var50,
         |[var51 IN collect(DISTINCT {UNKNOWN61: var8}) WHERE var51.UNKNOWN61 IS NOT NULL] AS var52, var9, var10,
         |[var53 IN collect(DISTINCT {UNKNOWN62: var30, UNKNOWN63: var31}) WHERE var53.UNKNOWN62 IS NOT NULL] AS var54,
         |[var55 IN collect(DISTINCT {UNKNOWN62: var33, UNKNOWN63: var34}) WHERE var55.UNKNOWN62 IS NOT NULL] AS var56,
         |[var57 IN collect(DISTINCT {UNKNOWN62: var36, UNKNOWN63: var37}) WHERE var57.UNKNOWN62 IS NOT NULL] AS var58
       """.stripMargin

    // Should plan without throwing exception
    // We actually execute it rather than just EXPLAIN, just to make sure that physical planning also happens in all versions of Neo4j
    val params = Map("param0" -> "", "param1" -> "", "param2" -> "")

    val result = executeWith(Configs.OptionalExpand, query, params = params)

    result.toList shouldBe empty

    // This is not a strict requirement. NodeRightOuterHashJoin would also be OK. Also if planner changes needs to happen, don't let this block you.
    result.executionPlanDescription should includeSomewhere.aPlan("NodeLeftOuterHashJoin")
  }

  test("should get degree of node from a parameter") {
    val node = createLabeledNode("Item")
    relate(createNode(), node, "CONTAINS")
    val result = executeWith(Configs.InterpretedAndSlottedAndMorsel, "UNWIND $param as p MATCH (p:Item) RETURN size((p)<-[:CONTAINS]-()) as total", params = Map("param" -> List(node)))
    result.toList should equal(List(Map("total" -> 1)))
  }

}
