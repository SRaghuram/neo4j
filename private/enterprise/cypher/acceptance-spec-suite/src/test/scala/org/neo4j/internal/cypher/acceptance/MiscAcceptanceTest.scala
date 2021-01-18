/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance

import java.util
import java.util.concurrent.TimeUnit

import com.neo4j.cypher.RunWithConfigTestSupport
import org.neo4j.configuration.GraphDatabaseInternalSettings.cypher_idp_solver_duration_threshold
import org.neo4j.configuration.GraphDatabaseInternalSettings.cypher_idp_solver_table_threshold
import org.neo4j.configuration.GraphDatabaseInternalSettings.cypher_pipelined_batch_size_small
import org.neo4j.cypher.ExecutionEngineFunSuite
import org.neo4j.internal.cypher.acceptance.comparisonsupport.Configs
import org.neo4j.internal.cypher.acceptance.comparisonsupport.CypherComparisonSupport

class MiscAcceptanceTest extends ExecutionEngineFunSuite with CypherComparisonSupport with RunWithConfigTestSupport {

  // This test verifies a bugfix in slotted runtime
  test("should be able to compare integers") {
    val query = """
      UNWIND range(0, 1) AS i
      UNWIND range(0, 1) AS j
      WITH i, j
      WHERE i <> j
      RETURN i, j"""

    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query)
    result.toList should equal(List(Map("j" -> 1, "i" -> 0), Map("j" -> 0, "i" -> 1)))
  }

  test("should be able to compare booleans") {
    val query = "WITH true AS t, false AS f RETURN t<f, t>f"

    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query)
    result.toList should equal(List(Map("t<f" -> false, "t>f" -> true)))
  }

  test("should type check param list accesses in inequalities") {
    val nodes = for(i <- 0 to 30) yield createLabeledNode(Map("id" -> i), "User")
    val q = "MATCH (user:User) WHERE ($range[0] > user.id OR user.id > $range[1]) RETURN user"
    val p = Map("range" -> util.Arrays.asList(10, 20))
    val r = executeSingle(q, p)
    r.toList should contain theSameElementsAs (nodes.take(10) ++ nodes.takeRight(10)).map(u => Map("user" -> u))
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

  test("Should not fail when invalidating cached property with no node/rel column") {
    val p = createLabeledNode(Map("id" -> 0), "Post")
    val space = createNode()
    relate(p, space, "IN_PART")
    val a = createLabeledNode(Map("has_file" -> true, "uploaded" -> false, "has_thumb" -> true, "thumb_uploaded" -> false), "Attachment")
    relate(a, p, "ATTACHED_TO")

    val query =
      """
        |MATCH (p:Post {id: $post_id})-[p_rel:IN_PART]->(space)
        |// This SetProperty will invalidate cached[a.uploaded],
        |// even though a is only added in the next pipeline.
        |// This is benign and we should ignore that and not fail.
        |REMOVE p._lock
        |WITH p, p_rel
        |OPTIONAL MATCH (a:Attachment)-[:ATTACHED_TO]->(p)
        |WHERE a.has_file AND (NOT a.uploaded OR (a.has_thumb AND NOT a.thumb_uploaded))
        |WITH p, p_rel, count(a) as missing_count
        |WHERE missing_count = 0
        |SET p.completed = true
        |WITH p, p_rel
        |RETURN p""".stripMargin

    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query, params = Map("post_id" -> 0))
    result.toList should be(empty)
  }

  test("should be able to use long values for LIMIT in interpreted runtime") {
    val a = createNode()
    val b = createNode()

    val limit: Long = Int.MaxValue + 1L
    // If we would use Ints for storing the limit, then we would end up with "limit 0"
    // thus, if we actually return the two nodes, then it proves that we used a long
    val query = "MATCH (n) RETURN n LIMIT " + limit
    val result = executeWith(Configs.All, query)
    result.toList should equal(List(Map("n" -> a), Map("n" -> b)))
  }

  test("should not explode on complex pattern comprehension projection in write query") {
    graph.withTx( tx => {
      val query =
        """UNWIND [{children : [
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

      val result = tx.execute(query)
      result.resultAsString() // should not explode
    })
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

    val countResult = executeWith(Configs.InterpretedAndSlottedAndPipelined, countQuery)

    countResult.toList should be(List(Map("count" -> 1)))

    val countDistinctQuery =
      """
        |OPTIONAL MATCH (:StartLabel1)-->(:MidLabel1)-->(end:End)
        |RETURN COUNT(DISTINCT end) as count
      """.stripMargin

    val countDistinctResult = executeWith(Configs.InterpretedAndSlottedAndPipelined, countDistinctQuery)

    countDistinctResult.toList should be(List(Map("count" -> 1)))
  }

  test("should be able to plan customer query using outer join and alias (ZenDesk ticket #6628)") {

    runWithConfig(
      cypher_idp_solver_duration_threshold -> java.lang.Long.valueOf(10000),
      cypher_idp_solver_table_threshold -> java.lang.Integer.valueOf(1024)
    ) { dbService =>

      graph = dbService
      graphOps = dbService.getGraphDatabaseService
      onNewGraphDatabase()

      executeSingle("CREATE INDEX FOR (n:L0) ON (n.p0)")
      executeSingle("CREATE INDEX FOR (n:L1) ON (n.p1)")
      executeSingle("CREATE INDEX FOR (n:L2) ON (n.p2,n.p3)")

      graph.withTx(tx => {
        tx.schema.awaitIndexesOnline(30, TimeUnit.SECONDS)
      })

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

      val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query, params = params)

      result.toList shouldBe empty

      // This is not a strict requirement. NodeRightOuterHashJoin would also be OK. Also if planner changes needs to happen, don't let this block you.
      result.executionPlanDescription should includeSomewhere.aPlan("NodeLeftOuterHashJoin")
    }
  }

  test("should get degree of node from a parameter") {
    val node = createLabeledNode("Item")
    relate(createNode(), node, "CONTAINS")
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, "UNWIND $param as p MATCH (p:Item) RETURN size((p)<-[:CONTAINS]-()) as total", params = Map("param" -> List(node)))
    result.toList should equal(List(Map("total" -> 1)))
  }

  test("unary add works") {
    val result = executeWith(Configs.All, "RETURN +1 AS x, +2")
    result.toList should be(List(Map("x" -> 1, "+2" -> 2)))
  }

  test("should handle complex COLLECT + UNWIND with multiple WHERE") {
    //See https://github.com/neo4j/neo4j/issues/12543
    //given
    val query =
      """MATCH (r)
        |WHERE false
        |WITH COLLECT(r) as rs
        |UNWIND rs as r
        |WITH DISTINCT r
        |  WHERE
        |  (r.prop >= 2000)
        |
        |  WITH r
        |  WHERE
        |    (r.prop >= 2005)
        |  WITH r
        |  WHERE
        |  	r.prop <= 2020
        |  RETURN ID(r) LIMIT 5""".stripMargin

    //then
    val result = executeWith(Configs.InterpretedAndSlottedAndPipelined, query)

    //then, we shouldn't crash
    result shouldBe empty
  }

  test("should handle UNION subquery followed by UNWIND and another subquery") {
    val morselSize = databaseConfig()
      .getOrElse(cypher_pipelined_batch_size_small, cypher_pipelined_batch_size_small.defaultValue())
      .asInstanceOf[Integer]

    val nodes = for (_ <- 0 until morselSize * 10) yield createNode()

    val query =
      """CALL {
        |  MATCH (n) RETURN n
        |  UNION
        |  MATCH (n:DoesNotExist) RETURN n
        |}
        |WITH n
        |UNWIND [n] AS x
        |CALL {
        |  WITH n, x
        |  MATCH (m)
        |  RETURN count(m) AS c
        |}
        |RETURN n, x, c
        |""".stripMargin

    val result = executeWith(Configs.All, query)

    val expectedResult = nodes.map(n => Map("n" -> n, "x" -> n, "c" -> nodes.size)).toSet
    result.toSet shouldBe expectedResult
  }

  test("should handle aggregation in reduce") {
    val query =
      """
        |WITH
        |     ['a','b','c','d'] AS list_1,
        |     ['X', 'Y', 'Z'] AS list_2
        |UNWIND list_2 as ul
        |WITH
        |     reduce(x=[], l1 IN list_1 | x + l1) as listOne,
        |     reduce(x=[], l2 IN list_2 | x + l2) as listTwo,
        |     reduce(cnt=0, x IN collect(ul) | cnt + 1) as cnt
        |RETURN listOne, listTwo""".stripMargin

    executeWith(Configs.All, query).toList should equal(List(Map("listOne" -> List("a", "b", "c", "d"), "listTwo" -> List("X", "Y", "Z"))))
  }
}
