/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance

import org.neo4j.configuration.GraphDatabaseInternalSettings
import org.neo4j.cypher.ExecutionEngineFunSuite
import org.neo4j.cypher.TxCounts
import org.neo4j.cypher.internal.RewindableExecutionResult
import org.neo4j.cypher.internal.plandescription.Arguments.DbHits
import org.neo4j.cypher.internal.plandescription.Arguments.Rows
import org.neo4j.cypher.internal.plandescription.InternalPlanDescription
import org.neo4j.cypher.internal.plandescription.InternalPlanDescription.TotalHits
import org.neo4j.cypher.internal.runtime.CreateTempFileTestSupport
import org.neo4j.cypher.internal.runtime.ProfileMode
import org.neo4j.cypher.internal.util.helpers.StringHelper.RichString
import org.neo4j.exceptions.ProfilerStatisticsNotReadyException
import org.neo4j.graphdb.QueryExecutionException
import org.neo4j.graphdb.Result
import org.neo4j.internal.cypher.acceptance.comparisonsupport.ComparePlansWithAssertion
import org.neo4j.internal.cypher.acceptance.comparisonsupport.Configs
import org.neo4j.internal.cypher.acceptance.comparisonsupport.CypherComparisonSupport
import org.neo4j.internal.cypher.acceptance.comparisonsupport.Planners
import org.neo4j.internal.cypher.acceptance.comparisonsupport.Runtimes
import org.neo4j.internal.cypher.acceptance.comparisonsupport.TestConfiguration

class ProfilerAcceptanceTest extends ExecutionEngineFunSuite with CreateTempFileTestSupport with CypherComparisonSupport {

  test("pipelined profile should include expected profiling data with fused operators") {
    createNode()
    val result = profileSingle("CYPHER runtime=pipelined MATCH (n) RETURN n")
    val planString = result.executionPlanDescription().toString
    planString should include("Estimated Rows")
    planString should include("Rows")
    planString should include("DB Hits")
    planString should include("Time (ms)")
    planString should include("Page Cache Hits/Misses")
  }

  test("pipelined profile should include expected profiling data with some fused operators") {
    createNode()
    val result = profileSingle("CYPHER runtime=pipelined MATCH (n) RETURN count(*), n")
    val planString = result.executionPlanDescription().toString
    planString should include("Estimated Rows")
    planString should include("Rows")
    planString should include("DB Hits")
    planString should include("Page Cache Hits/Misses")
    planString should include("Time (ms)")
  }

  test("pipelined profile should include expected profiling data with non-fused operators") {
    restartWithConfig(Map(GraphDatabaseInternalSettings.cypher_operator_engine -> GraphDatabaseInternalSettings.CypherOperatorEngine.INTERPRETED))

    createNode()
    val result = profileSingle("CYPHER runtime=pipelined MATCH (n) RETURN n")
    val planString = result.executionPlanDescription().toString
    planString should include("Estimated Rows")
    planString should include("Rows")
    planString should include("DB Hits")
    planString should include("Page Cache Hits/Misses")
    planString should include("Time (ms)")
  }

  test("profile simple query") {
    createNode()
    createNode()
    createNode()
    profile(Configs.All,
      "MATCH (n) RETURN n",
      plan => {
        plan should (
          includeSomewhere.aPlan("ProduceResults").withRows(3).withDBHits(0) and
            includeSomewhere.aPlan("AllNodesScan").withRows(3).withDBHits(4)
          )
        plan.totalDbHits shouldBe TotalHits(4, uncertain = false)
      })
  }

  test("track db hits in Projection") {
    createNode()
    createNode()
    createLabeledNode("Foo")

    profile(Configs.All,
      "MATCH (n) RETURN (n:Foo)",
      _ should (
        includeSomewhere.aPlan("ProduceResults").withRows(3).withDBHits(0) and
          includeSomewhere.aPlan("Projection").withDBHitsBetween(0, 6) and
          includeSomewhere.aPlan("AllNodesScan").withRows(3).withDBHits(4)
        ))
  }

  test("track time in Projection") {
    createNode()
    createNode()
    createNode()

    executeWith(
      Configs.All,
      "PROFILE MATCH (n) RETURN n.foo",
      planComparisonStrategy = ComparePlansWithAssertion(
        _ should
          includeSomewhere.aPlan("ProduceResults").withTime()
            .withLHS(
              includeSomewhere.aPlan("Projection").withTime()
                .withLHS(
                  includeSomewhere.aPlan("AllNodesScan").withTime()
                )
            ),
        expectPlansToFail = Configs.InterpretedAndSlotted + TestConfiguration(Planners.all, Runtimes.PipelinedFused))
    )
  }

  test("track memory in distinct") {
    createNode()
    createNode()
    createNode()

    executeWith(
      Configs.All,
      "PROFILE MATCH (n) RETURN DISTINCT n",
      planComparisonStrategy = ComparePlansWithAssertion(
        _ should
          haveAsRoot.aPlan.withGlobalMemory()
            .onTopOf(aPlan("Distinct").withMemory()))
    )
  }

  test("report total memory") {
    val result = profileSingle("MATCH (n) RETURN collect(n)")
    val planString = result.executionPlanDescription().toString
    planString should include regex "total allocated memory: [1-9][0-9]*"
  }

  test("report total memory outside transaction") {
    def drain(res: Result): Result = {
      while (res.hasNext) res.next()
      res
    }

    val result = inTx(tx => drain(tx.execute("PROFILE MATCH (n) RETURN collect(n)")))
    val planString = result.getExecutionPlanDescription.toString
    planString should include regex "total allocated memory: [1-9][0-9]*"
  }

  test("total memory should be tracked per query in a transaction") {
    def drain(res: Result): Result = {
      while (res.hasNext) res.next()
      res
    }

    val (p1, p2) = inTx { tx =>
      val r1 = drain(tx.execute("PROFILE UNWIND range(0,100) AS x RETURN collect(x)"))
      val r2 = drain(tx.execute("PROFILE UNWIND range(0,500) AS x RETURN collect(x)"))
      (r1.getExecutionPlanDescription, r2.getExecutionPlanDescription)
    }
    val mem1 = p1.getArguments.get("GlobalMemory").asInstanceOf[Long]
    val mem2 = p2.getArguments.get("GlobalMemory").asInstanceOf[Long]
    mem1 should be > 0L
    mem1 should be < mem2
  }

  test("profile standalone call") {
    createLabeledNode("Person")
    createLabeledNode("Animal")

    val result = profileSingle("CALL db.labels")
    val plan = result.executionPlanDescription()
    plan should includeSomewhere.aPlan("ProcedureCall")
      .withRows(2)
      .withExactVariables("label")
      .containingArgument("db.labels() :: (label :: STRING?)")
    plan.totalDbHits shouldBe TotalHits(0, uncertain = true)
  }

  test("profile call in query") {
    createLabeledNode("Person")
    createLabeledNode("Animal")

    profile(Configs.InterpretedAndSlottedAndPipelined,
      "MATCH (n:Person) CALL db.labels() YIELD label RETURN *",
      plan => {
        plan should includeSomewhere.aPlan("ProcedureCall")
          .withRows(2)
          .withExactVariables("n", "label")
          .containingArgument("db.labels() :: (label :: STRING?)")
          .onTopOf(aPlan("NodeByLabelScan").withRows(1).withDBHitsBetween(1, 2))
        plan.totalDbHits should (be (TotalHits(1, uncertain = true)) or be(TotalHits(2, uncertain = true)))
      }
    )
  }

  test("MATCH (n) WHERE (n)-[:FOO]->() RETURN *") {
    //GIVEN
    relate(createNode(), createNode(), "FOO")

    //WHEN
    profile(Configs.InterpretedAndSlottedAndPipelined,
      "MATCH (n) WHERE (n)-[:FOO]->() RETURN *",
      _ should (
        includeSomewhere.aPlan.withRows(1).withDBHits(0) // SemiApply or Apply/Limit
          .withRHS(includeSomewhere.aPlan("Expand(All)").withDBHitsBetween(2, 4))
          and includeSomewhere.aPlan("AllNodesScan").withRows(2).withDBHits(3)
        ))
  }

  test("MATCH (n:A)-->(x:B) RETURN *") {
    //GIVEN
    relate(createLabeledNode("A"), createLabeledNode("B"))

    //WHEN
    profile(Configs.All,
      "MATCH(n:A)-->(x:B) RETURN *",
      _ should (
        includeSomewhere.aPlan("ProduceResults").withRows(1).withDBHits(0) and
          includeSomewhere.aPlan("Filter").withRows(1).withDBHits(1) and
          includeSomewhere.aPlan("Expand(All)").withRows(1).withDBHits(2) and
          includeSomewhere.aPlan("NodeByLabelScan").withRows(1).withDBHits(2)
        ))
  }

  test("MATCH (n) WHERE NOT (n)-[:FOO]->() RETURN *") {
    //GIVEN
    relate(createNode(), createNode(), "FOO")

    //WHEN
    profile(Configs.InterpretedAndSlottedAndPipelined,
      "MATCH (n) WHERE NOT (n)-[:FOO]->() RETURN *",
      _ should (
        includeSomewhere.aPlan.withRows(1).withDBHits(0) // AntiSemiApply or Apply/Anti/Limit
          .withRHS(includeSomewhere.aPlan("Expand(All)").withDBHitsBetween(2, 4))
          and
          includeSomewhere.aPlan("AllNodesScan").withRows(2).withDBHits(3)
        ))
  }

  test("unfinished profiler complains [using MATCH]") {
    //GIVEN
    createNode("foo" -> "bar")
    graph.withTx( tx => {
      val result = tx.execute("PROFILE MATCH (n) WHERE id(n) = 0 RETURN n")

      //WHEN THEN
      val ex = intercept[QueryExecutionException](result.getExecutionPlanDescription)
      ex.getCause.getCause shouldBe a[ProfilerStatisticsNotReadyException]
      result.close() // ensure that the transaction is closed
    })
  }

  test("unfinished profiler complains [using CALL]") {
    //GIVEN
    createLabeledNode("Person")
    graph.withTx( tx => {
      val result = tx.execute("PROFILE CALL db.labels")

      //WHEN THEN
      val ex = intercept[QueryExecutionException](result.getExecutionPlanDescription)
      ex.getCause.getCause shouldBe a[ProfilerStatisticsNotReadyException]
      result.close() // ensure that the transaction is closed
    })
  }

  test("unfinished profiler complains [using CALL within larger query]") {
    //GIVEN
    createLabeledNode("Person")
    graph.withTx( tx => {
      val result = tx.execute("PROFILE CALL db.labels() YIELD label WITH label AS r RETURN r")

      //WHEN THEN
      val ex = intercept[QueryExecutionException](result.getExecutionPlanDescription)
      ex.getCause.getCause shouldBe a[ProfilerStatisticsNotReadyException]
      result.close() // ensure that the transaction is closed
    })
  }

  test("tracks number of rows") {
    // due to the cost model, we need a bunch of nodes for the planner to pick a plan that does lookup by id
    (1 to 100).foreach(_ => createNode())

    profile(Configs.InterpretedAndSlottedAndPipelined,
      "MATCH (n) WHERE id(n) = 0 RETURN n",
      _ should includeSomewhere.aPlan("NodeByIdSeek").withRows(1))
  }

  test("tracks number of graph accesses") {
    //GIVEN
    // due to the cost model, we need a bunch of nodes for the planner to pick a plan that does lookup by id
    (1 to 100).foreach(_ => createNode("foo" -> "bar"))

    profile(Configs.InterpretedAndSlottedAndPipelined,
      "MATCH (n) WHERE id(n) = 0 RETURN n.foo",
      _ should (
        includeSomewhere.aPlan("ProduceResults").withRows(1).withDBHits(0) and
          includeSomewhere.aPlan("Projection").withRows(1).withDBHitsBetween(1, 2) and
          includeSomewhere.aPlan("NodeByIdSeek").withRows(1).withDBHits(1)
        ))
  }

  test("no problem measuring creation") {
    //GIVEN
    val result = profileSingle("CYPHER runtime=slotted CREATE (n)")

    //WHEN THEN
    result.executionPlanDescription() should includeSomewhere.aPlan("EmptyResult").withDBHits(0)
  }

  test("tracks graph global queries") {
    createNode()

    //GIVEN
    profile(Configs.All,
      "MATCH (n) RETURN n.foo",
      _ should (
        includeSomewhere.aPlan("ProduceResults").withRows(1).withDBHits(0) and
          includeSomewhere.aPlan("Projection").withRows(1).withDBHitsBetween(0, 1) and
          includeSomewhere.aPlan("AllNodesScan").withRows(1).withDBHits(2)
        ))
  }

  test("tracks optional MATCHes") {
    //GIVEN
    createNode()

    // WHEN
    profile(Configs.InterpretedAndSlottedAndPipelined,
      "MATCH (n) OPTIONAL MATCH (n)-->(x) RETURN x",
      _ should (
        includeSomewhere.aPlan("ProduceResults").withDBHits(0) and
          includeSomewhere.aPlan("OptionalExpand(All)").withDBHitsBetween(0, 1) and
          includeSomewhere.aPlan("AllNodesScan").withDBHits(2)
        ))
  }

  test("allows optional MATCH to start a query") {
    // WHEN
    profile(Configs.InterpretedAndSlottedAndPipelined,
      "OPTIONAL MATCH (n) RETURN n",
      _ should includeSomewhere.aPlan("Optional").withRows(1))
  }

  test("should produce profile when using limit") {
    // GIVEN
    createNode()
    createNode()
    createNode()
    profile(Configs.All,
      "MATCH (n) RETURN n LIMIT 1",
      _ should (
        includeSomewhere.aPlan("AllNodesScan").withRowsBetween(1, 3).withDBHitsBetween(2, 4) and
          includeSomewhere.aPlan("ProduceResults").withRows(1).withDBHits(0)
        ))
  }

  test("should support profiling union queries") {
    profile(Configs.InterpretedAndSlottedAndPipelined,
      "RETURN 1 AS A union RETURN 2 AS A")
  }

  test("should support profiling merge_queries") {
    profile(Configs.InterpretedAndSlotted,
      "MERGE (a {x: 1}) RETURN a.x AS A")
  }

  test("should support profiling optional match queries") {
    createLabeledNode(Map("x" -> 1), "Label")
    profile(Configs.InterpretedAndSlottedAndPipelined,
      "MATCH (a:Label {x: 1}) OPTIONAL MATCH (a)-[:REL]->(b) RETURN a.x AS A, b.x AS B")
  }

  test("should support profiling optional match and with") {
    createLabeledNode(Map("x" -> 1), "Label")
    profile(Configs.InterpretedAndSlottedAndPipelined,
      "MATCH (n) OPTIONAL MATCH (n)--(m) WITH n, m WHERE m IS null RETURN n.x AS A")
  }

  test("should handle PERIODIC COMMIT when profiling") {
    val url = createTempFileURL("cypher", ".csv")(writer => {
      (1 to 100).foreach(writer.println)
    }).cypherEscape

    val query = s"USING PERIODIC COMMIT 10 LOAD CSV FROM '$url' AS line CREATE()"

    // given
    executeWith(Configs.InterpretedAndSlotted, query).toList
    deleteAllEntities()
    val initialTxCounts = graph.txCounts

    // when
    val result = profileSingle(query)

    // then
    val expectedTxCount = 10 // One per 10 rows of CSV file

    graph.txCounts - initialTxCounts should equal(TxCounts(commits = expectedTxCount))
    result.queryStatistics().containsUpdates should equal(true)
    result.queryStatistics().nodesCreated should equal(100)
  }

  test("should not have a problem profiling empty results") {
    profile(Configs.InterpretedAndSlottedAndPipelined,
      "MATCH (n) WHERE (n)-->() RETURN n",
      _ should includeSomewhere.aPlan("AllNodesScan"))
  }

  test("reports COST planner when showing plan description") {
    graph.withTx( tx => {
      val result = tx.execute("CYPHER planner=cost MATCH (n) RETURN n")
      result.resultAsString()
      result.getExecutionPlanDescription.toString should include("Planner COST" + System.lineSeparator())
    })
  }

  test("match (p:Person {name:'Seymour'}) return (p)-[:RELATED_TO]->()") {
    //GIVEN
    val seymour = createLabeledNode(Map("name" -> "Seymour"), "Person")
    relate(seymour, createLabeledNode(Map("name" -> "Buddy"), "Person"), "RELATED_TO")
    relate(seymour, createLabeledNode(Map("name" -> "Boo Boo"), "Person"), "RELATED_TO")
    relate(seymour, createLabeledNode(Map("name" -> "Walt"), "Person"), "RELATED_TO")
    relate(seymour, createLabeledNode(Map("name" -> "Waker"), "Person"), "RELATED_TO")
    relate(seymour, createLabeledNode(Map("name" -> "Zooey"), "Person"), "RELATED_TO")
    relate(seymour, createLabeledNode(Map("name" -> "Franny"), "Person"), "RELATED_TO")
    // pad with enough nodes to make index seek considered more efficient than label scan
    createLabeledNode(Map("name" -> "Dummy1"), "Person")
    createLabeledNode(Map("name" -> "Dummy2"), "Person")
    createLabeledNode(Map("name" -> "Dummy3"), "Person")

    graph.createUniqueConstraint("Person", "name")

    //WHEN
    profile(Configs.InterpretedAndSlottedAndPipelined,
      "MATCH (p:Person {name:'Seymour'}) RETURN (p)-[:RELATED_TO]->()",
      _ should (
        includeSomewhere.aPlan("Expand(All)").withDBHits(7) and
          includeSomewhere.aPlan("NodeUniqueIndexSeek").withDBHits(2)
        ))
  }

  test("should report correct dbhits and rows for label scan") {
    // given
    createLabeledNode("Label1")

    // when
    profile(Configs.All,
      "MATCH (n:Label1) RETURN n",
      _ should includeSomewhere.aPlan("NodeByLabelScan").withRows(1).withDBHits(2))
  }

  test("should report correct dbhits and rows for expand") {
    // given
    relate(createNode(), createNode())

    // then
    profile(Configs.All,
      "MATCH (n)-->(x) RETURN x",
      _ should includeSomewhere.aPlan("Expand(All)").withRows(1).withDBHitsBetween(2, 4))
  }

  test("should report correct dbhits and rows for literal addition") {
    profile(Configs.All,
      "RETURN 5 + 3",
      _ should (
        includeSomewhere.aPlan("Projection").withDBHits(0) and
          includeSomewhere.aPlan("ProduceResults").withRows(1).withDBHits(0)
        ))
  }

  test("should report correct dbhits and rows for property addition") {
    // given
    createNode("name" -> "foo")

    // then
    profile(Configs.All,
      "MATCH (n) RETURN n.name + 3",
      _ should includeSomewhere.aPlan("Projection").withRows(1).withDBHitsBetween(1, 2))
  }

  test("should report correct dbhits and rows for property subtraction") {
    // given
    createNode("name" -> 10)

    // then
    profile(Configs.All,
      "MATCH (n) RETURN n.name - 3",
      _ should includeSomewhere.aPlan("Projection").withRows(1).withDBHitsBetween(1, 2))
  }

  test("should throw if accessing profiled results before they have been materialized") {
    createNode()
    graph.withTx( tx => {
      val result = tx.execute("PROFILE MATCH (n) RETURN n")

      val ex = intercept[QueryExecutionException](result.getExecutionPlanDescription)
      ex.getCause.getCause shouldBe a[ProfilerStatisticsNotReadyException]
      result.close() // ensure that the transaction is closed
    })
  }

  test("should profile cartesian products") {
    createNode()
    createNode()
    createNode()
    createNode()

    profile(Configs.InterpretedAndSlottedAndPipelined,
      "MATCH (n), (m) RETURN n, m",
      _ should includeSomewhere.aPlan("CartesianProduct").withRows(16))
  }

  test("should profile filters") {
    // given
    val n = createLabeledNode(Map("name" -> "Seymour"), "Glass")
    val o = createNode()
    relate(n, o, "R1")
    relate(o, createLabeledNode(Map("name" -> "Zoey"), "Glass"), "R2")
    relate(o, createLabeledNode(Map("name" -> "Franny"), "Glass"), "R2")
    relate(o, createNode(), "R2")
    relate(o, createNode(), "R2")
    graph.createIndex("Glass", "name")

    // then
    profile(Configs.All,
      """MATCH (n:Glass {name: 'Seymour'})-[:R1]->(o)-[:R2]->(p:Glass)
        |  USING INDEX n:Glass(name)
        |RETURN p.name""".stripMargin,
      _ should includeSomewhere.aPlan("Filter").withRows(2))
  }

  test("should profile projections") {
    // given
    val n = createLabeledNode(Map("name" -> "Seymour"), "Glass")
    val o = createNode()
    relate(n, o, "R1")
    relate(o, createLabeledNode(Map("name" -> "Zoey"), "Glass"), "R2")
    relate(o, createLabeledNode(Map("name" -> "Franny"), "Glass"), "R2")
    relate(o, createNode(), "R2")
    relate(o, createNode(), "R2")
    graph.createIndex("Glass", "name")

    // then
    profile(Configs.All,
      """MATCH (n:Glass {name: 'Seymour'})-[:R1]->(o)-[:R2]->(p:Glass)
        |  USING INDEX n:Glass(name)
        |RETURN p.name""".stripMargin,
      _ should includeSomewhere.aPlan("Projection").withDBHitsBetween(2, 4))
  }

  test("profile filter") {
    // given
    val n = createLabeledNode(Map("name" -> "Seymour"), "Glass")
    val o = createNode()
    relate(n, o, "R1")
    relate(o, createLabeledNode(Map("name" -> "Zoey"), "Glass"), "R2")
    relate(o, createLabeledNode(Map("name" -> "Franny"), "Glass"), "R2")
    relate(o, createNode(), "R2")
    relate(o, createNode(), "R2")
    graph.createIndex("Glass", "name")

    // when
    profile(Configs.All,
      """MATCH (n:Glass {name: 'Seymour'})-[:R1]->(o)-[:R2]->(p)
        |  USING INDEX n:Glass(name)
        |  WHERE p.name = 'Franny'
        |RETURN p.name""".stripMargin,
      _ should includeSomewhere.aPlan("Filter").withDBHitsBetween(4, 6))
  }

  test("joins with identical scans") {
    //given
    val corp = createLabeledNode("Company")

    //force a plan to have a scan on corp in both the lhs and the rhs of join
    val query =
      """PROFILE MATCH (a:Company) RETURN a
        |UNION
        |MATCH (a:Company) RETURN a""".stripMargin

    //when
    val result = executeSingle(query, Map.empty)

    result.toSet should be(Set(Map("a" -> corp), Map("a" -> corp)))

    //then
    result.executionPlanDescription() should includeSomewhere.aPlan("NodeByLabelScan").withRows(1).withDBHits(2)
  }

  //this test asserts a specific optimization in pipe building and is not
  //valid for the compiled runtime
  test("distinct should not look up properties every time") {
    // GIVEN
    createNode("prop" -> 42)
    createNode("prop" -> 42)

    // WHEN
    val result = executeSingle("PROFILE CYPHER runtime=interpreted MATCH (n) RETURN DISTINCT n.prop", Map.empty)

    // THEN
    result.executionPlanDescription() should includeSomewhere.aPlan("Distinct").withDBHits(2)
  }

  test("profile with filter using nested expressions pipe should report dbhits correctly") {
    // GIVEN
    createLabeledNode(Map("category_type" -> "cat"), "Category")
    createLabeledNode(Map("category_type" -> "cat"), "Category")
    val e1 = createLabeledNode(Map("domain_id" -> "1"), "Entity")
    val e2 = createLabeledNode(Map("domain_id" -> "2"), "Entity")
    val aNode = createNode()
    relate(aNode, e1)
    val anotherNode = createNode()
    relate(anotherNode, e2)

    relate(aNode, createNode(), "HAS_CATEGORY")
    relate(anotherNode, createNode(), "HAS_CATEGORY")

    // THEN
    profile(Configs.InterpretedAndSlottedAndPipelined,
      """MATCH (cat:Category)
        |WITH collect(cat) as categories
        |MATCH (m:Entity)
        |WITH m, categories
        |MATCH (m)<-[r]-(n)
        |WHERE ANY(x IN categories WHERE (n)-[:HAS_CATEGORY]->(x))
        |RETURN count(n)""".stripMargin,
      _ should includeSomewhere.aPlan("Filter").withDBHits(16))
  }

  test("profile pruning var length expand") {
    //some graph
    val a = createLabeledNode("Start")
    val b1 = createLabeledNode("Node")
    val b2 = createLabeledNode("Node")
    val b3 = createLabeledNode("Node")
    val b4 = createLabeledNode("Node")
    relate(a, b1, "T1")
    relate(b1, b2, "T1")
    relate(b2, b3, "T1")
    relate(b2, b4, "T1")

    profile(Configs.InterpretedAndSlottedAndPipelined,
      "PROFILE MATCH (b:Start)-[*3]->(d) RETURN count(distinct d)",
      _ should includeSomewhere.aPlan("VarLengthExpand(Pruning)").withRows(2).withDBHits(7))
  }

  def profileSingle(query: String, params: (String, Any)*): RewindableExecutionResult = {
    val result = executeSingle("PROFILE " + query, params.toMap)
    result.executionMode should equal(ProfileMode)

    val planDescription: InternalPlanDescription = result.executionPlanDescription()
    planDescription.flatten.foreach(assertProfileData)
    result
  }

  def profile(configuration: TestConfiguration,
              query: String,
              planDescriptionAssertion: InternalPlanDescription => Unit = _ => (),
              params: Map[String, Any] = Map.empty,
              configsWithDifferentResult: TestConfiguration = Configs.Empty,
              configsWithFailingPlanAssertion: TestConfiguration = Configs.Empty): RewindableExecutionResult = {

    val result = executeWith(configuration,
      "PROFILE " + query,
      params = params,
      expectedDifferentResults = configsWithDifferentResult,
      planComparisonStrategy = ComparePlansWithAssertion(planDescription => {
        planDescription.flatten.foreach(assertProfileData)
        planDescriptionAssertion(planDescription)
      },
        expectPlansToFail = configsWithFailingPlanAssertion))
    result.executionMode should equal(ProfileMode)
    result
  }

  private def assertProfileData(p: InternalPlanDescription): Unit = {
    if (p.name != "ProcedureCall" && !p.arguments.exists(_.isInstanceOf[DbHits])) {
      fail("Found plan that was not profiled with DbHits: " + p.name)
    }
    if (!p.arguments.exists(_.isInstanceOf[Rows])) {
      fail("Found plan that was not profiled with Rows: " + p.name)
    }
  }
}
