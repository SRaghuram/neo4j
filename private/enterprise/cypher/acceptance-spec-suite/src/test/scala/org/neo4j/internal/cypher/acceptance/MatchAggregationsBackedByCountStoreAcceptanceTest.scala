/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.ExecutionEngineFunSuite
import org.neo4j.cypher.QueryPlanTestSupport
import org.neo4j.cypher.QueryStatisticsTestSupport
import org.neo4j.cypher.internal.RewindableExecutionResult
import org.neo4j.internal.cypher.acceptance.comparisonsupport.ComparePlansWithAssertion
import org.neo4j.internal.cypher.acceptance.comparisonsupport.Configs
import org.neo4j.internal.cypher.acceptance.comparisonsupport.CypherComparisonSupport
import org.neo4j.internal.cypher.acceptance.comparisonsupport.TestConfiguration
import org.neo4j.kernel.impl.coreapi.InternalTransaction

class MatchAggregationsBackedByCountStoreAcceptanceTest
  extends ExecutionEngineFunSuite with QueryStatisticsTestSupport with CypherComparisonSupport with QueryPlanTestSupport {

  test("do not plan counts store lookup for loop matches") {
    val n = createNode()
    // two loops
    relate(n, n)
    relate(n, n)
    // one non-loop
    relate(n, createNode())

    val resultStar = executeWith(Configs.InterpretedAndSlottedAndPipelined, "MATCH (a)-->(a) RETURN count(*)")
    val resultVar = executeWith(Configs.InterpretedAndSlottedAndPipelined, "MATCH (a)-[r]->(a) RETURN count(r)")

    resultStar.toList should equal(List(Map("count(*)" -> 2)))
    resultVar.toList should equal(List(Map("count(r)" -> 2)))

    resultStar.executionPlanDescription() shouldNot includeSomewhere.aPlan("RelationshipCountFromCountStore")
    resultVar.executionPlanDescription() shouldNot includeSomewhere.aPlan("RelationshipCountFromCountStore")
  }

  test("counts nodes using count store") {
    // Given
    def executeBefore(tx: InternalTransaction): Unit = setupModel(tx)
    val query = "MATCH (n) RETURN count(n)"

    // Then
    compareCount(query, 0)
    compareCount(query, 3, executeBefore = executeBefore)
  }

  test("capitalized COUNTS nodes using count store") {
    // Given
    def executeBefore(tx: InternalTransaction): Unit = setupModel(tx)
    val query = "MATCH (n) RETURN COUNT(n)"

    // Then
    compareCount(query, 0)
    compareCount(query, 3, executeBefore = executeBefore)
  }

  test("counts nodes using count store with count(*)") {
    // Given
    def executeBefore(tx: InternalTransaction): Unit = setupModel(tx)
    val query = "MATCH (n) RETURN count(*)"

    // Then
    compareCount(query, 0)
    compareCount(query, 3, executeBefore = executeBefore)
  }

  test("counts labeled nodes using count store") {
    // Given
    def executeBefore(tx: InternalTransaction): Unit = setupModel(tx, label1 = "Admin")
    val query = "MATCH (n:User) RETURN count(n)"

    // Then
    compareCount(query, 0)
    compareCount(query, 1, executeBefore = executeBefore)
  }

  test("counts nodes using count store and projection expression") {
    // Given
    def executeBefore(tx: InternalTransaction): Unit = setupModel(tx, label1 = "Admin")
    val query = "MATCH (n:User) RETURN count(n) > 0"

    // Then
    compareCount(query, false, Configs.InterpretedAndSlottedAndPipelined)
    compareCount(query, true, Configs.InterpretedAndSlottedAndPipelined, executeBefore = executeBefore)
  }

  test("counts nodes using count store and projection expression with variable") {
    // Given
    def executeBefore(tx: InternalTransaction): Unit = setupModel(tx)
    val query = "MATCH (n) RETURN count(n)/2.0*5 as someNum"

    // Then
    compareCount(query, 0)
    compareCount(query, 7.5, executeBefore = executeBefore)
  }

  test("counts relationships with unspecified type using count store") {
    // Given
    def executeBefore(tx: InternalTransaction): Unit = setupModel(tx)
    val query = "MATCH ()-[r]->() RETURN count(r)"

    // Then
    compareCount(query, 0, expectedLogicalPlan = "RelationshipCountFromCountStore")
    compareCount(query, 2, expectedLogicalPlan = "RelationshipCountFromCountStore", executeBefore = executeBefore)
  }

  test("counts relationships with unspecified type using count store with count(*)") {
    // Given
    def executeBefore(tx: InternalTransaction): Unit = setupModel(tx)
    val query = "MATCH ()-->() RETURN count(*)"

    // Then
    compareCount(query, 0, expectedLogicalPlan = "RelationshipCountFromCountStore")
    compareCount(query, 2, expectedLogicalPlan = "RelationshipCountFromCountStore", executeBefore = executeBefore)
  }

  test("counts relationships with type using count store") {
    // Given
    def executeBefore(tx: InternalTransaction): Unit = setupModel(tx)
    val query = "MATCH ()-[r:KNOWS]->() RETURN count(r)"

    // Then
    compareCount(query, 0, expectedLogicalPlan = "RelationshipCountFromCountStore")
    compareCount(query, 1, expectedLogicalPlan = "RelationshipCountFromCountStore", executeBefore = executeBefore)
  }

  test("counts relationships with type using count store with count(*)") {
    // Given
    def executeBefore(tx: InternalTransaction): Unit = setupModel(tx)
    val query = "MATCH ()-[r:KNOWS]->() RETURN count(*)"

    // Then
    compareCount(query, 0, expectedLogicalPlan = "RelationshipCountFromCountStore")
    compareCount(query, 1, expectedLogicalPlan = "RelationshipCountFromCountStore", executeBefore = executeBefore)
  }

  test("counts relationships with unspecified type and labeled source node using count store") {
    // Given
    def executeBefore(tx: InternalTransaction): Unit = setupModel(tx)
    val query = "MATCH (:User)-[r]->() RETURN count(r)"

    // Then
    compareCount(query, 0, expectedLogicalPlan = "RelationshipCountFromCountStore")
    compareCount(query, 1, expectedLogicalPlan = "RelationshipCountFromCountStore", executeBefore = executeBefore)
  }

  test("counts relationships with type and labeled source node using count store") {
    // Given
    def executeBefore(tx: InternalTransaction): Unit = setupModel(tx)
    val query = "MATCH ()<-[r:KNOWS]-(:User) RETURN count(r)"

    // Then
    compareCount(query, 0, expectedLogicalPlan = "RelationshipCountFromCountStore")
    compareCount(query, 1, expectedLogicalPlan = "RelationshipCountFromCountStore", executeBefore = executeBefore)
  }

  test("counts relationships with unspecified type and labeled destination node using count store") {
    // Given
    def executeBefore(tx: InternalTransaction): Unit = setupModel(tx)
    val query = "MATCH ()-[r]->(:User) RETURN count(r)"

    // Then
    compareCount(query, 0, expectedLogicalPlan = "RelationshipCountFromCountStore")
    compareCount(query, 1, expectedLogicalPlan = "RelationshipCountFromCountStore", executeBefore = executeBefore)
  }

  test("counts relationships with type and labeled destination node using count store") {
    // Given
    def executeBefore(tx: InternalTransaction): Unit = setupModel(tx)
    val query = "MATCH ()-[r:KNOWS]->(:User) RETURN count(r)"

    // Then
    compareCount(query, 0, expectedLogicalPlan = "RelationshipCountFromCountStore")
    compareCount(query, 1, expectedLogicalPlan = "RelationshipCountFromCountStore", executeBefore = executeBefore)
  }

  test("counts relationships with type and labeled source and destination without using count store") {
    // Given
    def executeBefore(tx: InternalTransaction): Unit = setupModel(tx)
    val query = "MATCH (:User)-[r:KNOWS]->(:User) RETURN count(r)"

    // Then
    compareCount(query, 0, Configs.All, expectedLogicalPlan = "NodeByLabelScan")
    compareCount(query, 1, Configs.All, expectedLogicalPlan = "NodeByLabelScan", executeBefore = executeBefore)
  }

  test("counts relationships with unspecified type and labeled source and destination without using count store") {
    // Given
    def executeBefore(tx: InternalTransaction): Unit = setupModel(tx)
    val query = "MATCH (:User)-[r]->(:User) RETURN count(r)"

    // Then
    compareCount(query, 0, Configs.All, expectedLogicalPlan = "NodeByLabelScan")
    compareCount(query, 1, Configs.All, expectedLogicalPlan = "NodeByLabelScan", executeBefore = executeBefore)
  }

  test("counts relationships with type, reverse direction and labeled source node using count store") {
    // Given
    def executeBefore(tx: InternalTransaction): Unit = setupModel(tx)
    val query = "MATCH (:User)<-[r:KNOWS]-() RETURN count(r)"

    // Then
    compareCount(query, 0, expectedLogicalPlan = "RelationshipCountFromCountStore")
    compareCount(query, 1, expectedLogicalPlan = "RelationshipCountFromCountStore", executeBefore = executeBefore)
  }

  test("counts relationships with type, reverse direction and labeled destination node using count store") {
    // Given
    def executeBefore(tx: InternalTransaction): Unit = setupModel(tx)
    val query = "MATCH ()<-[r:KNOWS]-(:User) RETURN count(r)"

    // Then
    compareCount(query, 0, expectedLogicalPlan = "RelationshipCountFromCountStore")
    compareCount(query, 1, expectedLogicalPlan = "RelationshipCountFromCountStore", executeBefore = executeBefore)
  }

  test("counts relationships with type, any direction and labeled source node without using count store") {
    // Given
    def executeBefore(tx: InternalTransaction): Unit = setupModel(tx)
    val query = "MATCH (:User)-[r:KNOWS]-() RETURN count(r)"

    // Then
    compareCount(query, 0, Configs.All, expectedLogicalPlan = "NodeByLabelScan")
    compareCount(query, 2, Configs.All, expectedLogicalPlan = "NodeByLabelScan", executeBefore = executeBefore)
  }

  test("counts relationships with type, any direction and labeled destination node without using count store") {
    // Given
    def executeBefore(tx: InternalTransaction): Unit = setupModel(tx)
    val query = "MATCH ()-[r:KNOWS]-(:User) RETURN count(r)"

    // Then
    compareCount(query, 0, Configs.All, expectedLogicalPlan = "NodeByLabelScan")
    compareCount(query, 2, Configs.All, expectedLogicalPlan = "NodeByLabelScan", executeBefore = executeBefore)
  }

  test("counts relationships with type, any direction and no labeled nodes without using count store") {
    // Given
    def executeBefore(tx: InternalTransaction): Unit = setupModel(tx)
    val query = "MATCH ()-[r:KNOWS]-() RETURN count(r)"

    // Then
    compareCount(query, 0, Configs.All, expectedLogicalPlan = "AllNodesScan")
    compareCount(query, 2, Configs.All, expectedLogicalPlan = "AllNodesScan", executeBefore = executeBefore)
  }

  test("counts nodes using count store considering transaction state") {
    // Given
    def executeBefore(tx: InternalTransaction):Unit = inTXModification(tx)
    val query = "MATCH (n:User) RETURN count(n)"

    // Then
    compareCount(query, 0)
    setupBigModel()
    compareCount(query, 3, assertCountInTransaction = true, executeBefore = executeBefore)
  }

  test("counts labeled nodes using count store considering transaction state (test1)") {
    // Given
    def executeBefore(tx: InternalTransaction):Unit = inTXModification(tx, label1 = "Admin", label2 = "User")
    val query = "MATCH (n:User) RETURN count(n)"

    // Then
    compareCount(query, 0)
    setupBigModel(label1 = "Admin", label2 = "User")
    compareCount(query, 2, assertCountInTransaction = true, executeBefore = executeBefore)
  }

  test("counts labeled nodes using count store considering transaction state (test2)") {
    // Given
    def executeBefore(tx: InternalTransaction):Unit = inTXModification(tx, label1 = "Admin", label2 = "User")
    val query = "MATCH (n:Admin) RETURN count(n)"

    // Then
    compareCount(query, 0)
    setupBigModel(label1 = "Admin", label2 = "User")
    compareCount(query, 1, assertCountInTransaction = true, executeBefore = executeBefore)
  }

  test("counts labeled nodes using count store considering transaction state containing newly created label (test1)") {
    // Given
    def executeBefore(tx: InternalTransaction):Unit = inTXModification(tx, label3 = "Admin")
    val query = "MATCH (n:Admin) RETURN count(n)"

    // Then
    compareCount(query, 0)
    setupBigModel()
    compareCount(query, 1, assertCountInTransaction = true, executeBefore = executeBefore)
  }


  test("counts labeled nodes using count store considering transaction state containing newly created label (test2)") {
    // Given
    def executeBefore(tx: InternalTransaction):Unit = inTXModification(tx, label3 = "Admin")
    val query = "MATCH (n:User) RETURN count(n)"

    // Then
    compareCount(query, 0)
    setupBigModel()
    compareCount(query, 2, assertCountInTransaction = true, executeBefore = executeBefore)
  }

  test("counts nodes using count store and projection expression considering transaction state") {
    // Given
    def executeBefore(tx: InternalTransaction):Unit = inTXModification(tx, label1 = "Admin")
    val query = "MATCH (n:User) RETURN count(n) > 1"

    // Then
    compareCount(query, false, Configs.InterpretedAndSlottedAndPipelined)
    setupBigModel(label1 = "Admin")
    compareCount(query, true, Configs.InterpretedAndSlottedAndPipelined, assertCountInTransaction = true, executeBefore = executeBefore)
  }

  test("counts nodes using count store and projection expression with variable considering transaction state") {
    // Given
    def executeBefore(tx: InternalTransaction):Unit = inTXModification(tx)
    val query = "MATCH (n) RETURN count(n)/3*5 as someNum"

    // Then
    compareCount(query, 0)
    setupBigModel()
    compareCount(query, 5, assertCountInTransaction = true, executeBefore = executeBefore)
  }

  test("counts relationships using count store considering transaction state") {
    // Given
    def executeBefore(tx: InternalTransaction):Unit = inTXModification(tx)
    val query = "MATCH ()-[r]->() RETURN count(r)"

    // Then
    compareCount(query, 0, expectedLogicalPlan = "RelationshipCountFromCountStore")
    setupBigModel()
    compareCount(query, 3, expectedLogicalPlan = "RelationshipCountFromCountStore", assertCountInTransaction = true, executeBefore = executeBefore)
  }

  test("counts relationships with type using count store considering transaction state") {
    // Given
    def executeBefore(tx: InternalTransaction):Unit = inTXModification(tx)
    val query = "MATCH ()-[r:KNOWS]->() RETURN count(r)"

    // Then
    compareCount(query, 0, expectedLogicalPlan = "RelationshipCountFromCountStore")
    setupBigModel()
    compareCount(query, 3, expectedLogicalPlan = "RelationshipCountFromCountStore", assertCountInTransaction = true, executeBefore = executeBefore)
  }

  test("counts relationships with multiple types using count store considering transaction state") {
    // Given
    def executeBefore(tx: InternalTransaction):Unit = inTXModification(tx, type2 = "FOLLOWS", type3 = "FRIEND_OF")
    val query = "MATCH ()-[r:KNOWS|FOLLOWS]->() RETURN count(r)"

    // Then
    compareCount(query, 0, expectedLogicalPlan = "RelationshipCountFromCountStore")
    setupBigModel()
    compareCount(query, 2, expectedLogicalPlan = "RelationshipCountFromCountStore", assertCountInTransaction = true, executeBefore = executeBefore)
  }

  test("counts relationships using count store and projection with expression considering transaction state") {
    // Given
    def executeBefore(tx: InternalTransaction):Unit = inTXModification(tx)
    val query = "MATCH ()-[r]->() RETURN count(r) > 2"

    // Then
    compareCount(query, false, Configs.InterpretedAndSlottedAndPipelined, expectedLogicalPlan = "RelationshipCountFromCountStore")
    setupBigModel()
    compareCount(query, true, Configs.InterpretedAndSlottedAndPipelined, expectedLogicalPlan = "RelationshipCountFromCountStore", assertCountInTransaction = true, executeBefore = executeBefore)
  }

  test("counts relationships using count store and projection with expression and variable considering transaction state") {
    // Given
    def executeBefore(tx: InternalTransaction):Unit = inTXModification(tx)
    val query = "MATCH ()-[r]->() RETURN count(r)/3*5 as someNum"

    // Then
    compareCount(query, 0, expectedLogicalPlan = "RelationshipCountFromCountStore")
    setupBigModel()
    compareCount(query, 5, expectedLogicalPlan = "RelationshipCountFromCountStore", assertCountInTransaction = true, executeBefore = executeBefore)
  }

  test("counts relationships using count store and horizon with further query") {
    // Given
    def executeBefore(tx:InternalTransaction):Unit = inTXModification(tx, label2 = "Admin", label3 = "Person")
    val query = """
                  |MATCH (:User)-[r:KNOWS]->() WITH count(r) as userKnows
                  |MATCH (n)-[r:KNOWS]->() WITH count(r) as otherKnows, n, userKnows WHERE otherKnows <> userKnows
                  |RETURN userKnows, otherKnows
                """.stripMargin
    val expectSucceed = Configs.InterpretedAndSlottedAndPipelined

    // Then
    val resultOnEmpty = executeWith(
      expectSucceed,
      query,
      executeBefore = executeBefore,
      planComparisonStrategy = ComparePlansWithAssertion(plan => {
        plan should includeSomewhere.aPlan("RelationshipCountFromCountStore")
      }))
    resultOnEmpty.toList should equal(List())

    setupBigModel(label2 = "Admin")

    val resultAssertionInTx: Option[RewindableExecutionResult => Unit] = Some({ result: RewindableExecutionResult => result.toList should equal(List(Map("userKnows" -> 2, "otherKnows" -> 1))) })
    executeWith(
      expectSucceed,
      query,
      executeBefore = executeBefore,
      planComparisonStrategy = ComparePlansWithAssertion(plan => {
        plan should includeSomewhere.aPlan("RelationshipCountFromCountStore")
      }),
      resultAssertionInTx = resultAssertionInTx)
  }

  //  MATCH (n:X)-[r:Y]->() WITH count(r) as rcount MATCH (n)-[r:Y]->() WHERE count(r) = rcount RETURN rcount, labels(n)
  test("counts relationships with type using count store considering transaction state and multiple types in model") {
    // Given
    def executeBefore(tx: InternalTransaction):Unit = inTXModification(tx, type2 = "FOLLOWS")
    val query = "MATCH ()-[r:KNOWS]->() RETURN count(r)"

    // Then
    compareCount(query, 0, expectedLogicalPlan = "RelationshipCountFromCountStore")
    setupBigModel()
    compareCount(query, 2, expectedLogicalPlan = "RelationshipCountFromCountStore", assertCountInTransaction = true, executeBefore = executeBefore)
  }

  test("counts relationships with type and labeled source using count store considering transaction state") {
    // Given
    def executeBefore(tx: InternalTransaction):Unit = inTXModification(tx)
    val query = "MATCH (:User)-[r:KNOWS]->() RETURN count(r)"

    // Then
    compareCount(query, 0, expectedLogicalPlan = "RelationshipCountFromCountStore")
    setupBigModel()
    compareCount(query, 3, expectedLogicalPlan = "RelationshipCountFromCountStore", assertCountInTransaction = true, executeBefore = executeBefore)
  }

  test("counts relationships with type, reverse direction and labeled source using count store considering transaction state") {
    // Given
    def executeBefore(tx: InternalTransaction):Unit = inTXModification(tx)
    val query = "MATCH (:User)<-[r:KNOWS]-() RETURN count(r)"

    // Then
    compareCount(query, 0, expectedLogicalPlan = "RelationshipCountFromCountStore")
    setupBigModel()
    compareCount(query, 3, expectedLogicalPlan = "RelationshipCountFromCountStore", assertCountInTransaction = true, executeBefore = executeBefore)
  }

  test("counts relationships with unspecified type and labeled source using count store considering transaction state") {
    // Given
    def executeBefore(tx: InternalTransaction):Unit = inTXModification(tx)
    val query = "MATCH (:User)-[r]->() RETURN count(r)"

    // Then
    compareCount(query, 0, expectedLogicalPlan = "RelationshipCountFromCountStore")
    setupBigModel()
    compareCount(query, 3, expectedLogicalPlan = "RelationshipCountFromCountStore", assertCountInTransaction = true, executeBefore = executeBefore)
  }

  test("counts relationships with type and labeled destination using count store considering transaction state") {
    // Given
    def executeBefore(tx: InternalTransaction):Unit = inTXModification(tx)
    val query = "MATCH ()-[r:KNOWS]->(:User) RETURN count(r)"

    // Then
    compareCount(query, 0, expectedLogicalPlan = "RelationshipCountFromCountStore")
    setupBigModel()
    compareCount(query, 3, expectedLogicalPlan = "RelationshipCountFromCountStore", assertCountInTransaction = true, executeBefore = executeBefore)
  }

  test("counts relationships with type, reverse direction and labeled destination using count store considering transaction state") {
    // Given
    def executeBefore(tx: InternalTransaction):Unit = inTXModification(tx)
    val query = "MATCH ()<-[r:KNOWS]-(:User) RETURN count(r)"

    // Then
    compareCount(query, 0, expectedLogicalPlan = "RelationshipCountFromCountStore")
    setupBigModel()
    compareCount(query, 3, expectedLogicalPlan = "RelationshipCountFromCountStore", assertCountInTransaction = true, executeBefore = executeBefore)
  }

  test("counts relationships with unspecified type and labeled destination using count store considering transaction state") {
    // Given
    def executeBefore(tx: InternalTransaction):Unit = inTXModification(tx)
    val query = "MATCH ()-[r]->(:User) RETURN count(r)"

    // Then
    compareCount(query, 0, expectedLogicalPlan = "RelationshipCountFromCountStore")
    setupBigModel()
    compareCount(query, 3, expectedLogicalPlan = "RelationshipCountFromCountStore", assertCountInTransaction = true, executeBefore = executeBefore)
  }

  test("counts relationships with type and labeled source and destination without using count store considering transaction state") {
    // Given
    def executeBefore(tx: InternalTransaction):Unit = inTXModification(tx)
    val query = "MATCH (:User)-[r:KNOWS]->(:User) RETURN count(r)"

    // Then
    compareCount(query, 0, Configs.All, expectedLogicalPlan = "NodeByLabelScan")
    setupBigModel()
    compareCount(query, 3, Configs.All, expectedLogicalPlan = "NodeByLabelScan", assertCountInTransaction = true, executeBefore = executeBefore)
  }

  test("counts relationships with unspecified type and labeled source and destination without using count store considering transaction state") {
    // Given
    def executeBefore(tx: InternalTransaction):Unit = inTXModification(tx)
    val query = "MATCH (:User)-[r]->(:User) RETURN count(r)"

    // Then
    compareCount(query, 0, Configs.All, expectedLogicalPlan = "NodeByLabelScan")
    setupBigModel()
    compareCount(query, 3, Configs.All, expectedLogicalPlan = "NodeByLabelScan", assertCountInTransaction = true, executeBefore = executeBefore)
  }

  test("should work even when the tokens are already known") {
    executeSingle(
      s"""
         |CREATE (p:User {name: 'Petra'})
         |CREATE (s:User {name: 'Steve'})
         |CREATE (p)-[:KNOWS]->(s)
      """.stripMargin)

    compareCount("MATCH (:User)-[r:KNOWS]->() RETURN count(r)", 1, expectedLogicalPlan = "RelationshipCountFromCountStore")
  }

  test("runtime checking of tokens - nodes - not existing when planning nor when running") {
    createLabeledNode("NotRelated")
    compareCount("MATCH (n:Nonexistent) RETURN count(n)", 0)
  }

  test("runtime checking of tokens - nodes - not existing when planning but exists when running") {
    createLabeledNode("NotRelated")
    compareCount("MATCH (n:justCreated) RETURN count(n)", 0)
    createLabeledNode("justCreated")
    compareCount("MATCH (n:justCreated) RETURN count(n)", 1)
  }

  test("runtime checking of tokens - relationships - not existing when planning nor when running") {
    relate(createNode(), createNode(), "UNRELATED")
    compareCount("MATCH ()-[r:Nonexistent]->() RETURN count(r)", 0, expectedLogicalPlan = "RelationshipCountFromCountStore")
  }

  test("runtime checking of tokens - relationships - not existing when planning but exists when running") {
    relate(createNode(), createNode(), "UNRELATED")
    compareCount("MATCH ()-[r:justCreated]->() RETURN count(r)", 0, expectedLogicalPlan = "RelationshipCountFromCountStore")
    relate(createNode(), createNode(), "justCreated")
    compareCount("MATCH ()-[r:justCreated]->() RETURN count(r)", 1, expectedLogicalPlan = "RelationshipCountFromCountStore")
  }

  test("count store on two unlabeled nodes") {
    // Given
    def executeBefore(tx: InternalTransaction): Unit = setupModel(tx)
    val query = "MATCH (n), (m) RETURN count(n)"

    // Then
    compareCount(query, 0)
    compareCount(query, 9, executeBefore = executeBefore)
  }

  test("count store on two unlabeled nodes and count(*)") {
    // Given
    def executeBefore(tx: InternalTransaction): Unit = setupModel(tx)
    val query = "MATCH (n), (m) RETURN count(*)"

    // Then
    compareCount(query, 0)
    compareCount(query, 9, executeBefore = executeBefore)
  }

  test("count store on one labeled node and one unlabeled") {
    // Given
    def executeBefore(tx: InternalTransaction): Unit = setupModel(tx)
    val query = "MATCH (n:User),(m) RETURN count(n)"

    // Then
    compareCount(query, 0)
    compareCount(query, 6, executeBefore = executeBefore)
  }

  test("count store on one labeled node and one unlabeled and count(*)") {
    // Given
    def executeBefore(tx: InternalTransaction): Unit = setupModel(tx)
    val query = "MATCH (n:User),(m) RETURN count(*)"

    // Then
    compareCount(query, 0)
    compareCount(query, 6, executeBefore = executeBefore)
  }

  test("count store on two labeled nodes") {
    // Given
    def executeBefore(tx: InternalTransaction): Unit = setupModel(tx)
    val query = "MATCH (n:User),(m:User) RETURN count(n)"

    // Then
    compareCount(query, 0)
    compareCount(query, 4, executeBefore = executeBefore)
  }

  test("count store with many nodes") {
    // Given
    def executeBefore(tx: InternalTransaction): Unit = setupModel(tx)
    val query = "MATCH (n:User),(m),(o:User),(p) RETURN count(*)"

    // Then
    compareCount(query, 0)
    compareCount(query, 36, executeBefore = executeBefore)
  }

  test("count store with many but odd number of nodes") {
    // Given
    def executeBefore(tx: InternalTransaction): Unit = setupModel(tx)
    val query = "MATCH (n:User),(m),(o:User),(p), (q) RETURN count(*)"

    // Then
    compareCount(query, 0)
    compareCount(query, 108, executeBefore = executeBefore)
  }

  private def setupModel(tx: InternalTransaction,
                         label1: String = "User",
                         label2: String = "User",
                         type1: String = "KNOWS"): Unit = {
    tx.execute(
      s"""
         |CREATE (p:$label1 {name: 'Petra'})
         |CREATE (s:$label2 {name: 'Steve'})
         |CREATE (p)-[:$type1]->(s)
         |CREATE (a)-[:LOOP]->(a)
      """.stripMargin)
  }

  private def setupBigModel(label1: String = "User",
                            label2: String = "User",
                            type1: String = "KNOWS"): Unit = {
    executeSingle(
      s"""
         |CREATE (m:X {name: 'Mats'})
         |CREATE (p:$label1 {name: 'Petra'})
         |CREATE (s:$label2 {name: 'Steve'})
         |CREATE (p)-[:$type1]->(s)
         |CREATE (m)-[:$type1]->(s)
      """.stripMargin)
  }

  private def inTXModification(tx: InternalTransaction,
                               label1: String = "User",
                               label2: String = "User",
                               label3: String = "User",
                               type2: String = "KNOWS",
                               type3: String = "KNOWS"): Unit = {
    tx.execute("MATCH (m:X)-[r]->() DELETE m, r")
    tx.execute(
      s"""
         |MATCH (p:$label1 {name: 'Petra'})
         |MATCH (s:$label2 {name: 'Steve'})
         |CREATE (c:$label3 {name: 'Craig'})
         |CREATE (p)-[:$type2]->(c)
         |CREATE (c)-[:$type3]->(s)
        """.stripMargin)
  }

  private def compareCount(query: String,
                           expectedCount: Any,
                           expectSucceed: TestConfiguration = Configs.InterpretedAndSlottedAndPipelined,
                           expectedLogicalPlan: String = "NodeCountFromCountStore",
                           assertCountInTransaction: Boolean = false,
                           executeBefore: InternalTransaction => Unit = _ => {}): Unit = {

    val resultAssertionInTx: Option[RewindableExecutionResult => Unit] =
      if (assertCountInTransaction) {
        Some({ result: RewindableExecutionResult => result.columnAs(result.columns.head).toSet[Any] should equal(Set(expectedCount)) })
      } else {
        None
      }

    val result = executeWith(
      expectSucceed,
      query,
      executeBefore = executeBefore,
      planComparisonStrategy = ComparePlansWithAssertion(plan => {
        plan should includeSomewhere.aPlan(expectedLogicalPlan)
      }),
      resultAssertionInTx = resultAssertionInTx)
    result.columnAs(result.columns.head).toSet[Any] should equal(Set(expectedCount))
  }

}
