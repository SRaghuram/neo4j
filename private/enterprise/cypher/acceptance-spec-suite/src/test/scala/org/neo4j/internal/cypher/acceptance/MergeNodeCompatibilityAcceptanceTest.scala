/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.ExecutionEngineFunSuite
import org.neo4j.cypher.QueryStatisticsTestSupport
import org.neo4j.cypher.internal.helpers.NodeKeyConstraintCreator
import org.neo4j.cypher.internal.helpers.UniquenessConstraintCreator
import org.neo4j.exceptions.MergeConstraintConflictException
import org.neo4j.graphdb.Node
import org.neo4j.internal.cypher.acceptance.comparisonsupport.Configs
import org.neo4j.internal.cypher.acceptance.comparisonsupport.CypherComparisonSupport

class MergeNodeCompatibilityAcceptanceTest extends ExecutionEngineFunSuite with QueryStatisticsTestSupport
                                           with CypherComparisonSupport {

  Seq(UniquenessConstraintCreator, NodeKeyConstraintCreator).foreach { constraintCreator =>

    test(s"$constraintCreator: should_match_on_merge_using_multiple_unique_indexes_if_only_found_single_node_for_both_indexes") {
      // given
      constraintCreator.createConstraint(graph, "Person", "id")
      constraintCreator.createConstraint(graph, "Person", "mail")

      createLabeledNode(Map("id" -> 23, "mail" -> "emil@neo.com"), "Person")

      // when
      val results =
        executeWith(Configs.InterpretedAndSlotted, "merge (a:Person {id: 23, mail: 'emil@neo.com'}) on match set a.country='Sweden' return a")
      val result = results.columnAs("a").next().asInstanceOf[Node]

      // then
      countNodes() should equal(1)
      graph.withTx( tx => {
        val node = tx.getNodeById(result.getId)
        node.getProperty("id") should equal(23)
        node.getProperty("mail") should equal("emil@neo.com")
        node.getProperty("country") should equal("Sweden")
      } )
    }

    test(s"$constraintCreator: should_match_on_merge_using_multiple_unique_indexes_and_labels_if_only_found_single_node_for_both_indexes") {
      // given
      constraintCreator.createConstraint(graph, "Person", "id")
      constraintCreator.createConstraint(graph, "User", "mail")

      createLabeledNode(Map("id" -> 23, "mail" -> "emil@neo.com"), "Person", "User")

      // when
      val results =
        executeWith(Configs.InterpretedAndSlotted, "merge (a:Person:User {id: 23, mail: 'emil@neo.com'}) on match set a.country='Sweden' return a")
      val result = results.columnAs("a").next().asInstanceOf[Node]

      // then
      countNodes() should equal(1)
      graph.withTx( tx => {
        val node = tx.getNodeById(result.getId)
        node.getProperty("id") should equal(23)
        node.getProperty("mail") should equal("emil@neo.com")
        node.getProperty("country") should equal("Sweden")
      } )
    }

    test(s"$constraintCreator: should_match_on_merge_using_multiple_unique_indexes_using_same_key_if_only_found_single_node_for_both_indexes") {
      // given
      constraintCreator.createConstraint(graph, "Person", "id")
      constraintCreator.createConstraint(graph, "User", "id")

      createLabeledNode(Map("id" -> 23), "Person", "User")

      // when
      val results =
        executeWith(Configs.InterpretedAndSlotted, "merge (a:Person:User {id: 23}) on match set a.country='Sweden' return a")
      val result = results.columnAs("a").next().asInstanceOf[Node]

      // then
      countNodes() should equal(1)
      graph.withTx( tx => {
        val node = tx.getNodeById(result.getId)
        node.getProperty("id") should equal(23)
        node.getProperty("country") should equal("Sweden")
      } )
    }

    test(s"$constraintCreator: should_fail_on_merge_using_multiple_unique_indexes_using_same_key_if_found_different_nodes") {
      // given
      constraintCreator.createConstraint(graph, "Person", "id")
      constraintCreator.createConstraint(graph, "User", "id")

      createLabeledNode(Map("id" -> 23), "Person")
      createLabeledNode(Map("id" -> 23), "User")

      // when + then
      failWithError(Configs.InterpretedAndSlotted, "merge (a:Person:User {id: 23}) return a",
        List("can not create a new node due to conflicts with existing unique nodes"))
      countNodes() should equal(2)
    }

    test(s"$constraintCreator: should_create_on_merge_using_multiple_unique_indexes_if_found_no_nodes") {
      // given
      constraintCreator.createConstraint(graph, "Person", "id")
      constraintCreator.createConstraint(graph, "Person", "mail")

      // when
      val results =
        executeWith(Configs.InterpretedAndSlotted, "merge (a:Person {id: 23, mail: 'emil@neo.com'}) on create set a.country='Sweden' return a.id, a.mail, a.country, labels(a)")

      // then
      results.toSet should equal(Set(Map("a.id" -> 23, "a.mail" -> "emil@neo.com", "a.country" -> "Sweden", "labels(a)" -> List("Person"))))
    }

    test(s"$constraintCreator: should_create_on_merge_using_multiple_unique_indexes_and_labels_if_found_no_nodes") {
      // given
      constraintCreator.createConstraint(graph, "Person", "id")
      constraintCreator.createConstraint(graph, "User", "mail")

      // when
      val results =
        executeWith(Configs.InterpretedAndSlotted, "merge (a:Person:User {id: 23, mail: 'emil@neo.com'}) on create set a.country='Sweden' return a.id, a.mail, a.country, labels(a)")

      // then
      results.toSet should equal(Set(Map("a.id" -> 23, "a.mail" -> "emil@neo.com", "a.country" -> "Sweden", "labels(a)" -> List("Person", "User"))))
    }

    test(s"$constraintCreator: should_fail_on_merge_using_multiple_unique_indexes_if_found_different_nodes") {
      // given
      constraintCreator.createConstraint(graph, "Person", "id")
      constraintCreator.createConstraint(graph, "Person", "mail")

      createLabeledNode(Map("id" -> 23, "mail" -> "dummy"), "Person")
      createLabeledNode(Map("id" -> -1, "mail" -> "emil@neo.com"), "Person")

      expectMergeConstraintConflictException(
        "merge (a:Person {id: 23, mail: 'emil@neo.com'}) return a", Seq(
          "Merge did not find a matching node",
          "can not create a new node due to conflicts with existing unique nodes"
        ))

      countNodes() should equal(2)
    }

    def expectMergeConstraintConflictException(query: String, messages: Seq[String]): Unit = {
      val exception = intercept[MergeConstraintConflictException] {
        executeSingle(query, Map.empty)
      }
      messages.foreach { message =>
        exception.getMessage should include(message)
      }
    }

    test(s"$constraintCreator: should_fail_on_merge_using_multiple_unique_indexes_if_it_found_a_node_matching_single_property_only") {
      // given
      constraintCreator.createConstraint(graph, "Person", "id")
      constraintCreator.createConstraint(graph, "Person", "mail")

      createLabeledNode(Map("id" -> 23, "mail" -> "dummy"), "Person")

      // when + then
      expectMergeConstraintConflictException(
        "merge (a:Person {id: 23, mail: 'emil@neo.com'}) return a", Seq(
          "Merge did not find a matching node",
          "can not create a new node due to conflicts",
          "unique nodes"
        ))

      countNodes() should equal(1)
    }

    test(s"$constraintCreator: should_fail_on_merge_using_multiple_unique_indexes_and_labels_if_found_different_nodes") {
      // given
      constraintCreator.createConstraint(graph, "Person", "id")
      constraintCreator.createConstraint(graph, "User", "mail")

      createLabeledNode(Map("id" -> 23), "Person")
      createLabeledNode(Map("mail" -> "emil@neo.com"), "User")

      // when
      expectMergeConstraintConflictException(
        "merge (a:Person:User {id: 23, mail: 'emil@neo.com'}) return a", Seq(
          "Merge did not find a matching node",
          "can not create a new node due to conflicts with existing unique nodes"
        ))

      // then
      countNodes() should equal(2)
    }
  }
}
