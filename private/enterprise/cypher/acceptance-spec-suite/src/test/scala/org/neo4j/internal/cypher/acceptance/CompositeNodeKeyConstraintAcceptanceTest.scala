/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance

import java.time.LocalDate
import java.time.LocalDateTime

import org.neo4j.cypher.ExecutionEngineFunSuite
import org.neo4j.cypher.QueryStatisticsTestSupport
import org.neo4j.cypher.internal.util.helpers.StringHelper.RichString
import org.neo4j.graphdb.ConstraintViolationException
import org.neo4j.internal.cypher.acceptance.comparisonsupport.Configs
import org.neo4j.internal.cypher.acceptance.comparisonsupport.CypherComparisonSupport

class CompositeNodeKeyConstraintAcceptanceTest extends ExecutionEngineFunSuite with CypherComparisonSupport with QueryStatisticsTestSupport {

  test("Node key constraint creation should be reported") {
    // When
    val result = executeSingle("CREATE CONSTRAINT ON (n:Person) ASSERT (n.email) IS NODE KEY", Map.empty)

    // Then
    assertStats(result, nodekeyConstraintsAdded = 1)
  }

  test("Uniqueness constraint creation should be reported") {
    // When
    val result = executeSingle("CREATE CONSTRAINT ON (n:Person) ASSERT n.email IS UNIQUE", Map.empty)

    // Then
    assertStats(result, uniqueConstraintsAdded = 1)
  }

  test("should be able to create and remove single property NODE KEY") {
    // When
    executeSingle("CREATE CONSTRAINT ON (n:Person) ASSERT (n.email) IS NODE KEY")

    // Then
    graph should haveConstraints("NODE_KEY:Person(email)")

    // When
    executeSingle("DROP CONSTRAINT ON (n:Person) ASSERT (n.email) IS NODE KEY")

    // Then
    graph should not(haveConstraints("NODE_KEY:Person(email)"))
  }

  test("should be able to create and remove multiple property NODE KEY") {
    // When
    executeSingle("CREATE CONSTRAINT ON (n:Person) ASSERT (n.email) IS NODE KEY")
    executeSingle("CREATE CONSTRAINT ON (n:Person) ASSERT (n.firstname,n.lastname) IS NODE KEY")

    // Then
    graph should haveConstraints("NODE_KEY:Person(email)", "NODE_KEY:Person(firstname,lastname)")

    // When
    executeSingle("DROP CONSTRAINT ON (n:Person) ASSERT (n.firstname,n.lastname) IS NODE KEY")

    // Then
    graph should haveConstraints("NODE_KEY:Person(email)")
    graph should not(haveConstraints("NODE_KEY:Person(firstname,lastname)"))
  }

  test("composite NODE KEY constraint should not block adding nodes with different properties") {
    // When
    executeSingle("CREATE CONSTRAINT ON (n:User) ASSERT (n.firstname,n.lastname) IS NODE KEY")

    // Then
    createLabeledNode(Map("firstname" -> "Joe", "lastname" -> "Soap"), "User")
    createLabeledNode(Map("firstname" -> "Joe", "lastname" -> "Smoke"), "User")
    createLabeledNode(Map("firstname" -> "Jake", "lastname" -> "Soap"), "User")
  }

  test("composite NODE KEY constraint should not block adding nodes with different properties (named constraint)") {
    // When
    executeSingle("CREATE CONSTRAINT user_constraint ON (n:User) ASSERT (n.firstname,n.lastname) IS NODE KEY")

    // Then
    createLabeledNode(Map("firstname" -> "Joe", "lastname" -> "Soap"), "User")
    createLabeledNode(Map("firstname" -> "Joe", "lastname" -> "Smoke"), "User")
    createLabeledNode(Map("firstname" -> "Jake", "lastname" -> "Soap"), "User")
  }

  test("composite NODE KEY constraint should block adding nodes with same properties") {
    // When
    executeSingle("CREATE CONSTRAINT ON (n:User) ASSERT (n.firstname,n.lastname) IS NODE KEY")
    createLabeledNode(Map("firstname" -> "Joe", "lastname" -> "Soap"), "User")
    createLabeledNode(Map("firstname" -> "Joe", "lastname" -> "Smoke"), "User")

    // Then
    a[ConstraintViolationException] should be thrownBy {
      createLabeledNode(Map("firstname" -> "Joe", "lastname" -> "Soap"), "User")
    }
  }

  test("composite NODE KEY constraint should block adding nodes with same properties (named constraint)") {
    // When
    executeSingle("CREATE CONSTRAINT user_constraint ON (n:User) ASSERT (n.firstname,n.lastname) IS NODE KEY")
    createLabeledNode(Map("firstname" -> "Joe", "lastname" -> "Soap"), "User")
    createLabeledNode(Map("firstname" -> "Joe", "lastname" -> "Smoke"), "User")

    // Then
    a[ConstraintViolationException] should be thrownBy {
      createLabeledNode(Map("firstname" -> "Joe", "lastname" -> "Soap"), "User")
    }
  }

  test("single property NODE KEY constraint should block adding nodes with missing property") {
    // When
    executeSingle("CREATE CONSTRAINT ON (n:User) ASSERT (n.email) IS NODE KEY")
    createLabeledNode(Map("email" -> "joe@soap.tv"), "User")
    createLabeledNode(Map("email" -> "jake@soap.tv"), "User")

    // Then
    a[ConstraintViolationException] should be thrownBy {
      createLabeledNode(Map("firstname" -> "Joe", "lastname" -> "Soap"), "User")
    }
  }

  test("single property NODE KEY constraint should block adding nodes with missing property (named constraint)") {
    // When
    executeSingle("CREATE CONSTRAINT user_constraint ON (n:User) ASSERT (n.email) IS NODE KEY")
    createLabeledNode(Map("email" -> "joe@soap.tv"), "User")
    createLabeledNode(Map("email" -> "jake@soap.tv"), "User")

    // Then
    a[ConstraintViolationException] should be thrownBy {
      createLabeledNode(Map("firstname" -> "Joe", "lastname" -> "Soap"), "User")
    }
  }

  test("composite NODE KEY constraint should block adding nodes with missing properties") {
    // When
    executeSingle("CREATE CONSTRAINT ON (n:User) ASSERT (n.firstname,n.lastname) IS NODE KEY")
    createLabeledNode(Map("firstname" -> "Joe", "lastname" -> "Soap"), "User")
    createLabeledNode(Map("firstname" -> "Joe", "lastname" -> "Smoke"), "User")

    // Then
    a[ConstraintViolationException] should be thrownBy {
      createLabeledNode(Map("firstname" -> "Joe", "lastnamex" -> "Soap"), "User")
    }
  }

  test("composite NODE KEY constraint should not fail when we have nodes with different properties") {
    // When
    createLabeledNode(Map("firstname" -> "Joe", "lastname" -> "Soap"), "User")
    createLabeledNode(Map("firstname" -> "Joe", "lastname" -> "Smoke"), "User")
    createLabeledNode(Map("firstname" -> "Jake", "lastname" -> "Soap"), "User")

    // Then
    executeSingle("CREATE CONSTRAINT ON (n:User) ASSERT (n.firstname,n.lastname) IS NODE KEY")
  }

  test("composite NODE KEY constraint should fail when we have nodes with same properties") {
    // When
    val a = createLabeledNode(Map("firstname" -> "Joe", "lastname" -> "Soap"), "User").getId
    createLabeledNode(Map("firstname" -> "Joe", "lastname" -> "Smoke"), "User").getId
    val c = createLabeledNode(Map("firstname" -> "Joe", "lastname" -> "Soap"), "User").getId

    // Then
    val query = "CREATE CONSTRAINT ON (n:User) ASSERT (n.firstname,n.lastname) IS NODE KEY"
    val errorMessage = "Both Node(%d) and Node(%d) have the label `User` and properties `firstname` = 'Joe', `lastname` = 'Soap'".format(a, c)
    failWithError(Configs.All, query, errorMessage)
  }

  test("trying to add duplicate node when node key constraint exists") {
    createLabeledNode(Map("name" -> "A"), "Person")
    executeSingle("CREATE CONSTRAINT ON (person:Person) ASSERT (person.name) IS NODE KEY".fixNewLines)

    failWithError(
      Configs.InterpretedAndSlottedAndPipelined,
      "CREATE (n:Person) SET n.name = 'A'",
      "Node(0) already exists with label `Person` and property `name` = 'A'"
    )
  }

  test("trying to add duplicate node when composite NODE KEY constraint exists") {
    createLabeledNode(Map("name" -> "A", "surname" -> "B"), "Person")
    executeSingle("CREATE CONSTRAINT ON (person:Person) ASSERT (person.name, person.surname) IS NODE KEY".fixNewLines)

    failWithError(
      Configs.InterpretedAndSlottedAndPipelined,
      "CREATE (n:Person) SET n.name = 'A', n.surname = 'B'",
      String.format("Node(0) already exists with label `Person` and properties `name` = 'A', `surname` = 'B'")
    )
  }

  test("trying to add a composite node key constraint when duplicates exist") {
    val a = createLabeledNode(Map("name" -> "A", "surname" -> "B"), "Person").getId
    val b = createLabeledNode(Map("name" -> "A", "surname" -> "B"), "Person").getId

    failWithError(
      Configs.All,
      "CREATE CONSTRAINT ON (person:Person) ASSERT (person.name, person.surname) IS NODE KEY",
      ("Unable to create Constraint( name='constraint_745a766d', type='NODE KEY', schema=(:Person {name, surname}) ):%s" +
        "Both Node(%d) and Node(%d) have the label `Person` and properties `name` = 'A', `surname` = 'B'").format(String.format("%n"), a, b)
    )
  }

  test("trying to add a node key constraint when duplicates exist") {
    val a = createLabeledNode(Map("name" -> "A"), "Person").getId
    val b = createLabeledNode(Map("name" -> "A"), "Person").getId

    failWithError(
      Configs.All,
      "CREATE CONSTRAINT ON (person:Person) ASSERT (person.name) IS NODE KEY",
      ("Unable to create Constraint( name='constraint_9b73711d', type='NODE KEY', schema=(:Person {name}) ):%s" +
        "Both Node(%d) and Node(%d) have the label `Person` and property `name` = 'A'").format(String.format("%n"), a, b)
    )
  }

  test("trying to add a named node key constraint when duplicates exist") {
    val a = createLabeledNode(Map("name" -> "A"), "Person").getId
    val b = createLabeledNode(Map("name" -> "A"), "Person").getId

    failWithError(
      Configs.All,
      "CREATE CONSTRAINT person_constraint ON (person:Person) ASSERT (person.name) IS NODE KEY",
      ("Unable to create Constraint( name='person_constraint', type='NODE KEY', schema=(:Person {name}) ):%s" +
        "Both Node(%d) and Node(%d) have the label `Person` and property `name` = 'A'").format(String.format("%n"), a, b)
    )
  }

  test("drop a non existent node key constraint") {
    failWithError(
      Configs.All,
      "DROP CONSTRAINT ON (person:Person) ASSERT (person.name) IS NODE KEY",
      "No such constraint"
    )
  }

  test("trying to add duplicate node when composite node key constraint exists") {
    createLabeledNode(Map("name" -> "A", "surname" -> "B"), "Person")
    executeSingle("CREATE CONSTRAINT ON (person:Person) ASSERT (person.name, person.surname) IS NODE KEY".fixNewLines)

    failWithError(
      Configs.InterpretedAndSlottedAndPipelined,
      "CREATE (n:Person) SET n.name = 'A', n.surname = 'B'",
      String.format("Node(0) already exists with label `Person` and properties `name` = 'A', `surname` = 'B'")
    )
  }

  test("should give appropriate error message when there is already an index") {
    // Given
    executeSingle("CREATE INDEX FOR (n:Person) ON (n.firstname, n.lastname)".fixNewLines)

    // then
    failWithError(Configs.All,
      "CREATE CONSTRAINT ON (n:Person) ASSERT (n.firstname,n.lastname) IS NODE KEY",
      "There already exists an index (:Person {firstname, lastname}). " +
        "A constraint cannot be created until the index has been dropped.")
  }

  test("should give appropriate error message when there is already an index (named constraint)") {
    // Given
    executeSingle("CREATE INDEX FOR (n:Person) ON (n.firstname, n.lastname)".fixNewLines)

    // then
    failWithError(Configs.All,
      "CREATE CONSTRAINT my_contraint ON (n:Person) ASSERT (n.firstname,n.lastname) IS NODE KEY",
      "There already exists an index (:Person {firstname, lastname}). " +
        "A constraint cannot be created until the index has been dropped.")
  }

  test("should give appropriate error message when there is already an named index") {
    // Given
    executeSingle("CREATE INDEX my_index FOR (n:Person) ON (n.firstname, n.lastname)".fixNewLines)

    // then
    failWithError(Configs.All,
      "CREATE CONSTRAINT ON (n:Person) ASSERT (n.firstname,n.lastname) IS NODE KEY",
      "There already exists an index (:Person {firstname, lastname}). " +
        "A constraint cannot be created until the index has been dropped.")
  }

  test("should give appropriate error message when there is already an named index (named constraint)") {
    // Given
    executeSingle("CREATE INDEX my_index FOR (n:Person) ON (n.firstname, n.lastname)".fixNewLines)

    // then
    failWithError(Configs.All,
      "CREATE CONSTRAINT my_constraint ON (n:Person) ASSERT (n.firstname,n.lastname) IS NODE KEY",
      "There already exists an index (:Person {firstname, lastname}). " +
        "A constraint cannot be created until the index has been dropped.")
  }

  test("should give appropriate error message when there is already an named index (same name and schema for constraint)") {
    // Given
    executeSingle("CREATE INDEX my_person FOR (n:Person) ON (n.firstname, n.lastname)".fixNewLines)

    // then
    failWithError(Configs.All,
      "CREATE CONSTRAINT my_person ON (n:Person) ASSERT (n.firstname,n.lastname) IS NODE KEY",
      "There already exists an index called 'my_person'.")
  }

  test("should give appropriate error message when there is already an named index (same name for constraint, different schema)") {
    // Given
    executeSingle("CREATE INDEX my_person FOR (n:Person) ON (n.firstname, n.lastname)".fixNewLines)

    // then
    failWithError(Configs.All,
      "CREATE CONSTRAINT my_person ON (n:Person) ASSERT (n.firstname,n.surname) IS NODE KEY",
      "There already exists an index called 'my_person'.")
  }

  test("should give appropriate error message when there is already a NODE KEY constraint") {
    // Given
    executeSingle("CREATE CONSTRAINT ON (n:Person) ASSERT (n.firstname,n.lastname) IS NODE KEY".fixNewLines)

    // then
    failWithError(
      Configs.All,
      "CREATE INDEX FOR (n:Person) ON (n.firstname, n.lastname)",
      "There is a uniqueness constraint on (:Person {firstname, lastname}), " +
        "so an index is already created that matches this.")
  }

  test("should give appropriate error message when there is already a named NODE KEY constraint") {
    // Given
    executeSingle("CREATE CONSTRAINT my_constraint ON (n:Person) ASSERT (n.firstname,n.lastname) IS NODE KEY".fixNewLines)

    // then
    failWithError(
      Configs.All,
      "CREATE INDEX FOR (n:Person) ON (n.firstname, n.lastname)",
      "There is a uniqueness constraint on (:Person {firstname, lastname}), " +
        "so an index is already created that matches this.")
  }

  test("should give appropriate error message when there is already a NODE KEY constraint (named index)") {
    // Given
    executeSingle("CREATE CONSTRAINT ON (n:Person) ASSERT (n.firstname,n.lastname) IS NODE KEY".fixNewLines)

    // then
    failWithError(
      Configs.All,
      "CREATE INDEX my_index FOR (n:Person) ON (n.firstname, n.lastname)",
      "There is a uniqueness constraint on (:Person {firstname, lastname}), " +
        "so an index is already created that matches this.")
  }

  test("should give appropriate error message when there is already a named NODE KEY constraint (named index)") {
    // Given
    executeSingle("CREATE CONSTRAINT my_constraint ON (n:Person) ASSERT (n.firstname,n.lastname) IS NODE KEY".fixNewLines)

    // then
    failWithError(
      Configs.All,
      "CREATE INDEX my_index FOR (n:Person) ON (n.firstname, n.lastname)",
      "There is a uniqueness constraint on (:Person {firstname, lastname}), " +
        "so an index is already created that matches this.")
  }

  test("should give appropriate error message when there is already a named NODE KEY constraint (same name and schema for index)") {
    // Given
    executeSingle("CREATE CONSTRAINT my_person ON (n:Person) ASSERT (n.firstname,n.lastname) IS NODE KEY".fixNewLines)

    // then
    failWithError(
      Configs.All,
      "CREATE INDEX my_person FOR (n:Person) ON (n.firstname, n.lastname)",
      "There already exists a constraint called 'my_person'.")
  }


  test("should give appropriate error message when there is already a named NODE KEY constraint (same name for index, different schema)") {
    // Given
    executeSingle("CREATE CONSTRAINT my_person ON (n:Person) ASSERT (n.firstname,n.lastname) IS NODE KEY".fixNewLines)

    // then
    failWithError(
      Configs.All,
      "CREATE INDEX my_person FOR (n:Person) ON (n.firstname, n.lastname)",
      "There already exists a constraint called 'my_person'.")
  }

  test("Should give a nice error message when trying to remove property with node key constraint") {
    // Given
    executeSingle("CREATE CONSTRAINT ON (n:Person) ASSERT (n.firstname, n.surname) IS NODE KEY".fixNewLines)
    val id = createLabeledNode(Map("firstname" -> "John", "surname" -> "Wood"), "Person").getId

    // Expect
    failWithError(Configs.InterpretedAndSlottedAndPipelined,
      "MATCH (p:Person {firstname: 'John', surname: 'Wood'}) REMOVE p.surname",
      s"Node($id) with label `Person` must have the properties (firstname, surname)")

  }

  test("Should be able to remove non constrained property") {
    // Given
    executeSingle("CREATE CONSTRAINT ON (n:Person) ASSERT (n.firstname, n.surname) IS NODE KEY".fixNewLines)
    val node = createLabeledNode(Map("firstname" -> "John", "surname" -> "Wood", "foo" -> "bar"), "Person")

    executeWith(Configs.InterpretedAndSlottedAndPipelined, "MATCH (p:Person {firstname: 'John', surname: 'Wood'}) REMOVE p.foo".fixNewLines)

    // Then
    graph.withTx( tx => {
      tx.getNodeById(node.getId).hasProperty("foo") shouldBe false
    } )
  }

  test("Should be able to delete node constrained with node key constraint") {
    // Given
    executeSingle("CREATE CONSTRAINT ON (n:Person) ASSERT (n.firstname, n.surname) IS NODE KEY".fixNewLines)
    createLabeledNode(Map("firstname" -> "John", "surname" -> "Wood", "foo" -> "bar"), "Person")

    executeWith(Configs.InterpretedAndSlotted, "MATCH (p:Person {firstname: 'John', surname: 'Wood'}) DELETE p".fixNewLines)

    // Then
    inTx {tx =>
      tx.execute("MATCH (p:Person {firstname: 'John', surname: 'Wood'}) RETURN p".fixNewLines).hasNext shouldBe false
    }
  }

  test("Should be able to remove label when node key constraint") {
    // Given
    executeSingle("CREATE CONSTRAINT ON (n:Person) ASSERT (n.firstname, n.surname) IS NODE KEY".fixNewLines)
    createLabeledNode(Map("firstname" -> "John", "surname" -> "Wood", "foo" -> "bar"), "Person")

    executeWith(Configs.InterpretedAndSlotted, "MATCH (p:Person {firstname: 'John', surname: 'Wood'}) REMOVE p:Person".fixNewLines)

    // Then
    inTx { tx =>
      tx.execute("MATCH (p:Person {firstname: 'John', surname: 'Wood'}) RETURN p".fixNewLines).hasNext shouldBe false
    }
  }

  test("Should handle temporal with node key constraint") {
    // When
    executeSingle("CREATE CONSTRAINT ON (n:User) ASSERT (n.birthday) IS NODE KEY")

    // Then
    createLabeledNode(Map("birthday" -> LocalDate.of(1991, 10, 18)), "User")
    createLabeledNode(Map("birthday" -> LocalDateTime.of(1991, 10, 18, 0, 0, 0, 0)), "User")
    createLabeledNode(Map("birthday" -> "1991-10-18"), "User")
    a[ConstraintViolationException] should be thrownBy {
      createLabeledNode(Map("birthday" -> LocalDate.of(1991, 10, 18)), "User")
    }
  }

  test("Should handle temporal with composite node key constraint") {
    // When
    executeSingle("CREATE CONSTRAINT ON (n:User) ASSERT (n.name, n.birthday) IS NODE KEY")

    // Then
    createLabeledNode(Map("name" -> "Neo", "birthday" -> LocalDate.of(1991, 10, 18)), "User")
    createLabeledNode(Map("name" -> "Neo", "birthday" -> LocalDateTime.of(1991, 10, 18, 0, 0, 0, 0)), "User")
    createLabeledNode(Map("name" -> "Neo", "birthday" -> "1991-10-18"), "User")
    createLabeledNode(Map("name" -> "Neolina", "birthday" -> "1991-10-18"), "User")
    a[ConstraintViolationException] should be thrownBy {
      createLabeledNode(Map("name" -> "Neo", "birthday" -> LocalDate.of(1991, 10, 18)), "User")
    }
  }
}
