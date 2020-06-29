/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.internal.cypher.acceptance

import org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME
import org.neo4j.graphdb.security.AuthorizationViolationException

//noinspection RedundantDefaultArgument
class CreateDeletePrivilegeAdministrationCommandAcceptanceTest extends AdministrationCommandAcceptanceTestBase with EnterpriseComponentVersionTestSupport {

  test("should return empty counts to the outside for commands that update the system graph internally") {
    // GIVEN
    execute("CREATE ROLE custom")

    // Notice: They are executed in succession so they have to make sense in that order
    assertQueriesAndSubQueryCounts(List(
      "GRANT CREATE ON GRAPH * ELEMENTS * TO custom" -> 2,
      "REVOKE CREATE ON GRAPH * ELEMENTS * FROM custom" -> 2,
      "DENY CREATE ON GRAPH * ELEMENTS * TO custom" -> 2,
      "REVOKE DENY CREATE ON GRAPH * ELEMENTS * FROM custom" -> 2,

      "GRANT CREATE ON GRAPH * NODES * TO custom" -> 1,
      "REVOKE CREATE ON GRAPH * NODES * FROM custom" -> 1,
      "DENY CREATE ON GRAPH * NODES * TO custom" -> 1,
      "REVOKE DENY CREATE ON GRAPH * NODES * FROM custom" -> 1,

      "GRANT CREATE ON GRAPH * RELATIONSHIPS * TO custom" -> 1,
      "REVOKE CREATE ON GRAPH * RELATIONSHIPS * FROM custom" -> 1,
      "DENY CREATE ON GRAPH * RELATIONSHIPS * TO custom" -> 1,
      "REVOKE DENY CREATE ON GRAPH * RELATIONSHIPS * FROM custom" -> 1,

      "GRANT CREATE ON GRAPH * ELEMENTS * TO custom" -> 2,
      "DENY CREATE ON GRAPH * ELEMENTS * TO custom" -> 2,
      "REVOKE CREATE ON GRAPH * ELEMENTS * FROM custom" -> 4,
    ))
  }

  Seq(
    ("grant", "GRANT", granted: privilegeFunction),
    ("deny", "DENY", denied: privilegeFunction),
  ).foreach {
    case (grantOrDeny, grantOrDenyCommand, grantedOrDenied) =>
      Seq(
        ("CREATE", create),
        ("DELETE", delete)
      ).foreach {
        case (createOrDelete, privilege) =>
          // Tests for granting and denying create privileges

          test(s"should $grantOrDeny $createOrDelete privilege to custom role for all graphs and all elements") {
            // GIVEN
            execute("CREATE ROLE custom")

            // WHEN
            execute(s"$grantOrDenyCommand $createOrDelete ON GRAPH * ELEMENTS * TO custom")

            // THEN
            execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
              grantedOrDenied(privilege).role("custom").node("*").map,
              grantedOrDenied(privilege).role("custom").relationship("*").map
            ))
          }

          test(s"should $grantOrDeny $createOrDelete privilege to custom role for a specific graph and all elements") {
            // GIVEN
            execute("CREATE ROLE custom")
            execute("CREATE DATABASE foo")

            // WHEN
            execute(s"$grantOrDenyCommand $createOrDelete ON GRAPH foo ELEMENTS * TO custom")

            // THEN
            execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
              grantedOrDenied(privilege).role("custom").database("foo").node("*").map,
              grantedOrDenied(privilege).role("custom").database("foo").relationship("*").map
            ))
          }

          test(s"should $grantOrDeny $createOrDelete privilege to custom role for multiple graphs and all elements") {
            // GIVEN
            execute("CREATE ROLE custom")
            execute("CREATE DATABASE foo")
            execute("CREATE DATABASE bar")

            // WHEN
            execute(s"$grantOrDenyCommand $createOrDelete ON GRAPH foo, bar ELEMENTS * TO custom")

            // THEN
            execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
              grantedOrDenied(privilege).role("custom").database("foo").node("*").map,
              grantedOrDenied(privilege).role("custom").database("foo").relationship("*").map,
              grantedOrDenied(privilege).role("custom").database("bar").node("*").map,
              grantedOrDenied(privilege).role("custom").database("bar").relationship("*").map
            ))
          }

          test(s"should $grantOrDeny $createOrDelete privilege to custom role for specific graph and all elements using parameter") {
            // GIVEN
            execute("CREATE ROLE custom")

            // WHEN
            execute(s"$grantOrDenyCommand $createOrDelete ON GRAPH $$graph ELEMENTS * TO custom", Map("graph" -> DEFAULT_DATABASE_NAME))

            // THEN
            execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
              grantedOrDenied(privilege).database(DEFAULT_DATABASE_NAME).role("custom").node("*").map,
              grantedOrDenied(privilege).database(DEFAULT_DATABASE_NAME).role("custom").relationship("*").map
            ))
          }

          test(s"should $grantOrDeny $createOrDelete privilege to multiple roles in a single grant") {
            // GIVEN
            execute("CREATE ROLE role1")
            execute("CREATE ROLE role2")
            execute("CREATE ROLE role3")
            execute("CREATE DATABASE foo")

            // WHEN
            execute(s"$grantOrDenyCommand $createOrDelete ON GRAPH foo ELEMENTS * TO role1, role2, role3")

            // THEN
            val expected: Seq[PrivilegeMapBuilder] = Seq(
              grantedOrDenied(privilege).database("foo").node("*"),
              grantedOrDenied(privilege).database("foo").relationship("*")
            )

            execute("SHOW ROLE role1 PRIVILEGES").toSet should be(expected.map(_.role("role1").map).toSet)
            execute("SHOW ROLE role2 PRIVILEGES").toSet should be(expected.map(_.role("role2").map).toSet)
            execute("SHOW ROLE role3 PRIVILEGES").toSet should be(expected.map(_.role("role3").map).toSet)
          }

          // Tests for revoke grant and revoke deny privilege privileges

          test(s"should revoke correct $grantOrDeny $createOrDelete privilege different graphs") {
            // GIVEN
            execute("CREATE ROLE custom")
            execute("CREATE DATABASE foo")
            execute("CREATE DATABASE bar")
            execute(s"$grantOrDenyCommand $createOrDelete ON GRAPH * ELEMENTS * TO custom")
            execute(s"$grantOrDenyCommand $createOrDelete ON GRAPH foo ELEMENTS * TO custom")
            execute(s"$grantOrDenyCommand $createOrDelete ON GRAPH bar ELEMENTS * TO custom")

            // WHEN
            execute(s"REVOKE $grantOrDenyCommand $createOrDelete ON GRAPH foo ELEMENTS * FROM custom")

            // THEN
            execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
              grantedOrDenied(privilege).role("custom").node("*").map,
              grantedOrDenied(privilege).role("custom").node("*").database("bar").map,
              grantedOrDenied(privilege).role("custom").relationship("*").map,
              grantedOrDenied(privilege).role("custom").relationship("*").database("bar").map
            ))

            // WHEN
            execute(s"REVOKE $grantOrDenyCommand $createOrDelete ON GRAPH * ELEMENTS * FROM custom")

            // THEN
            execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
              grantedOrDenied(privilege).role("custom").node("*").database("bar").map,
              grantedOrDenied(privilege).role("custom").relationship("*").database("bar").map
            ))
          }

          test(s"should be able to revoke $createOrDelete privilege if only having $grantOrDeny") {
            // GIVEN
            execute("CREATE ROLE custom")

            // WHEN
            execute(s"$grantOrDenyCommand $createOrDelete ON GRAPH * ELEMENTS * TO custom")

            // THEN
            execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
              grantedOrDenied(privilege).role("custom").node("*").map,
              grantedOrDenied(privilege).role("custom").relationship("*").map
            ))

            // WHEN
            execute(s"REVOKE $createOrDelete ON GRAPH * ELEMENTS * FROM custom")

            // THEN
            execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set.empty)
          }

          test(s"should be able to revoke $grantOrDeny $createOrDelete using parameter") {
            // GIVEN
            execute("CREATE ROLE custom")
            execute("CREATE DATABASE foo")
            execute(s"$grantOrDenyCommand $createOrDelete ON GRAPH foo TO custom")

            // WHEN
            execute(s"REVOKE $grantOrDenyCommand $createOrDelete ON GRAPH $$graph FROM custom", Map("graph" -> "foo"))

            // THEN
            execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set.empty)
          }

          test(s"should do nothing when revoking $grantOrDeny $createOrDelete privilege from non-existent role") {
            // GIVEN
            execute("CREATE ROLE custom")
            execute("CREATE DATABASE foo")
            execute(s"$grantOrDenyCommand $createOrDelete ON GRAPH * ELEMENTS * TO custom")

            // WHEN
            execute(s"REVOKE $grantOrDenyCommand $createOrDelete ON GRAPH * ELEMENTS * FROM wrongRole")

            // THEN
            execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
              grantedOrDenied(privilege).role("custom").node("*").map,
              grantedOrDenied(privilege).role("custom").relationship("*").map
            ))
          }

          test(s"should do nothing when revoking $grantOrDeny $createOrDelete privilege not granted to role") {
            // GIVEN
            execute("CREATE ROLE custom")
            execute("CREATE ROLE role")
            execute(s"$grantOrDenyCommand $createOrDelete ON GRAPH * ELEMENTS * TO custom")

            // WHEN
            execute(s"REVOKE $grantOrDenyCommand $createOrDelete ON GRAPH * ELEMENTS * FROM role")
            // THEN
            execute("SHOW ROLE role PRIVILEGES").toSet should be(Set.empty)
          }
      }
  }

  // Revoke tests

  Seq(
    ("CREATE", create),
    ("DELETE", delete)
  ).foreach {
    case (verb, action) =>

      test(s"should revoke both grant and deny $verb privilege") {
        // GIVEN
        execute("CREATE ROLE custom")

        // WHEN
        execute(s"GRANT $verb ON GRAPH * NODES foo TO custom")
        execute(s"DENY $verb ON GRAPH * NODES foo TO custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          granted(action).role("custom").node("foo").map,
          denied(action).role("custom").node("foo").map
        ))

        // WHEN
        execute(s"REVOKE $verb ON GRAPH * NODES foo FROM custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set.empty)
      }

      test(s"should do nothing when revoking not granted $verb privilege") {
        // GIVEN
        execute("CREATE ROLE custom")

        // WHEN
        execute(s"REVOKE $verb ON GRAPH * FROM custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set.empty)
      }

      test(s"should revoke $verb privilege with two granted and one revoked") {
        // GIVEN
        execute("CREATE ROLE custom")
        execute(s"GRANT $verb ON GRAPH * ELEMENTS foo TO custom")

        // WHEN
        execute(s"REVOKE $verb ON GRAPH * RELATIONSHIPS foo FROM custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(granted(action).role("custom").node("foo").map))
      }

      test(s"should revoke $verb privilege with one granted and two revoked") {
        // GIVEN
        execute("CREATE ROLE custom")
        execute(s"GRANT $verb ON GRAPH * NODES * TO custom")

        // WHEN
        execute(s"REVOKE $verb ON GRAPH * ELEMENTS * FROM custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set.empty)
      }
  }

  // Enforcement for create

  withAllSystemGraphVersions(unsupportedWhenNotLatest) {

    test("granting create node allows creation of nodes") {
      setupUserWithCustomRole()
      execute("GRANT CREATE ON GRAPH * NODES * TO custom")

      // WHEN
      executeOnDefault("joe", "soap", "CREATE ()")

      // THEN
      execute("MATCH (n) RETURN n").toSet should have size 1
    }

    test("granting create node allows creation of nodes with specific label") {
      setupUserWithCustomRole()
      execute("GRANT CREATE ON GRAPH * NODES * TO custom")
      selectDatabase(DEFAULT_DATABASE_NAME)
      execute("CALL db.createLabel('Foo')")

      // WHEN
      executeOnDefault("joe", "soap", "CREATE (:Foo)")

      // THEN
      execute("MATCH (n:Foo) RETURN n").toSet should have size 1
    }
  }

  test("should fail create node without privilege")
  {
    setupUserWithCustomRole()

    the[AuthorizationViolationException] thrownBy {
      // WHEN
      executeOnDefault("joe", "soap", "CREATE ()")
      // THEN
    } should have message "Create node with labels '' is not allowed for user 'joe' with roles [PUBLIC, custom]."
  }

  test("should fail to create node with properties with create privilege but without set property privilege")
  {
    setupUserWithCustomRole()
    execute("GRANT CREATE ON GRAPH * NODES * TO custom")

    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CALL db.createLabel('Foo')")
    execute("CALL db.createProperty('prop')")

    the[AuthorizationViolationException] thrownBy {
      // WHEN
      executeOnDefault("joe", "soap", "CREATE (:Foo{prop:'foo'})")
    } should have message "Set property for property 'prop' is not allowed for user 'joe' with roles [PUBLIC, custom]."

    // THEN
    execute("MATCH (n) RETURN n").toSet should have size 0
  }

  test("should fail create node with label without privilege")
  {
    setupUserWithCustomRole()
    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CALL db.createLabel('Foo')")

    the[AuthorizationViolationException] thrownBy {
      // WHEN
      executeOnDefault("joe", "soap", "CREATE (:Foo)")
      // THEN
    } should have message "Create node with labels 'Foo' is not allowed for user 'joe' with roles [PUBLIC, custom]."
  }

  test("should fail create node with label when denied")
  {
    setupUserWithCustomRole()
    execute("GRANT CREATE ON GRAPH * NODES * TO custom")
    execute("DENY CREATE ON GRAPH * NODES Foo TO custom")
    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CALL db.createLabel('Foo')")

    the[AuthorizationViolationException] thrownBy {
      // WHEN
      executeOnDefault("joe", "soap", "CREATE (:Foo)")
      // THEN
    } should have message "Create node with labels 'Foo' is not allowed for user 'joe' with roles [PUBLIC, custom]."
  }

  test("should fail create node with labels when only granted one")
  {
    setupUserWithCustomRole()
    execute("GRANT CREATE ON GRAPH * NODES Foo TO custom")
    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CALL db.createLabel('Foo')")
    execute("CALL db.createLabel('Bar')")

    the[AuthorizationViolationException] thrownBy {
      // WHEN
      executeOnDefault("joe", "soap", "CREATE (:Foo:Bar)")
      // THEN
    } should have message "Create node with labels 'Foo,Bar' is not allowed for user 'joe' with roles [PUBLIC, custom]."
  }

  test("granting create relationship allows creation of relationships")
  {
    setupUserWithCustomRole()
    execute("GRANT CREATE ON GRAPH * RELATIONSHIPS * TO custom")
    execute("GRANT TRAVERSE ON GRAPH * TO custom")
    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CALL db.createRelationshipType('REL')")
    execute("CREATE (:A), (:B)")

    // WHEN
    executeOnDefault("joe", "soap", "MATCH (a:A), (b:B) CREATE (a)-[:REL]->(b)")

    // THEN
    execute("MATCH (:A)-[r:REL]->(:B) RETURN r").toSet should have size 1
  }

  test("should fail create relationship without privilege")
  {
    setupUserWithCustomRole()
    execute("GRANT TRAVERSE ON GRAPH * TO custom")
    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CALL db.createRelationshipType('REL')")
    execute("CREATE (:A), (:B)")

    the[AuthorizationViolationException] thrownBy {
      // WHEN
      executeOnDefault("joe", "soap", "MATCH (a:A), (b:B) CREATE (a)-[:REL]->(b)")
      // THEN
    } should have message "Create relationship with type 'REL' is not allowed for user 'joe' with roles [PUBLIC, custom]."
  }

  test("should fail create relationship when denied")
  {
    setupUserWithCustomRole()
    execute("GRANT CREATE ON GRAPH * RELATIONSHIPS * TO custom")
    execute("DENY CREATE ON GRAPH * RELATIONSHIPS REL TO custom")
    execute("GRANT TRAVERSE ON GRAPH * TO custom")
    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CALL db.createRelationshipType('REL')")
    execute("CREATE (:A), (:B)")

    the[AuthorizationViolationException] thrownBy {
      // WHEN
      executeOnDefault("joe", "soap", "MATCH (a:A), (b:B) CREATE (a)-[:REL]->(b)")
      // THEN
    } should have message "Create relationship with type 'REL' is not allowed for user 'joe' with roles [PUBLIC, custom]."
  }

  // Enforcement for delete

  test("should delete node when granted privilege")
  {
    setupUserWithCustomRole()
    execute("GRANT TRAVERSE ON GRAPH * TO custom")
    execute("GRANT DELETE ON GRAPH * NODES * TO custom")
    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE (:A:B)")

    // WHEN
    executeOnDefault("joe", "soap", "MATCH (a:A) DELETE a")

    // THEN
    execute("MATCH (a:A) RETURN a").toSet should have size 0
  }

  test("should delete node when granted privilege for specific labels")
  {
    setupUserWithCustomRole()
    execute("GRANT TRAVERSE ON GRAPH * TO custom")
    execute("GRANT DELETE ON GRAPH * NODES A,B TO custom")
    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE (:A:B)")

    // WHEN
    executeOnDefault("joe", "soap", "MATCH (a:A) DELETE a")

    // THEN
    execute("MATCH (a:A) RETURN a").toSet should have size 0
  }

  test("should fail delete node when not granted privilege for all labels on the node")
  {
    setupUserWithCustomRole()
    execute("GRANT TRAVERSE ON GRAPH * TO custom")
    execute("GRANT DELETE ON GRAPH * NODES A TO custom")
    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE (:A:B)")

    // WHEN
    the[AuthorizationViolationException] thrownBy {
      // WHEN
      executeOnDefault("joe", "soap", "MATCH (a:A) DELETE a")
      // THEN
    } should have message "Delete node with labels 'A,B' is not allowed for user 'joe' with roles [PUBLIC, custom]."
  }

  test("should delete relationship when granted privilege")
  {
    setupUserWithCustomRole()
    execute("GRANT DELETE ON GRAPH * RELATIONSHIPS * TO custom")
    execute("GRANT TRAVERSE ON GRAPH * TO custom")
    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE (:A)-[:REL]->(:B)")

    // WHEN
    executeOnDefault("joe", "soap", "MATCH (:A)-[r:REL]->(:B) DELETE r")

    // THEN
    execute("MATCH (:A)-[r:REL]->(:B) RETURN r").toSet should have size 0
  }

  test("should fail delete relationship without privilege")
  {
    setupUserWithCustomRole()
    execute("GRANT TRAVERSE ON GRAPH * TO custom")
    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CALL db.createRelationshipType('REL')")
    execute("CREATE (:A)-[:REL]->(:B)")

    the[AuthorizationViolationException] thrownBy {
      // WHEN
      executeOnDefault("joe", "soap", "MATCH (:A)-[r:REL]->(:B) DELETE r")
      // THEN
    } should have message "Delete relationship with type 'REL' is not allowed for user 'joe' with roles [PUBLIC, custom]."
  }

  test("should fail delete relationship when denied")
  {
    setupUserWithCustomRole()
    execute("GRANT DELETE ON GRAPH * RELATIONSHIPS * TO custom")
    execute("DENY DELETE ON GRAPH * RELATIONSHIPS REL TO custom")
    execute("GRANT TRAVERSE ON GRAPH * TO custom")
    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CALL db.createRelationshipType('REL')")
    execute("CREATE (:A)-[:REL]->(:B)")

    the[AuthorizationViolationException] thrownBy {
      // WHEN
      executeOnDefault("joe", "soap", "MATCH (:A)-[r:REL]->(:B) DELETE r")
      // THEN
    } should have message "Delete relationship with type 'REL' is not allowed for user 'joe' with roles [PUBLIC, custom]."
  }

  test("should detach delete node with no relationships when not granted delete relationship")
  {
    setupUserWithCustomRole()
    execute("GRANT TRAVERSE ON GRAPH * TO custom")
    execute("GRANT DELETE ON GRAPH * NODES A TO custom")
    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE (:A)")

    // WHEN
    executeOnDefault("joe", "soap", "MATCH (a:A) DETACH DELETE a")

    // THEN
    execute("MATCH (a:A) RETURN a").toSet should have size 0
  }

  test("should fail detach delete node when not granted privilege for all relationships on the node")
  {
    setupUserWithCustomRole()
    execute("GRANT TRAVERSE ON GRAPH * TO custom")
    execute("GRANT DELETE ON GRAPH * NODES A TO custom")
    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE (:A)-[:REL]->(:B)")

    // WHEN
    the[AuthorizationViolationException] thrownBy {
      // WHEN
      executeOnDefault("joe", "soap", "MATCH (a:A) DETACH DELETE a")
      // THEN
    } should have message "Delete relationship with type 'REL' is not allowed for user 'joe' with roles [PUBLIC, custom]."
  }

  test("should delete node created in transaction even without delete privilege") {
    setupUserWithCustomRole()
    execute("GRANT TRAVERSE ON GRAPH * TO custom")
    execute("GRANT CREATE ON GRAPH * TO custom")
    execute("GRANT CREATE NEW LABEL ON DATABASE * TO custom")

    executeOnDefault("joe", "soap", "MATCH (a:A) DELETE a", executeBefore = tx => {
      tx.execute("CREATE (:A)")
    })
  }

  // Mix of create/delete and write privileges

  test("denying all writes prevents create node") {
    // GIVEN
    setupUserWithCustomRole()
    execute("GRANT CREATE ON GRAPH * NODES * TO custom")
    execute("DENY WRITE ON GRAPH * TO custom")

    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CALL db.createLabel('Label')")

    the[AuthorizationViolationException] thrownBy {
      // WHEN
      executeOnDefault("joe", "soap", "CREATE (:Label)")
      // THEN
    } should have message "Create node with labels 'Label' is not allowed for user 'joe' with roles [PUBLIC, custom]."

    // THEN
    execute("MATCH (n) RETURN n").toSet should have size 0
  }

  test("deny create node should override general write permission") {
    // GIVEN
    setupUserWithCustomRole()
    execute("GRANT WRITE ON GRAPH * TO custom")
    execute("DENY CREATE ON GRAPH * NODES * TO custom")

    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CALL db.createLabel('Label')")

    the[AuthorizationViolationException] thrownBy {
      // WHEN
      executeOnDefault("joe", "soap", "CREATE (:Label)")
      // THEN
    } should have message "Create node with labels 'Label' is not allowed for user 'joe' with roles [PUBLIC, custom]."

    // THEN
    execute("MATCH (n) RETURN n").toSet should have size 0
  }

  test("denying all writes prevents create relationship") {
    // GIVEN
    setupUserWithCustomRole()
    execute("GRANT MATCH {*} ON GRAPH * TO custom")
    execute("GRANT CREATE ON GRAPH * RELATIONSHIPS * TO custom")
    execute("DENY WRITE ON GRAPH * TO custom")

    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE (:A),(:B)")
    execute("CALL db.createRelationshipType('R')")

    the[AuthorizationViolationException] thrownBy {
      // WHEN
      executeOnDefault("joe", "soap", "MATCH (a:A), (b:B) CREATE (a)-[:R]->(b)")
      // THEN
    } should have message "Create relationship with type 'R' is not allowed for user 'joe' with roles [PUBLIC, custom]."

    // THEN
    execute("MATCH ()-[r]->() RETURN r").toSet should have size 0
  }

  test("deny create relationship should override general write permission") {
    // GIVEN
    setupUserWithCustomRole()
    execute("GRANT MATCH {*} ON GRAPH * TO custom")
    execute("GRANT WRITE ON GRAPH * TO custom")
    execute("DENY CREATE ON GRAPH * RELATIONSHIPS * TO custom")

    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE (:A),(:B)")
    execute("CALL db.createRelationshipType('R')")

    the[AuthorizationViolationException] thrownBy {
      // WHEN
      executeOnDefault("joe", "soap", "MATCH (a:A), (b:B) CREATE (a)-[:R]->(b)")
      // THEN
    } should have message "Create relationship with type 'R' is not allowed for user 'joe' with roles [PUBLIC, custom]."

    // THEN
    execute("MATCH ()-[r]->() RETURN r").toSet should have size 0
  }

  test("denying all writes prevents delete node") {
    // GIVEN
    setupUserWithCustomRole()
    execute("GRANT MATCH {*} ON GRAPH * TO custom")
    execute("GRANT DELETE ON GRAPH * NODES * TO custom")
    execute("DENY WRITE ON GRAPH * TO custom")

    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE ()")

    the[AuthorizationViolationException] thrownBy {
      // WHEN
      executeOnDefault("joe", "soap", "MATCH (n) DELETE n")
      // THEN
    } should have message "Delete node with labels '' is not allowed for user 'joe' with roles [PUBLIC, custom]."

    // THEN
    execute("MATCH (n) RETURN n").toSet should have size 1
  }

  test("deny delete node should override general write permission") {
    // GIVEN
    setupUserWithCustomRole()
    execute("GRANT MATCH {*} ON GRAPH * TO custom")
    execute("GRANT WRITE ON GRAPH * TO custom")
    execute("DENY DELETE ON GRAPH * NODES * TO custom")

    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE ()")

    the[AuthorizationViolationException] thrownBy {
      // WHEN
      executeOnDefault("joe", "soap", "MATCH (n) DELETE n")
      // THEN
    } should have message "Delete node with labels '' is not allowed for user 'joe' with roles [PUBLIC, custom]."

    // THEN
    execute("MATCH (n) RETURN n").toSet should have size 1
  }

  test("denying all writes prevents delete relationship") {
    // GIVEN
    setupUserWithCustomRole()
    execute("GRANT MATCH {*} ON GRAPH * TO custom")
    execute("GRANT DELETE ON GRAPH * RELATIONSHIPS * TO custom")
    execute("DENY WRITE ON GRAPH * TO custom")

    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE ()-[:R]->()")

    the[AuthorizationViolationException] thrownBy {
      // WHEN
      executeOnDefault("joe", "soap", "MATCH ()-[r:R]->() DELETE r")
      // THEN
    } should have message "Delete relationship with type 'R' is not allowed for user 'joe' with roles [PUBLIC, custom]."

    // THEN
    execute("MATCH ()-[r]->() RETURN r").toSet should have size 1
  }

  test("deny delete relationship should override general write permission") {
    // GIVEN
    setupUserWithCustomRole()
    execute("GRANT MATCH {*} ON GRAPH * TO custom")
    execute("GRANT WRITE ON GRAPH * TO custom")
    execute("DENY DELETE ON GRAPH * RELATIONSHIPS * TO custom")

    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE ()-[:R]->()")

    the[AuthorizationViolationException] thrownBy {
      // WHEN
      executeOnDefault("joe", "soap", "MATCH ()-[r:R]->() DELETE r")
      // THEN
    } should have message "Delete relationship with type 'R' is not allowed for user 'joe' with roles [PUBLIC, custom]."

    // THEN
    execute("MATCH ()-[r]->() RETURN r").toSet should have size 1
  }
}
