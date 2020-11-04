/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.internal.cypher.acceptance

import com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.PUBLIC
import org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME
import org.neo4j.graphdb.security.AuthorizationViolationException

//noinspection RedundantDefaultArgument
class CreateDeletePrivilegeAdministrationCommandAcceptanceTest extends AdministrationCommandAcceptanceTestBase with EnterpriseComponentVersionTestSupport {

  test("should return empty counts to the outside for commands that update the system graph internally") {
    // GIVEN
    execute(s"CREATE ROLE $roleName")

    // Notice: They are executed in succession so they have to make sense in that order
    assertQueriesAndSubQueryCounts(List(
      s"GRANT CREATE ON GRAPH * ELEMENTS * TO $roleName" -> 2,
      s"REVOKE CREATE ON GRAPH * ELEMENTS * FROM $roleName" -> 2,
      s"DENY CREATE ON GRAPH * ELEMENTS * TO $roleName" -> 2,
      s"REVOKE DENY CREATE ON GRAPH * ELEMENTS * FROM $roleName" -> 2,

      s"GRANT CREATE ON GRAPH * NODES * TO $roleName" -> 1,
      s"REVOKE CREATE ON GRAPH * NODES * FROM $roleName" -> 1,
      s"DENY CREATE ON GRAPH * NODES * TO $roleName" -> 1,
      s"REVOKE DENY CREATE ON GRAPH * NODES * FROM $roleName" -> 1,

      s"GRANT CREATE ON GRAPH * RELATIONSHIPS * TO $roleName" -> 1,
      s"REVOKE CREATE ON GRAPH * RELATIONSHIPS * FROM $roleName" -> 1,
      s"DENY CREATE ON GRAPH * RELATIONSHIPS * TO $roleName" -> 1,
      s"REVOKE DENY CREATE ON GRAPH * RELATIONSHIPS * FROM $roleName" -> 1,

      s"GRANT DELETE ON GRAPH * ELEMENTS * TO $roleName" -> 2,
      s"DENY DELETE ON GRAPH * ELEMENTS * TO $roleName" -> 2,
      s"REVOKE DELETE ON GRAPH * ELEMENTS * FROM $roleName" -> 4,
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
            execute(s"CREATE ROLE $roleName")

            // WHEN
            execute(s"$grantOrDenyCommand $createOrDelete ON GRAPH * ELEMENTS * TO $roleName")

            // THEN
            execute(s"SHOW ROLE $roleName PRIVILEGES").toSet should be(Set(
              grantedOrDenied(privilege).role(roleName).node("*").map,
              grantedOrDenied(privilege).role(roleName).relationship("*").map
            ))
          }

          test(s"should $grantOrDeny $createOrDelete privilege to custom role for a default graph and all elements") {
            // GIVEN
            execute(s"CREATE ROLE $roleName")

            // WHEN
            execute(s"$grantOrDenyCommand $createOrDelete ON DEFAULT GRAPH ELEMENTS * TO $roleName")

            // THEN
            execute(s"SHOW ROLE $roleName PRIVILEGES").toSet should be(Set(
              grantedOrDenied(privilege).role(roleName).graph(DEFAULT).node("*").map,
              grantedOrDenied(privilege).role(roleName).graph(DEFAULT).relationship("*").map
            ))
          }

          test(s"should $grantOrDeny $createOrDelete privilege to custom role for a default graph and named elements") {
            // GIVEN
            execute(s"CREATE ROLE $roleName")

            // WHEN
            execute(s"$grantOrDenyCommand $createOrDelete ON DEFAULT GRAPH ELEMENTS A TO $roleName")

            // THEN
            execute(s"SHOW ROLE $roleName PRIVILEGES").toSet should be(Set(
              grantedOrDenied(privilege).role(roleName).graph(DEFAULT).node("A").map,
              grantedOrDenied(privilege).role(roleName).graph(DEFAULT).relationship("A").map
            ))
          }

          test(s"should $grantOrDeny $createOrDelete privilege to custom role for a default graph and named relationship") {
            // GIVEN
            execute(s"CREATE ROLE $roleName")

            // WHEN
            execute(s"$grantOrDenyCommand $createOrDelete ON DEFAULT GRAPH RELATIONSHIPS Rel1, Rel2 TO $roleName")

            // THEN
            execute(s"SHOW ROLE $roleName PRIVILEGES").toSet should be(Set(
              grantedOrDenied(privilege).role(roleName).graph(DEFAULT).relationship("Rel1").map,
              grantedOrDenied(privilege).role(roleName).graph(DEFAULT).relationship("Rel2").map
            ))
          }

          test(s"should $grantOrDeny $createOrDelete privilege to custom role for a default graph and named node") {
            // GIVEN
            execute(s"CREATE ROLE $roleName")

            // WHEN
            execute(s"$grantOrDenyCommand $createOrDelete ON DEFAULT GRAPH NODE A TO $roleName")

            // THEN
            execute(s"SHOW ROLE $roleName PRIVILEGES").toSet should be(Set(
              grantedOrDenied(privilege).role(roleName).graph(DEFAULT).node("A").map,
            ))
          }

          test(s"should $grantOrDeny $createOrDelete privilege to custom role for a specific graph and all elements") {
            // GIVEN
            execute(s"CREATE ROLE $roleName")
            execute(s"CREATE DATABASE $databaseString")

            // WHEN
            execute(s"$grantOrDenyCommand $createOrDelete ON GRAPH $databaseString ELEMENTS * TO $roleName")

            // THEN
            execute(s"SHOW ROLE $roleName PRIVILEGES").toSet should be(Set(
              grantedOrDenied(privilege).role(roleName).graph(databaseString).node("*").map,
              grantedOrDenied(privilege).role(roleName).graph(databaseString).relationship("*").map
            ))
          }

          test(s"should $grantOrDeny $createOrDelete privilege to custom role for multiple graphs and all elements") {
            // GIVEN
            execute(s"CREATE ROLE $roleName")
            execute(s"CREATE DATABASE $databaseString")
            execute(s"CREATE DATABASE $databaseString2")

            // WHEN
            execute(s"$grantOrDenyCommand $createOrDelete ON GRAPH $databaseString, $databaseString2 ELEMENTS * TO $roleName")

            // THEN
            execute(s"SHOW ROLE $roleName PRIVILEGES").toSet should be(Set(
              grantedOrDenied(privilege).role(roleName).graph(databaseString).node("*").map,
              grantedOrDenied(privilege).role(roleName).graph(databaseString).relationship("*").map,
              grantedOrDenied(privilege).role(roleName).graph(databaseString2).node("*").map,
              grantedOrDenied(privilege).role(roleName).graph(databaseString2).relationship("*").map
            ))
          }

          test(s"should $grantOrDeny $createOrDelete privilege to custom role for specific graph and all elements using parameter") {
            // GIVEN
            execute(s"CREATE ROLE $roleName")

            // WHEN
            execute(s"$grantOrDenyCommand $createOrDelete ON GRAPH $$graph ELEMENTS * TO $roleName", Map("graph" -> DEFAULT_DATABASE_NAME))

            // THEN
            execute(s"SHOW ROLE $roleName PRIVILEGES").toSet should be(Set(
              grantedOrDenied(privilege).graph(DEFAULT_DATABASE_NAME).role(roleName).node("*").map,
              grantedOrDenied(privilege).graph(DEFAULT_DATABASE_NAME).role(roleName).relationship("*").map
            ))
          }

          test(s"should $grantOrDeny $createOrDelete privilege to multiple roles in a single grant") {
            // GIVEN
            execute("CREATE ROLE role1")
            execute("CREATE ROLE role2")
            execute("CREATE ROLE role3")
            execute(s"CREATE DATABASE $databaseString")

            // WHEN
            execute(s"$grantOrDenyCommand $createOrDelete ON GRAPH $databaseString ELEMENTS * TO role1, role2, role3")

            // THEN
            val expected: Seq[PrivilegeMapBuilder] = Seq(
              grantedOrDenied(privilege).graph(databaseString).node("*"),
              grantedOrDenied(privilege).graph(databaseString).relationship("*")
            )

            execute("SHOW ROLE role1 PRIVILEGES").toSet should be(expected.map(_.role("role1").map).toSet)
            execute("SHOW ROLE role2 PRIVILEGES").toSet should be(expected.map(_.role("role2").map).toSet)
            execute("SHOW ROLE role3 PRIVILEGES").toSet should be(expected.map(_.role("role3").map).toSet)
          }

          // Tests for revoke grant and revoke deny privilege privileges

          test(s"should revoke correct $grantOrDeny $createOrDelete privilege different graphs") {
            // GIVEN
            execute(s"CREATE ROLE $roleName")
            execute(s"CREATE DATABASE $databaseString")
            execute(s"CREATE DATABASE $databaseString2")
            execute(s"$grantOrDenyCommand $createOrDelete ON GRAPH * ELEMENTS * TO $roleName")
            execute(s"$grantOrDenyCommand $createOrDelete ON GRAPH $databaseString ELEMENTS * TO $roleName")
            execute(s"$grantOrDenyCommand $createOrDelete ON GRAPH $databaseString2 ELEMENTS * TO $roleName")

            // WHEN
            execute(s"REVOKE $grantOrDenyCommand $createOrDelete ON GRAPH $databaseString ELEMENTS * FROM $roleName")

            // THEN
            execute(s"SHOW ROLE $roleName PRIVILEGES").toSet should be(Set(
              grantedOrDenied(privilege).role(roleName).node("*").map,
              grantedOrDenied(privilege).role(roleName).node("*").graph(databaseString2).map,
              grantedOrDenied(privilege).role(roleName).relationship("*").map,
              grantedOrDenied(privilege).role(roleName).relationship("*").graph(databaseString2).map
            ))

            // WHEN
            execute(s"REVOKE $grantOrDenyCommand $createOrDelete ON GRAPH * ELEMENTS * FROM $roleName")

            // THEN
            execute(s"SHOW ROLE $roleName PRIVILEGES").toSet should be(Set(
              grantedOrDenied(privilege).role(roleName).node("*").graph(databaseString2).map,
              grantedOrDenied(privilege).role(roleName).relationship("*").graph(databaseString2).map
            ))
          }

          test(s"should be able to revoke $createOrDelete privilege if only having $grantOrDeny") {
            // GIVEN
            execute(s"CREATE ROLE $roleName")

            // WHEN
            execute(s"$grantOrDenyCommand $createOrDelete ON GRAPH * ELEMENTS * TO $roleName")

            // THEN
            execute(s"SHOW ROLE $roleName PRIVILEGES").toSet should be(Set(
              grantedOrDenied(privilege).role(roleName).node("*").map,
              grantedOrDenied(privilege).role(roleName).relationship("*").map
            ))

            // WHEN
            execute(s"REVOKE $createOrDelete ON GRAPH * ELEMENTS * FROM $roleName")

            // THEN
            execute(s"SHOW ROLE $roleName PRIVILEGES").toSet should be(Set.empty)
          }

          test(s"should be able to revoke $grantOrDeny $createOrDelete using parameter") {
            // GIVEN
            execute(s"CREATE ROLE $roleName")
            execute(s"CREATE DATABASE $databaseString")
            execute(s"$grantOrDenyCommand $createOrDelete ON GRAPH $databaseString TO $roleName")

            // WHEN
            execute(s"REVOKE $grantOrDenyCommand $createOrDelete ON GRAPH $$graph FROM $roleName", Map("graph" -> databaseString))

            // THEN
            execute(s"SHOW ROLE $roleName PRIVILEGES").toSet should be(Set.empty)
          }

          test(s"should do nothing when revoking $grantOrDeny $createOrDelete privilege from non-existent role") {
            // GIVEN
            execute(s"CREATE ROLE $roleName")
            execute(s"CREATE DATABASE $databaseString")
            execute(s"$grantOrDenyCommand $createOrDelete ON GRAPH * ELEMENTS * TO $roleName")

            // WHEN
            execute(s"REVOKE $grantOrDenyCommand $createOrDelete ON GRAPH * ELEMENTS * FROM wrongRole")

            // THEN
            execute(s"SHOW ROLE $roleName PRIVILEGES").toSet should be(Set(
              grantedOrDenied(privilege).role(roleName).node("*").map,
              grantedOrDenied(privilege).role(roleName).relationship("*").map
            ))
          }

          test(s"should do nothing when revoking $grantOrDeny $createOrDelete privilege not granted to role") {
            // GIVEN
            execute("CREATE ROLE roleWithPrivilege")
            execute("CREATE ROLE role")
            execute(s"$grantOrDenyCommand $createOrDelete ON GRAPH * ELEMENTS * TO roleWithPrivilege")

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
    case (createOrDelete, privilege) =>

      test(s"should revoke both grant and deny $createOrDelete privilege") {
        // GIVEN
        execute(s"CREATE ROLE $roleName")

        // WHEN
        execute(s"GRANT $createOrDelete ON GRAPH * NODES foo TO $roleName")
        execute(s"DENY $createOrDelete ON GRAPH * NODES foo TO $roleName")

        // THEN
        execute(s"SHOW ROLE $roleName PRIVILEGES").toSet should be(Set(
          granted(privilege).role(roleName).node("foo").map,
          denied(privilege).role(roleName).node("foo").map
        ))

        // WHEN
        execute(s"REVOKE $createOrDelete ON GRAPH * NODES foo FROM $roleName")

        // THEN
        execute(s"SHOW ROLE $roleName PRIVILEGES").toSet should be(Set.empty)
      }

      test(s"should do nothing when revoking not granted $createOrDelete privilege") {
        // GIVEN
        execute(s"CREATE ROLE $roleName")

        // WHEN
        execute(s"REVOKE $createOrDelete ON GRAPH * FROM $roleName")

        // THEN
        execute(s"SHOW ROLE $roleName PRIVILEGES").toSet should be(Set.empty)
      }

      test(s"should revoke $createOrDelete privilege with two granted and one revoked") {
        // GIVEN
        execute(s"CREATE ROLE $roleName")
        execute(s"GRANT $createOrDelete ON GRAPH * ELEMENTS foo TO $roleName")

        // WHEN
        execute(s"REVOKE $createOrDelete ON GRAPH * RELATIONSHIPS foo FROM $roleName")

        // THEN
        execute(s"SHOW ROLE $roleName PRIVILEGES").toSet should be(Set(granted(privilege).role(roleName).node("foo").map))
      }

      test(s"should revoke $createOrDelete privilege with one granted and two revoked") {
        // GIVEN
        execute(s"CREATE ROLE $roleName")
        execute(s"GRANT $createOrDelete ON GRAPH * NODES * TO $roleName")

        // WHEN
        execute(s"REVOKE $createOrDelete ON GRAPH * ELEMENTS * FROM $roleName")

        // THEN
        execute(s"SHOW ROLE $roleName PRIVILEGES").toSet should be(Set.empty)
      }
  }

  // Enforcement for create

  withAllSystemGraphVersions(unsupportedBefore41) {
    test("granting create node allows creation of nodes") {
      setupUserWithCustomRole()
      execute(s"GRANT CREATE ON GRAPH * NODES * TO $roleName")

      // WHEN
      executeOnDefault(username, password, "CREATE ()")

      // THEN
      execute("MATCH (n) RETURN n").toSet should have size 1
    }

    test("granting create node allows creation of nodes with specific label") {
      setupUserWithCustomRole()
      execute(s"GRANT CREATE ON GRAPH * NODES * TO $roleName")
      selectDatabase(DEFAULT_DATABASE_NAME)
      execute("CALL db.createLabel('Foo')")

      // WHEN
      executeOnDefault(username, password, "CREATE (:Foo)")

      // THEN
      execute("MATCH (n:Foo) RETURN n").toSet should have size 1
    }
  }

  test("should fail create node without privilege") {
    setupUserWithCustomRole()

    the[AuthorizationViolationException] thrownBy {
      // WHEN
      executeOnDefault(username, password, "CREATE ()")
      // THEN
    } should have message s"Create node with labels '' is not allowed for user '$username' with roles [$PUBLIC, $roleName]."
  }

  test("should fail to create node with properties with create privilege but without set property privilege") {
    setupUserWithCustomRole()
    execute(s"GRANT CREATE ON GRAPH * NODES * TO $roleName")

    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CALL db.createLabel('Foo')")
    execute("CALL db.createProperty('prop')")

    the[AuthorizationViolationException] thrownBy {
      // WHEN
      executeOnDefault(username, password, "CREATE (:Foo{prop:'foo'})")
    } should have message s"Set property for property 'prop' is not allowed for user '$username' with roles [$PUBLIC, $roleName]."

    // THEN
    execute("MATCH (n) RETURN n").toSet should have size 0
  }

  test("should fail create node with label without privilege") {
    setupUserWithCustomRole()
    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CALL db.createLabel('Foo')")

    the[AuthorizationViolationException] thrownBy {
      // WHEN
      executeOnDefault(username, password, "CREATE (:Foo)")
      // THEN
    } should have message s"Create node with labels 'Foo' is not allowed for user '$username' with roles [$PUBLIC, $roleName]."
  }

  test("should fail create node with label when denied") {
    setupUserWithCustomRole()
    execute(s"GRANT CREATE ON GRAPH * NODES * TO $roleName")
    execute(s"DENY CREATE ON GRAPH * NODES Foo TO $roleName")
    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CALL db.createLabel('Foo')")

    the[AuthorizationViolationException] thrownBy {
      // WHEN
      executeOnDefault(username, password, "CREATE (:Foo)")
      // THEN
    } should have message s"Create node with labels 'Foo' is not allowed for user '$username' with roles [$PUBLIC, $roleName]."
  }

  test("should fail create node with labels when only granted one") {
    setupUserWithCustomRole()
    execute(s"GRANT CREATE ON GRAPH * NODES Foo TO $roleName")
    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CALL db.createLabel('Foo')")
    execute("CALL db.createLabel('Bar')")

    the[AuthorizationViolationException] thrownBy {
      // WHEN
      executeOnDefault(username, password, "CREATE (:Foo:Bar)")
      // THEN
    } should have message s"Create node with labels 'Foo,Bar' is not allowed for user '$username' with roles [$PUBLIC, $roleName]."
  }

  test("granting create relationship allows creation of relationships") {
    setupUserWithCustomRole()
    execute(s"GRANT CREATE ON GRAPH * RELATIONSHIPS * TO $roleName")
    execute(s"GRANT TRAVERSE ON GRAPH * TO $roleName")
    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CALL db.createRelationshipType('REL')")
    execute("CREATE (:A), (:B)")

    // WHEN
    executeOnDefault(username, password, "MATCH (a:A), (b:B) CREATE (a)-[:REL]->(b)")

    // THEN
    execute("MATCH (:A)-[r:REL]->(:B) RETURN r").toSet should have size 1
  }

  test("should fail create relationship without privilege") {
    setupUserWithCustomRole()
    execute(s"GRANT TRAVERSE ON GRAPH * TO $roleName")
    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CALL db.createRelationshipType('REL')")
    execute("CREATE (:A), (:B)")

    the[AuthorizationViolationException] thrownBy {
      // WHEN
      executeOnDefault(username, password, "MATCH (a:A), (b:B) CREATE (a)-[:REL]->(b)")
      // THEN
    } should have message s"Create relationship with type 'REL' is not allowed for user '$username' with roles [$PUBLIC, $roleName]."
  }

  test("should fail create relationship when denied") {
    setupUserWithCustomRole()
    execute(s"GRANT CREATE ON GRAPH * RELATIONSHIPS * TO $roleName")
    execute(s"DENY CREATE ON GRAPH * RELATIONSHIPS REL TO $roleName")
    execute(s"GRANT TRAVERSE ON GRAPH * TO $roleName")
    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CALL db.createRelationshipType('REL')")
    execute("CREATE (:A), (:B)")

    the[AuthorizationViolationException] thrownBy {
      // WHEN
      executeOnDefault(username, password, "MATCH (a:A), (b:B) CREATE (a)-[:REL]->(b)")
      // THEN
    } should have message s"Create relationship with type 'REL' is not allowed for user '$username' with roles [$PUBLIC, $roleName]."
  }

  // Enforcement for delete

  test("should delete node when granted privilege") {
    setupUserWithCustomRole()
    execute(s"GRANT TRAVERSE ON GRAPH * TO $roleName")
    execute(s"GRANT DELETE ON GRAPH * NODES * TO $roleName")
    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE (:A:B)")

    // WHEN
    executeOnDefault(username, password, "MATCH (a:A) DELETE a")

    // THEN
    execute("MATCH (a:A) RETURN a").toSet should have size 0
  }

  test("should delete node when granted privilege for specific labels") {
    setupUserWithCustomRole()
    execute(s"GRANT TRAVERSE ON GRAPH * TO $roleName")
    execute(s"GRANT DELETE ON GRAPH * NODES A,B TO $roleName")
    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE (:A:B)")

    // WHEN
    executeOnDefault(username, password, "MATCH (a:A) DELETE a")

    // THEN
    execute("MATCH (a:A) RETURN a").toSet should have size 0
  }

  test("should fail delete node when not granted privilege for all labels on the node") {
    setupUserWithCustomRole()
    execute(s"GRANT TRAVERSE ON GRAPH * TO $roleName")
    execute(s"GRANT DELETE ON GRAPH * NODES A TO $roleName")
    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE (:A:B)")

    // WHEN
    the[AuthorizationViolationException] thrownBy {
      // WHEN
      executeOnDefault(username, password, "MATCH (a:A) DELETE a")
      // THEN
    } should have message s"Delete node with labels 'A,B' is not allowed for user '$username' with roles [$PUBLIC, $roleName]."
  }

  test("should delete relationship when granted privilege") {
    setupUserWithCustomRole()
    execute(s"GRANT DELETE ON GRAPH * RELATIONSHIPS * TO $roleName")
    execute(s"GRANT TRAVERSE ON GRAPH * TO $roleName")
    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE (:A)-[:REL]->(:B)")

    // WHEN
    executeOnDefault(username, password, "MATCH (:A)-[r:REL]->(:B) DELETE r")

    // THEN
    execute("MATCH (:A)-[r:REL]->(:B) RETURN r").toSet should have size 0
  }

  test("should fail delete relationship without privilege") {
    setupUserWithCustomRole()
    execute(s"GRANT TRAVERSE ON GRAPH * TO $roleName")
    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CALL db.createRelationshipType('REL')")
    execute("CREATE (:A)-[:REL]->(:B)")

    the[AuthorizationViolationException] thrownBy {
      // WHEN
      executeOnDefault(username, password, "MATCH (:A)-[r:REL]->(:B) DELETE r")
      // THEN
    } should have message s"Delete relationship with type 'REL' is not allowed for user '$username' with roles [$PUBLIC, $roleName]."
  }

  test("should fail delete relationship when denied") {
    setupUserWithCustomRole()
    execute(s"GRANT DELETE ON GRAPH * RELATIONSHIPS * TO $roleName")
    execute(s"DENY DELETE ON GRAPH * RELATIONSHIPS REL TO $roleName")
    execute(s"GRANT TRAVERSE ON GRAPH * TO $roleName")
    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CALL db.createRelationshipType('REL')")
    execute("CREATE (:A)-[:REL]->(:B)")

    the[AuthorizationViolationException] thrownBy {
      // WHEN
      executeOnDefault(username, password, "MATCH (:A)-[r:REL]->(:B) DELETE r")
      // THEN
    } should have message s"Delete relationship with type 'REL' is not allowed for user '$username' with roles [$PUBLIC, $roleName]."
  }

  test("should detach delete node with no relationships when not granted delete relationship") {
    setupUserWithCustomRole()
    execute(s"GRANT TRAVERSE ON GRAPH * TO $roleName")
    execute(s"GRANT DELETE ON GRAPH * NODES A TO $roleName")
    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE (:A)")

    // WHEN
    executeOnDefault(username, password, "MATCH (a:A) DETACH DELETE a")

    // THEN
    execute("MATCH (a:A) RETURN a").toSet should have size 0
  }

  test("should fail detach delete node when not granted privilege for all relationships on the node") {
    setupUserWithCustomRole()
    execute(s"GRANT TRAVERSE ON GRAPH * TO $roleName")
    execute(s"GRANT DELETE ON GRAPH * NODES A TO $roleName")
    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE (:A)-[:REL]->(:B)")

    // WHEN
    the[AuthorizationViolationException] thrownBy {
      // WHEN
      executeOnDefault(username, password, "MATCH (a:A) DETACH DELETE a")
      // THEN
    } should have message s"Delete relationship with type 'REL' is not allowed for user '$username' with roles [$PUBLIC, $roleName]."
  }

  test("should delete node created in transaction even without delete privilege") {
    setupUserWithCustomRole()
    execute(s"GRANT TRAVERSE ON GRAPH * TO $roleName")
    execute(s"GRANT CREATE ON GRAPH * TO $roleName")
    execute(s"GRANT CREATE NEW LABEL ON DATABASE * TO $roleName")

    executeOnDefault(username, password, "MATCH (a:A) DELETE a", executeBefore = tx => {
      tx.execute("CREATE (:A)")
    })
  }

  // Mix of create/delete and write privileges

  test("denying all writes prevents create node") {
    // GIVEN
    setupUserWithCustomRole()
    execute(s"GRANT CREATE ON GRAPH * NODES * TO $roleName")
    execute(s"DENY WRITE ON GRAPH * TO $roleName")

    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CALL db.createLabel('Label')")

    the[AuthorizationViolationException] thrownBy {
      // WHEN
      executeOnDefault(username, password, "CREATE (:Label)")
      // THEN
    } should have message s"Create node with labels 'Label' is not allowed for user '$username' with roles [$PUBLIC, $roleName]."

    // THEN
    execute("MATCH (n) RETURN n").toSet should have size 0
  }

  test("deny create node should override general write permission") {
    // GIVEN
    setupUserWithCustomRole()
    execute(s"GRANT WRITE ON GRAPH * TO $roleName")
    execute(s"DENY CREATE ON GRAPH * NODES * TO $roleName")

    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CALL db.createLabel('Label')")

    the[AuthorizationViolationException] thrownBy {
      // WHEN
      executeOnDefault(username, password, "CREATE (:Label)")
      // THEN
    } should have message s"Create node with labels 'Label' is not allowed for user '$username' with roles [$PUBLIC, $roleName]."

    // THEN
    execute("MATCH (n) RETURN n").toSet should have size 0
  }

  test("denying all writes prevents create relationship") {
    // GIVEN
    setupUserWithCustomRole()
    execute(s"GRANT MATCH {*} ON GRAPH * TO $roleName")
    execute(s"GRANT CREATE ON GRAPH * RELATIONSHIPS * TO $roleName")
    execute(s"DENY WRITE ON GRAPH * TO $roleName")

    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE (:A),(:B)")
    execute("CALL db.createRelationshipType('R')")

    the[AuthorizationViolationException] thrownBy {
      // WHEN
      executeOnDefault(username, password, "MATCH (a:A), (b:B) CREATE (a)-[:R]->(b)")
      // THEN
    } should have message s"Create relationship with type 'R' is not allowed for user '$username' with roles [$PUBLIC, $roleName]."

    // THEN
    execute("MATCH ()-[r]->() RETURN r").toSet should have size 0
  }

  test("deny create relationship should override general write permission") {
    // GIVEN
    setupUserWithCustomRole()
    execute(s"GRANT MATCH {*} ON GRAPH * TO $roleName")
    execute(s"GRANT WRITE ON GRAPH * TO $roleName")
    execute(s"DENY CREATE ON GRAPH * RELATIONSHIPS * TO $roleName")

    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE (:A),(:B)")
    execute("CALL db.createRelationshipType('R')")

    the[AuthorizationViolationException] thrownBy {
      // WHEN
      executeOnDefault(username, password, "MATCH (a:A), (b:B) CREATE (a)-[:R]->(b)")
      // THEN
    } should have message s"Create relationship with type 'R' is not allowed for user '$username' with roles [$PUBLIC, $roleName]."

    // THEN
    execute("MATCH ()-[r]->() RETURN r").toSet should have size 0
  }

  test("denying all writes prevents delete node") {
    // GIVEN
    setupUserWithCustomRole()
    execute(s"GRANT MATCH {*} ON GRAPH * TO $roleName")
    execute(s"GRANT DELETE ON GRAPH * NODES * TO $roleName")
    execute(s"DENY WRITE ON GRAPH * TO $roleName")

    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE ()")

    the[AuthorizationViolationException] thrownBy {
      // WHEN
      executeOnDefault(username, password, "MATCH (n) DELETE n")
      // THEN
    } should have message s"Delete node with labels '' is not allowed for user '$username' with roles [$PUBLIC, $roleName]."

    // THEN
    execute("MATCH (n) RETURN n").toSet should have size 1
  }

  test("deny delete node should override general write permission") {
    // GIVEN
    setupUserWithCustomRole()
    execute(s"GRANT MATCH {*} ON GRAPH * TO $roleName")
    execute(s"GRANT WRITE ON GRAPH * TO $roleName")
    execute(s"DENY DELETE ON GRAPH * NODES * TO $roleName")

    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE ()")

    the[AuthorizationViolationException] thrownBy {
      // WHEN
      executeOnDefault(username, password, "MATCH (n) DELETE n")
      // THEN
    } should have message s"Delete node with labels '' is not allowed for user '$username' with roles [$PUBLIC, $roleName]."

    // THEN
    execute("MATCH (n) RETURN n").toSet should have size 1
  }

  test("denying all writes prevents delete relationship") {
    // GIVEN
    setupUserWithCustomRole()
    execute(s"GRANT MATCH {*} ON GRAPH * TO $roleName")
    execute(s"GRANT DELETE ON GRAPH * RELATIONSHIPS * TO $roleName")
    execute(s"DENY WRITE ON GRAPH * TO $roleName")

    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE ()-[:R]->()")

    the[AuthorizationViolationException] thrownBy {
      // WHEN
      executeOnDefault(username, password, "MATCH ()-[r:R]->() DELETE r")
      // THEN
    } should have message s"Delete relationship with type 'R' is not allowed for user '$username' with roles [$PUBLIC, $roleName]."

    // THEN
    execute("MATCH ()-[r]->() RETURN r").toSet should have size 1
  }

  test("deny delete relationship should override general write permission") {
    // GIVEN
    setupUserWithCustomRole()
    execute(s"GRANT MATCH {*} ON GRAPH * TO $roleName")
    execute(s"GRANT WRITE ON GRAPH * TO $roleName")
    execute(s"DENY DELETE ON GRAPH * RELATIONSHIPS * TO $roleName")

    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE ()-[:R]->()")

    the[AuthorizationViolationException] thrownBy {
      // WHEN
      executeOnDefault(username, password, "MATCH ()-[r:R]->() DELETE r")
      // THEN
    } should have message s"Delete relationship with type 'R' is not allowed for user '$username' with roles [$PUBLIC, $roleName]."

    // THEN
    execute("MATCH ()-[r]->() RETURN r").toSet should have size 1
  }

  test("grant create on default graph") {
    // GIVEN
    setupUserWithCustomRole()
    execute(s"GRANT CREATE ON DEFAULT GRAPH TO $roleName")

    // WHEN
    executeOnDefault(username, password, "CREATE (n)")

    //THEN
    execute("MATCH(n) RETURN count(n)").toSet should be(Set(Map("count(n)" -> 1)))
  }

  test("grant create on default graph, should not allow on other graph") {
    // GIVEN
    setupUserWithCustomRole()
    execute(s"GRANT CREATE ON DEFAULT GRAPH TO $roleName")
    execute(s"CREATE DATABASE $databaseString")

    // WHEN, THEN
    the[AuthorizationViolationException] thrownBy {
      executeOn(databaseString, username, password, "CREATE (n)")
    } should have message s"Create node with labels '' is not allowed for user '$username' with roles [$PUBLIC, $roleName]."
  }

  test("grant delete on default graph") {
    // GIVEN
    setupUserWithCustomRole()
    execute(s"GRANT TRAVERSE ON GRAPH * TO $roleName")
    execute(s"GRANT DELETE ON DEFAULT GRAPH TO $roleName")
    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE (n)")

    // WHEN
    executeOnDefault(username, password, "MATCH (n) DELETE n")

    //THEN
    execute("MATCH(n) RETURN count(n)").toSet should be(Set(Map("count(n)" -> 0)))
  }

  test("grant delete on default graph, should not allow on other graph") {
    // GIVEN
    setupUserWithCustomRole()
    execute(s"GRANT TRAVERSE ON GRAPH * TO $roleName")
    execute(s"GRANT CREATE ON DEFAULT GRAPH TO $roleName")
    execute(s"CREATE DATABASE $databaseString")
    selectDatabase(databaseString)
    execute("CREATE (n)")

    // WHEN, THEN
    the[AuthorizationViolationException] thrownBy {
      executeOn(databaseString, username, password, "MATCH (n) DELETE n")
    } should have message s"Delete node with labels '' is not allowed for user '$username' with roles [$PUBLIC, $roleName]."
  }

  test("deny create on default graph, should allow on other graph") {
    // GIVEN
    setupUserWithCustomRole()
    execute(s"GRANT CREATE ON GRAPH * TO $roleName")
    execute(s"DENY CREATE ON DEFAULT GRAPH TO $roleName")
    execute(s"CREATE DATABASE $databaseString")

    // WHEN
    executeOn(databaseString, username, password, "CREATE (n)")

    // THEN
    execute("MATCH(n) RETURN count(n)").toSet should be(Set(Map("count(n)" -> 1)))
  }

  test("deny delete on default graph") {
    // GIVEN
    setupUserWithCustomRole()
    execute(s"GRANT TRAVERSE ON GRAPH * TO $roleName")
    execute(s"DENY DELETE ON DEFAULT GRAPH TO $roleName")
    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE (n)")

    // WHEN, THEN
    the[AuthorizationViolationException] thrownBy {
      executeOnDefault(username, password, "MATCH (n) DELETE n")
    } should have message s"Delete node with labels '' is not allowed for user '$username' with roles [$PUBLIC, $roleName]."
  }

}
