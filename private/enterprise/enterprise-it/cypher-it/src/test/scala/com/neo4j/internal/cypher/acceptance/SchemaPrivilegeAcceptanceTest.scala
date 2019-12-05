/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.internal.cypher.acceptance

import org.neo4j.configuration.GraphDatabaseSettings.{DEFAULT_DATABASE_NAME, SYSTEM_DATABASE_NAME}
import org.neo4j.graphdb.QueryExecutionException
import org.neo4j.graphdb.config.Setting
import org.neo4j.graphdb.security.AuthorizationViolationException
import org.neo4j.internal.kernel.api.security.PrivilegeAction

class SchemaPrivilegeAcceptanceTest extends AdministrationCommandAcceptanceTestBase {

  test("should return empty counts to the outside for commands that update the system graph internally") {
    //TODO: ADD ANY NEW UPDATING COMMANDS HERE

    // GIVEN
    setup()
    execute("CREATE ROLE custom")
    execute("CREATE DATABASE foo")

    // Notice: They are executed in succession so they have to make sense in that order
    assertQueriesAndSubQueryCounts(List(
      "GRANT CREATE INDEX ON DATABASE * TO custom" -> 1,
      "REVOKE GRANT CREATE INDEX ON DATABASE * FROM custom" -> 1,
      "DENY CREATE INDEX ON DATABASE * TO custom" -> 1,
      "REVOKE DENY CREATE INDEX ON DATABASE * FROM custom" -> 1,

      "GRANT DROP INDEX ON DATABASE * TO custom" -> 1,
      "DENY DROP INDEX ON DATABASE * TO custom" -> 1,
      "REVOKE DROP INDEX ON DATABASE * FROM custom" -> 2,

      "GRANT INDEX MANAGEMENT ON DATABASES * TO custom" -> 2,
      "REVOKE GRANT INDEX MANAGEMENT ON DATABASES * FROM custom" -> 2,
      "DENY INDEX MANAGEMENT ON DATABASES * TO custom" -> 2,
      "REVOKE DENY INDEX MANAGEMENT ON DATABASES * FROM custom" -> 2,
      "GRANT INDEX MANAGEMENT ON DATABASES * TO custom" -> 2,
      "DENY INDEX MANAGEMENT ON DATABASES * TO custom" -> 2,
      "REVOKE INDEX MANAGEMENT ON DATABASES * FROM custom" -> 4,


      "GRANT CREATE CONSTRAINT ON DATABASE * TO custom" -> 1,
      "DENY CREATE CONSTRAINT ON DATABASE * TO custom" -> 1,
      "REVOKE CREATE CONSTRAINT ON DATABASE * FROM custom" -> 2,

      "GRANT DROP CONSTRAINT ON DATABASE * TO custom" -> 1,
      "REVOKE GRANT DROP CONSTRAINT ON DATABASE * FROM custom" -> 1,
      "DENY DROP CONSTRAINT ON DATABASE * TO custom" -> 1,
      "REVOKE DENY DROP CONSTRAINT ON DATABASE * FROM custom" -> 1,

      "GRANT CONSTRAINT MANAGEMENT ON DATABASES * TO custom" -> 2,
      "REVOKE CONSTRAINT MANAGEMENT ON DATABASES * FROM custom" -> 2,
      "DENY CONSTRAINT MANAGEMENT ON DATABASES * TO custom" -> 2,
      "REVOKE CONSTRAINT MANAGEMENT ON DATABASES * FROM custom" -> 2,
      "GRANT CONSTRAINT MANAGEMENT ON DATABASES * TO custom" -> 2,
      "DENY CONSTRAINT MANAGEMENT ON DATABASES * TO custom" -> 2,
      "REVOKE CONSTRAINT MANAGEMENT ON DATABASES * FROM custom" -> 4,


      "GRANT CREATE NEW LABEL ON DATABASE * TO custom" -> 1,
      "REVOKE GRANT CREATE NEW LABEL ON DATABASE * FROM custom" -> 1,
      "DENY CREATE NEW LABEL ON DATABASE * TO custom" -> 1,
      "REVOKE DENY CREATE NEW LABEL ON DATABASE * FROM custom" -> 1,

      "GRANT CREATE NEW TYPE ON DATABASE * TO custom" -> 1,
      "REVOKE CREATE NEW TYPE ON DATABASE * FROM custom" -> 1,
      "DENY CREATE NEW TYPE ON DATABASE * TO custom" -> 1,
      "REVOKE CREATE NEW TYPE ON DATABASE * FROM custom" -> 1,

      "GRANT CREATE NEW NAME ON DATABASE * TO custom" -> 1,
      "DENY CREATE NEW NAME ON DATABASE * TO custom" -> 1,
      "REVOKE CREATE NEW NAME ON DATABASE * FROM custom" -> 2,

      "GRANT NAME MANAGEMENT ON DATABASES * TO custom" -> 3,
      "REVOKE GRANT NAME MANAGEMENT ON DATABASES * FROM custom" -> 3,
      "DENY NAME MANAGEMENT ON DATABASES * TO custom" -> 3,
      "REVOKE DENY NAME MANAGEMENT ON DATABASES * FROM custom" -> 3,
      "GRANT NAME MANAGEMENT ON DATABASES * TO custom" -> 3,
      "DENY NAME MANAGEMENT ON DATABASES * TO custom" -> 3,
      "REVOKE NAME MANAGEMENT ON DATABASES * FROM custom" -> 6,


      "GRANT ALL DATABASE PRIVILEGES ON DATABASES foo TO custom" -> 10,
      "DENY ALL DATABASE PRIVILEGES ON DATABASES foo TO custom" -> 10,
      "REVOKE ALL DATABASE PRIVILEGES ON DATABASES foo FROM custom" -> 20
    ))
  }

  // Tests for granting, denying and revoking schema privileges

  test("should list different index management privileges") {
    // GIVEN
    setup()
    execute("CREATE DATABASE foo")
    execute("CREATE DATABASE bar")
    execute("CREATE ROLE role")

    // WHEN
    execute("GRANT CREATE INDEX ON DATABASE foo TO role")
    execute("GRANT DROP INDEX ON DATABASE foo TO role")

    // THEN
    execute("SHOW ROLE role PRIVILEGES").toSet should be(Set(
      createIndex().database("foo").role("role").map,
      dropIndex().database("foo").role("role").map
    ))

    // WHEN
    execute("REVOKE CREATE INDEX ON DATABASE foo FROM role")
    execute("GRANT CREATE INDEX ON DATABASE * TO role")

    // THEN
    execute("SHOW ROLE role PRIVILEGES").toSet should be(Set(
      createIndex().role("role").map,
      dropIndex().database("foo").role("role").map
    ))

    // WHEN
    execute("DENY CREATE INDEX ON DATABASE bar TO role")

    // THEN
    execute("SHOW ROLE role PRIVILEGES").toSet should be(Set(
      createIndex().role("role").map,
      dropIndex().database("foo").role("role").map,
      createIndex("DENIED").database("bar").role("role").map
    ))

    // WHEN
    execute("REVOKE CREATE INDEX ON DATABASE bar FROM role")

    // THEN
    execute("SHOW ROLE role PRIVILEGES").toSet should be(Set(
      createIndex().role("role").map,
      dropIndex().database("foo").role("role").map
    ))

    // WHEN
    execute("GRANT INDEX MANAGEMENT ON DATABASE bar TO role")

    // THEN
    execute("SHOW ROLE role PRIVILEGES").toSet should be(Set(
      createIndex().role("role").map,
      dropIndex().database("foo").role("role").map,
      createIndex().database("bar").role("role").map,
      dropIndex().database("bar").role("role").map
    ))

    // WHEN
    execute("REVOKE INDEX MANAGEMENT ON DATABASE bar FROM role")
    execute("REVOKE INDEX MANAGEMENT ON DATABASE foo FROM role")
    execute("REVOKE INDEX MANAGEMENT ON DATABASE * FROM role")

    // THEN
    execute("SHOW ROLE role PRIVILEGES").toSet should be(Set.empty)

    // WHEN
    execute("GRANT CREATE INDEX ON DEFAULT DATABASE TO role")

    // THEN
    execute("SHOW ROLE role PRIVILEGES").toSet should be(Set(
      createIndex().database(DEFAULT_DATABASE_NAME).role("role").map
    ))
  }

  test("should list different constraint management privileges") {
    // GIVEN
    setup()
    execute("CREATE DATABASE foo")
    execute("CREATE DATABASE bar")
    execute("CREATE ROLE role")

    // WHEN
    execute("GRANT CREATE CONSTRAINT ON DATABASE foo TO role")
    execute("GRANT DROP CONSTRAINT ON DATABASE foo TO role")

    // THEN
    execute("SHOW ROLE role PRIVILEGES").toSet should be(Set(
      createConstraint().database("foo").role("role").map,
      dropConstraint().database("foo").role("role").map
    ))

    // WHEN
    execute("REVOKE CREATE CONSTRAINT ON DATABASE foo FROM role")
    execute("GRANT CREATE CONSTRAINT ON DATABASE * TO role")

    // THEN
    execute("SHOW ROLE role PRIVILEGES").toSet should be(Set(
      createConstraint().role("role").map,
      dropConstraint().database("foo").role("role").map
    ))

    // WHEN
    execute("DENY CREATE CONSTRAINT ON DATABASE bar TO role")

    // THEN
    execute("SHOW ROLE role PRIVILEGES").toSet should be(Set(
      createConstraint().role("role").map,
      dropConstraint().database("foo").role("role").map,
      createConstraint("DENIED").database("bar").role("role").map
    ))

    // WHEN
    execute("REVOKE CREATE CONSTRAINT ON DATABASE bar FROM role")

    // THEN
    execute("SHOW ROLE role PRIVILEGES").toSet should be(Set(
      createConstraint().role("role").map,
      dropConstraint().database("foo").role("role").map
    ))

    // WHEN
    execute("GRANT CONSTRAINT MANAGEMENT ON DATABASE bar TO role")

    // THEN
    execute("SHOW ROLE role PRIVILEGES").toSet should be(Set(
      createConstraint().role("role").map,
      dropConstraint().database("foo").role("role").map,
      createConstraint().database("bar").role("role").map,
      dropConstraint().database("bar").role("role").map
    ))

    // WHEN
    execute("REVOKE CONSTRAINT MANAGEMENT ON DATABASE bar FROM role")
    execute("REVOKE CONSTRAINT MANAGEMENT ON DATABASE foo FROM role")
    execute("REVOKE CONSTRAINT MANAGEMENT ON DATABASE * FROM role")

    // THEN
    execute("SHOW ROLE role PRIVILEGES").toSet should be(Set.empty)

    // WHEN
    execute("GRANT DROP CONSTRAINT ON DEFAULT DATABASE TO role")

    // THEN
    execute("SHOW ROLE role PRIVILEGES").toSet should be(Set(
      dropConstraint().database(DEFAULT_DATABASE_NAME).role("role").map
    ))
  }

  test("should list different name management privileges") {
    // GIVEN
    setup()
    execute("CREATE DATABASE foo")
    execute("CREATE DATABASE bar")
    execute("CREATE ROLE role")

    // WHEN
    execute("GRANT CREATE NEW NODE LABEL ON DATABASE foo TO role")
    execute("GRANT CREATE NEW TYPE ON DATABASE foo TO role")
    execute("GRANT CREATE NEW PROPERTY NAME ON DATABASE foo TO role")

    // THEN
    execute("SHOW ROLE role PRIVILEGES").toSet should be(Set(
      createNodeLabel().database("foo").role("role").map,
      createRelationshipType().database("foo").role("role").map,
      createPropertyKey().database("foo").role("role").map
    ))

    // WHEN
    execute("REVOKE CREATE NEW LABEL ON DATABASE foo FROM role")
    execute("GRANT CREATE NEW LABEL ON DATABASE * TO role")

    // THEN
    execute("SHOW ROLE role PRIVILEGES").toSet should be(Set(
      createNodeLabel().role("role").map,
      createRelationshipType().database("foo").role("role").map,
      createPropertyKey().database("foo").role("role").map
    ))

    // WHEN
    execute("DENY CREATE NEW NAME ON DATABASE bar TO role")

    // THEN
    execute("SHOW ROLE role PRIVILEGES").toSet should be(Set(
      createNodeLabel().role("role").map,
      createRelationshipType().database("foo").role("role").map,
      createPropertyKey().database("foo").role("role").map,
      createPropertyKey("DENIED").database("bar").role("role").map
    ))

    // WHEN
    execute("REVOKE CREATE NEW RELATIONSHIP TYPE ON DATABASE foo FROM role")
    execute("REVOKE CREATE NEW PROPERTY NAME ON DATABASE bar FROM role")

    // THEN
    execute("SHOW ROLE role PRIVILEGES").toSet should be(Set(
      createNodeLabel().role("role").map,
      createPropertyKey().database("foo").role("role").map
    ))

    // WHEN
    execute("GRANT NAME MANAGEMENT ON DATABASE bar TO role")

    // THEN
    execute("SHOW ROLE role PRIVILEGES").toSet should be(Set(
      createNodeLabel().role("role").map,
      createPropertyKey().database("foo").role("role").map,
      createNodeLabel().database("bar").role("role").map,
      createRelationshipType().database("bar").role("role").map,
      createPropertyKey().database("bar").role("role").map
    ))

    // WHEN
    execute("REVOKE NAME MANAGEMENT ON DATABASE bar FROM role")
    execute("REVOKE NAME MANAGEMENT ON DATABASE foo FROM role")
    execute("REVOKE NAME MANAGEMENT ON DATABASE * FROM role")

    // THEN
    execute("SHOW ROLE role PRIVILEGES").toSet should be(Set.empty)

    // WHEN
    execute("GRANT CREATE NEW LABEL ON DEFAULT DATABASE TO role")

    // THEN
    execute("SHOW ROLE role PRIVILEGES").toSet should be(Set(
      createNodeLabel().database(DEFAULT_DATABASE_NAME).role("role").map
    ))
  }

  test("Should get correct privileges for combinations of schema and token write") {
    setup()
    setupUserWithCustomRole()

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      access().role("custom").map,
    ))

    execute("GRANT CREATE NEW NODE LABEL ON DATABASE * TO custom")
    execute("GRANT CREATE INDEX ON DATABASE * TO custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      access().role("custom").map,
      createIndex().role("custom").map,
      createNodeLabel().role("custom").map
    ))

    execute("GRANT NAME MANAGEMENT ON DATABASE * TO custom")
    execute("GRANT INDEX MANAGEMENT ON DATABASE * TO custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      access().role("custom").map,
      createIndex().role("custom").map,
      dropIndex().role("custom").map,
      createNodeLabel().role("custom").map,
      createRelationshipType().role("custom").map,
      createPropertyKey().role("custom").map
    ))
  }

  test("Should get correct privileges for all database privileges") {
    // Given
    setup()
    val dbx: Set[PrivilegeMapBuilder] = Set(access(), startDatabase(), stopDatabase())
    val schema = Set(createIndex(), dropIndex(), createConstraint(), dropConstraint())
    val token = Set(createNodeLabel(), createRelationshipType(), createPropertyKey())
    def toPriv(e: Set[PrivilegeMapBuilder]) = e.map(p => p.database("foo").role("custom").map)
    setupUserWithCustomRole()
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE DATABASE foo")

    // When/Then
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      access().role("custom").map,
    ))

    // When
    execute("GRANT ALL DATABASE PRIVILEGES ON DATABASE foo TO custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(access().role("custom").map) ++ toPriv(dbx ++ schema ++ token))

    // When
    execute("REVOKE NAME MANAGEMENT ON DATABASE foo FROM custom")

    // Then
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(access().role("custom").map) ++ toPriv(dbx ++ schema))

    // When
    execute("REVOKE INDEX MANAGEMENT ON DATABASE foo FROM custom")
    execute("REVOKE CONSTRAINT MANAGEMENT ON DATABASE foo FROM custom")

    // Then
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(access().role("custom").map) ++ toPriv(dbx))

    // When
    execute("REVOKE ACCESS ON DATABASE foo FROM custom")

    // Then
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(access().role("custom").map) ++ toPriv(Set(startDatabase(), stopDatabase())))
  }

  test("Should revoke subset of token and index management with superset revokes") {
    setup()
    setupUserWithCustomRole()

    // WHEN
    execute("GRANT CREATE NEW RELATIONSHIP TYPE ON DATABASE * TO custom")
    execute("GRANT DROP INDEX ON DATABASE * TO custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      access().role("custom").map,
      dropIndex().role("custom").map,
      createRelationshipType().role("custom").map
    ))

    // WHEN
    execute("REVOKE NAME MANAGEMENT ON DATABASE * FROM custom")
    execute("REVOKE INDEX MANAGEMENT ON DATABASE * FROM custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      access().role("custom").map
    ))
  }

  test("Should revoke both grant, deny, create and drop in one command (index management)") {
    // GIVEN
    setup()
    execute("CREATE ROLE custom")

    // WHEN
    execute("GRANT CREATE INDEX ON DATABASE * TO custom")
    execute("DENY CREATE INDEX ON DATABASE * TO custom")
    execute("GRANT DROP INDEX ON DATABASE * TO custom")
    execute("DENY DROP INDEX ON DATABASE * TO custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      createIndex().role("custom").map,
      createIndex("DENIED").role("custom").map,
      dropIndex().role("custom").map,
      dropIndex("DENIED").role("custom").map
    ))

    // WHEN
    val result = execute("REVOKE INDEX MANAGEMENT ON DATABASE * FROM custom")

    // THEN
    result.queryStatistics().systemUpdates should be(4)
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set.empty)
  }

  test("Should revoke both grant, deny, create and drop in one command (constraint management)") {
    // GIVEN
    setup()
    execute("CREATE ROLE custom")

    // WHEN
    execute("GRANT CREATE CONSTRAINT ON DATABASE * TO custom")
    execute("DENY CREATE CONSTRAINT ON DATABASE * TO custom")
    execute("GRANT DROP CONSTRAINT ON DATABASE * TO custom")
    execute("DENY DROP CONSTRAINT ON DATABASE * TO custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      createConstraint().role("custom").map,
      createConstraint("DENIED").role("custom").map,
      dropConstraint().role("custom").map,
      dropConstraint("DENIED").role("custom").map
    ))

    // WHEN
    val result = execute("REVOKE CONSTRAINT MANAGEMENT ON DATABASE * FROM custom")

    // THEN
    result.queryStatistics().systemUpdates should be(4)
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set.empty)
  }

  test("Should revoke both grant and deny for all create tokens in one command (name management)") {
    // GIVEN
    setup()
    execute("CREATE ROLE custom")

    // WHEN
    execute("GRANT CREATE NEW LABEL ON DATABASE * TO custom")
    execute("DENY CREATE NEW LABEL ON DATABASE * TO custom")
    execute("GRANT CREATE NEW TYPE ON DATABASE * TO custom")
    execute("DENY CREATE NEW TYPE ON DATABASE * TO custom")
    execute("GRANT CREATE NEW NAME ON DATABASE * TO custom")
    execute("DENY CREATE NEW NAME ON DATABASE * TO custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      createNodeLabel().role("custom").map,
      createNodeLabel("DENIED").role("custom").map,
      createRelationshipType().role("custom").map,
      createRelationshipType("DENIED").role("custom").map,
      createPropertyKey().role("custom").map,
      createPropertyKey("DENIED").role("custom").map
    ))

    // WHEN
    val result = execute("REVOKE NAME MANAGEMENT ON DATABASE * FROM custom")

    // THEN
    result.queryStatistics().systemUpdates should be(6)
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set.empty)
  }

  test("Should revoke compound TOKEN privileges from built-in roles") {
    // Given
    setup()
    execute("CREATE ROLE custom AS COPY OF admin")
    val expected = defaultRolePrivilegesFor("admin", "custom")

    // When && Then
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(expected)

    // When
    execute("REVOKE NAME MANAGEMENT ON DATABASES * FROM custom")

    // Then
    val expectedWithoutNameManagement = expected.filter(_ ("action") != PrivilegeAction.TOKEN.toString)
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(expectedWithoutNameManagement)

    // Now add and revoke each sub-privilege in turn, checking also the compound revokes work on sub-privileges
    Seq(
      ("CREATE NEW NODE LABEL", "create_label"),
      ("CREATE NEW RELATIONSHIP TYPE", "create_reltype"),
      ("CREATE NEW PROPERTY NAME", "create_propertykey")
    ).foreach {
      case (privilege, action) =>
        Seq("NAME MANAGEMENT", privilege).foreach {
          revokePrivilege =>
            // When
            execute(s"GRANT $privilege ON DATABASES * TO custom")

            // Then
            val expectedWithPrivilege = expectedWithoutNameManagement + grantToken().action(action).role("custom").map
            withClue(s"After GRANT $privilege") {
              execute("SHOW ROLE custom PRIVILEGES").toSet should be(expectedWithPrivilege)
            }

            // When
            execute(s"REVOKE $revokePrivilege ON DATABASES * FROM custom")

            // Then
            withClue(s"After REVOKE $revokePrivilege") {
              execute("SHOW ROLE custom PRIVILEGES").toSet should be(expectedWithoutNameManagement)
            }
        }
    }
  }

  test("Should get sub-privileges when revoking compound and re-granting token privilege") {
    // Given
    setup()
    execute("CREATE ROLE custom AS COPY OF publisher")
    execute("REVOKE READ {*} ON GRAPH * FROM custom")
    execute("REVOKE TRAVERSE ON GRAPH * FROM custom")
    execute("REVOKE WRITE ON GRAPH * FROM custom")
    execute("REVOKE ACCESS ON DATABASE * FROM custom")

    // Role have compound token privilege
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(grantToken().role("custom").map))

    // When: revoking
    execute("REVOKE NAME MANAGEMENT ON DATABASES * FROM custom")

    // Then: role has no privileges
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set.empty)

    // When: granting
    execute("GRANT NAME MANAGEMENT ON DATABASES * TO custom")

    // Then: role has the sub-privileges
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      createNodeLabel().role("custom").map,
      createRelationshipType().role("custom").map,
      createPropertyKey().role("custom").map
    ))
  }

  test("Should revoke compound ALL DATABASE privileges from built-in roles") {
    // Given
    setup()
    execute("CREATE ROLE custom AS COPY OF admin")
    val expected = defaultRolePrivilegesFor("admin", "custom")

    // When && Then
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(expected)

    // When
    execute("REVOKE ALL DATABASE PRIVILEGES ON DATABASES * FROM custom")

    // Then
    // TODO: The START/STOP are not correctly revoked yet, but this requires more complex work
    val expectedWithoutAllDatabasePrivileges = expected.filter(a => PrivilegeAction.from(a("action").toString) match {
      case PrivilegeAction.SCHEMA => false
      case PrivilegeAction.TOKEN => false
      case PrivilegeAction.ACCESS => false
      case PrivilegeAction.START_DATABASE => true // should be false but currently not being revoked: part of compound which should not be fully revoked
      case PrivilegeAction.STOP_DATABASE => true // should be false but currently not being revoked: part of compound which should not be fully revoked
      case _ => true
    })
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(expectedWithoutAllDatabasePrivileges)

    // Now add and revoke each sub-privilege in turn, checking also the compound revokes work on sub-privileges
    Seq(
      ("CREATE INDEX", Seq("create_index")),
      ("DROP INDEX", Seq("drop_index")),
      ("INDEX MANAGEMENT", Seq("create_index", "drop_index")),
      ("CREATE CONSTRAINT", Seq("create_constraint")),
      ("DROP CONSTRAINT", Seq("drop_constraint")),
      ("CONSTRAINT MANAGEMENT", Seq("create_constraint", "drop_constraint")),
      ("CREATE NEW NODE LABEL", Seq("create_label")),
      ("CREATE NEW RELATIONSHIP TYPE", Seq("create_reltype")),
      ("CREATE NEW PROPERTY NAME", Seq("create_propertykey")),
      ("NAME MANAGEMENT", Seq("create_label", "create_reltype", "create_propertykey")),
      ("ACCESS", Seq("access"))
    ).foreach {
      case (privilege, actions) =>
        Seq("ALL DATABASE PRIVILEGES", privilege).foreach {
          revokePrivilege =>
            // When
            execute(s"GRANT $privilege ON DATABASES * TO custom")

            // Then
            val expectedWithPrivilege = expectedWithoutAllDatabasePrivileges ++ actions.map(action => grantToken().action(action).role("custom").map)
            withClue(s"After GRANT $privilege") {
              execute("SHOW ROLE custom PRIVILEGES").toSet should be(expectedWithPrivilege)
            }

            // When
            execute(s"REVOKE $revokePrivilege ON DATABASES * FROM custom")

            // Then
            withClue(s"After REVOKE $revokePrivilege") {
              execute("SHOW ROLE custom PRIVILEGES").toSet should be(expectedWithoutAllDatabasePrivileges)
            }
        }
    }
  }

  test("Should get error when revoking a subset of a compound admin privilege") {
    // Given
    setup()
    execute("CREATE ROLE custom AS COPY OF admin")
    execute("REVOKE READ {*} ON GRAPH * FROM custom")
    execute("REVOKE TRAVERSE ON GRAPH * FROM custom")
    execute("REVOKE WRITE ON GRAPH * FROM custom")
    execute("REVOKE ACCESS ON DATABASE * FROM custom")
    execute("REVOKE ALL ON DATABASE * FROM custom")
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(grantAdmin().role("custom").map))

    // Now try to revoke each sub-privilege (that we have syntax for) in turn
    Seq(
      ("CREATE ROLE ON DBMS", "CREATE ROLE"),
      ("DROP ROLE ON DBMS", "DROP ROLE"),
      ("SHOW ROLE ON DBMS", "SHOW ROLE"),
      ("ASSIGN ROLE ON DBMS", "ASSIGN ROLE"),
      ("REMOVE ROLE ON DBMS", "REMOVE ROLE"),
      ("ROLE MANAGEMENT ON DBMS", "ROLE MANAGEMENT"),
      ("START ON DATABASES *", "START"),
      ("STOP ON DATABASES *", "STOP"),
    ).foreach {
      case (queryPart, privilege) =>
        // When && Then
        the[IllegalStateException] thrownBy {
          execute(s"REVOKE $queryPart FROM custom")
        } should have message s"Unsupported to revoke a sub-privilege '$privilege' from a compound privilege 'ALL ADMIN PRIVILEGES', consider using DENY instead."
    }
  }

  test("Should get error when revoking a subset of a compound token privilege") {
    // Given
    setup()
    execute("CREATE ROLE custom AS COPY OF publisher")
    execute("REVOKE READ {*} ON GRAPH * FROM custom")
    execute("REVOKE TRAVERSE ON GRAPH * FROM custom")
    execute("REVOKE WRITE ON GRAPH * FROM custom")
    execute("REVOKE ACCESS ON DATABASE * FROM custom")
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(grantToken().role("custom").map))

    // Now try to revoke each sub-privilege in turn
    Seq(
      "CREATE NEW NODE LABEL",
      "CREATE NEW RELATIONSHIP TYPE",
      "CREATE NEW PROPERTY NAME"
    ).foreach { privilege =>
      // When && Then
      the[IllegalStateException] thrownBy {
        execute(s"REVOKE $privilege ON DATABASE * FROM custom")
      } should have message s"Unsupported to revoke a sub-privilege '$privilege' from a compound privilege 'NAME MANAGEMENT', consider using DENY instead."
    }
  }

  test("Should get error when revoking a subset of a compound schema privilege") {
    // Given
    setup()
    execute("CREATE ROLE custom AS COPY OF admin")
    execute("REVOKE READ {*} ON GRAPH * FROM custom")
    execute("REVOKE TRAVERSE ON GRAPH * FROM custom")
    execute("REVOKE WRITE ON GRAPH * FROM custom")
    execute("REVOKE ACCESS ON DATABASE * FROM custom")
    execute("REVOKE NAME MANAGEMENT ON DATABASE * FROM custom")
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(grantAdmin().role("custom").map, grantSchema().role("custom").map))

    // Now try to revoke each sub-privilege in turn
    Seq(
      "CREATE INDEX",
      "DROP INDEX",
      "INDEX MANAGEMENT",
      "CREATE CONSTRAINT",
      "DROP CONSTRAINT",
      "CONSTRAINT MANAGEMENT"
    ).foreach { privilege =>
      // When && Then
      the[IllegalStateException] thrownBy {
        execute(s"REVOKE $privilege ON DATABASE * FROM custom")
      } should have message s"Unsupported to revoke a sub-privilege '$privilege' from a compound privilege 'SCHEMA MANAGEMENT', consider using DENY instead."
    }
  }

  // Tests for actual behaviour of authorization rules for restricted users based on privileges

  // Index Management
  test("Should not allow index creation on non-existing tokens for normal user without token create privilege") {
    setup()
    setupUserWithCustomRole()
    execute("GRANT CREATE INDEX ON DATABASE * TO custom")

    // WHEN & THEN
    the[AuthorizationViolationException] thrownBy {
      executeOnDefault("joe", "soap", "CREATE INDEX FOR (u:User) ON (u.name)")
    } should have message "'create_label' operations are not allowed for user 'joe' with roles [custom]."

    // THEN
    assert(graph.getMaybeIndex("User", Seq("name")).isEmpty)
  }

  test("Should allow index creation on already existing tokens for normal user without token create privilege") {
    setup()
    setupUserWithCustomRole()
    execute("GRANT CREATE INDEX ON DATABASE * TO custom")
    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE (:User {name: 'Me'})")

    // WHEN
    executeOnDefault("joe", "soap", "CREATE INDEX FOR (u:User) ON (u.name)") should be(0)

    // THEN
    assert(graph.getMaybeIndex("User", Seq("name")).isDefined)
  }

  test("Should not allow index creation for normal user without index create privilege") {
    setup()
    setupUserWithCustomRole()
    execute("GRANT NAME MANAGEMENT ON DATABASE * TO custom")

    // WHEN & THEN
    the[AuthorizationViolationException] thrownBy {
      executeOnDefault("joe", "soap", "CREATE INDEX FOR (u:User) ON (u.name)")
    } should have message "Schema operations are not allowed for user 'joe' with roles [custom]."
  }

  test("Should not allow index create for normal user with only index drop privilege") {
    // Given
    setup()
    setupUserWithCustomRole()
    execute("GRANT NAME MANAGEMENT ON DATABASE * TO custom")
    execute("GRANT DROP INDEX ON DATABASE * TO custom")

    // WHEN & THEN
    the[AuthorizationViolationException] thrownBy {
      executeOnDefault("joe", "soap", "CREATE INDEX FOR (u:User) ON (u.name)")
    } should have message "Schema operation 'create_index' is not allowed for user 'joe' with roles [custom]."
  }

  test("Should not allow index drop for normal user with only index create privilege") {
    // Given
    setup()
    setupUserWithCustomRole()
    execute("GRANT NAME MANAGEMENT ON DATABASE * TO custom")
    execute("GRANT INDEX MANAGEMENT ON DATABASE * TO custom")
    executeOnDefault("joe", "soap", "CREATE INDEX my_index FOR (u:User) ON (u.name)") should be(0)

    // When
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("REVOKE GRANT DROP INDEX ON DATABASE * FROM custom")

    // WHEN & THEN
    the[AuthorizationViolationException] thrownBy {
      executeOnDefault("joe", "soap", "DROP INDEX my_index")
    } should have message "Schema operation 'drop_index' is not allowed for user 'joe' with roles [custom]."
  }

  test("Should allow index creation for normal user with index create privilege") {
    setup()
    setupUserWithCustomRole()
    execute("GRANT NAME MANAGEMENT ON DATABASE * TO custom")
    execute("GRANT CREATE INDEX ON DATABASE * TO custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      access().role("custom").map,
      createIndex().role("custom").map,
      createNodeLabel().role("custom").map,
      createRelationshipType().role("custom").map,
      createPropertyKey().role("custom").map
    ))

    // WHEN & THEN
    executeOnDefault("joe", "soap", "CREATE INDEX FOR (u:User) ON (u.name)") should be(0)
  }

  test("Should allow index dropping for normal user with index drop privilege") {
    selectDatabase(DEFAULT_DATABASE_NAME)
    graph.createIndexWithName("my_index", "Label", "prop")
    setupUserWithCustomRole()
    execute("GRANT DROP INDEX ON DATABASE * TO custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      access().role("custom").map,
      dropIndex().role("custom").map
    ))

    // WHEN
    executeOnDefault("joe", "soap", "DROP INDEX my_index") should be(0)

    // THEN
    graph.getMaybeIndex("Label", Seq("prop")) should be(None)
  }

  test("Should allow index creation and dropping for normal user with index management privilege") {
    setupUserWithCustomRole()
    execute("GRANT NAME MANAGEMENT ON DATABASE * TO custom")
    execute("GRANT INDEX ON DATABASE * TO custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      access().role("custom").map,
      createNodeLabel().role("custom").map,
      createRelationshipType().role("custom").map,
      createPropertyKey().role("custom").map,
      createIndex().role("custom").map,
      dropIndex().role("custom").map
    ))

    // WHEN
    executeOnDefault("joe", "soap", "CREATE INDEX my_index FOR (u:User) ON (u.name)") should be(0)

    // THEN
    graph.getMaybeIndex("User", Seq("name")).isDefined should be(true)

    // WHEN
    executeOnDefault("joe", "soap", "DROP INDEX my_index") should be(0)

    // THEN
    graph.getMaybeIndex("User", Seq("name")).isDefined should be(false)
  }

  test("Should allow index creation for normal user with all database privileges") {
    setup()
    setupUserWithCustomRole()
    execute("CREATE DATABASE foo")
    execute("GRANT ALL PRIVILEGES ON DATABASE foo TO custom")

    // WHEN & THEN
    executeOn("foo", "joe", "soap", "CREATE INDEX FOR (u:User) ON (u.name)") should be(0)
  }

  test("Should not allow index creation for normal user with all database privileges and explicit deny") {
    setup()
    setupUserWithCustomRole()
    execute("CREATE DATABASE foo")
    execute("GRANT ALL PRIVILEGES ON DATABASE * TO custom")
    execute("DENY CREATE INDEX ON DATABASE foo TO custom")

    // WHEN & THEN
    the[AuthorizationViolationException] thrownBy {
      executeOn("foo", "joe", "soap", "CREATE INDEX FOR (u:User) ON (u.name)")
    } should have message "Schema operation 'create_index' is not allowed for user 'joe' with roles [custom]."
  }

  // Constraint Management
  test("Should not allow constraint creation on non-existing tokens for normal user without token create privilege") {
    setup()
    setupUserWithCustomRole()
    execute("GRANT CREATE CONSTRAINT ON DATABASE * TO custom")

    // WHEN & THEN
    the[AuthorizationViolationException] thrownBy {
      executeOnDefault("joe", "soap", "CREATE CONSTRAINT ON (n:User) ASSERT exists(n.name)")
    } should have message "'create_label' operations are not allowed for user 'joe' with roles [custom]."

    // THEN
    assert(graph.getMaybeNodeConstraint("User", Seq("name")).isEmpty)
  }

  test("Should allow constraint creation on already existing tokens for normal user without token create privilege") {
    setup()
    setupUserWithCustomRole()
    execute("GRANT CREATE CONSTRAINT ON DATABASE * TO custom")
    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE (:User {name: 'Me'})")

    // WHEN
    executeOnDefault("joe", "soap", "CREATE CONSTRAINT ON (n:User) ASSERT exists(n.name)") should be(0)

    // THEN
    assert(graph.getMaybeNodeConstraint("User", Seq("name")).isDefined)
  }

  test("Should not allow constraint creation for normal user without constraint create privilege") {
    setup()
    setupUserWithCustomRole()
    execute("GRANT NAME MANAGEMENT ON DATABASE * TO custom")

    // WHEN & THEN
    the[AuthorizationViolationException] thrownBy {
      executeOnDefault("joe", "soap", "CREATE CONSTRAINT ON (n:User) ASSERT exists(n.name)")
    } should have message "Schema operations are not allowed for user 'joe' with roles [custom]."
  }

  test("Should not allow constraint create for normal user with only constraint drop privilege") {
    // Given
    setup()
    setupUserWithCustomRole()
    execute("GRANT NAME MANAGEMENT ON DATABASE * TO custom")
    execute("GRANT DROP CONSTRAINT ON DATABASE * TO custom")

    // WHEN & THEN
    the[AuthorizationViolationException] thrownBy {
      executeOnDefault("joe", "soap", "CREATE CONSTRAINT ON (n:User) ASSERT exists(n.name)")
    } should have message "Schema operation 'create_constraint' is not allowed for user 'joe' with roles [custom]."
  }

  test("Should not allow constraint drop for normal user with only constraint create privilege") {
    // Given
    setup()
    setupUserWithCustomRole()
    execute("GRANT NAME MANAGEMENT ON DATABASE * TO custom")
    execute("GRANT CONSTRAINT MANAGEMENT ON DATABASE * TO custom")
    executeOnDefault("joe", "soap", "CREATE CONSTRAINT my_constraint ON (n:User) ASSERT exists(n.name)") should be(0)

    // When
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("REVOKE GRANT DROP CONSTRAINT ON DATABASE * FROM custom")

    // WHEN & THEN
    the[AuthorizationViolationException] thrownBy {
      executeOnDefault("joe", "soap", "DROP CONSTRAINT my_constraint")
    } should have message "Schema operation 'drop_constraint' is not allowed for user 'joe' with roles [custom]."
  }

  test("Should allow constraint creation for normal user with constraint create privilege") {
    setup()
    setupUserWithCustomRole()
    execute("GRANT NAME MANAGEMENT ON DATABASE * TO custom")
    execute("GRANT CREATE CONSTRAINT ON DATABASE * TO custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      access().role("custom").map,
      createConstraint().role("custom").map,
      createNodeLabel().role("custom").map,
      createRelationshipType().role("custom").map,
      createPropertyKey().role("custom").map
    ))

    // WHEN & THEN
    executeOnDefault("joe", "soap", "CREATE CONSTRAINT ON (n:User) ASSERT exists(n.name)") should be(0)
  }

  test("Should allow constraint dropping for normal user with constraint drop privilege") {
    selectDatabase(DEFAULT_DATABASE_NAME)
    graph.createNodeExistenceConstraintWithName("my_constraint", "Label", "prop")
    setupUserWithCustomRole()
    execute("GRANT DROP CONSTRAINT ON DATABASE * TO custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      access().role("custom").map,
      dropConstraint().role("custom").map
    ))

    // WHEN
    executeOnDefault("joe", "soap", "DROP CONSTRAINT my_constraint") should be(0)

    // THEN
    graph.getMaybeNodeConstraint("Label", Seq("prop")) should be(None)
  }

  test("Should allow constraint creation and dropping for normal user with constraint management privilege") {
    setupUserWithCustomRole()
    execute("GRANT NAME MANAGEMENT ON DATABASE * TO custom")
    execute("GRANT CONSTRAINT ON DATABASE * TO custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      access().role("custom").map,
      createNodeLabel().role("custom").map,
      createRelationshipType().role("custom").map,
      createPropertyKey().role("custom").map,
      createConstraint().role("custom").map,
      dropConstraint().role("custom").map
    ))

    // WHEN
    executeOnDefault("joe", "soap", "CREATE CONSTRAINT my_constraint ON (u:User) ASSERT exists(u.name)") should be(0)

    // THEN
    graph.getMaybeNodeConstraint("User", Seq("name")).isDefined should be(true)

    // WHEN
    executeOnDefault("joe", "soap", "DROP CONSTRAINT my_constraint") should be(0)

    // THEN
    graph.getMaybeNodeConstraint("User", Seq("name")).isDefined should be(false)
  }

  test("Should allow constraint creation for normal user with all database privileges") {
    setup()
    setupUserWithCustomRole()
    execute("CREATE DATABASE foo")
    execute("GRANT ALL PRIVILEGES ON DATABASE foo TO custom")

    // WHEN & THEN
    executeOn("foo", "joe", "soap", "CREATE CONSTRAINT ON (n:User) ASSERT exists(n.name)") should be(0)
  }

  test("Should not allow constraint creation for normal user with all database privileges and explicit deny") {
    setup()
    setupUserWithCustomRole()
    execute("CREATE DATABASE foo")
    execute("GRANT ALL PRIVILEGES ON DATABASE * TO custom")
    execute("DENY CREATE CONSTRAINT ON DATABASE foo TO custom")

    // WHEN & THEN
    the[AuthorizationViolationException] thrownBy {
      executeOn("foo", "joe", "soap", "CREATE CONSTRAINT ON (n:User) ASSERT exists(n.name)")
    } should have message "Schema operation 'create_constraint' is not allowed for user 'joe' with roles [custom]."
  }

  // Name Management
  test("Should allow label creation for normal user with label create privilege") {
    setup()
    setupUserWithCustomRole()
    execute("GRANT CREATE NEW LABEL ON DATABASE * TO custom")

    // WHEN & THEN
    executeOnDefault("joe", "soap", "CALL db.createLabel('A')") should be(0)
  }

  test("Should not allow label creation for normal user with explicit deny") {
    setup()
    setupUserWithCustomRole()
    execute("GRANT WRITE ON GRAPH * TO custom")
    execute("GRANT NAME MANAGEMENT ON DATABASE * TO custom")
    execute("DENY CREATE NEW LABEL ON DATABASE * TO custom")

    // WHEN & THEN
    the[AuthorizationViolationException] thrownBy {
      executeOnDefault("joe", "soap", "CREATE (n:User) RETURN n")
    } should have message "'create_label' operations are not allowed for user 'joe' with roles [custom]."

    // WHEN & THEN
    the[QueryExecutionException] thrownBy {
      executeOnDefault("joe", "soap", "CALL db.createLabel('A')")
    } should have message "'create_label' operations are not allowed for user 'joe' with roles [custom] restricted to TOKEN_WRITE."
  }

  test("Should allow type creation for normal user with type create privilege") {
    setup()
    setupUserWithCustomRole()
    execute("GRANT CREATE NEW TYPE ON DATABASE * TO custom")

    // WHEN & THEN
    executeOnDefault("joe", "soap", "CALL db.createRelationshipType('A')") should be(0)
  }

  test("Should not allow type creation for normal user with explicit deny") {
    setup()
    setupUserWithCustomRole()
    execute("GRANT WRITE ON GRAPH * TO custom")
    execute("GRANT NAME MANAGEMENT ON DATABASE * TO custom")
    execute("DENY CREATE NEW TYPE ON DATABASE * TO custom")

    // WHEN & THEN
    the[AuthorizationViolationException] thrownBy {
      executeOnDefault("joe", "soap", "CREATE ()-[n:Rel]->() RETURN n")
    } should have message "'create_reltype' operations are not allowed for user 'joe' with roles [custom]."

    // WHEN & THEN
    the[QueryExecutionException] thrownBy {
      executeOnDefault("joe", "soap", "CALL db.createRelationshipType('A')")
    } should have message "'create_reltype' operations are not allowed for user 'joe' with roles [custom] restricted to TOKEN_WRITE."
  }

  test("Should allow property key creation for normal user with name creation privilege") {
    setup()
    setupUserWithCustomRole()
    execute("GRANT CREATE NEW NAME ON DATABASE * TO custom")
    selectDatabase(DEFAULT_DATABASE_NAME)

    // WHEN & THEN
    executeOnDefault("joe", "soap", "CALL db.createProperty('age')") should be(0)
  }

  test("Should not allow property key creation for normal user with explicit deny") {
    setup()
    setupUserWithCustomRole()
    execute("GRANT WRITE ON GRAPH * TO custom")
    execute("GRANT NAME MANAGEMENT ON DATABASE * TO custom")
    execute("DENY CREATE NEW NAME ON DATABASE * TO custom")

    // WHEN & THEN
    the[AuthorizationViolationException] thrownBy {
      executeOnDefault("joe", "soap", "CREATE (n:User {name: 'Alice'}) RETURN n")
    } should have message "'create_propertykey' operations are not allowed for user 'joe' with roles [custom]."

    // WHEN & THEN
    the[AuthorizationViolationException] thrownBy {
      executeOnDefault("joe", "soap", "CREATE ()-[n:Rel {prop: 'value'}]->() RETURN n")
    } should have message "'create_propertykey' operations are not allowed for user 'joe' with roles [custom]."

    // WHEN & THEN
    the[QueryExecutionException] thrownBy {
      executeOnDefault("joe", "soap", "CALL db.createProperty('age')")
    } should have message "'create_propertykey' operations are not allowed for user 'joe' with roles [custom] restricted to TOKEN_WRITE."
  }

  test("Should not allow property key creation for normal user with only label creation privilege") {
    setup()
    setupUserWithCustomRole()
    execute("GRANT WRITE ON GRAPH * TO custom")
    execute("GRANT CREATE NEW LABEL ON DATABASE * TO custom")

    // WHEN & THEN
    the[AuthorizationViolationException] thrownBy {
      executeOnDefault("joe", "soap", "CREATE (n:User {name: 'Alice'}) RETURN n.name")
    } should have message "'create_propertykey' operations are not allowed for user 'joe' with roles [custom]."
  }

  test("Should not allow property key creation for normal user with only type creation privilege") {
    setup()
    setupUserWithCustomRole()
    execute("GRANT WRITE ON GRAPH * TO custom")
    execute("GRANT CREATE NEW TYPE ON DATABASE * TO custom")

    // WHEN & THEN
    the[AuthorizationViolationException] thrownBy {
      executeOnDefault("joe", "soap", "CREATE ()-[r:Rel {prop: 'value'}]->() RETURN r.prop")
    } should have message "'create_propertykey' operations are not allowed for user 'joe' with roles [custom]."
  }

  test("Should allow all creation for normal user with name management privilege") {
    setup()
    setupUserWithCustomRole()
    execute("GRANT WRITE ON GRAPH * TO custom")
    execute("GRANT NAME MANAGEMENT ON DATABASE * TO custom")

    // WHEN & THEN
    executeOnDefault("joe", "soap", "CREATE (n:User {name: 'Alice'})-[:KNOWS {since: 2019}]->(:User {name: 'Bob'}) RETURN n.name", resultHandler = (row, _) => {
      row.get("n.name") should be("Alice")
    }) should be(1)
  }

  test("Should allow all creation for normal user with all database privileges") {
    setup()
    setupUserWithCustomRole()
    execute("GRANT WRITE ON GRAPH * TO custom")
    execute("GRANT ALL ON DATABASE * TO custom")

    // WHEN & THEN
    executeOnDefault("joe", "soap", "CREATE (n:User {name: 'Alice'})-[:KNOWS {since: 2019}]->(:User {name: 'Bob'}) RETURN n.name", resultHandler = (row, _) => {
      row.get("n.name") should be("Alice")
    }) should be(1)
  }

  // Disable normal database creation because we need different settings on each test
  override protected def initTest() {}

  // Use the default value instead of the new value in AdministrationCommandAcceptanceTestBase
  override def databaseConfig(): Map[Setting[_], Object] = Map()
}
