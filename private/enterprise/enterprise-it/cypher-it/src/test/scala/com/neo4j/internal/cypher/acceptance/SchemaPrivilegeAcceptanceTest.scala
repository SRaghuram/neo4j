/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.internal.cypher.acceptance

import org.neo4j.configuration.GraphDatabaseSettings.{DEFAULT_DATABASE_NAME, SYSTEM_DATABASE_NAME}
import org.neo4j.graphdb.QueryExecutionException
import org.neo4j.graphdb.security.AuthorizationViolationException

class SchemaPrivilegeAcceptanceTest extends AdministrationCommandAcceptanceTestBase {

  test("should return empty counts to the outside for commands that update the system graph internally") {
    //TODO: ADD ANY NEW UPDATING COMMANDS HERE

    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")

    // Notice: They are executed in succession so they have to make sense in that order
    assertQueriesAndSubQueryCounts(List(
      "GRANT CREATE INDEX ON DATABASE * TO custom" -> 1,
      "REVOKE GRANT CREATE INDEX ON DATABASE * FROM custom" -> 1,
      "DENY CREATE INDEX ON DATABASE * TO custom" -> 1,
      "REVOKE DENY CREATE INDEX ON DATABASE * FROM custom" -> 1,

      "GRANT DROP INDEX ON DATABASE * TO custom" -> 1,
      "DENY DROP INDEX ON DATABASE * TO custom" -> 1,
      "REVOKE DROP INDEX ON DATABASE * FROM custom" -> 1,

      "GRANT INDEX MANAGEMENT ON DATABASES * TO custom" -> 2,
      "REVOKE GRANT INDEX MANAGEMENT ON DATABASES * FROM custom" -> 2,
      "DENY INDEX MANAGEMENT ON DATABASES * TO custom" -> 2,
      "REVOKE DENY INDEX MANAGEMENT ON DATABASES * FROM custom" -> 2,
      "GRANT INDEX MANAGEMENT ON DATABASES * TO custom" -> 2,
      "DENY INDEX MANAGEMENT ON DATABASES * TO custom" -> 2,
      "REVOKE INDEX MANAGEMENT ON DATABASES * FROM custom" -> 2,


      "GRANT CREATE CONSTRAINT ON DATABASE * TO custom" -> 1,
      "DENY CREATE CONSTRAINT ON DATABASE * TO custom" -> 1,
      "REVOKE CREATE CONSTRAINT ON DATABASE * FROM custom" -> 1,

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
      "REVOKE CONSTRAINT MANAGEMENT ON DATABASES * FROM custom" -> 2,


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
      "REVOKE CREATE NEW NAME ON DATABASE * FROM custom" -> 1,

      "GRANT NAME MANAGEMENT ON DATABASES * TO custom" -> 3,
      "REVOKE GRANT NAME MANAGEMENT ON DATABASES * FROM custom" -> 3,
      "DENY NAME MANAGEMENT ON DATABASES * TO custom" -> 3,
      "REVOKE DENY NAME MANAGEMENT ON DATABASES * FROM custom" -> 3,
      "GRANT NAME MANAGEMENT ON DATABASES * TO custom" -> 3,
      "DENY NAME MANAGEMENT ON DATABASES * TO custom" -> 3,
      "REVOKE NAME MANAGEMENT ON DATABASES * FROM custom" -> 3
    ))
  }

  // Tests for granting, denying and revoking schema privileges

  test("should list different index management privileges") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
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
  }

  test("should list different constraint management privileges") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
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
  }

  test("should list different name management privileges") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
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
  }

  test("Should get correct privileges for combinations of schema and token write") {
    setupUserWithCustomRole()
    selectDatabase(SYSTEM_DATABASE_NAME)

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
    setupUserWithCustomRole()
    selectDatabase(SYSTEM_DATABASE_NAME)

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
    selectDatabase(SYSTEM_DATABASE_NAME)
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
    result.queryStatistics().systemUpdates should be(2)
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set.empty)
  }

  test("Should revoke both grant, deny, create and drop in one command (constraint management)") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
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
    result.queryStatistics().systemUpdates should be(2)
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set.empty)
  }

  test("Should revoke both grant and deny for all create tokens in one command (name management)") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
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
    result.queryStatistics().systemUpdates should be(3)
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set.empty)
  }

  // Tests for actual behaviour of authorization rules for restricted users based on privileges

  // Index Management
  test("Should not allow index creation on non-existing tokens for normal user without token create privilege") {
    setupUserWithCustomRole()
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT CREATE INDEX ON DATABASE * TO custom")

    // WHEN & THEN
    the[AuthorizationViolationException] thrownBy {
      executeOnDefault("joe", "soap", "CREATE INDEX FOR (u:User) ON (u.name)")
    } should have message "'create_label' operations are not allowed for user 'joe' with roles [custom]."

    // THEN
    assert(graph.getMaybeIndex("User", Seq("name")).isEmpty)
  }

  test("Should allow index creation on already existing tokens for normal user without token create privilege") {
    setupUserWithCustomRole()
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT CREATE INDEX ON DATABASE * TO custom")
    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE (:User {name: 'Me'})")

    // WHEN
    executeOnDefault("joe", "soap", "CREATE INDEX FOR (u:User) ON (u.name)") should be(0)

    // THEN
    assert(graph.getMaybeIndex("User", Seq("name")).isDefined)
  }

  test("Should not allow index creation for normal user without index create privilege") {
    setupUserWithCustomRole()
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT NAME MANAGEMENT ON DATABASE * TO custom")

    // WHEN & THEN
    the[AuthorizationViolationException] thrownBy {
      executeOnDefault("joe", "soap", "CREATE INDEX FOR (u:User) ON (u.name)")
    } should have message "Schema operations are not allowed for user 'joe' with roles [custom]."
  }

  test("Should not allow index create for normal user with only index drop privilege") {
    // Given
    setupUserWithCustomRole()
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT NAME MANAGEMENT ON DATABASE * TO custom")
    execute("GRANT DROP INDEX ON DATABASE * TO custom")

    // WHEN & THEN
    the[AuthorizationViolationException] thrownBy {
      executeOnDefault("joe", "soap", "CREATE INDEX FOR (u:User) ON (u.name)")
    } should have message "Schema operation 'create_index' is not allowed for user 'joe' with roles [custom]."
  }

  test("Should not allow index drop for normal user with only index create privilege") {
    // Given
    setupUserWithCustomRole()
    selectDatabase(SYSTEM_DATABASE_NAME)
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
    setupUserWithCustomRole()
    selectDatabase(SYSTEM_DATABASE_NAME)
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

  test("Should allow index creation for normal user with all database privileges") {
    setupUserWithCustomRole()
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE DATABASE foo")
    execute("GRANT ALL PRIVILEGES ON DATABASE foo TO custom")

    // WHEN & THEN
    executeOn("foo", "joe", "soap", "CREATE INDEX FOR (u:User) ON (u.name)") should be(0)
  }

  test("Should not allow index creation for normal user with all database privileges and explicit deny") {
    setupUserWithCustomRole()
    selectDatabase(SYSTEM_DATABASE_NAME)
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
    setupUserWithCustomRole()
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT CREATE CONSTRAINT ON DATABASE * TO custom")

    // WHEN & THEN
    the[AuthorizationViolationException] thrownBy {
      executeOnDefault("joe", "soap", "CREATE CONSTRAINT ON (n:User) ASSERT exists(n.name)")
    } should have message "'create_label' operations are not allowed for user 'joe' with roles [custom]."

    // THEN
    assert(graph.getMaybeNodeConstraint("User", Seq("name")).isEmpty)
  }

  test("Should allow constraint creation on already existing tokens for normal user without token create privilege") {
    setupUserWithCustomRole()
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT CREATE CONSTRAINT ON DATABASE * TO custom")
    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE (:User {name: 'Me'})")

    // WHEN
    executeOnDefault("joe", "soap", "CREATE CONSTRAINT ON (n:User) ASSERT exists(n.name)") should be(0)

    // THEN
    assert(graph.getMaybeNodeConstraint("User", Seq("name")).isDefined)
  }

  test("Should not allow constraint creation for normal user without constraint create privilege") {
    setupUserWithCustomRole()
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT NAME MANAGEMENT ON DATABASE * TO custom")

    // WHEN & THEN
    the[AuthorizationViolationException] thrownBy {
      executeOnDefault("joe", "soap", "CREATE CONSTRAINT ON (n:User) ASSERT exists(n.name)")
    } should have message "Schema operations are not allowed for user 'joe' with roles [custom]."
  }

  test("Should not allow constraint create for normal user with only constraint drop privilege") {
    // Given
    setupUserWithCustomRole()
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT NAME MANAGEMENT ON DATABASE * TO custom")
    execute("GRANT DROP CONSTRAINT ON DATABASE * TO custom")

    // WHEN & THEN
    the[AuthorizationViolationException] thrownBy {
      executeOnDefault("joe", "soap", "CREATE CONSTRAINT ON (n:User) ASSERT exists(n.name)")
    } should have message "Schema operation 'create_constraint' is not allowed for user 'joe' with roles [custom]."
  }

  test("Should not allow constraint drop for normal user with only constraint create privilege") {
    // Given
    setupUserWithCustomRole()
    selectDatabase(SYSTEM_DATABASE_NAME)
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
    setupUserWithCustomRole()
    selectDatabase(SYSTEM_DATABASE_NAME)
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

  test("Should allow constraint creation for normal user with all database privileges") {
    setupUserWithCustomRole()
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE DATABASE foo")
    execute("GRANT ALL PRIVILEGES ON DATABASE foo TO custom")

    // WHEN & THEN
    executeOn("foo", "joe", "soap", "CREATE CONSTRAINT ON (n:User) ASSERT exists(n.name)") should be(0)
  }

  test("Should not allow constraint creation for normal user with all database privileges and explicit deny") {
    setupUserWithCustomRole()
    selectDatabase(SYSTEM_DATABASE_NAME)
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
    setupUserWithCustomRole()
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT WRITE ON GRAPH * TO custom")
    execute("GRANT CREATE NEW LABEL ON DATABASE * TO custom")

    // WHEN & THEN
    executeOnDefault("joe", "soap", "CREATE (n:User) RETURN n") should be(1)

    // WHEN & THEN
    executeOnDefault("joe", "soap", "CALL db.createLabel('A')") should be(0)
  }

  test("Should not allow label creation for normal user without write privilege") {
    setupUserWithCustomRole()
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT CREATE NEW LABEL ON DATABASE * TO custom")

    // WHEN & THEN
    the[AuthorizationViolationException] thrownBy {
      executeOnDefault("joe", "soap", "CALL db.createLabel('A')")
    } should have message "Write operations are not allowed for user 'joe' with roles [custom]."
  }

  test("Should not allow label creation for normal user with explicit deny") {
    setupUserWithCustomRole()
    selectDatabase(SYSTEM_DATABASE_NAME)
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
    setupUserWithCustomRole()
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT WRITE ON GRAPH * TO custom")
    execute("GRANT CREATE NEW TYPE ON DATABASE * TO custom")

    // WHEN & THEN
    executeOnDefault("joe", "soap", "CREATE ()-[n:Rel]->() RETURN n") should be(1)

    // WHEN & THEN
    executeOnDefault("joe", "soap", "CALL db.createRelationshipType('A')") should be(0)
  }

  test("Should not allow type creation for normal user without write privilege") {
    setupUserWithCustomRole()
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT CREATE NEW TYPE ON DATABASE * TO custom")

    // WHEN & THEN
    the[AuthorizationViolationException] thrownBy {
      executeOnDefault("joe", "soap", "CALL db.createRelationshipType('A')")
    } should have message "Write operations are not allowed for user 'joe' with roles [custom]."
  }

  test("Should not allow type creation for normal user with explicit deny") {
    setupUserWithCustomRole()
    selectDatabase(SYSTEM_DATABASE_NAME)
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
    setupUserWithCustomRole()
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT WRITE ON GRAPH * TO custom")
    execute("GRANT CREATE NEW NAME ON DATABASE * TO custom")
    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE (:User)-[:Rel]->()")

    // WHEN & THEN
    executeOnDefault("joe", "soap", "CREATE (n:User {name: 'Alice'}) RETURN n.name", resultHandler = (row, _) => {
      row.get("n.name") should be("Alice")
    }) should be(1)

    // WHEN & THEN
    executeOnDefault("joe", "soap", "CREATE ()-[r:Rel {prop: 'value'}]->() RETURN r.prop", resultHandler = (row, _) => {
      row.get("r.prop") should be("value")
    }) should be(1)

    // WHEN & THEN
    executeOnDefault("joe", "soap", "CALL db.createProperty('age')") should be(0)
  }

  test("Should not allow property key creation for normal user without write privilege") {
    setupUserWithCustomRole()
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT CREATE NEW NAME ON DATABASE * TO custom")

    // WHEN & THEN
    the[AuthorizationViolationException] thrownBy {
      executeOnDefault("joe", "soap", "CALL db.createProperty('age')")
    } should have message "Write operations are not allowed for user 'joe' with roles [custom]."
  }

  test("Should not allow property key creation for normal user with explicit deny") {
    setupUserWithCustomRole()
    selectDatabase(SYSTEM_DATABASE_NAME)
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
    setupUserWithCustomRole()
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT WRITE ON GRAPH * TO custom")
    execute("GRANT CREATE NEW LABEL ON DATABASE * TO custom")

    // WHEN & THEN
    the[AuthorizationViolationException] thrownBy {
      executeOnDefault("joe", "soap", "CREATE (n:User {name: 'Alice'}) RETURN n.name")
    } should have message "'create_propertykey' operations are not allowed for user 'joe' with roles [custom]."
  }

  test("Should not allow property key creation for normal user with only type creation privilege") {
    setupUserWithCustomRole()
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT WRITE ON GRAPH * TO custom")
    execute("GRANT CREATE NEW TYPE ON DATABASE * TO custom")

    // WHEN & THEN
    the[AuthorizationViolationException] thrownBy {
      executeOnDefault("joe", "soap", "CREATE ()-[r:Rel {prop: 'value'}]->() RETURN r.prop")
    } should have message "'create_propertykey' operations are not allowed for user 'joe' with roles [custom]."
  }

  test("Should allow all creation for normal user with name management privilege") {
    setupUserWithCustomRole()
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT WRITE ON GRAPH * TO custom")
    execute("GRANT NAME MANAGEMENT ON DATABASE * TO custom")

    // WHEN & THEN
    executeOnDefault("joe", "soap", "CREATE (n:User {name: 'Alice'})-[:KNOWS {since: 2019}]->(:User {name: 'Bob'}) RETURN n.name", resultHandler = (row, _) => {
      row.get("n.name") should be("Alice")
    }) should be(1)
  }

  test("Should not allow creation for normal user without write privilege") {
    setupUserWithCustomRole()
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT NAME MANAGEMENT ON DATABASE * TO custom")

    // WHEN & THEN
    the[AuthorizationViolationException] thrownBy {
      executeOnDefault("joe", "soap", "CALL db.createLabel('Label')")
    } should have message "Write operations are not allowed for user 'joe' with roles [custom]."

    // WHEN & THEN
    the[AuthorizationViolationException] thrownBy {
      executeOnDefault("joe", "soap", "CALL db.createRelationshipType('RelType')")
    } should have message "Write operations are not allowed for user 'joe' with roles [custom]."

    // WHEN & THEN
    the[AuthorizationViolationException] thrownBy {
      executeOnDefault("joe", "soap", "CALL db.createProperty('prop')")
    } should have message "Write operations are not allowed for user 'joe' with roles [custom]."
  }

  test("Should allow all creation for normal user with all database privileges") {
    setupUserWithCustomRole()
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT WRITE ON GRAPH * TO custom")
    execute("GRANT ALL ON DATABASE * TO custom")

    // WHEN & THEN
    executeOnDefault("joe", "soap", "CREATE (n:User {name: 'Alice'})-[:KNOWS {since: 2019}]->(:User {name: 'Bob'}) RETURN n.name", resultHandler = (row, _) => {
      row.get("n.name") should be("Alice")
    }) should be(1)
  }
}
