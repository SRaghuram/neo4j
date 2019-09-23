/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.internal.cypher.acceptance

import org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME
import org.neo4j.graphdb.security.AuthorizationViolationException

class SchemaPrivilegeAcceptanceTest extends AdministrationCommandAcceptanceTestBase {

  test("should return empty counts to the outside for commands that update the system graph internally") {
    //TODO: ADD ANY NEW UPDATING COMMANDS HERE

    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    execute("CREATE DATABASE foo")
    execute("CREATE DATABASE bar")

    // Notice: They are executed in succession so they have to make sense in that order
    assertQueriesAndSubQueryCounts(List(
      "GRANT CREATE INDEX ON DATABASE * TO custom" -> 1,
      "GRANT DROP INDEX ON DATABASE * TO custom" -> 1,
      "GRANT CREATE CONSTRAINT ON DATABASE * TO custom" -> 1,
      "GRANT DROP CONSTRAINT ON DATABASE * TO custom" -> 1,
      "GRANT INDEX MANAGEMENT ON DATABASES foo TO custom" -> 2,
      "GRANT CONSTRAINT MANAGEMENT ON DATABASES bar TO custom" -> 2
    ))
  }

  test("should list create and drop index privileges") {
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

  test("Should not allow index creation for normal user without token create privilege") {
    setupUserWithCustomRole()
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT CREATE INDEX ON DATABASE * TO custom")

    // WHEN & THEN
    the[AuthorizationViolationException] thrownBy {
      executeOnDefault("joe", "soap", "CREATE INDEX ON :User(name)")
    } should have message "'create_label' operations are not allowed for user 'joe' with roles [custom]."
  }

  test("Should not allow index creation for normal user without index create privilege") {
    setupUserWithCustomRole()
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT NAME MANAGEMENT ON DATABASE * TO custom")

    // WHEN & THEN
    the[AuthorizationViolationException] thrownBy {
      executeOnDefault("joe", "soap", "CREATE INDEX ON :User(name)")
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
      executeOnDefault("joe", "soap", "CREATE INDEX ON :User(name)")
    } should have message "Schema operation 'create_index' is not allowed for user 'joe' with roles [custom]."
  }

  test("Should not allow index drop for normal user with only index create privilege") {
    // Given
    setupUserWithCustomRole()
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT NAME MANAGEMENT ON DATABASE * TO custom")
    execute("GRANT INDEX MANAGEMENT ON DATABASE * TO custom")
    executeOnDefault("joe", "soap", "CREATE INDEX ON :User(name)") should be(0)

    // When
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("REVOKE GRANT DROP INDEX ON DATABASE * FROM custom")

    // WHEN & THEN
    the[AuthorizationViolationException] thrownBy {
      executeOnDefault("joe", "soap", "DROP INDEX ON :User(name)")
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
    executeOnDefault("joe", "soap", "CREATE INDEX ON :User(name)") should be(0)
  }
}
