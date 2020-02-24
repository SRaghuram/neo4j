/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.internal.cypher.acceptance

import java.lang.Boolean.TRUE

import org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME
import org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME
import org.neo4j.configuration.GraphDatabaseSettings.default_database
import org.neo4j.configuration.Config
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.dbms.api.DatabaseNotFoundException
import org.neo4j.graphdb.QueryExecutionException
import org.neo4j.graphdb.config.Setting
import org.neo4j.graphdb.security.AuthorizationViolationException
import org.neo4j.internal.kernel.api.security.PrivilegeAction
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException

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

      "GRANT INDEX MANAGEMENT ON DATABASES * TO custom" -> 1,
      "REVOKE GRANT INDEX MANAGEMENT ON DATABASES * FROM custom" -> 1,
      "DENY INDEX MANAGEMENT ON DATABASES * TO custom" -> 1,
      "REVOKE DENY INDEX MANAGEMENT ON DATABASES * FROM custom" -> 1,
      "GRANT INDEX MANAGEMENT ON DATABASES * TO custom" -> 1,
      "DENY INDEX MANAGEMENT ON DATABASES * TO custom" -> 1,
      "REVOKE INDEX MANAGEMENT ON DATABASES * FROM custom" -> 2,


      "GRANT CREATE CONSTRAINT ON DATABASE * TO custom" -> 1,
      "DENY CREATE CONSTRAINT ON DATABASE * TO custom" -> 1,
      "REVOKE CREATE CONSTRAINT ON DATABASE * FROM custom" -> 2,

      "GRANT DROP CONSTRAINT ON DATABASE * TO custom" -> 1,
      "REVOKE GRANT DROP CONSTRAINT ON DATABASE * FROM custom" -> 1,
      "DENY DROP CONSTRAINT ON DATABASE * TO custom" -> 1,
      "REVOKE DENY DROP CONSTRAINT ON DATABASE * FROM custom" -> 1,

      "GRANT CONSTRAINT MANAGEMENT ON DATABASES * TO custom" -> 1,
      "REVOKE CONSTRAINT MANAGEMENT ON DATABASES * FROM custom" -> 1,
      "DENY CONSTRAINT MANAGEMENT ON DATABASES * TO custom" -> 1,
      "REVOKE CONSTRAINT MANAGEMENT ON DATABASES * FROM custom" -> 1,
      "GRANT CONSTRAINT MANAGEMENT ON DATABASES * TO custom" -> 1,
      "DENY CONSTRAINT MANAGEMENT ON DATABASES * TO custom" -> 1,
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
      "REVOKE CREATE NEW NAME ON DATABASE * FROM custom" -> 2,

      "GRANT NAME MANAGEMENT ON DATABASES * TO custom" -> 1,
      "REVOKE GRANT NAME MANAGEMENT ON DATABASES * FROM custom" -> 1,
      "DENY NAME MANAGEMENT ON DATABASES * TO custom" -> 1,
      "REVOKE DENY NAME MANAGEMENT ON DATABASES * FROM custom" -> 1,
      "GRANT NAME MANAGEMENT ON DATABASES * TO custom" -> 1,
      "DENY NAME MANAGEMENT ON DATABASES * TO custom" -> 1,
      "REVOKE NAME MANAGEMENT ON DATABASES * FROM custom" -> 2,


      "GRANT ALL DATABASE PRIVILEGES ON DATABASES foo TO custom" -> 1,
      "DENY ALL DATABASE PRIVILEGES ON DATABASES foo TO custom" -> 1,
      "REVOKE ALL DATABASE PRIVILEGES ON DATABASES foo FROM custom" -> 2
    ))
  }

  // Tests for granting, denying and revoking schema privileges

  test("should grant create index privilege") {
    // GIVEN
    setup()
    execute("CREATE DATABASE foo")
    execute("CREATE ROLE role")

    // WHEN
    execute("GRANT CREATE INDEX ON DATABASE foo TO role")
    execute("GRANT CREATE INDEX ON DATABASE * TO role")

    // THEN
    execute("SHOW ROLE role PRIVILEGES").toSet should be(Set(
      createIndex().database("foo").role("role").map,
      createIndex().role("role").map
    ))
  }

  test("should deny create index privilege") {
    // GIVEN
    setup()
    execute("CREATE DATABASE foo")
    execute("CREATE ROLE role")

    // WHEN
    execute("DENY CREATE INDEX ON DATABASE foo TO role")
    execute("DENY CREATE INDEX ON DEFAULT DATABASE TO role")

    // THEN
    execute("SHOW ROLE role PRIVILEGES").toSet should be(Set(
      createIndex(DENIED).database("foo").role("role").map,
      createIndex(DENIED).database(DEFAULT).role("role").map
    ))
  }

  test("should revoke create index privilege") {
    // GIVEN
    setup()
    execute("CREATE ROLE role")
    execute("GRANT CREATE INDEX ON DATABASE * TO role")
    execute("DENY CREATE INDEX ON DATABASE * TO role")

    // WHEN
    execute("REVOKE CREATE INDEX ON DATABASE * FROM role")

    // THEN
    execute("SHOW ROLE role PRIVILEGES").toSet should be(Set.empty)
  }

  test("should fail to grant create index to non-existing role") {
    // GIVEN
    setup()

    the[InvalidArgumentsException] thrownBy {
      // WHEN
      execute("GRANT CREATE INDEX ON DATABASE * TO role")
      // THEN
    } should have message "Failed to grant create_index privilege to role 'role': Role 'role' does not exist."
  }

  test("should fail to grant create index with missing database") {
    // GIVEN
    setup()
    execute("CREATE ROLE role")

    the[DatabaseNotFoundException] thrownBy {
      // WHEN
      execute("GRANT CREATE INDEX ON DATABASE foo TO role")
      // THEN
    } should have message "Failed to grant create_index privilege to role 'role': Database 'foo' does not exist."
  }

  test("should grant drop index privilege") {
    // GIVEN
    setup()
    execute("CREATE DATABASE foo")
    execute("CREATE ROLE role")

    // WHEN
    execute("GRANT DROP INDEX ON DATABASE foo TO role")
    execute("GRANT DROP INDEX ON DATABASE * TO role")

    // THEN
    execute("SHOW ROLE role PRIVILEGES").toSet should be(Set(
      dropIndex().database("foo").role("role").map,
      dropIndex().role("role").map
    ))
  }

  test("should deny drop index privilege") {
    // GIVEN
    setup()
    execute("CREATE DATABASE foo")
    execute("CREATE ROLE role")

    // WHEN
    execute("DENY DROP INDEX ON DATABASE foo TO role")
    execute("DENY DROP INDEX ON DEFAULT DATABASE TO role")

    // THEN
    execute("SHOW ROLE role PRIVILEGES").toSet should be(Set(
      dropIndex(DENIED).database("foo").role("role").map,
      dropIndex(DENIED).database(DEFAULT).role("role").map
    ))
  }

  test("should revoke drop index privilege") {
    // GIVEN
    setup()
    execute("CREATE DATABASE foo")
    execute("CREATE ROLE role")
    execute("GRANT DROP INDEX ON DATABASE foo TO role")
    execute("DENY DROP INDEX ON DATABASE foo TO role")

    // WHEN
    execute("REVOKE DROP INDEX ON DATABASE foo FROM role")

    // THEN
    execute("SHOW ROLE role PRIVILEGES").toSet should be(Set.empty)
  }

  test("should fail to deny drop index to non-existing role") {
    // GIVEN
    setup()

    the[InvalidArgumentsException] thrownBy {
      // WHEN
      execute("DENY DROP INDEX ON DATABASE * TO role")
      // THEN
    } should have message "Failed to deny drop_index privilege to role 'role': Role 'role' does not exist."
  }

  test("should fail to deny drop index with missing database") {
    // GIVEN
    setup()
    execute("CREATE ROLE role")

    the[DatabaseNotFoundException] thrownBy {
      // WHEN
      execute("DENY DROP INDEX ON DATABASE foo TO role")
      // THEN
    } should have message "Failed to deny drop_index privilege to role 'role': Database 'foo' does not exist."
  }

  test("should grant index management privilege") {
    // GIVEN
    setup()
    execute("CREATE DATABASE foo")
    execute("CREATE ROLE role")

    // WHEN
    execute("GRANT INDEX ON DATABASE foo TO role")
    execute("GRANT INDEX ON DATABASE * TO role")

    // THEN
    execute("SHOW ROLE role PRIVILEGES").toSet should be(Set(
      indexManagement().database("foo").role("role").map,
      indexManagement().role("role").map
    ))
  }

  test("should deny index management privilege") {
    // GIVEN
    setup()
    execute("CREATE DATABASE foo")
    execute("CREATE ROLE role")

    // WHEN
    execute("DENY INDEX ON DATABASE foo TO role")
    execute("DENY INDEX ON DEFAULT DATABASE TO role")

    // THEN
    execute("SHOW ROLE role PRIVILEGES").toSet should be(Set(
      indexManagement(DENIED).database("foo").role("role").map,
      indexManagement(DENIED).database(DEFAULT).role("role").map
    ))
  }

  test("should revoke index management privilege") {
    // GIVEN
    setup()
    execute("CREATE ROLE role")
    execute("GRANT CREATE INDEX ON DEFAULT DATABASE TO role")
    execute("DENY DROP INDEX ON DEFAULT DATABASE TO role")
    execute("GRANT INDEX ON DEFAULT DATABASE TO role")
    execute("DENY INDEX ON DEFAULT DATABASE TO role")

    // WHEN
    execute("REVOKE INDEX ON DEFAULT DATABASE FROM role")

    // THEN
    execute("SHOW ROLE role PRIVILEGES").toSet should be(Set(
      createIndex().database(DEFAULT).role("role").map,
      dropIndex(DENIED).database(DEFAULT).role("role").map
    ))
  }

  test("should grant index management privilege on custom default database") {
    // GIVEN
    val config = Config.defaults()
    config.set(default_database, "foo")
    setup(config)
    execute("CREATE ROLE role")

    // WHEN
    execute("GRANT INDEX ON DEFAULT DATABASE TO role")

    // THEN
    execute("SHOW ROLE role PRIVILEGES").toSet should be(Set(
      indexManagement().database(DEFAULT).role("role").map
    ))

    // WHEN
    execute("REVOKE INDEX ON DEFAULT DATABASE FROM role")

    // THEN
    execute("SHOW ROLE role PRIVILEGES").toSet should be(Set.empty)
  }

  test("should do nothing when revoking index management from non-existing role") {
    // GIVEN
    setup()
    execute("CREATE ROLE role")
    execute("GRANT INDEX ON DATABASE * TO role")

    // WHEN
    execute("REVOKE INDEX ON DATABASE * FROM wrongRole")
  }

  test("should do nothing when revoking index management with missing database") {
    // GIVEN
    setup()
    execute("CREATE ROLE role")
    execute("CREATE DATABASE bar")
    execute("GRANT INDEX ON DATABASE bar TO role")

    // WHEN
    execute("REVOKE INDEX ON DATABASE foo FROM role")
  }

  test("should grant create constraint privilege") {
    // GIVEN
    setup()
    execute("CREATE DATABASE foo")
    execute("CREATE ROLE role")

    // WHEN
    execute("GRANT CREATE CONSTRAINT ON DATABASE foo TO role")
    execute("GRANT CREATE CONSTRAINT ON DATABASE * TO role")

    // THEN
    execute("SHOW ROLE role PRIVILEGES").toSet should be(Set(
      createConstraint().database("foo").role("role").map,
      createConstraint().role("role").map
    ))
  }

  test("should deny create constraint privilege") {
    // GIVEN
    setup()
    execute("CREATE DATABASE foo")
    execute("CREATE ROLE role")

    // WHEN
    execute("DENY CREATE CONSTRAINT ON DATABASE foo TO role")
    execute("DENY CREATE CONSTRAINT ON DEFAULT DATABASE TO role")

    // THEN
    execute("SHOW ROLE role PRIVILEGES").toSet should be(Set(
      createConstraint(DENIED).database("foo").role("role").map,
      createConstraint(DENIED).database(DEFAULT).role("role").map
    ))
  }

  test("should revoke create constraint privilege") {
    // GIVEN
    setup()
    execute("CREATE ROLE role")
    execute("GRANT CREATE CONSTRAINT ON DATABASE * TO role")
    execute("DENY CREATE CONSTRAINT ON DATABASE * TO role")

    // WHEN
    execute("REVOKE CREATE CONSTRAINT ON DATABASE * FROM role")

    // THEN
    execute("SHOW ROLE role PRIVILEGES").toSet should be(Set.empty)
  }

  test("should fail to grant create constraint to non-existing role") {
    // GIVEN
    setup()

    the[InvalidArgumentsException] thrownBy {
      // WHEN
      execute("GRANT CREATE CONSTRAINT ON DATABASE * TO role")
      // THEN
    } should have message "Failed to grant create_constraint privilege to role 'role': Role 'role' does not exist."
  }

  test("should fail to grant create constraint with missing database") {
    // GIVEN
    setup()
    execute("CREATE ROLE role")

    the[DatabaseNotFoundException] thrownBy {
      // WHEN
      execute("GRANT CREATE CONSTRAINT ON DATABASE foo TO role")
      // THEN
    } should have message "Failed to grant create_constraint privilege to role 'role': Database 'foo' does not exist."
  }

  test("should grant drop constraint privilege") {
    // GIVEN
    setup()
    execute("CREATE DATABASE foo")
    execute("CREATE ROLE role")

    // WHEN
    execute("GRANT DROP CONSTRAINT ON DATABASE foo TO role")
    execute("GRANT DROP CONSTRAINT ON DATABASE * TO role")

    // THEN
    execute("SHOW ROLE role PRIVILEGES").toSet should be(Set(
      dropConstraint().database("foo").role("role").map,
      dropConstraint().role("role").map
    ))
  }

  test("should deny drop constraint privilege") {
    // GIVEN
    setup()
    execute("CREATE DATABASE foo")
    execute("CREATE ROLE role")

    // WHEN
    execute("DENY DROP CONSTRAINT ON DATABASE foo TO role")
    execute("DENY DROP CONSTRAINT ON DEFAULT DATABASE TO role")

    // THEN
    execute("SHOW ROLE role PRIVILEGES").toSet should be(Set(
      dropConstraint(DENIED).database("foo").role("role").map,
      dropConstraint(DENIED).database(DEFAULT).role("role").map
    ))
  }

  test("should revoke drop constraint privilege") {
    // GIVEN
    setup()
    execute("CREATE DATABASE foo")
    execute("CREATE ROLE role")
    execute("GRANT DROP CONSTRAINT ON DATABASE foo TO role")
    execute("DENY DROP CONSTRAINT ON DATABASE foo TO role")

    // WHEN
    execute("REVOKE DROP CONSTRAINT ON DATABASE foo FROM role")

    // THEN
    execute("SHOW ROLE role PRIVILEGES").toSet should be(Set.empty)
  }

  test("should fail to deny drop constraint to non-existing role") {
    // GIVEN
    setup()

    the[InvalidArgumentsException] thrownBy {
      // WHEN
      execute("DENY DROP CONSTRAINT ON DATABASE * TO role")
      // THEN
    } should have message "Failed to deny drop_constraint privilege to role 'role': Role 'role' does not exist."
  }

  test("should fail to deny drop constraint with missing database") {
    // GIVEN
    setup()
    execute("CREATE ROLE role")

    the[DatabaseNotFoundException] thrownBy {
      // WHEN
      execute("DENY DROP CONSTRAINT ON DATABASE foo TO role")
      // THEN
    } should have message "Failed to deny drop_constraint privilege to role 'role': Database 'foo' does not exist."
  }

  test("should grant constraint management privilege") {
    // GIVEN
    setup()
    execute("CREATE DATABASE foo")
    execute("CREATE ROLE role")

    // WHEN
    execute("GRANT CONSTRAINT ON DATABASE foo TO role")
    execute("GRANT CONSTRAINT ON DATABASE * TO role")

    // THEN
    execute("SHOW ROLE role PRIVILEGES").toSet should be(Set(
      constraintManagement().database("foo").role("role").map,
      constraintManagement().role("role").map
    ))
  }

  test("should deny constraint management privilege") {
    // GIVEN
    setup()
    execute("CREATE DATABASE foo")
    execute("CREATE ROLE role")

    // WHEN
    execute("DENY CONSTRAINT ON DATABASE foo TO role")
    execute("DENY CONSTRAINT ON DEFAULT DATABASE TO role")

    // THEN
    execute("SHOW ROLE role PRIVILEGES").toSet should be(Set(
      constraintManagement(DENIED).database("foo").role("role").map,
      constraintManagement(DENIED).database(DEFAULT).role("role").map
    ))
  }

  test("should revoke constraint management privilege") {
    // GIVEN
    setup()
    execute("CREATE ROLE role")
    execute("GRANT CREATE CONSTRAINT ON DEFAULT DATABASE TO role")
    execute("DENY DROP CONSTRAINT ON DEFAULT DATABASE TO role")
    execute("GRANT CONSTRAINT ON DEFAULT DATABASE TO role")
    execute("DENY CONSTRAINT ON DEFAULT DATABASE TO role")

    // WHEN
    execute("REVOKE CONSTRAINT ON DEFAULT DATABASE FROM role")

    // THEN
    execute("SHOW ROLE role PRIVILEGES").toSet should be(Set(
      createConstraint().database(DEFAULT).role("role").map,
      dropConstraint(DENIED).database(DEFAULT).role("role").map
    ))
  }

  test("should grant constraint management privilege on custom default database") {
    // GIVEN
    val config = Config.defaults()
    config.set(default_database, "foo")
    setup(config)
    execute("CREATE ROLE role")

    // WHEN
    execute("GRANT CONSTRAINT ON DEFAULT DATABASE TO role")

    // THEN
    execute("SHOW ROLE role PRIVILEGES").toSet should be(Set(
      constraintManagement().database(DEFAULT).role("role").map
    ))

    // WHEN
    execute("REVOKE CONSTRAINT ON DEFAULT DATABASE FROM role")

    // THEN
    execute("SHOW ROLE role PRIVILEGES").toSet should be(Set.empty)
  }

  test("should do nothing when revoking constraint management from non-existing role") {
    // GIVEN
    setup()
    execute("CREATE ROLE role")
    execute("GRANT CONSTRAINT ON DATABASE * TO role")

    // WHEN
    execute("REVOKE CONSTRAINT ON DATABASE * FROM wrongRole")
  }

  test("should do nothing when revoking constraint management with missing database") {
    // GIVEN
    setup()
    execute("CREATE ROLE role")
    execute("CREATE DATABASE bar")
    execute("GRANT CONSTRAINT ON DATABASE bar TO role")

    // WHEN
    execute("REVOKE CONSTRAINT ON DATABASE foo FROM role")
  }

  test("should grant create label privilege") {
    // GIVEN
    setup()
    execute("CREATE DATABASE foo")
    execute("CREATE ROLE role")

    // WHEN
    execute("GRANT CREATE NEW LABEL ON DATABASE foo TO role")
    execute("GRANT CREATE NEW LABEL ON DATABASE * TO role")

    // THEN
    execute("SHOW ROLE role PRIVILEGES").toSet should be(Set(
      createNodeLabel().database("foo").role("role").map,
      createNodeLabel().role("role").map
    ))
  }

  test("should deny create label privilege") {
    // GIVEN
    setup()
    execute("CREATE DATABASE foo")
    execute("CREATE ROLE role")

    // WHEN
    execute("DENY CREATE NEW NODE LABEL ON DATABASE foo TO role")
    execute("DENY CREATE NEW NODE LABEL ON DEFAULT DATABASE TO role")

    // THEN
    execute("SHOW ROLE role PRIVILEGES").toSet should be(Set(
      createNodeLabel(DENIED).database("foo").role("role").map,
      createNodeLabel(DENIED).database(DEFAULT).role("role").map
    ))
  }

  test("should revoke create label privilege") {
    // GIVEN
    setup()
    execute("CREATE ROLE role")
    execute("GRANT CREATE NEW NODE LABEL ON DATABASE * TO role")
    execute("DENY CREATE NEW LABEL ON DATABASE * TO role")

    // WHEN
    execute("REVOKE CREATE NEW LABEL ON DATABASE * FROM role")

    // THEN
    execute("SHOW ROLE role PRIVILEGES").toSet should be(Set.empty)
  }

  test("should fail to grant create label to non-existing role") {
    // GIVEN
    setup()

    the[InvalidArgumentsException] thrownBy {
      // WHEN
      execute("GRANT CREATE NEW LABEL ON DATABASE * TO role")
      // THEN
    } should have message "Failed to grant create_label privilege to role 'role': Role 'role' does not exist."
  }

  test("should fail to grant create label with missing database") {
    // GIVEN
    setup()
    execute("CREATE ROLE role")

    the[DatabaseNotFoundException] thrownBy {
      // WHEN
      execute("GRANT CREATE NEW LABEL ON DATABASE foo TO role")
      // THEN
    } should have message "Failed to grant create_label privilege to role 'role': Database 'foo' does not exist."
  }

  test("should grant create type privilege") {
    // GIVEN
    setup()
    execute("CREATE DATABASE foo")
    execute("CREATE ROLE role")

    // WHEN
    execute("GRANT CREATE NEW TYPE ON DATABASE foo TO role")
    execute("GRANT CREATE NEW TYPE ON DATABASE * TO role")

    // THEN
    execute("SHOW ROLE role PRIVILEGES").toSet should be(Set(
      createRelationshipType().database("foo").role("role").map,
      createRelationshipType().role("role").map
    ))
  }

  test("should deny create type privilege") {
    // GIVEN
    setup()
    execute("CREATE DATABASE foo")
    execute("CREATE ROLE role")

    // WHEN
    execute("DENY CREATE NEW RELATIONSHIP TYPE ON DATABASE foo TO role")
    execute("DENY CREATE NEW RELATIONSHIP TYPE ON DEFAULT DATABASE TO role")

    // THEN
    execute("SHOW ROLE role PRIVILEGES").toSet should be(Set(
      createRelationshipType(DENIED).database("foo").role("role").map,
      createRelationshipType(DENIED).database(DEFAULT).role("role").map
    ))
  }

  test("should revoke create type privilege") {
    // GIVEN
    setup()
    execute("CREATE ROLE role")
    execute("GRANT CREATE NEW RELATIONSHIP TYPE ON DATABASE * TO role")
    execute("DENY CREATE NEW TYPE ON DATABASE * TO role")

    // WHEN
    execute("REVOKE CREATE NEW TYPE ON DATABASE * FROM role")

    // THEN
    execute("SHOW ROLE role PRIVILEGES").toSet should be(Set.empty)
  }

  test("should fail to deny create type to non-existing role") {
    // GIVEN
    setup()

    the[InvalidArgumentsException] thrownBy {
      // WHEN
      execute("DENY CREATE NEW TYPE ON DATABASE * TO role")
      // THEN
    } should have message "Failed to deny create_reltype privilege to role 'role': Role 'role' does not exist."
  }

  test("should fail to deny create type with missing database") {
    // GIVEN
    setup()
    execute("CREATE ROLE role")

    the[DatabaseNotFoundException] thrownBy {
      // WHEN
      execute("DENY CREATE NEW TYPE ON DATABASE foo TO role")
      // THEN
    } should have message "Failed to deny create_reltype privilege to role 'role': Database 'foo' does not exist."
  }

  test("should grant create property key privilege") {
    // GIVEN
    setup()
    execute("CREATE DATABASE foo")
    execute("CREATE ROLE role")

    // WHEN
    execute("GRANT CREATE NEW NAME ON DATABASE foo TO role")
    execute("GRANT CREATE NEW NAME ON DATABASE * TO role")

    // THEN
    execute("SHOW ROLE role PRIVILEGES").toSet should be(Set(
      createPropertyKey().database("foo").role("role").map,
      createPropertyKey().role("role").map
    ))
  }

  test("should deny create property key privilege") {
    // GIVEN
    setup()
    execute("CREATE DATABASE foo")
    execute("CREATE ROLE role")

    // WHEN
    execute("DENY CREATE NEW PROPERTY NAME ON DATABASE foo TO role")
    execute("DENY CREATE NEW PROPERTY NAME ON DEFAULT DATABASE TO role")

    // THEN
    execute("SHOW ROLE role PRIVILEGES").toSet should be(Set(
      createPropertyKey(DENIED).database("foo").role("role").map,
      createPropertyKey(DENIED).database(DEFAULT).role("role").map
    ))
  }

  test("should revoke create property key privilege") {
    // GIVEN
    setup()
    execute("CREATE ROLE role")
    execute("GRANT CREATE NEW PROPERTY NAME ON DATABASE * TO role")
    execute("DENY CREATE NEW NAME ON DATABASE * TO role")

    // WHEN
    execute("REVOKE CREATE NEW PROPERTY NAME ON DATABASE * FROM role")

    // THEN
    execute("SHOW ROLE role PRIVILEGES").toSet should be(Set.empty)
  }

  test("should do nothing when revoking create property key from non-existing role") {
    // GIVEN
    setup()
    execute("CREATE ROLE role")
    execute("GRANT CREATE NEW PROPERTY NAME ON DATABASE * TO role")

    // WHEN
    execute("REVOKE CREATE NEW PROPERTY NAME ON DATABASE * FROM wrongRole")
  }

  test("should do nothing when revoking create property key with missing database") {
    // GIVEN
    setup()
    execute("CREATE ROLE role")
    execute("CREATE DATABASE bar")
    execute("GRANT CREATE NEW PROPERTY NAME ON DATABASE bar TO role")

    // WHEN
    execute("REVOKE CREATE NEW PROPERTY NAME ON DATABASE foo FROM role")
  }

  test("should grant name management privilege") {
    // GIVEN
    setup()
    execute("CREATE DATABASE foo")
    execute("CREATE ROLE role")

    // WHEN
    execute("GRANT NAME ON DATABASE foo TO role")
    execute("GRANT NAME ON DATABASE * TO role")

    // THEN
    execute("SHOW ROLE role PRIVILEGES").toSet should be(Set(
      nameManagement().database("foo").role("role").map,
      nameManagement().role("role").map
    ))
  }

  test("should deny name management privilege") {
    // GIVEN
    setup()
    execute("CREATE DATABASE foo")
    execute("CREATE ROLE role")

    // WHEN
    execute("DENY NAME ON DATABASE foo TO role")
    execute("DENY NAME ON DEFAULT DATABASE TO role")

    // THEN
    execute("SHOW ROLE role PRIVILEGES").toSet should be(Set(
      nameManagement(DENIED).database("foo").role("role").map,
      nameManagement(DENIED).database(DEFAULT).role("role").map
    ))
  }

  test("should revoke name management privilege") {
    // GIVEN
    setup()
    execute("CREATE ROLE role")
    execute("GRANT CREATE NEW LABEL ON DEFAULT DATABASE TO role")
    execute("DENY CREATE NEW TYPE ON DEFAULT DATABASE TO role")
    execute("GRANT CREATE NEW PROPERTY NAME ON DEFAULT DATABASE TO role")
    execute("GRANT NAME ON DEFAULT DATABASE TO role")
    execute("DENY NAME ON DEFAULT DATABASE TO role")

    // WHEN
    execute("REVOKE NAME ON DEFAULT DATABASE FROM role")

    // THEN
    execute("SHOW ROLE role PRIVILEGES").toSet should be(Set(
      createNodeLabel().database(DEFAULT).role("role").map,
      createRelationshipType(DENIED).database(DEFAULT).role("role").map,
      createPropertyKey().database(DEFAULT).role("role").map
    ))
  }

  test("should grant name management privilege on custom default database") {
    // GIVEN
    val config = Config.defaults()
    config.set(default_database, "foo")
    setup(config)
    execute("CREATE ROLE role")

    // WHEN
    execute("GRANT NAME ON DEFAULT DATABASE TO role")

    // THEN
    execute("SHOW ROLE role PRIVILEGES").toSet should be(Set(
      nameManagement().database(DEFAULT).role("role").map
    ))

    // WHEN
    execute("REVOKE NAME ON DEFAULT DATABASE FROM role")

    // THEN
    execute("SHOW ROLE role PRIVILEGES").toSet should be(Set.empty)
  }

  test("should fail to grant name management to non-existing role") {
    // GIVEN
    setup()

    the[InvalidArgumentsException] thrownBy {
      // WHEN
      execute("GRANT NAME MANAGEMENT ON DATABASE * TO role")
      // THEN
    } should have message "Failed to grant token privilege to role 'role': Role 'role' does not exist."
  }

  test("should fail to grant name management with missing database") {
    // GIVEN
    setup()
    execute("CREATE ROLE role")

    the[DatabaseNotFoundException] thrownBy {
      // WHEN
      execute("GRANT NAME MANAGEMENT ON DATABASE foo TO role")
      // THEN
    } should have message "Failed to grant token privilege to role 'role': Database 'foo' does not exist."
  }

  test("should grant all database privilege") {
    // Given
    setup()
    execute("CREATE ROLE custom")
    execute("CREATE DATABASE foo")

    // When
    execute("GRANT ALL DATABASE PRIVILEGES ON DATABASE foo TO custom")
    execute("GRANT ALL DATABASE PRIVILEGES ON DATABASE * TO custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      allDatabasePrivilege().database("foo").role("custom").map,
      allDatabasePrivilege().role("custom").map
    ))
  }

  test("should deny all database privilege") {
    // Given
    setup()
    execute("CREATE ROLE custom")

    // When
    execute("DENY ALL DATABASE PRIVILEGES ON DEFAULT DATABASE TO custom")
    execute("DENY ALL DATABASE PRIVILEGES ON DATABASE * TO custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      allDatabasePrivilege(DENIED).database(DEFAULT).role("custom").map,
      allDatabasePrivilege(DENIED).role("custom").map
    ))
  }

  test("should revoke all database privilege") {
    // Given
    setup()
    execute("CREATE ROLE custom")
    execute("CREATE DATABASE foo")
    execute("GRANT ACCESS ON DATABASE foo TO custom")
    execute("DENY INDEX ON DATABASE foo TO custom")
    execute("GRANT ALL DATABASE PRIVILEGES ON DATABASE foo TO custom")
    execute("DENY ALL DATABASE PRIVILEGES ON DATABASE foo TO custom")

    // When
    execute("REVOKE DENY ALL DATABASE PRIVILEGES ON DATABASE foo FROM custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      access().database("foo").role("custom").map,
      indexManagement(DENIED).database("foo").role("custom").map,
      allDatabasePrivilege().database("foo").role("custom").map
    ))

    // When
    execute("REVOKE ALL DATABASE PRIVILEGES ON DATABASE foo FROM custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      access().database("foo").role("custom").map,
      indexManagement(DENIED).database("foo").role("custom").map
    ))
  }

  test("should fail to deny all database privilege to non-existing role") {
    // GIVEN
    setup()

    the[InvalidArgumentsException] thrownBy {
      // WHEN
      execute("DENY ALL ON DATABASE * TO role")
      // THEN
    } should have message "Failed to deny database_actions privilege to role 'role': Role 'role' does not exist."
  }

  test("should fail to deny all database privilege with missing database") {
    // GIVEN
    setup()
    execute("CREATE ROLE role")

    the[DatabaseNotFoundException] thrownBy {
      // WHEN
      execute("DENY ALL ON DATABASE foo TO role")
      // THEN
    } should have message "Failed to deny database_actions privilege to role 'role': Database 'foo' does not exist."
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
    execute("REVOKE NAME ON DATABASE * FROM custom")
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(grantAdmin().role("custom").map, indexManagement().role("custom").map, constraintManagement().role("custom").map))

    // Now try to revoke each sub-privilege (that we have syntax for) in turn
    //TODO: ADD ANY NEW SUB-PRIVILEGES HERE
    Seq(
      ("CREATE ROLE ON DBMS", "CREATE ROLE"),
      ("DROP ROLE ON DBMS", "DROP ROLE"),
      ("SHOW ROLE ON DBMS", "SHOW ROLE"),
      ("ASSIGN ROLE ON DBMS", "ASSIGN ROLE"),
      ("REMOVE ROLE ON DBMS", "REMOVE ROLE"),
      ("ROLE MANAGEMENT ON DBMS", "ROLE MANAGEMENT"),
      ("CREATE USER ON DBMS", "CREATE USER"),
      ("DROP USER ON DBMS", "DROP USER"),
      ("SHOW USER ON DBMS", "SHOW USER"),
      ("ALTER USER ON DBMS", "ALTER USER"),
      ("USER MANAGEMENT ON DBMS", "USER MANAGEMENT"),
      ("SHOW TRANSACTION (*) ON DATABASES *", "SHOW TRANSACTION"),
      ("TERMINATE TRANSACTION (*) ON DATABASES *", "TERMINATE TRANSACTION"),
      ("TRANSACTION MANAGEMENT ON DATABASES *", "TRANSACTION MANAGEMENT"),
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
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(nameManagement().role("custom").map))

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

  test("Should get error when revoking a subset of a compound index or constraint privilege") {
    // Given
    setup()
    execute("CREATE ROLE custom AS COPY OF architect")
    execute("REVOKE READ {*} ON GRAPH * FROM custom")
    execute("REVOKE TRAVERSE ON GRAPH * FROM custom")
    execute("REVOKE WRITE ON GRAPH * FROM custom")
    execute("REVOKE ACCESS ON DATABASE * FROM custom")
    execute("REVOKE NAME MANAGEMENT ON DATABASE * FROM custom")
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(indexManagement().role("custom").map, constraintManagement().role("custom").map))

    // Now try to revoke each sub-privilege in turn
    Seq(
      "CREATE INDEX",
      "DROP INDEX",
      "CREATE CONSTRAINT",
      "DROP CONSTRAINT"
    ).foreach { privilege =>
      // When && Then
      val exception = the[IllegalStateException] thrownBy {
        execute(s"REVOKE $privilege ON DATABASE * FROM custom")
      }
      exception.getMessage should (be(s"Unsupported to revoke a sub-privilege '$privilege' from a compound privilege 'INDEX MANAGEMENT', consider using DENY instead.") or
        be(s"Unsupported to revoke a sub-privilege '$privilege' from a compound privilege 'CONSTRAINT MANAGEMENT', consider using DENY instead."))
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
    } should have message "'create_label' operations are not allowed for user 'joe' with roles [PUBLIC, custom]."

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
    } should have message "Schema operations are not allowed for user 'joe' with roles [PUBLIC, custom]."
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
    } should have message "Schema operation 'create_index' is not allowed for user 'joe' with roles [PUBLIC, custom]."
  }

  test("Should not allow index drop for normal user with only index create privilege") {
    // Given
    setup()
    setupUserWithCustomRole()
    execute("GRANT NAME MANAGEMENT ON DATABASE * TO custom")
    execute("GRANT CREATE INDEX ON DATABASE * TO custom")
    executeOnDefault("joe", "soap", "CREATE INDEX my_index FOR (u:User) ON (u.name)") should be(0)

    // WHEN & THEN
    the[AuthorizationViolationException] thrownBy {
      executeOnDefault("joe", "soap", "DROP INDEX my_index")
    } should have message "Schema operation 'drop_index' is not allowed for user 'joe' with roles [PUBLIC, custom]."
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
      nameManagement().role("custom").map
    ))

    // WHEN & THEN
    executeOnDefault("joe", "soap", "CREATE INDEX FOR (u:User) ON (u.name)") should be(0)
  }

  test("Should allow index dropping for normal user with index drop privilege") {
    setup()
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
    setup()
    setupUserWithCustomRole()
    execute("GRANT NAME MANAGEMENT ON DATABASE * TO custom")
    execute("GRANT INDEX ON DATABASE * TO custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      access().role("custom").map,
      nameManagement().role("custom").map,
      indexManagement().role("custom").map
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
    } should have message "Schema operation 'create_index' is not allowed for user 'joe' with roles [PUBLIC, custom]."
  }

  test("should have index management privilege on new default after switch of default database") {
    // GIVEN
    val newDefaultDatabase = "foo"
    val config = Config.defaults(GraphDatabaseSettings.auth_enabled, TRUE)
    setup(config, impermanent = false)
    setupUserWithCustomRole("alice", "abc", "role")
    execute("GRANT NAME MANAGEMENT ON DATABASE * TO role")
    execute(s"CREATE database $newDefaultDatabase")

    // Confirm default database
    execute(s"SHOW DEFAULT DATABASE").toSet should be(Set(defaultDb(DEFAULT_DATABASE_NAME)))

    // WHEN: Grant on default database
    execute(s"GRANT INDEX MANAGEMENT ON DEFAULT DATABASE TO role")

    // THEN: Get privilege on default
    execute("SHOW ROLE role PRIVILEGES").toSet should be(Set(
      access().role("role").map,
      nameManagement().role("role").map,
      indexManagement().database(DEFAULT).role("role").map
    ))

    // WHEN: creating index on default
    executeOn(DEFAULT_DATABASE_NAME, "alice", "abc", "CREATE INDEX neo_index FOR (n:Label) ON (n.prop)") should be(0)

    // THEN
    graph.getMaybeIndex("Label", Seq("prop")).isDefined should be(true)

    // WHEN: creating index on foo
    the[AuthorizationViolationException] thrownBy {
      executeOn(newDefaultDatabase, "alice", "abc", "CREATE INDEX foo_index FOR (n:Label) ON (n.prop)")
    } should have message "Schema operations are not allowed for user 'alice' with roles [PUBLIC, role]."

    // THEN
    graph.getMaybeIndex("Label", Seq("prop")).isDefined should be(false)

    // WHEN: switch default database and create index on foo
    config.set(default_database, newDefaultDatabase)
    restart(config)
    selectDatabase(newDefaultDatabase)
    graph.createIndexWithName("foo_index", "Label", "prop")

    // Confirm default database
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute(s"SHOW DEFAULT DATABASE").toSet should be(Set(defaultDb(newDefaultDatabase)))

    // THEN: confirm privilege
    execute("SHOW ROLE role PRIVILEGES").toSet should be(Set(
      access().role("role").map,
      nameManagement().role("role").map,
      indexManagement().database(DEFAULT).role("role").map
    ))

    // WHEN: dropping index on default
    the[AuthorizationViolationException] thrownBy {
      executeOn(DEFAULT_DATABASE_NAME, "alice", "abc", "DROP INDEX neo_index")
    } should have message "Schema operations are not allowed for user 'alice' with roles [PUBLIC, role]."

    // THEN
    graph.getMaybeIndex("Label", Seq("prop")).isEmpty should be(false)

    // WHEN: dropping index on foo
    executeOn(newDefaultDatabase, "alice", "abc", "DROP INDEX foo_index") should be(0)

    // THEN
    graph.getMaybeIndex("Label", Seq("prop")).isEmpty should be(true)
  }

  // Constraint Management
  test("Should not allow constraint creation on non-existing tokens for normal user without token create privilege") {
    setup()
    setupUserWithCustomRole()
    execute("GRANT CREATE CONSTRAINT ON DATABASE * TO custom")

    // WHEN & THEN
    the[AuthorizationViolationException] thrownBy {
      executeOnDefault("joe", "soap", "CREATE CONSTRAINT ON (n:User) ASSERT exists(n.name)")
    } should have message "'create_label' operations are not allowed for user 'joe' with roles [PUBLIC, custom]."

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
    } should have message "Schema operations are not allowed for user 'joe' with roles [PUBLIC, custom]."
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
    } should have message "Schema operation 'create_constraint' is not allowed for user 'joe' with roles [PUBLIC, custom]."
  }

  test("Should not allow constraint drop for normal user with only constraint create privilege") {
    // Given
    setup()
    setupUserWithCustomRole()
    execute("GRANT NAME MANAGEMENT ON DATABASE * TO custom")
    execute("GRANT CREATE CONSTRAINT ON DATABASE * TO custom")
    executeOnDefault("joe", "soap", "CREATE CONSTRAINT my_constraint ON (n:User) ASSERT exists(n.name)") should be(0)

    // WHEN & THEN
    the[AuthorizationViolationException] thrownBy {
      executeOnDefault("joe", "soap", "DROP CONSTRAINT my_constraint")
    } should have message "Schema operation 'drop_constraint' is not allowed for user 'joe' with roles [PUBLIC, custom]."
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
      nameManagement().role("custom").map
    ))

    // WHEN & THEN
    executeOnDefault("joe", "soap", "CREATE CONSTRAINT ON (n:User) ASSERT exists(n.name)") should be(0)
  }

  test("Should allow constraint dropping for normal user with constraint drop privilege") {
    setup()
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
    setup()
    setupUserWithCustomRole()
    execute("GRANT NAME MANAGEMENT ON DATABASE * TO custom")
    execute("GRANT CONSTRAINT ON DATABASE * TO custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      access().role("custom").map,
      nameManagement().role("custom").map,
      constraintManagement().role("custom").map
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
    } should have message "Schema operation 'create_constraint' is not allowed for user 'joe' with roles [PUBLIC, custom]."
  }

  test("should have constraint management privilege on new default after switch of default database") {
    // GIVEN
    val newDefaultDatabase = "foo"
    val config = Config.defaults(GraphDatabaseSettings.auth_enabled, TRUE)
    setup(config, impermanent = false)
    setupUserWithCustomRole("alice", "abc", "role")
    execute("GRANT NAME MANAGEMENT ON DATABASE * TO role")
    execute(s"CREATE database $newDefaultDatabase")

    // Confirm default database
    execute(s"SHOW DEFAULT DATABASE").toSet should be(Set(defaultDb(DEFAULT_DATABASE_NAME)))

    // WHEN: Grant on default database
    execute(s"GRANT CONSTRAINT MANAGEMENT ON DEFAULT DATABASE TO role")

    // THEN: Get privilege on default
    execute("SHOW ROLE role PRIVILEGES").toSet should be(Set(
      access().role("role").map,
      nameManagement().role("role").map,
      constraintManagement().database(DEFAULT).role("role").map
    ))

    // WHEN: creating constraint on default
    executeOn(DEFAULT_DATABASE_NAME, "alice", "abc", "CREATE CONSTRAINT neo_constraint ON (n:Label) ASSERT exists(n.prop)") should be(0)

    // THEN
    graph.getMaybeNodeConstraint("Label", Seq("prop")).isDefined should be(true)

    // WHEN: creating constraint on foo
    the[AuthorizationViolationException] thrownBy {
      executeOn(newDefaultDatabase, "alice", "abc", "CREATE CONSTRAINT foo_constraint ON (n:Label) ASSERT exists(n.prop)")
    } should have message "Schema operations are not allowed for user 'alice' with roles [PUBLIC, role]."

    // THEN
    graph.getMaybeNodeConstraint("Label", Seq("prop")).isDefined should be(false)

    // WHEN: switch default database and create constraint on foo
    config.set(default_database, newDefaultDatabase)
    restart(config)
    selectDatabase(newDefaultDatabase)
    graph.createNodeExistenceConstraintWithName("foo_constraint", "Label", "prop")

    // Confirm default database
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute(s"SHOW DEFAULT DATABASE").toSet should be(Set(defaultDb(newDefaultDatabase)))

    // THEN: confirm privilege
    execute("SHOW ROLE role PRIVILEGES").toSet should be(Set(
      access().role("role").map,
      nameManagement().role("role").map,
      constraintManagement().database(DEFAULT).role("role").map
    ))

    // WHEN: dropping constraint on default
    the[AuthorizationViolationException] thrownBy {
      executeOn(DEFAULT_DATABASE_NAME, "alice", "abc", "DROP CONSTRAINT neo_constraint")
    } should have message "Schema operations are not allowed for user 'alice' with roles [PUBLIC, role]."

    // THEN
    graph.getMaybeNodeConstraint("Label", Seq("prop")).isEmpty should be(false)

    // WHEN: dropping constraint on foo
    executeOn(newDefaultDatabase, "alice", "abc", "DROP CONSTRAINT foo_constraint") should be(0)

    // THEN
    graph.getMaybeNodeConstraint("Label", Seq("prop")).isEmpty should be(true)
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
    } should have message "'create_label' operations are not allowed for user 'joe' with roles [PUBLIC, custom]."

    // WHEN & THEN
    the[QueryExecutionException] thrownBy {
      executeOnDefault("joe", "soap", "CALL db.createLabel('A')")
    } should have message "'create_label' operations are not allowed for user 'joe' with roles [PUBLIC, custom] restricted to TOKEN_WRITE."
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
    } should have message "'create_reltype' operations are not allowed for user 'joe' with roles [PUBLIC, custom]."

    // WHEN & THEN
    the[QueryExecutionException] thrownBy {
      executeOnDefault("joe", "soap", "CALL db.createRelationshipType('A')")
    } should have message "'create_reltype' operations are not allowed for user 'joe' with roles [PUBLIC, custom] restricted to TOKEN_WRITE."
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
    } should have message "'create_propertykey' operations are not allowed for user 'joe' with roles [PUBLIC, custom]."

    // WHEN & THEN
    the[AuthorizationViolationException] thrownBy {
      executeOnDefault("joe", "soap", "CREATE ()-[n:Rel {prop: 'value'}]->() RETURN n")
    } should have message "'create_propertykey' operations are not allowed for user 'joe' with roles [PUBLIC, custom]."

    // WHEN & THEN
    the[QueryExecutionException] thrownBy {
      executeOnDefault("joe", "soap", "CALL db.createProperty('age')")
    } should have message "'create_propertykey' operations are not allowed for user 'joe' with roles [PUBLIC, custom] restricted to TOKEN_WRITE."
  }

  test("Should not allow property key creation for normal user with only label creation privilege") {
    setup()
    setupUserWithCustomRole()
    execute("GRANT WRITE ON GRAPH * TO custom")
    execute("GRANT CREATE NEW LABEL ON DATABASE * TO custom")

    // WHEN & THEN
    the[AuthorizationViolationException] thrownBy {
      executeOnDefault("joe", "soap", "CREATE (n:User {name: 'Alice'}) RETURN n.name")
    } should have message "'create_propertykey' operations are not allowed for user 'joe' with roles [PUBLIC, custom]."
  }

  test("Should not allow property key creation for normal user with only type creation privilege") {
    setup()
    setupUserWithCustomRole()
    execute("GRANT WRITE ON GRAPH * TO custom")
    execute("GRANT CREATE NEW TYPE ON DATABASE * TO custom")

    // WHEN & THEN
    the[AuthorizationViolationException] thrownBy {
      executeOnDefault("joe", "soap", "CREATE ()-[r:Rel {prop: 'value'}]->() RETURN r.prop")
    } should have message "'create_propertykey' operations are not allowed for user 'joe' with roles [PUBLIC, custom]."
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

  test("should have name management privilege on new default after switch of default database") {
    // GIVEN
    val newDefaultDatabase = "foo"
    val config = Config.defaults(GraphDatabaseSettings.auth_enabled, TRUE)
    setup(config, impermanent = false)
    setupUserWithCustomRole("alice", "abc", "role")
    execute("GRANT WRITE ON GRAPH * TO role")
    execute(s"CREATE database $newDefaultDatabase")

    // Confirm default database
    execute(s"SHOW DEFAULT DATABASE").toSet should be(Set(defaultDb(DEFAULT_DATABASE_NAME)))

    // WHEN: Grant on default database
    execute(s"GRANT NAME MANAGEMENT ON DEFAULT DATABASE TO role")

    // THEN: Get privilege on default
    execute("SHOW ROLE role PRIVILEGES").toSet should be(Set(
      access().role("role").map,
      write().node("*").role("role").map,
      write().relationship("*").role("role").map,
      nameManagement().database(DEFAULT).role("role").map
    ))

    // WHEN: creating on default
    executeOn(DEFAULT_DATABASE_NAME, "alice", "abc", "CREATE (n:Label1)-[:Type1]->({prop1: 1}) RETURN n") should be(1)

    // THEN
    execute("MATCH (n:Label1) RETURN n").isEmpty should be(false)

    // WHEN: creating on foo
    val exception1 = the[AuthorizationViolationException] thrownBy {
      executeOn(newDefaultDatabase, "alice", "abc", "CREATE (n:Label1)-[:Type1]->({prop1: 1}) RETURN n")
    }
    exception1.getMessage should (
      be("'create_label' operations are not allowed for user 'alice' with roles [PUBLIC, role].") or (
        be("'create_reltype' operations are not allowed for user 'alice' with roles [PUBLIC, role].") or
          be("'create_propertykey' operations are not allowed for user 'alice' with roles [PUBLIC, role]."))
      )

    // THEN
    execute("MATCH (n:Label1) RETURN n").isEmpty should be(true)

    // WHEN: switch default database
    config.set(default_database, newDefaultDatabase)
    restart(config)

    // Confirm default database
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute(s"SHOW DEFAULT DATABASE").toSet should be(Set(defaultDb(newDefaultDatabase)))

    // THEN: confirm privilege
    execute("SHOW ROLE role PRIVILEGES").toSet should be(Set(
      access().role("role").map,
      write().node("*").role("role").map,
      write().relationship("*").role("role").map,
      nameManagement().database(DEFAULT).role("role").map
    ))

    // WHEN: creating on default
    val exception2 = the[AuthorizationViolationException] thrownBy {
      executeOn(DEFAULT_DATABASE_NAME, "alice", "abc", "CREATE (n:Label2)-[:Type2]->({prop2: 1}) RETURN n")
    }
    exception2.getMessage should (
      be("'create_label' operations are not allowed for user 'alice' with roles [PUBLIC, role].") or (
        be("'create_reltype' operations are not allowed for user 'alice' with roles [PUBLIC, role].") or
          be("'create_propertykey' operations are not allowed for user 'alice' with roles [PUBLIC, role]."))
      )

    // THEN
    execute("MATCH (n:Label2) RETURN n").isEmpty should be(true)

    // WHEN: creating on foo
    executeOn(newDefaultDatabase, "alice", "abc", "CREATE (n:Label2)-[:Type2]->({prop2: 1}) RETURN n") should be(1)

    // THEN
    execute("MATCH (n:Label2) RETURN n").isEmpty should be(false)
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

}
