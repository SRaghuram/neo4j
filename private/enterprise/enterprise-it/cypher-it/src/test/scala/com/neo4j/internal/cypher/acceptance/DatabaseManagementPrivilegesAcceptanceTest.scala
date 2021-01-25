/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.internal.cypher.acceptance

import org.neo4j.graphdb.security.AuthorizationViolationException

class DatabaseManagementPrivilegesAcceptanceTest extends AdministrationCommandAcceptanceTestBase {

  // Privilege tests

  test("should not revoke other database management privileges when revoking database management") {
    // GIVEN
    execute(s"CREATE ROLE $roleName")
    execute(s"GRANT CREATE DATABASE ON DBMS TO $roleName")
    execute(s"GRANT DROP DATABASE ON DBMS TO $roleName")
    execute(s"GRANT DATABASE MANAGEMENT ON DBMS TO $roleName")

    // WHEN
    execute(s"REVOKE DATABASE MANAGEMENT ON DBMS FROM $roleName")

    // THEN
    execute(s"SHOW ROLE $roleName PRIVILEGES").toSet should be(Set(
      granted(adminAction("create_database")).role(roleName).map,
      granted(adminAction("drop_database")).role(roleName).map
    ))
  }

  test("Should revoke sub-privilege even if database management exists") {
    // Given
    execute(s"CREATE ROLE $roleName")
    execute(s"GRANT CREATE DATABASE ON DBMS TO $roleName")
    execute(s"GRANT DROP DATABASE ON DBMS TO $roleName")
    execute(s"GRANT DATABASE MANAGEMENT ON DBMS TO $roleName")

    // When
    // Now revoke each sub-privilege in turn
    Seq(
      "CREATE DATABASE",
      "DROP DATABASE"
    ).foreach(privilege => execute(s"REVOKE $privilege ON DBMS FROM $roleName"))

    // Then
    execute(s"SHOW ROLE $roleName PRIVILEGES").toSet should be(Set(
      granted(adminAction("database_management")).role(roleName).map
    ))
  }

  // Enforcement tests

  // CREATE DATABASE

  test("should enforce create database privilege") {
    // GIVEN
    setupUserWithCustomRole()

    // WHEN
    execute(s"GRANT CREATE DATABASE ON DBMS TO $roleName")

    // THEN
    executeOnSystem(username, password, s"CREATE DATABASE $databaseString")
    execute(s"SHOW DATABASE $databaseString").toSet should be(Set(db(databaseString)))

    // WHEN
    execute(s"DROP DATABASE $databaseString")
    execute(s"REVOKE CREATE DATABASE ON DBMS FROM $roleName")

    // THEN
    the[AuthorizationViolationException] thrownBy {
      executeOnSystem(username, password, s"CREATE DATABASE $databaseString")
    } should have message PERMISSION_DENIED_CREATE_DATABASE

    execute(s"SHOW DATABASE $databaseString").toSet should be(Set.empty)
  }

  test("should fail when creating database when denied create database privilege") {
    // GIVEN
    setupUserWithCustomAdminRole()

    // WHEN
    execute(s"DENY CREATE DATABASE ON DBMS TO $roleName")

    // THEN
    the[AuthorizationViolationException] thrownBy {
      executeOnSystem(username, password, s"CREATE DATABASE $databaseString")
    } should have message PERMISSION_DENIED_CREATE_DATABASE

    execute(s"SHOW DATABASE $databaseString").toSet should be(Set.empty)
  }

  test("should fail when replacing database with denied create database privilege") {
    // GIVEN
    setupUserWithCustomAdminRole()

    // WHEN
    execute(s"DENY CREATE DATABASE ON DBMS TO $roleName")

    // THEN
    the[AuthorizationViolationException] thrownBy {
      executeOnSystem(username, password, s"CREATE OR REPLACE DATABASE $databaseString")
    } should have message PERMISSION_DENIED_CREATE_OR_DROP_DATABASE
  }

  test("should fail when replacing database with denied drop database privilege") {
    // GIVEN
    setupUserWithCustomAdminRole()

    // WHEN
    execute(s"DENY DROP DATABASE ON DBMS TO $roleName")

    // THEN
    the[AuthorizationViolationException] thrownBy {
      executeOnSystem(username, password, s"CREATE OR REPLACE DATABASE $databaseString")
    } should have message PERMISSION_DENIED_CREATE_OR_DROP_DATABASE
  }

  test("should fail when replacing database without create database privilege") {
    // GIVEN
    setupUserWithCustomRole()

    // WHEN
    execute(s"GRANT DROP DATABASE ON DBMS TO $roleName")

    // THEN
    the[AuthorizationViolationException] thrownBy {
      executeOnSystem(username, password, s"CREATE OR REPLACE DATABASE $databaseString")
    } should have message PERMISSION_DENIED_CREATE_OR_DROP_DATABASE
  }

  test("should fail when replacing database without drop database privilege") {
    // GIVEN
    setupUserWithCustomRole()

    // WHEN
    execute(s"GRANT CREATE DATABASE ON DBMS TO $roleName")

    // THEN
    the[AuthorizationViolationException] thrownBy {
      executeOnSystem(username, password, s"CREATE OR REPLACE DATABASE $databaseString")
    } should have message PERMISSION_DENIED_CREATE_OR_DROP_DATABASE
  }

  // DROP DATABASE

  test("should enforce drop database privilege") {
    // GIVEN
    setupUserWithCustomRole()

    // WHEN
    execute(s"CREATE DATABASE $databaseString")
    execute(s"GRANT DROP DATABASE ON DBMS TO $roleName")

    // THEN
    executeOnSystem(username, password, s"DROP DATABASE $databaseString")
    execute(s"SHOW DATABASE $databaseString").toSet should be(Set.empty)

    // WHEN
    execute(s"CREATE DATABASE $databaseString")
    execute(s"REVOKE DROP DATABASE ON DBMS FROM $roleName")

    // THEN
    the[AuthorizationViolationException] thrownBy {
      executeOnSystem(username, password, s"DROP DATABASE $databaseString")
    } should have message PERMISSION_DENIED_DROP_DATABASE

    execute(s"SHOW DATABASE $databaseString").toSet should be(Set(db(databaseString)))
  }

  test("should fail when dropping database when denied drop database privilege") {
    // GIVEN
    setupUserWithCustomAdminRole()

    // WHEN
    execute(s"CREATE DATABASE $databaseString")
    execute(s"DENY DROP DATABASE ON DBMS TO $roleName")

    // THEN
    the[AuthorizationViolationException] thrownBy {
      executeOnSystem(username, password, s"DROP DATABASE $databaseString")
    } should have message PERMISSION_DENIED_DROP_DATABASE

    execute(s"SHOW DATABASE $databaseString").toSet should be(Set(db(databaseString)))
  }

  // DATABASE MANAGEMENT

  test("should enforce database management privilege") {
    // GIVEN
    setupUserWithCustomRole()

    // WHEN
    execute(s"GRANT DATABASE MANAGEMENT ON DBMS TO $roleName")

    // THEN
    executeOnSystem(username, password, s"CREATE DATABASE $databaseString")
    executeOnSystem(username, password, s"DROP DATABASE $databaseString")

    // WHEN
    execute(s"REVOKE DATABASE MANAGEMENT ON DBMS FROM $roleName")
    execute(s"CREATE DATABASE $databaseString2")

    // THEN
    the[AuthorizationViolationException] thrownBy {
      executeOnSystem(username, password, s"CREATE DATABASE $databaseString")
    } should have message PERMISSION_DENIED_CREATE_DATABASE
    the[AuthorizationViolationException] thrownBy {
      executeOnSystem(username, password, s"DROP DATABASE $databaseString2")
    } should have message PERMISSION_DENIED_DROP_DATABASE
  }

  test("should fail database management when denied database management privilege") {
    // GIVEN
    setupUserWithCustomAdminRole()

    // WHEN
    execute(s"GRANT CREATE DATABASE ON DBMS TO $roleName")
    execute(s"GRANT DROP DATABASE ON DBMS TO $roleName")
    execute(s"DENY DATABASE MANAGEMENT ON DBMS TO $roleName")

    execute(s"CREATE DATABASE $databaseString2")

    // THEN
    the[AuthorizationViolationException] thrownBy {
      executeOnSystem(username, password, s"CREATE DATABASE $databaseString")
    } should have message PERMISSION_DENIED_CREATE_DATABASE
    the[AuthorizationViolationException] thrownBy {
      executeOnSystem(username, password, s"DROP DATABASE $databaseString2")
    } should have message PERMISSION_DENIED_DROP_DATABASE
  }
}
