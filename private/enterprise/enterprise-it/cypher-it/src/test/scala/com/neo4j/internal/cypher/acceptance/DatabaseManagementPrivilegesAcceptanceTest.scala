/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.internal.cypher.acceptance

import org.neo4j.graphdb.security.AuthorizationViolationException

class DatabaseManagementPrivilegesAcceptanceTest extends AdministrationCommandAcceptanceTestBase {

  // Privilege tests

  test("should not revoke other database management privileges when revoking database management") {
    // GIVEN
    createRoleWithOnlyAdminPrivilege()
    execute("CREATE ROLE custom AS COPY OF adminOnly")
    execute("GRANT CREATE DATABASE ON DBMS TO custom")
    execute("GRANT DROP DATABASE ON DBMS TO custom")
    execute("GRANT DATABASE MANAGEMENT ON DBMS TO custom")

    // WHEN
    execute("REVOKE DATABASE MANAGEMENT ON DBMS FROM custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      granted(adminPrivilege).role("custom").map,
      granted(adminAction("create_database")).role("custom").map,
      granted(adminAction("drop_database")).role("custom").map
    ))
  }

  test("Should revoke sub-privilege even if database management exists") {
    // Given
    execute("CREATE ROLE custom")
    execute("GRANT CREATE DATABASE ON DBMS TO custom")
    execute("GRANT DROP DATABASE ON DBMS TO custom")
    execute("GRANT DATABASE MANAGEMENT ON DBMS TO custom")

    // When
    // Now revoke each sub-privilege in turn
    Seq(
      "CREATE DATABASE",
      "DROP DATABASE"
    ).foreach(privilege => execute(s"REVOKE $privilege ON DBMS FROM custom"))

    // Then
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      granted(adminAction("database_management")).role("custom").map
    ))
  }

  // Enforcement tests

  // CREATE DATABASE

  test("should enforce create database privilege") {
    // GIVEN
    setupUserWithCustomRole("foo", "bar")

    // WHEN
    execute("GRANT CREATE DATABASE ON DBMS TO custom")

    // THEN
    executeOnSystem("foo", "bar", "CREATE DATABASE baz")
    execute("SHOW DATABASE baz").toSet should be(Set(db("baz")))

    // WHEN
    execute("DROP DATABASE baz")
    execute("REVOKE CREATE DATABASE ON DBMS FROM custom")

    // THEN
    the[AuthorizationViolationException] thrownBy {
      executeOnSystem("foo", "bar", "CREATE DATABASE baz")
    } should have message "Permission denied."

    execute("SHOW DATABASE baz").toSet should be(Set.empty)
  }

  test("should fail when creating database when denied database user privilege") {
    // GIVEN
    setupUserWithCustomAdminRole("foo", "bar")

    // WHEN
    execute("DENY CREATE DATABASE ON DBMS TO custom")

    // THEN
    the[AuthorizationViolationException] thrownBy {
      executeOnSystem("foo", "bar", "CREATE DATABASE baz")
    } should have message "Permission denied."

    execute("SHOW DATABASE baz").toSet should be(Set.empty)
  }

  test("should fail when replacing database with denied create database privilege") {
    // GIVEN
    setupUserWithCustomAdminRole("foo", "bar")

    // WHEN
    execute("DENY CREATE DATABASE ON DBMS TO custom")

    // THEN
    the[AuthorizationViolationException] thrownBy {
      executeOnSystem("foo", "bar", "CREATE OR REPLACE DATABASE myDb")
    } should have message "Permission denied."
  }

  test("should fail when replacing database with denied drop database privilege") {
    // GIVEN
    setupUserWithCustomAdminRole("foo", "bar")

    // WHEN
    execute("DENY DROP DATABASE ON DBMS TO custom")

    // THEN
    the[AuthorizationViolationException] thrownBy {
      executeOnSystem("foo", "bar", "CREATE OR REPLACE DATABASE myDb")
    } should have message "Permission denied."
  }

  test("should fail when replacing database without create database privilege") {
    // GIVEN
    setupUserWithCustomRole("foo", "bar")

    // WHEN
    execute("GRANT DROP DATABASE ON DBMS TO custom")

    // THEN
    the[AuthorizationViolationException] thrownBy {
      executeOnSystem("foo", "bar", "CREATE OR REPLACE DATABASE myDb")
    } should have message "Permission denied."
  }

  test("should fail when replacing database without drop database privilege") {
    // GIVEN
    setupUserWithCustomRole("foo", "bar")

    // WHEN
    execute("GRANT CREATE DATABASE ON DBMS TO custom")

    // THEN
    the[AuthorizationViolationException] thrownBy {
      executeOnSystem("foo", "bar", "CREATE OR REPLACE DATABASE myDb")
    } should have message "Permission denied."
  }

  // DROP DATABASE

  test("should enforce drop database privilege") {
    // GIVEN
    setupUserWithCustomRole("foo", "bar")

    // WHEN
    execute("CREATE DATABASE baz")
    execute("GRANT DROP DATABASE ON DBMS TO custom")

    // THEN
    executeOnSystem("foo", "bar", "DROP DATABASE baz")
    execute("SHOW DATABASE baz").toSet should be(Set.empty)

    // WHEN
    execute("CREATE DATABASE baz")
    execute("REVOKE DROP DATABASE ON DBMS FROM custom")

    // THEN
    the[AuthorizationViolationException] thrownBy {
      executeOnSystem("foo", "bar", "DROP DATABASE baz")
    } should have message "Permission denied."

    execute("SHOW DATABASE baz").toSet should be(Set(db("baz")))
  }

  test("should fail when dropping database when denied drop database privilege") {
    // GIVEN
    setupUserWithCustomAdminRole("foo", "bar")

    // WHEN
    execute("CREATE DATABASE baz")
    execute("DENY DROP DATABASE ON DBMS TO custom")

    // THEN
    the[AuthorizationViolationException] thrownBy {
      executeOnSystem("foo", "bar", "DROP DATABASE baz")
    } should have message "Permission denied."

    execute("SHOW DATABASE baz").toSet should be(Set(db("baz")))
  }

  // DATABASE MANAGEMENT

  test("should enforce database management privilege") {
    // GIVEN
    setupUserWithCustomRole("foo", "bar")

    // WHEN
    execute("GRANT DATABASE MANAGEMENT ON DBMS TO custom")

    // THEN
    executeOnSystem("foo", "bar", "CREATE DATABASE baz")
    executeOnSystem("foo", "bar", "DROP DATABASE baz")

    // WHEN
    execute("REVOKE DATABASE MANAGEMENT ON DBMS FROM custom")
    execute("CREATE DATABASE baz")

    // THEN
    the[AuthorizationViolationException] thrownBy {
      executeOnSystem("foo", "bar", "CREATE DATABASE userDb")
    } should have message "Permission denied."
    the[AuthorizationViolationException] thrownBy {
      executeOnSystem("foo", "bar", "DROP DATABASE baz")
    } should have message "Permission denied."
  }

  test("should fail database management when denied database management privilege") {
    // GIVEN
    setupUserWithCustomAdminRole("foo", "bar")

    // WHEN
    execute("GRANT CREATE DATABASE ON DBMS TO custom")
    execute("GRANT DROP DATABASE ON DBMS TO custom")
    execute("DENY DATABASE MANAGEMENT ON DBMS TO custom")

    execute("CREATE DATABASE baz")

    // THEN
    the[AuthorizationViolationException] thrownBy {
      executeOnSystem("foo", "bar", "CREATE DATABASE userDb")
    } should have message "Permission denied."
    the[AuthorizationViolationException] thrownBy {
      executeOnSystem("foo", "bar", "DROP DATABASE baz")
    } should have message "Permission denied."
  }
}
