/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.internal.cypher.acceptance

import org.neo4j.graphdb.security.AuthorizationViolationException

class DbmsPrivilegeAcceptanceTest extends AdministrationCommandAcceptanceTestBase {

  // Privilege tests
  test("should grant dbms privileges") {
    // GIVEN
    execute("CREATE ROLE custom")

    dbmsPrivileges.foreach {
      case (command, action) =>
        withClue(s"$command: \n") {
          // WHEN
          execute(s"GRANT $command ON DBMS TO custom")

          // THEN
          execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
            granted(action).role("custom").map
          ))

          // WHEN
          execute(s"REVOKE GRANT $command ON DBMS FROM custom")

          // THEN
          execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set.empty)
        }
    }
  }

  test("should deny dbms privileges") {
    // GIVEN
    execute("CREATE ROLE custom")

    dbmsPrivileges.foreach {
      case (command, action) =>
        withClue(s"$command: \n") {
          // WHEN
          execute(s"DENY $command ON DBMS TO custom")

          // THEN
          execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
            denied(action).role("custom").map
          ))

          // WHEN
          execute(s"REVOKE DENY $command ON DBMS FROM custom")

          // THEN
          execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set.empty)
        }
    }
  }

  test("should not revoke other dbms privileges when revoking all dbms privileges") {
    // GIVEN
    createRoleWithOnlyAdminPrivilege()
    execute("CREATE ROLE custom AS COPY OF adminOnly")
    allDbmsPrivileges("GRANT", includingCompound = true)

    // WHEN
    execute("REVOKE ALL DBMS PRIVILEGES ON DBMS FROM $role", Map("role" -> "custom"))

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      granted(adminPrivilege).role("custom").map,
      granted(adminAction("create_role")).role("custom").map,
      granted(adminAction("drop_role")).role("custom").map,
      granted(adminAction("assign_role")).role("custom").map,
      granted(adminAction("remove_role")).role("custom").map,
      granted(adminAction("show_role")).role("custom").map,
      granted(adminAction("role_management")).role("custom").map,
      granted(adminAction("create_user")).role("custom").map,
      granted(adminAction("drop_user")).role("custom").map,
      granted(adminAction("set_user_status")).role("custom").map,
      granted(adminAction("set_passwords")).role("custom").map,
      granted(adminAction("alter_user")).role("custom").map,
      granted(adminAction("show_user")).role("custom").map,
      granted(adminAction("user_management")).role("custom").map,
      granted(adminAction("create_database")).role("custom").map,
      granted(adminAction("drop_database")).role("custom").map,
      granted(adminAction("database_management")).role("custom").map,
      granted(adminAction("show_privilege")).role("custom").map,
      granted(adminAction("assign_privilege")).role("custom").map,
      granted(adminAction("remove_privilege")).role("custom").map,
      granted(adminAction("privilege_management")).role("custom").map
    ))
  }

  test("Should revoke sub-privilege even if all dbms privilege exists") {
    // Given
    createRoleWithOnlyAdminPrivilege()
    execute("CREATE ROLE custom AS COPY OF adminOnly")
    allDbmsPrivileges("GRANT", includingCompound = true)

    // When
    // Now revoke each sub-privilege in turn
    allDbmsPrivileges("REVOKE", includingCompound = false)

    // Then
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      granted(adminPrivilege).role("custom").map,
      granted(adminAction("dbms_actions")).role("custom").map
    ))
  }

  test("Should do nothing when revoking a non-existing subset of a compound (mostly dbms) admin privilege") {
    // Given
    setup()
    createRoleWithOnlyAdminPrivilege("custom")

    // When
    // Now try to revoke each sub-privilege (that we have syntax for) in turn
    //TODO: ADD ANY NEW SUB-PRIVILEGES HERE
    Seq(
      "CREATE ROLE ON DBMS",
      "DROP ROLE ON DBMS",
      "SHOW ROLE ON DBMS",
      "ASSIGN ROLE ON DBMS",
      "REMOVE ROLE ON DBMS",
      "ROLE MANAGEMENT ON DBMS",
      "CREATE USER ON DBMS",
      "DROP USER ON DBMS",
      "SHOW USER ON DBMS",
      "ALTER USER ON DBMS",
      "SET PASSWORDS ON DBMS",
      "SET USER STATUS ON DBMS",
      "USER MANAGEMENT ON DBMS",
      "CREATE DATABASE ON DBMS",
      "DROP DATABASE ON DBMS",
      "DATABASE MANAGEMENT ON DBMS",
      "SHOW PRIVILEGE ON DBMS",
      "ASSIGN PRIVILEGE ON DBMS",
      "REMOVE PRIVILEGE ON DBMS",
      "PRIVILEGE MANAGEMENT ON DBMS",
      "ALL DBMS PRIVILEGES ON DBMS",
      "SHOW TRANSACTION (*) ON DATABASES *",
      "TERMINATE TRANSACTION (*) ON DATABASES *",
      "TRANSACTION MANAGEMENT ON DATABASES *",
      "START ON DATABASES *",
      "STOP ON DATABASES *"
    ).foreach(queryPart => execute(s"REVOKE $queryPart FROM custom"))

    // Then
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(granted(adminPrivilege).role("custom").map))
  }

  // Enforcement tests

  test("should enforce all dbms privileges privilege") {
    // GIVEN
    setupUserWithCustomRole("foo", "bar")

    // WHEN
    execute("GRANT ALL DBMS PRIVILEGES ON DBMS TO custom")

    // THEN

    // Should be able to do role management
    executeOnSystem("foo", "bar", "CREATE ROLE otherRole")
    execute("SHOW ROLES").toSet should be(defaultRoles ++ Set(role("custom").map, role("otherRole").map))

    // Should be able to do user management
    executeOnSystem("foo", "bar", "DROP USER neo4j")
    execute("SHOW USERS").toSet should be(Set(user("foo", Seq("custom"), passwordChangeRequired = false)))

    // Should be able to do database management
    executeOnSystem("foo", "bar", "CREATE DATABASE baz")
    execute("SHOW DATABASE baz").toSet should be(Set(db("baz")))

    // Should be able to do privilege management
    executeOnSystem("foo", "bar", "GRANT TRAVERSE ON GRAPH * NODES A TO otherRole")
    execute("SHOW ROLE otherRole PRIVILEGES").toSet should be(Set(granted(traverse).node("A").role("otherRole").database("*").map))

    // WHEN
    execute("REVOKE ALL DBMS PRIVILEGES ON DBMS FROM custom")

    // THEN

    // Should not be able to do role management
    the[AuthorizationViolationException] thrownBy {
      executeOnSystem("foo", "bar", "CREATE ROLE role")
    } should have message "Permission denied."
    execute("SHOW ROLES").toSet should be(defaultRoles ++ Set(role("custom").map, role("otherRole").map))

    // Should not be able to do user management
    the[AuthorizationViolationException] thrownBy {
      executeOnSystem("foo", "bar", "CREATE USER alice SET PASSWORD 'secret'")
    } should have message "Permission denied."
    execute("SHOW USERS").toSet should be(Set(user("foo", Seq("custom"), passwordChangeRequired = false)))

    // Should not be able to do database management
    the[AuthorizationViolationException] thrownBy {
      executeOnSystem("foo", "bar", "DROP DATABASE baz")
    } should have message "Permission denied."
    execute("SHOW DATABASE baz").toSet should be(Set(db("baz")))

    // Should not be able to do privilege management
    the[AuthorizationViolationException] thrownBy {
      executeOnSystem("foo", "bar", "GRANT TRAVERSE ON GRAPH * NODES B TO otherRole")
    } should have message "Permission denied."
    execute("SHOW ROLE otherRole PRIVILEGES").toSet should be(Set(granted(traverse).node("A").role("otherRole").database("*").map))
  }

  test("should fail dbms management when denied all dbms privileges privilege") {
    // GIVEN
    setupUserWithCustomAdminRole("foo", "bar")
    execute("CREATE ROLE otherRole")

    // WHEN
    allDbmsPrivileges("GRANT", includingCompound = false)
    execute("DENY ALL DBMS PRIVILEGES ON DBMS TO custom")

    // THEN

    // Should not be able to do role management
    the[AuthorizationViolationException] thrownBy {
      executeOnSystem("foo", "bar", "CREATE ROLE role")
    } should have message "Permission denied."
    execute("SHOW ROLES").toSet should be(defaultRoles ++ Set(role("custom").map, role("otherRole").map))

    // Should not be able to do user management
    the[AuthorizationViolationException] thrownBy {
      executeOnSystem("foo", "bar", "DROP USER neo4j")
    } should have message "Permission denied."
    execute("SHOW USERS").toSet should be(Set(user("neo4j", Seq("admin")), user("foo", Seq("custom"), passwordChangeRequired = false)))

    // Should not be able to do database management
    the[AuthorizationViolationException] thrownBy {
      executeOnSystem("foo", "bar", "CREATE DATABASE baz")
    } should have message "Permission denied."
    execute("SHOW DATABASE baz").toSet should be(Set.empty)

    // Should not be able to do privilege management
    the[AuthorizationViolationException] thrownBy {
      executeOnSystem("foo", "bar", "GRANT TRAVERSE ON GRAPH * NODES A TO otherRole")
    } should have message "Permission denied."
    execute("SHOW ROLE otherRole PRIVILEGES").toSet should be(Set.empty)
  }

  // helper methods

  private def allDbmsPrivileges(privType: String, includingCompound: Boolean): Unit = {
    val preposition = if (privType.equals("REVOKE")) "FROM" else "TO"
    val commands = if (includingCompound) dbmsCommands else dbmsCommands.filterNot(s => s.equals("ALL DBMS PRIVILEGES"))

    commands.foreach { command =>
      execute(s"$privType $command ON DBMS $preposition custom")
    }
  }
}
