/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.internal.cypher.acceptance

import org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME
import org.neo4j.graphdb.security.AuthorizationViolationException
import org.neo4j.server.security.systemgraph.ComponentVersion.Neo4jVersions.VERSION_40
import org.neo4j.server.security.systemgraph.ComponentVersion.Neo4jVersions.VERSION_41D1

class DbmsPrivilegeAcceptanceTest extends AdministrationCommandAcceptanceTestBase with EnterpriseComponentVersionTestSupport {

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

  Seq((VERSION_40, dbmsPrivilegesNotIn40()), (VERSION_41D1, dbmsPrivilegesNotIn41d01())).foreach {
    case (version, dbmsPrivileges) =>
      withVersion(version) {

        test("should fail to grant dbms privileges") {
          // GIVEN
          execute("CREATE ROLE custom")

          dbmsPrivileges.foreach { command =>
            withClue(s"$command: \n") {
              an[UnsupportedOperationException] should be thrownBy {
                execute(s"GRANT $command ON DBMS TO custom")
              }
            }
          }
        }

        test("should fail to deny dbms privileges") {
          // GIVEN
          execute("CREATE ROLE custom")

          dbmsPrivileges.foreach { command =>
            withClue(s"$command: \n") {
              an[UnsupportedOperationException] should be thrownBy {
                execute(s"DENY $command ON DBMS TO custom")
              }
            }
          }
        }
      }
  }

  private def dbmsPrivilegesNotIn40(): Seq[String] = {
    dbmsCommands.filter(!_.contains("ROLE")).toSeq
  }

  private def dbmsPrivilegesNotIn41d01(): Seq[String] = {
    Seq.empty
  }

  test("should not revoke other dbms privileges when revoking all dbms privileges") {
    // GIVEN
    execute("CREATE ROLE custom")
    allDbmsPrivileges("GRANT", includingCompound = true)

    // WHEN
    execute("REVOKE ALL DBMS PRIVILEGES ON DBMS FROM $role", Map("role" -> "custom"))

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      granted(adminAction("create_role")).role("custom").map,
      granted(adminAction("drop_role")).role("custom").map,
      granted(adminAction("assign_role")).role("custom").map,
      granted(adminAction("remove_role")).role("custom").map,
      granted(adminAction("show_role")).role("custom").map,
      granted(adminAction("role_management")).role("custom").map,
      granted(adminAction("create_user")).role("custom").map,
      granted(adminAction("drop_user")).role("custom").map,
      granted(adminAction("set_user_status")).role("custom").map,
      granted(adminAction("set_user_default_database")).role("custom").map,
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
      granted(adminAction("privilege_management")).role("custom").map,
      granted(adminAction("execute_admin")).role("custom").map
    ))
  }

  test("Should revoke sub-privilege even if all dbms privilege exists") {
    // Given
    execute("CREATE ROLE custom")
    allDbmsPrivileges("GRANT", includingCompound = true)

    // When
    // Now revoke each sub-privilege in turn
    allDbmsPrivileges("REVOKE", includingCompound = false)

    // Then
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      granted(allDbmsPrivilege).role("custom").map,
      granted(adminAction("dbms_actions")).role("custom").map
    ))
  }

  // Enforcement tests

  test("should enforce all dbms privileges privilege") {
    // GIVEN
    clearPublicRole()
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
    execute("SHOW ROLE otherRole PRIVILEGES").toSet should be(Set(granted(traverse).node("A").role("otherRole").graph("*").map))

    // Should be able to execute @admin procedure
    executeOnDBMSDefault("foo", "bar", "CALL dbms.listConfig('dbms.default_database')") should be(1)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("REVOKE ALL DBMS PRIVILEGES ON DBMS FROM custom")

    // THEN

    // Should not be able to do role management
    the[AuthorizationViolationException] thrownBy {
      executeOnSystem("foo", "bar", "CREATE ROLE role")
    } should have message PERMISSION_DENIED_CREATE_ROLE
    execute("SHOW ROLES").toSet should be(defaultRoles ++ Set(role("custom").map, role("otherRole").map))

    // Should not be able to do user management
    the[AuthorizationViolationException] thrownBy {
      executeOnSystem("foo", "bar", "CREATE USER alice SET PASSWORD 'secret'")
    } should have message PERMISSION_DENIED_CREATE_USER
    execute("SHOW USERS").toSet should be(Set(user("foo", Seq("custom"), passwordChangeRequired = false)))

    // Should not be able to do database management
    the[AuthorizationViolationException] thrownBy {
      executeOnSystem("foo", "bar", "DROP DATABASE baz")
    } should have message PERMISSION_DENIED_DROP_DATABASE
    execute("SHOW DATABASE baz").toSet should be(Set(db("baz")))

    // Should not be able to do privilege management
    the[AuthorizationViolationException] thrownBy {
      executeOnSystem("foo", "bar", "GRANT TRAVERSE ON GRAPH * NODES B TO otherRole")
    } should have message PERMISSION_DENIED_ASSIGN_PRIVILEGE
    execute("SHOW ROLE otherRole PRIVILEGES").toSet should be(Set(granted(traverse).node("A").role("otherRole").graph("*").map))

    // Should not be able to execute (@admin) procedure
    (the[AuthorizationViolationException] thrownBy {
      executeOnDBMSDefault("foo", "bar", "CALL dbms.listConfig('dbms.default_database')")
    }).getMessage should include(FAIL_EXECUTE_PROC)
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
    } should have message PERMISSION_DENIED_CREATE_ROLE
    execute("SHOW ROLES").toSet should be(defaultRoles ++ Set(role("custom").map, role("otherRole").map))

    // Should not be able to do user management
    the[AuthorizationViolationException] thrownBy {
      executeOnSystem("foo", "bar", "DROP USER neo4j")
    } should have message PERMISSION_DENIED_DROP_USER
    execute("SHOW USERS").toSet should be(Set(user("neo4j", Seq("admin")), user("foo", Seq("custom"), passwordChangeRequired = false)))

    // Should not be able to do database management
    the[AuthorizationViolationException] thrownBy {
      executeOnSystem("foo", "bar", "CREATE DATABASE baz")
    } should have message PERMISSION_DENIED_CREATE_DATABASE
    execute("SHOW DATABASE baz").toSet should be(Set.empty)

    // Should not be able to do privilege management
    the[AuthorizationViolationException] thrownBy {
      executeOnSystem("foo", "bar", "GRANT TRAVERSE ON GRAPH * NODES A TO otherRole")
    } should have message PERMISSION_DENIED_ASSIGN_PRIVILEGE
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
