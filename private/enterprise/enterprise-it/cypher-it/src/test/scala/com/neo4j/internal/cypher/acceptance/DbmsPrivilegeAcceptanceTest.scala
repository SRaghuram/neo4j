/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.internal.cypher.acceptance

import org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME
import org.neo4j.dbms.database.ComponentVersion.Neo4jVersions.VERSION_40
import org.neo4j.dbms.database.ComponentVersion.Neo4jVersions.VERSION_41D1
import org.neo4j.graphdb.security.AuthorizationViolationException

class DbmsPrivilegeAcceptanceTest extends AdministrationCommandAcceptanceTestBase with EnterpriseComponentVersionTestSupport {

  // Privilege tests

  test("should grant dbms privileges") {
    // GIVEN
    execute(s"CREATE ROLE $roleName")

    dbmsPrivileges.foreach {
      case (command, action) =>
        withClue(s"$command: \n") {
          // WHEN
          execute(s"GRANT $command ON DBMS TO $roleName")

          // THEN
          execute(s"SHOW ROLE $roleName PRIVILEGES").toSet should be(Set(
            granted(action).role(roleName).map
          ))

          // WHEN
          execute(s"REVOKE GRANT $command ON DBMS FROM $roleName")

          // THEN
          execute(s"SHOW ROLE $roleName PRIVILEGES").toSet should be(Set.empty)
        }
    }
  }

  test("should deny dbms privileges") {
    // GIVEN
    execute(s"CREATE ROLE $roleName")

    dbmsPrivileges.foreach {
      case (command, action) =>
        withClue(s"$command: \n") {
          // WHEN
          execute(s"DENY $command ON DBMS TO $roleName")

          // THEN
          execute(s"SHOW ROLE $roleName PRIVILEGES").toSet should be(Set(
            denied(action).role(roleName).map
          ))

          // WHEN
          execute(s"REVOKE DENY $command ON DBMS FROM $roleName")

          // THEN
          execute(s"SHOW ROLE $roleName PRIVILEGES").toSet should be(Set.empty)
        }
    }
  }

  Seq((VERSION_40, dbmsPrivilegesNotIn40()), (VERSION_41D1, dbmsPrivilegesNotIn41d01())).foreach {
    case (version, dbmsPrivileges) =>
      withVersion(version) {

        test("should fail to grant dbms privileges") {
          // GIVEN
          execute(s"CREATE ROLE $roleName")

          dbmsPrivileges.foreach { command =>
            withClue(s"$command: \n") {
              an[UnsupportedOperationException] should be thrownBy {
                execute(s"GRANT $command ON DBMS TO $roleName")
              }
            }
          }
        }

        test("should fail to deny dbms privileges") {
          // GIVEN
          execute(s"CREATE ROLE $roleName")

          dbmsPrivileges.foreach { command =>
            withClue(s"$command: \n") {
              an[UnsupportedOperationException] should be thrownBy {
                execute(s"DENY $command ON DBMS TO $roleName")
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
    execute(s"CREATE ROLE $roleName")
    allDbmsPrivileges("GRANT", includingCompound = true)

    // WHEN
    execute("REVOKE ALL DBMS PRIVILEGES ON DBMS FROM $role", Map("role" -> roleName))

    // THEN
    execute(s"SHOW ROLE $roleName PRIVILEGES").toSet should be(Set(
      granted(adminAction("create_role")).role(roleName).map,
      granted(adminAction("drop_role")).role(roleName).map,
      granted(adminAction("assign_role")).role(roleName).map,
      granted(adminAction("remove_role")).role(roleName).map,
      granted(adminAction("show_role")).role(roleName).map,
      granted(adminAction("role_management")).role(roleName).map,
      granted(adminAction("create_user")).role(roleName).map,
      granted(adminAction("drop_user")).role(roleName).map,
      granted(adminAction("set_user_status")).role(roleName).map,
      granted(adminAction("set_passwords")).role(roleName).map,
      granted(adminAction("alter_user")).role(roleName).map,
      granted(adminAction("show_user")).role(roleName).map,
      granted(adminAction("user_management")).role(roleName).map,
      granted(adminAction("create_database")).role(roleName).map,
      granted(adminAction("drop_database")).role(roleName).map,
      granted(adminAction("database_management")).role(roleName).map,
      granted(adminAction("show_privilege")).role(roleName).map,
      granted(adminAction("assign_privilege")).role(roleName).map,
      granted(adminAction("remove_privilege")).role(roleName).map,
      granted(adminAction("privilege_management")).role(roleName).map,
      granted(adminAction("execute_admin")).role(roleName).map
    ))
  }

  test("Should revoke sub-privilege even if all dbms privilege exists") {
    // Given
    execute(s"CREATE ROLE $roleName")
    allDbmsPrivileges("GRANT", includingCompound = true)

    // When
    // Now revoke each sub-privilege in turn
    allDbmsPrivileges("REVOKE", includingCompound = false)

    // Then
    execute(s"SHOW ROLE $roleName PRIVILEGES").toSet should be(Set(
      granted(adminAction("dbms_actions")).role(roleName).map
    ))
  }

  // Enforcement tests

  test("should enforce all dbms privileges privilege") {
    // GIVEN
    clearPublicRole()
    setupUserWithCustomRole()

    // WHEN
    execute(s"GRANT ALL DBMS PRIVILEGES ON DBMS TO $roleName")

    // THEN

    // Should be able to do role management
    executeOnSystem(username, password, s"CREATE ROLE $roleName2")
    execute("SHOW ROLES").toSet should be(defaultRoles ++ Set(role(roleName).map, role(roleName2).map))

    // Should be able to do user management
    executeOnSystem(username, password, s"DROP USER $defaultUsername")
    execute("SHOW USERS").toSet should be(Set(user(username, Seq(roleName), passwordChangeRequired = false)))

    // Should be able to do database management
    executeOnSystem(username, password, s"CREATE DATABASE $databaseString")
    execute(s"SHOW DATABASE $databaseString").toSet should be(Set(db(databaseString)))

    // Should be able to do privilege management
    executeOnSystem(username, password, s"GRANT TRAVERSE ON GRAPH * NODES A TO $roleName2")
    execute(s"SHOW ROLE $roleName2 PRIVILEGES").toSet should be(Set(granted(traverse).node("A").role(roleName2).graph("*").map))

    // Should be able to execute @admin procedure
    executeOnDBMSDefault(username, password, "CALL dbms.listConfig('dbms.default_database')") should be(1)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute(s"REVOKE ALL DBMS PRIVILEGES ON DBMS FROM $roleName")

    // THEN

    // Should not be able to do role management
    the[AuthorizationViolationException] thrownBy {
      executeOnSystem(username, password, s"DROP ROLE $roleName2")
    } should have message PERMISSION_DENIED_DROP_ROLE
    execute("SHOW ROLES").toSet should be(defaultRoles ++ Set(role(roleName).map, role(roleName2).map))

    // Should not be able to do user management
    the[AuthorizationViolationException] thrownBy {
      executeOnSystem(username, password, "CREATE USER alice SET PASSWORD 'secret'")
    } should have message PERMISSION_DENIED_CREATE_USER
    execute("SHOW USERS").toSet should be(Set(user(username, Seq(roleName), passwordChangeRequired = false)))

    // Should not be able to do database management
    the[AuthorizationViolationException] thrownBy {
      executeOnSystem(username, password, s"DROP DATABASE $databaseString")
    } should have message PERMISSION_DENIED_DROP_DATABASE
    execute(s"SHOW DATABASE $databaseString").toSet should be(Set(db(databaseString)))

    // Should not be able to do privilege management
    the[AuthorizationViolationException] thrownBy {
      executeOnSystem(username, password, s"GRANT TRAVERSE ON GRAPH * NODES B TO $roleName2")
    } should have message PERMISSION_DENIED_ASSIGN_PRIVILEGE
    execute(s"SHOW ROLE $roleName2 PRIVILEGES").toSet should be(Set(granted(traverse).node("A").role(roleName2).graph("*").map))

    // Should not be able to execute (@admin) procedure
    (the[AuthorizationViolationException] thrownBy {
      executeOnDBMSDefault(username, password, "CALL dbms.listConfig('dbms.default_database')")
    }).getMessage should include(FAIL_EXECUTE_PROC)
  }

  test("should fail dbms management when denied all dbms privileges privilege") {
    // GIVEN
    setupUserWithCustomAdminRole()
    execute(s"CREATE ROLE $roleName2")

    // WHEN
    allDbmsPrivileges("GRANT", includingCompound = false)
    execute(s"DENY ALL DBMS PRIVILEGES ON DBMS TO $roleName")

    // THEN

    // Should not be able to do role management
    the[AuthorizationViolationException] thrownBy {
      executeOnSystem(username, password, "CREATE ROLE role")
    } should have message PERMISSION_DENIED_CREATE_ROLE
    execute("SHOW ROLES").toSet should be(defaultRoles ++ Set(role(roleName).map, role(roleName2).map))

    // Should not be able to do user management
    the[AuthorizationViolationException] thrownBy {
      executeOnSystem(username, password, s"DROP USER $defaultUsername")
    } should have message PERMISSION_DENIED_DROP_USER
    execute("SHOW USERS").toSet should be(Set(user(defaultUsername, Seq("admin")), user(username, Seq(roleName), passwordChangeRequired = false)))

    // Should not be able to do database management
    the[AuthorizationViolationException] thrownBy {
      executeOnSystem(username, password, s"CREATE DATABASE $databaseString")
    } should have message PERMISSION_DENIED_CREATE_DATABASE
    execute(s"SHOW DATABASE $databaseString").toSet should be(Set.empty)

    // Should not be able to do privilege management
    the[AuthorizationViolationException] thrownBy {
      executeOnSystem(username, password, s"GRANT TRAVERSE ON GRAPH * NODES A TO $roleName2")
    } should have message PERMISSION_DENIED_ASSIGN_PRIVILEGE
    execute(s"SHOW ROLE $roleName2 PRIVILEGES").toSet should be(Set.empty)

    // Should not be able to execute (@admin) procedure
    (the[AuthorizationViolationException] thrownBy {
      executeOnDBMSDefault(username, password, "CALL dbms.listConfig('dbms.default_database')")
    }).getMessage should include(FAIL_EXECUTE_PROC)
  }

  // helper methods

  private def allDbmsPrivileges(privType: String, includingCompound: Boolean): Unit = {
    val preposition = if (privType.equals("REVOKE")) "FROM" else "TO"
    val commands = if (includingCompound) dbmsCommands else dbmsCommands.filterNot(s => s.equals("ALL DBMS PRIVILEGES"))

    commands.foreach { command =>
      execute(s"$privType $command ON DBMS $preposition $roleName")
    }
  }
}
