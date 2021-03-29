/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.internal.cypher.acceptance

import java.lang.Boolean.TRUE

import com.neo4j.server.security.enterprise.auth.PrivilegeResolver.EXECUTE_BOOSTED_FROM_CONFIG
import com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.PUBLIC
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME
import org.neo4j.exceptions.InvalidArgumentException
import org.neo4j.graphdb.config.Setting

import scala.collection.mutable

class ExecuteFromSettingPrivilegeAcceptanceTest extends ExecutePrivilegeAcceptanceTestBase {

  private val username2 = "alice"
  private val username3 = "bob"

  override protected def onNewGraphDatabase(): Unit = {
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute(s"REVOKE ACCESS ON HOME DATABASE FROM $PUBLIC")
    execute(s"REVOKE EXECUTE PROCEDURES * ON DBMS FROM $PUBLIC")
    execute(s"REVOKE EXECUTE FUNCTIONS * ON DBMS FROM $PUBLIC")
    execute(s"SHOW ROLE $PUBLIC PRIVILEGES").toSet should be(grantedFromConfig(listUsersProcGlob, PUBLIC))
  }

  //noinspection ScalaDeprecation
  override def databaseConfig(): Map[Setting[_], Object] = {
    super.databaseConfig() ++ Map(
      GraphDatabaseSettings.auth_enabled -> TRUE,
      GraphDatabaseSettings.procedure_roles -> s"$labelsProcGlob:$roleName2,$defaultRole;$propertyProcGlob:$roleName2;$listUsersProcGlob:$PUBLIC",
      GraphDatabaseSettings.default_allowed -> defaultRole
    )
  }

  test("should show execute boosted privileges for roles from config settings") {
    // GIVEN
    execute(s"CREATE ROLE $roleName2")
    execute(s"CREATE ROLE $defaultRole")

    // THEN
    execute(s"SHOW ROLE $roleName2, $defaultRole PRIVILEGES").toSet should be(
      grantedFromConfig(labelsProcGlob, roleName2) ++
      grantedFromConfig(propertyProcGlob, roleName2) ++
      grantedFromConfig(labelsProcGlob, defaultRole) ++
      grantedFromConfig("*", defaultRole) ++
      deniedFromConfig(propertyProcGlob, defaultRole) ++
      deniedFromConfig(listUsersProcGlob, defaultRole)
    )
  }

  test("should show execute boosted privileges for user from config settings") {
    // GIVEN
    setupUserWithCustomRole(rolename = roleName2)

    // THEN
    execute(s"SHOW USER $username PRIVILEGES").toSet should be(
      grantedFromConfig(labelsProcGlob, roleName2, username) ++
      grantedFromConfig(propertyProcGlob, roleName2, username) ++
      grantedFromConfig(listUsersProcGlob, PUBLIC, username) ++
      Set(granted(access).role(roleName2).user(username).map)
    )

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute(s"GRANT EXECUTE FUNCTION dbms.a* ON DBMS TO $roleName2")
    execute(s"GRANT EXECUTE PROCEDURE dbms.b* ON DBMS TO $roleName2")
    execute(s"DENY EXECUTE BOOSTED FUNCTION apoc.a* ON DBMS TO $roleName2")
    execute(s"DENY EXECUTE BOOSTED PROCEDURE apoc.b* ON DBMS TO $roleName2")

    // THEN
    execute(s"SHOW USER $username PRIVILEGES").toSet should be(
      grantedFromConfig(labelsProcGlob, roleName2, username) ++
      grantedFromConfig(propertyProcGlob, roleName2, username) ++
      grantedFromConfig(listUsersProcGlob, PUBLIC, username) ++
      Set(
        granted(access).role(roleName2).user(username).map,
        granted(executeFunction).function("dbms.a*").role(roleName2).user(username).map,
        granted(executeProcedure).procedure("dbms.b*").role(roleName2).user(username).map,
        denied(executeBoosted).function("apoc.a*").role(roleName2).user(username).map,
        denied(executeBoosted).procedure("apoc.b*").role(roleName2).user(username).map
      )
    )
  }

  test("should show execute boosted privileges for multiple users from config settings") {
    // GIVEN
    val username4 = "charlie"
    setupUserWithCustomRole(rolename = roleName2)
    setupUserWithCustomRole(username2, rolename = defaultRole, access = false)
    setupUserWithCustomRole(username3)

    execute(s"CREATE USER $username4 SET PASSWORD 'abc123'")
    execute(s"GRANT ROLE $roleName2, $defaultRole TO $username4")

    // THEN
    execute(s"SHOW USER $username, $$user, $username3, $username4 PRIVILEGES", Map("user" -> username2)).toSet should be(
      grantedFromConfig(labelsProcGlob, roleName2, username) ++
      grantedFromConfig(propertyProcGlob, roleName2, username) ++
      grantedFromConfig(listUsersProcGlob, PUBLIC, username) ++

      grantedFromConfig(labelsProcGlob, defaultRole, username2) ++
      grantedFromConfig("*", defaultRole, username2) ++
      deniedFromConfig(propertyProcGlob, defaultRole, username2) ++
      deniedFromConfig(listUsersProcGlob, defaultRole, username2) ++
      grantedFromConfig(listUsersProcGlob, PUBLIC, username2) ++

      grantedFromConfig(listUsersProcGlob, PUBLIC, username3) ++

      grantedFromConfig(labelsProcGlob, roleName2, username4) ++
      grantedFromConfig(propertyProcGlob, roleName2, username4) ++
      grantedFromConfig(labelsProcGlob, defaultRole, username4) ++
      grantedFromConfig("*", defaultRole, username4) ++
      deniedFromConfig(propertyProcGlob, defaultRole, username4) ++
      deniedFromConfig(listUsersProcGlob, defaultRole, username4) ++
      grantedFromConfig(listUsersProcGlob, PUBLIC, username4) ++
      Set(
        granted(access).role(roleName2).user(username).map,
        granted(access).role(roleName2).user(username4).map,
        granted(access).role(roleName).user(username3).map
      )
    )
  }

  test("should show execute boosted privileges for current user from config settings") {
    // GIVEN
    setupUserWithCustomRole(rolename = roleName2)

    val result = new mutable.HashSet[Map[String, AnyRef]]
    executeOnSystem(username, password, "SHOW USER PRIVILEGES", resultHandler = (row, _) => {
      result.add(asPrivilegesResult(row))
    })
    result should be(
      grantedFromConfig(labelsProcGlob, roleName2, username) ++
      grantedFromConfig(propertyProcGlob, roleName2, username) ++
      Set(granted(access).role(roleName2).user(username).map) ++
      grantedFromConfig(listUsersProcGlob, PUBLIC, username)
    )
  }

  test("should show all privileges including from config settings") {
    // GIVEN
    setupUserWithCustomRole(rolename = roleName2)
    setupUserWithCustomRole(username2, rolename = defaultRole, access = false)

    // THEN
    execute(s"SHOW ALL PRIVILEGES WHERE role IN ['$roleName2', '$defaultRole', '$roleName']").toSet should be(
      grantedFromConfig(labelsProcGlob, roleName2) ++
      grantedFromConfig(propertyProcGlob, roleName2) ++

      grantedFromConfig(labelsProcGlob, defaultRole) ++
      grantedFromConfig("*", defaultRole) ++
      deniedFromConfig(propertyProcGlob, defaultRole) ++
      deniedFromConfig(listUsersProcGlob, defaultRole) ++
      Set(granted(access).role(roleName2).map)
    )

    // WHEN restore PUBLIC role
    execute(s"GRANT EXECUTE FUNCTION * ON DBMS TO $PUBLIC")
    execute(s"GRANT EXECUTE PROCEDURE * ON DBMS TO $PUBLIC")
    execute(s"GRANT ACCESS ON HOME DATABASE TO $PUBLIC")

    // THEN
    execute("SHOW ALL PRIVILEGES").toSet should be(defaultRolePrivileges ++
      grantedFromConfig(labelsProcGlob, roleName2) ++
      grantedFromConfig(propertyProcGlob, roleName2) ++

      grantedFromConfig(labelsProcGlob, defaultRole) ++
      grantedFromConfig("*", defaultRole) ++
      deniedFromConfig(propertyProcGlob, defaultRole) ++
      deniedFromConfig(listUsersProcGlob, defaultRole) ++
      grantedFromConfig(listUsersProcGlob, PUBLIC) ++
      Set(granted(access).role(roleName2).map)
    )
  }

  test("should filter privileges from config settings") {
    // GIVEN
    setupUserWithCustomRole(rolename = roleName2)
    setupUserWithCustomRole(username2, rolename = defaultRole, access = false)
    setupUserWithCustomRole(username3)

    // THEN
    execute(s"SHOW ALL PRIVILEGES YIELD segment, action, role, access WHERE role IN ['$roleName2', '$defaultRole', '$roleName']").toSet should be(Set(
      Map("segment" -> s"FUNCTION($labelsProcGlob)", "action" -> EXECUTE_BOOSTED_FROM_CONFIG, "role" -> roleName2, "access" -> "GRANTED"),
      Map("segment" -> s"PROCEDURE($labelsProcGlob)", "action" -> EXECUTE_BOOSTED_FROM_CONFIG, "role" -> roleName2, "access" -> "GRANTED"),
      Map("segment" -> s"FUNCTION($propertyProcGlob)", "action" -> EXECUTE_BOOSTED_FROM_CONFIG, "role" -> roleName2, "access" -> "GRANTED"),
      Map("segment" -> s"PROCEDURE($propertyProcGlob)", "action" -> EXECUTE_BOOSTED_FROM_CONFIG, "role" -> roleName2, "access" -> "GRANTED"),
      Map("segment" -> "database", "action" -> "access", "role" -> roleName2, "access" -> "GRANTED"),

      Map("segment" -> s"FUNCTION($labelsProcGlob)", "action" -> EXECUTE_BOOSTED_FROM_CONFIG, "role" -> defaultRole, "access" -> "GRANTED"),
      Map("segment" -> s"PROCEDURE($labelsProcGlob)", "action" -> EXECUTE_BOOSTED_FROM_CONFIG, "role" -> defaultRole, "access" -> "GRANTED"),
      Map("segment" -> "FUNCTION(*)", "action" -> EXECUTE_BOOSTED_FROM_CONFIG, "role" -> defaultRole, "access" -> "GRANTED"),
      Map("segment" -> "PROCEDURE(*)", "action" -> EXECUTE_BOOSTED_FROM_CONFIG, "role" -> defaultRole, "access" -> "GRANTED"),
      Map("segment" -> s"FUNCTION($propertyProcGlob)", "action" -> EXECUTE_BOOSTED_FROM_CONFIG, "role" -> defaultRole, "access" -> "DENIED"),
      Map("segment" -> s"PROCEDURE($propertyProcGlob)", "action" -> EXECUTE_BOOSTED_FROM_CONFIG, "role" -> defaultRole, "access" -> "DENIED"),
      Map("segment" -> s"FUNCTION($listUsersProcGlob)", "action" -> EXECUTE_BOOSTED_FROM_CONFIG, "role" -> defaultRole, "access" -> "DENIED"),
      Map("segment" -> s"PROCEDURE($listUsersProcGlob)", "action" -> EXECUTE_BOOSTED_FROM_CONFIG, "role" -> defaultRole, "access" -> "DENIED"),

      Map("segment" -> "database", "action" -> "access", "role" -> roleName, "access" -> "GRANTED")
    ))
  }

  test("should allow grant/revoke of function/procedure privileges to role in dbms.security.functions.roles/default_allowed") {
    // GIVEN
    execute(s"CREATE ROLE $roleName2")
    execute(s"CREATE ROLE $defaultRole")

    // WHEN
    execute(s"GRANT EXECUTE BOOSTED PROCEDURE dbms.showCurrentUser ON DBMS TO $roleName2, $defaultRole")
    execute(s"GRANT EXECUTE BOOSTED FUNCTION dbms.* ON DBMS TO $roleName2, $defaultRole")

    // THEN
    execute(s"SHOW ROLE $roleName2, $defaultRole PRIVILEGES").toSet should be(
      grantedFromConfig(labelsProcGlob, roleName2) ++
      grantedFromConfig(propertyProcGlob, roleName2) ++
      Set(
        granted(executeBoosted).procedure("dbms.showCurrentUser").role(roleName2).map,
        granted(executeBoosted).function("dbms.*").role(roleName2).map
      ) ++

      grantedFromConfig("*", defaultRole) ++
      grantedFromConfig(labelsProcGlob, defaultRole) ++
      deniedFromConfig(propertyProcGlob, defaultRole) ++
      deniedFromConfig(listUsersProcGlob, defaultRole) ++
      Set(
        granted(executeBoosted).procedure("dbms.showCurrentUser").role(defaultRole).map,
        granted(executeBoosted).function("dbms.*").role(defaultRole).map
      )
    )

    // WHEN
    execute(s"REVOKE GRANT EXECUTE BOOSTED PROCEDURE dbms.showCurrentUser ON DBMS FROM $roleName2, $defaultRole")
    execute(s"REVOKE GRANT EXECUTE BOOSTED FUNCTION dbms.* ON DBMS FROM $roleName2, $defaultRole")

    // THEN
    execute(s"SHOW ROLE $roleName2, $defaultRole PRIVILEGES").toSet should be(
      grantedFromConfig(labelsProcGlob, roleName2) ++
      grantedFromConfig(propertyProcGlob, roleName2) ++

      grantedFromConfig("*", defaultRole) ++
      grantedFromConfig(labelsProcGlob, defaultRole) ++
      deniedFromConfig(propertyProcGlob, defaultRole) ++
      deniedFromConfig(listUsersProcGlob, defaultRole)
    )
  }

  test("should allow grant/revoke of same function/procedure privileges to role in dbms.security.functions.roles/default_allowed") {
    // GIVEN
    execute(s"CREATE ROLE $roleName2")
    execute(s"CREATE ROLE $defaultRole")

    // WHEN
    execute(s"GRANT EXECUTE BOOSTED FUNCTION $labelsProcGlob ON DBMS TO $roleName2, $defaultRole")
    execute(s"GRANT EXECUTE BOOSTED PROCEDURE $propertyProcGlob ON DBMS TO $roleName2, $defaultRole")

    // THEN
    execute(s"SHOW ROLE $roleName2, $defaultRole PRIVILEGES").toSet should be(
      grantedFromConfig(labelsProcGlob, roleName2) ++
      grantedFromConfig(propertyProcGlob, roleName2) ++

      grantedFromConfig("*", defaultRole) ++
      grantedFromConfig(labelsProcGlob, defaultRole) ++
      deniedFromConfig(propertyProcGlob, defaultRole) ++
      deniedFromConfig(listUsersProcGlob, defaultRole) ++
      Set(
        granted(executeBoosted).function(labelsProcGlob).role(roleName2).map,
        granted(executeBoosted).function(labelsProcGlob).role(defaultRole).map,
        granted(executeBoosted).procedure(propertyProcGlob).role(roleName2).map,
        granted(executeBoosted).procedure(propertyProcGlob).role(defaultRole).map
      )
    )

    // WHEN
    execute(s"REVOKE GRANT EXECUTE BOOSTED FUNCTION $labelsProcGlob ON DBMS FROM $roleName2, $defaultRole")
    execute(s"REVOKE GRANT EXECUTE BOOSTED PROCEDURE $propertyProcGlob ON DBMS FROM $roleName2, $defaultRole")

    // THEN
    execute(s"SHOW ROLE $roleName2, $defaultRole PRIVILEGES").toSet should be(
      grantedFromConfig(labelsProcGlob, roleName2) ++
      grantedFromConfig(propertyProcGlob, roleName2) ++

      grantedFromConfig("*", defaultRole) ++
      grantedFromConfig(labelsProcGlob, defaultRole) ++
      deniedFromConfig(propertyProcGlob, defaultRole) ++
      deniedFromConfig(listUsersProcGlob, defaultRole)
    )
  }

  test("should have nice error message when trying to revoke function privilege from dbms.security.functions.roles") {
    // GIVEN
    execute(s"CREATE ROLE $roleName2")

    // WHEN .. THEN
    (the[InvalidArgumentException] thrownBy {
      execute(s"REVOKE EXECUTE BOOSTED FUNCTION $propertyProcGlob ON DBMS FROM $roleName2")
    }).getMessage should be
    s"""Failed to revoke execute_boosted privilege from role '$roleName2': This privilege was granted through the configuration file.
      |Altering the settings 'dbms.security.procedures.roles' and 'dbms.security.procedures.default_allowed' will affect this privilege after restart.""".stripMargin

    // THEN
    execute(s"SHOW ROLE $roleName2 PRIVILEGES").toSet should be(
      grantedFromConfig(labelsProcGlob, roleName2) ++
      grantedFromConfig(propertyProcGlob, roleName2)
    )
  }

  test("should have nice error message when trying to revoke procedure privilege from dbms.security.functions.roles") {
    // GIVEN
    execute(s"CREATE ROLE $roleName2")

    // WHEN .. THEN
    (the[InvalidArgumentException] thrownBy {
      execute(s"REVOKE EXECUTE BOOSTED PROCEDURE $propertyProcGlob ON DBMS FROM $roleName2")
    }).getMessage should be
    s"""Failed to revoke execute_boosted privilege from role '$roleName2': This privilege was granted through the configuration file.
      |Altering the settings 'dbms.security.procedures.roles' and 'dbms.security.procedures.default_allowed' will affect this privilege after restart.""".stripMargin

    // THEN
    execute(s"SHOW ROLE $roleName2 PRIVILEGES").toSet should be(
      grantedFromConfig(labelsProcGlob, roleName2) ++
        grantedFromConfig(propertyProcGlob, roleName2)
    )
  }

  test("should have nice error message when trying to revoke function privilege from dbms.security.functions.default_allowed") {
    // GIVEN
    execute(s"CREATE ROLE $defaultRole")

    // WHEN .. THEN
    (the[InvalidArgumentException] thrownBy {
      execute(s"REVOKE EXECUTE BOOSTED FUNCTION * ON DBMS FROM $defaultRole")
    }).getMessage should be
    s"""Failed to revoke execute_boosted privilege from role '$defaultRole': This privilege was granted through the configuration file.
      |Altering the settings 'dbms.security.procedures.roles' and 'dbms.security.procedures.default_allowed' will affect this privilege after restart.""".stripMargin

    // THEN
    execute(s"SHOW ROLE $defaultRole PRIVILEGES").toSet should be(
      grantedFromConfig("*", defaultRole) ++
        grantedFromConfig(labelsProcGlob, defaultRole) ++
        deniedFromConfig(propertyProcGlob, defaultRole) ++
        deniedFromConfig(listUsersProcGlob, defaultRole)
    )
  }

  test("should have nice error message when trying to revoke procedure privilege from dbms.security.functions.default_allowed") {
    // GIVEN
    execute(s"CREATE ROLE $defaultRole")

    // WHEN .. THEN
    (the[InvalidArgumentException] thrownBy {
      execute(s"REVOKE EXECUTE BOOSTED PROCEDURE * ON DBMS FROM $defaultRole")
    }).getMessage should be
    s"""Failed to revoke execute_boosted privilege from role '$defaultRole': This privilege was granted through the configuration file.
      |Altering the settings 'dbms.security.procedures.roles' and 'dbms.security.procedures.default_allowed' will affect this privilege after restart.""".stripMargin

    // THEN
    execute(s"SHOW ROLE $defaultRole PRIVILEGES").toSet should be(
      grantedFromConfig("*", defaultRole) ++
      grantedFromConfig(labelsProcGlob, defaultRole) ++
      deniedFromConfig(propertyProcGlob, defaultRole) ++
      deniedFromConfig(listUsersProcGlob, defaultRole)
    )
  }
}
