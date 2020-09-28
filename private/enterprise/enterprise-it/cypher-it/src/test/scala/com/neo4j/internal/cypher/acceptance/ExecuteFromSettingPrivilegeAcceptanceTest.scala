/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.internal.cypher.acceptance

import java.lang.Boolean.TRUE

import com.neo4j.server.security.enterprise.auth.PrivilegeResolver.EXECUTE_BOOSTED_FROM_CONFIG
import com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME
import org.neo4j.graphdb.config.Setting
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException

import scala.collection.mutable

class ExecuteFromSettingPrivilegeAcceptanceTest extends AdministrationCommandAcceptanceTestBase {

  override protected def onNewGraphDatabase(): Unit = {
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute(s"REVOKE ACCESS ON DEFAULT DATABASE FROM ${PredefinedRoles.PUBLIC}")
    execute(s"REVOKE EXECUTE PROCEDURES * ON DBMS FROM ${PredefinedRoles.PUBLIC}")
    execute(s"REVOKE EXECUTE FUNCTIONS * ON DBMS FROM ${PredefinedRoles.PUBLIC}")
    execute("SHOW ROLE PUBLIC PRIVILEGES").toSet should be(grantedFromConfig("dbms.security.listUsers", PredefinedRoles.PUBLIC))
  }

  //noinspection ScalaDeprecation
  override def databaseConfig(): Map[Setting[_], Object] = super.databaseConfig() ++ Map(
    GraphDatabaseSettings.auth_enabled -> TRUE,
    GraphDatabaseSettings.procedure_roles -> "db.labels:configRole,default;db.property*:configRole;dbms.security.listUsers:PUBLIC",
    GraphDatabaseSettings.default_allowed -> "default"
  )

  test("should show execute boosted privileges for roles from config settings") {
    // GIVEN
    execute("CREATE ROLE configRole")
    execute("CREATE ROLE default")

    // THEN
    execute("SHOW ROLE configRole, default PRIVILEGES").toSet should be(
      grantedFromConfig("db.labels", "configRole") ++
      grantedFromConfig("db.property*", "configRole") ++
      grantedFromConfig("db.labels", "default") ++
      grantedFromConfig("*", "default") ++
      deniedFromConfig("db.property*", "default") ++
      deniedFromConfig("dbms.security.listUsers", "default")
    )
  }

  test("should show execute boosted privileges for user from config settings") {
    // GIVEN
    setupUserWithCustomRole(rolename = "configRole")

    // THEN
    execute("SHOW USER joe PRIVILEGES").toSet should be(
      grantedFromConfig("db.labels", "configRole", "joe") ++
      grantedFromConfig("db.property*", "configRole", "joe") ++
      grantedFromConfig("dbms.security.listUsers", "PUBLIC", "joe") ++
      Set(granted(access).role("configRole").user("joe").map)
    )

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT EXECUTE FUNCTION dbms.a* ON DBMS TO configRole")
    execute("GRANT EXECUTE PROCEDURE dbms.b* ON DBMS TO configRole")
    execute("DENY EXECUTE BOOSTED FUNCTION apoc.a* ON DBMS TO configRole")
    execute("DENY EXECUTE BOOSTED PROCEDURE apoc.b* ON DBMS TO configRole")

    // THEN
    execute("SHOW USER joe PRIVILEGES").toSet should be(
      grantedFromConfig("db.labels", "configRole", "joe") ++
      grantedFromConfig("db.property*", "configRole", "joe") ++
      grantedFromConfig("dbms.security.listUsers", "PUBLIC", "joe") ++
      Set(
        granted(access).role("configRole").user("joe").map,
        granted(executeFunction).function("dbms.a*").role("configRole").user("joe").map,
        granted(executeProcedure).procedure("dbms.b*").role("configRole").user("joe").map,
        denied(executeBoosted).function("apoc.a*").role("configRole").user("joe").map,
        denied(executeBoosted).procedure("apoc.b*").role("configRole").user("joe").map
      )
    )
  }

  test("should show execute boosted privileges for multiple users from config settings") {
    // GIVEN
    setupUserWithCustomRole(rolename = "configRole")
    setupUserWithCustomRole("alice", rolename = "default", access = false)
    setupUserWithCustomRole("bob")

    execute("CREATE USER charlie SET PASSWORD 'abc123'")
    execute("GRANT ROLE configRole, default TO charlie")

    // THEN
    execute("SHOW USER joe, $user, bob, charlie PRIVILEGES", Map("user" -> "alice")).toSet should be(
      grantedFromConfig("db.labels", "configRole", "joe") ++
      grantedFromConfig("db.property*", "configRole", "joe") ++
      grantedFromConfig("dbms.security.listUsers", "PUBLIC", "joe") ++

      grantedFromConfig("db.labels", "default", "alice") ++
      grantedFromConfig("*", "default", "alice") ++
      deniedFromConfig("db.property*", "default", "alice") ++
      deniedFromConfig("dbms.security.listUsers", "default", "alice") ++
      grantedFromConfig("dbms.security.listUsers", "PUBLIC", "alice") ++

      grantedFromConfig("dbms.security.listUsers", "PUBLIC", "bob") ++

      grantedFromConfig("db.labels", "configRole", "charlie") ++
      grantedFromConfig("db.property*", "configRole", "charlie") ++
      grantedFromConfig("db.labels", "default", "charlie") ++
      grantedFromConfig("*", "default", "charlie") ++
      deniedFromConfig("db.property*", "default", "charlie") ++
      deniedFromConfig("dbms.security.listUsers", "default", "charlie") ++
      grantedFromConfig("dbms.security.listUsers", "PUBLIC", "charlie") ++
      Set(
        granted(access).role("configRole").user("joe").map,
        granted(access).role("configRole").user("charlie").map,
        granted(access).role("custom").user("bob").map
      )
    )
  }

  test("should show execute boosted privileges for current user from config settings") {
    // GIVEN
    setupUserWithCustomRole(rolename = "configRole")

    val result = new mutable.HashSet[Map[String, AnyRef]]
    executeOnSystem("joe", "soap", "SHOW USER PRIVILEGES", resultHandler = (row, _) => {
      result.add(asPrivilegesResult(row))
    })
    result should be(
      grantedFromConfig("db.labels", "configRole", "joe") ++
      grantedFromConfig("db.property*", "configRole", "joe") ++
      Set(granted(access).role("configRole").user("joe").map) ++
      grantedFromConfig("dbms.security.listUsers", "PUBLIC", "joe")
    )
  }

  test("should show all privileges including from config settings") {
    // GIVEN
    setupUserWithCustomRole(rolename = "configRole")
    setupUserWithCustomRole("alice", rolename = "default", access = false)

    // THEN
    execute("SHOW ALL PRIVILEGES WHERE role IN ['configRole', 'default', 'custom']").toSet should be(
      grantedFromConfig("db.labels", "configRole") ++
      grantedFromConfig("db.property*", "configRole") ++

      grantedFromConfig("db.labels", "default") ++
      grantedFromConfig("*", "default") ++
      deniedFromConfig("db.property*", "default") ++
      deniedFromConfig("dbms.security.listUsers", "default") ++
      Set(granted(access).role("configRole").map)
    )

    // WHEN restore PUBLIC role
    execute("GRANT EXECUTE FUNCTION * ON DBMS TO PUBLIC")
    execute("GRANT EXECUTE PROCEDURE * ON DBMS TO PUBLIC")
    execute("GRANT ACCESS ON DEFAULT DATABASE TO PUBLIC")

    // THEN
    execute("SHOW ALL PRIVILEGES").toSet should be(defaultRolePrivileges ++
      grantedFromConfig("db.labels", "configRole") ++
      grantedFromConfig("db.property*", "configRole") ++

      grantedFromConfig("db.labels", "default") ++
      grantedFromConfig("*", "default") ++
      deniedFromConfig("db.property*", "default") ++
      deniedFromConfig("dbms.security.listUsers", "default") ++
      grantedFromConfig("dbms.security.listUsers", PredefinedRoles.PUBLIC) ++
      Set(granted(access).role("configRole").map)
    )
  }

  test("should filter privileges from config settings") {
    // GIVEN
    setupUserWithCustomRole(rolename = "configRole")
    setupUserWithCustomRole("alice", rolename = "default", access = false)
    setupUserWithCustomRole("bob")

    // THEN
    execute("SHOW ALL PRIVILEGES YIELD segment, action, role, access WHERE role IN ['configRole', 'default', 'custom']").toSet should be(Set(
      Map("segment" -> "FUNCTION(db.labels)", "action" -> EXECUTE_BOOSTED_FROM_CONFIG, "role" -> "configRole", "access" -> "GRANTED"),
      Map("segment" -> "PROCEDURE(db.labels)", "action" -> EXECUTE_BOOSTED_FROM_CONFIG, "role" -> "configRole", "access" -> "GRANTED"),
      Map("segment" -> "FUNCTION(db.property*)", "action" -> EXECUTE_BOOSTED_FROM_CONFIG, "role" -> "configRole", "access" -> "GRANTED"),
      Map("segment" -> "PROCEDURE(db.property*)", "action" -> EXECUTE_BOOSTED_FROM_CONFIG, "role" -> "configRole", "access" -> "GRANTED"),
      Map("segment" -> "database", "action" -> "access", "role" -> "configRole", "access" -> "GRANTED"),

      Map("segment" -> "FUNCTION(db.labels)", "action" -> EXECUTE_BOOSTED_FROM_CONFIG, "role" -> "default", "access" -> "GRANTED"),
      Map("segment" -> "PROCEDURE(db.labels)", "action" -> EXECUTE_BOOSTED_FROM_CONFIG, "role" -> "default", "access" -> "GRANTED"),
      Map("segment" -> "FUNCTION(*)", "action" -> EXECUTE_BOOSTED_FROM_CONFIG, "role" -> "default", "access" -> "GRANTED"),
      Map("segment" -> "PROCEDURE(*)", "action" -> EXECUTE_BOOSTED_FROM_CONFIG, "role" -> "default", "access" -> "GRANTED"),
      Map("segment" -> "FUNCTION(db.property*)", "action" -> EXECUTE_BOOSTED_FROM_CONFIG, "role" -> "default", "access" -> "DENIED"),
      Map("segment" -> "PROCEDURE(db.property*)", "action" -> EXECUTE_BOOSTED_FROM_CONFIG, "role" -> "default", "access" -> "DENIED"),
      Map("segment" -> "FUNCTION(dbms.security.listUsers)", "action" -> EXECUTE_BOOSTED_FROM_CONFIG, "role" -> "default", "access" -> "DENIED"),
      Map("segment" -> "PROCEDURE(dbms.security.listUsers)", "action" -> EXECUTE_BOOSTED_FROM_CONFIG, "role" -> "default", "access" -> "DENIED"),

      Map("segment" -> "database", "action" -> "access", "role" -> "custom", "access" -> "GRANTED")
    ))
  }

  test("should allow grant/revoke of function/procedure privileges to role in dbms.security.functions.roles/default_allowed") {
    // GIVEN
    execute("CREATE ROLE configRole")
    execute("CREATE ROLE default")

    // WHEN
    execute("GRANT EXECUTE BOOSTED PROCEDURE dbms.showCurrentUser ON DBMS TO configRole, default")
    execute("GRANT EXECUTE BOOSTED FUNCTION dbms.* ON DBMS TO configRole, default")

    // THEN
    execute("SHOW ROLE configRole, default PRIVILEGES").toSet should be(
      grantedFromConfig("db.labels", "configRole") ++
      grantedFromConfig("db.property*", "configRole") ++
      Set(
        granted(executeBoosted).procedure("dbms.showCurrentUser").role("configRole").map,
        granted(executeBoosted).function("dbms.*").role("configRole").map
      ) ++

      grantedFromConfig("*", "default") ++
      grantedFromConfig("db.labels", "default") ++
      deniedFromConfig("db.property*", "default") ++
      deniedFromConfig("dbms.security.listUsers", "default") ++
      Set(
        granted(executeBoosted).procedure("dbms.showCurrentUser").role("default").map,
        granted(executeBoosted).function("dbms.*").role("default").map
      )
    )

    // WHEN
    execute("REVOKE GRANT EXECUTE BOOSTED PROCEDURE dbms.showCurrentUser ON DBMS FROM configRole, default")
    execute("REVOKE GRANT EXECUTE BOOSTED FUNCTION dbms.* ON DBMS FROM configRole, default")

    // THEN
    execute("SHOW ROLE configRole, default PRIVILEGES").toSet should be(
      grantedFromConfig("db.labels", "configRole") ++
      grantedFromConfig("db.property*", "configRole") ++

      grantedFromConfig("*", "default") ++
      grantedFromConfig("db.labels", "default") ++
      deniedFromConfig("db.property*", "default") ++
      deniedFromConfig("dbms.security.listUsers", "default")
    )
  }

  test("should allow grant/revoke of same function/procedure privileges to role in dbms.security.functions.roles/default_allowed") {
    // GIVEN
    execute("CREATE ROLE configRole")
    execute("CREATE ROLE default")

    // WHEN
    execute("GRANT EXECUTE BOOSTED FUNCTION db.labels ON DBMS TO configRole, default")
    execute("GRANT EXECUTE BOOSTED PROCEDURE db.property* ON DBMS TO configRole, default")

    // THEN
    execute("SHOW ROLE configRole, default PRIVILEGES").toSet should be(
      grantedFromConfig("db.labels", "configRole") ++
      grantedFromConfig("db.property*", "configRole") ++

      grantedFromConfig("*", "default") ++
      grantedFromConfig("db.labels", "default") ++
      deniedFromConfig("db.property*", "default") ++
      deniedFromConfig("dbms.security.listUsers", "default") ++
      Set(
        granted(executeBoosted).function("db.labels").role("configRole").map,
        granted(executeBoosted).function("db.labels").role("default").map,
        granted(executeBoosted).procedure("db.property*").role("configRole").map,
        granted(executeBoosted).procedure("db.property*").role("default").map
      )
    )

    // WHEN
    execute("REVOKE GRANT EXECUTE BOOSTED FUNCTION db.labels ON DBMS FROM configRole, default")
    execute("REVOKE GRANT EXECUTE BOOSTED PROCEDURE db.property* ON DBMS FROM configRole, default")

    // THEN
    execute("SHOW ROLE configRole, default PRIVILEGES").toSet should be(
      grantedFromConfig("db.labels", "configRole") ++
      grantedFromConfig("db.property*", "configRole") ++

      grantedFromConfig("*", "default") ++
      grantedFromConfig("db.labels", "default") ++
      deniedFromConfig("db.property*", "default") ++
      deniedFromConfig("dbms.security.listUsers", "default")
    )
  }

  test("should have nice error message when trying to revoke function privilege from dbms.security.functions.roles") {
    // GIVEN
    execute("CREATE ROLE configRole")

    // WHEN .. THEN
    (the[InvalidArgumentsException] thrownBy {
      execute("REVOKE EXECUTE BOOSTED FUNCTION db.property* ON DBMS FROM configRole")
    }).getMessage should be
    """Failed to revoke execute_boosted privilege from role 'configRole': This privilege was granted through the config.
      |Altering the settings 'dbms.security.procedures.roles' and 'dbms.security.procedures.default_allowed' will affect this privilege after restart.""".stripMargin

    // THEN
    execute("SHOW ROLE configRole PRIVILEGES").toSet should be(
      grantedFromConfig("db.labels", "configRole") ++
      grantedFromConfig("db.property*", "configRole")
    )
  }

  test("should have nice error message when trying to revoke procedure privilege from dbms.security.functions.roles") {
    // GIVEN
    execute("CREATE ROLE configRole")

    // WHEN .. THEN
    (the[InvalidArgumentsException] thrownBy {
      execute("REVOKE EXECUTE BOOSTED PROCEDURE db.property* ON DBMS FROM configRole")
    }).getMessage should be
    """Failed to revoke execute_boosted privilege from role 'configRole': This privilege was granted through the config.
      |Altering the settings 'dbms.security.procedures.roles' and 'dbms.security.procedures.default_allowed' will affect this privilege after restart.""".stripMargin

    // THEN
    execute("SHOW ROLE configRole PRIVILEGES").toSet should be(
      grantedFromConfig("db.labels", "configRole") ++
        grantedFromConfig("db.property*", "configRole")
    )
  }

  test("should have nice error message when trying to revoke function privilege from dbms.security.functions.default_allowed") {
    // GIVEN
    execute("CREATE ROLE default")

    // WHEN .. THEN
    (the[InvalidArgumentsException] thrownBy {
      execute("REVOKE EXECUTE BOOSTED FUNCTION * ON DBMS FROM default")
    }).getMessage should be
    """Failed to revoke execute_boosted privilege from role 'default': This privilege was granted through the config.
      |Altering the settings 'dbms.security.procedures.roles' and 'dbms.security.procedures.default_allowed' will affect this privilege after restart.""".stripMargin

    // THEN
    execute("SHOW ROLE default PRIVILEGES").toSet should be(
      grantedFromConfig("*", "default") ++
        grantedFromConfig("db.labels", "default") ++
        deniedFromConfig("db.property*", "default") ++
        deniedFromConfig("dbms.security.listUsers", "default")
    )
  }

  test("should have nice error message when trying to revoke procedure privilege from dbms.security.functions.default_allowed") {
    // GIVEN
    execute("CREATE ROLE default")

    // WHEN .. THEN
    (the[InvalidArgumentsException] thrownBy {
      execute("REVOKE EXECUTE BOOSTED PROCEDURE * ON DBMS FROM default")
    }).getMessage should be
    """Failed to revoke execute_boosted privilege from role 'default': This privilege was granted through the config.
      |Altering the settings 'dbms.security.procedures.roles' and 'dbms.security.procedures.default_allowed' will affect this privilege after restart.""".stripMargin

    // THEN
    execute("SHOW ROLE default PRIVILEGES").toSet should be(
      grantedFromConfig("*", "default") ++
      grantedFromConfig("db.labels", "default") ++
      deniedFromConfig("db.property*", "default") ++
      deniedFromConfig("dbms.security.listUsers", "default")
    )
  }
}
