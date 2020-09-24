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
    execute("SHOW ROLE PUBLIC PRIVILEGES").toList should be(List(
      granted(executeBoostedFromConfig).function("dbms.security.listUsers").role("PUBLIC").map,
      granted(executeBoostedFromConfig).procedure("dbms.security.listUsers").role("PUBLIC").map
    ))
  }

  //noinspection ScalaDeprecation
  override def databaseConfig(): Map[Setting[_], Object] = super.databaseConfig() ++ Map(
    GraphDatabaseSettings.auth_enabled -> TRUE,
    GraphDatabaseSettings.procedure_roles -> "db.labels:procRole,default;db.property*:procRole;dbms.security.listUsers:PUBLIC",
    GraphDatabaseSettings.default_allowed -> "default"
  )

  test("should show execute boosted privileges for roles from config settings") {
    // GIVEN
    execute("CREATE ROLE funcRole")
    execute("CREATE ROLE default")

    // THEN
    execute("SHOW ROLE funcRole, default PRIVILEGES").toSet should be(Set(
      granted(executeBoostedFromConfig).function("db.labels").role("funcRole").map,
      granted(executeBoostedFromConfig).procedure("db.labels").role("funcRole").map,
      granted(executeBoostedFromConfig).function("db.labels").role("default").map,
      granted(executeBoostedFromConfig).procedure("db.labels").role("default").map,
      granted(executeBoostedFromConfig).function("db.property*").role("funcRole").map,
      granted(executeBoostedFromConfig).procedure("db.property*").role("funcRole").map,
      granted(executeBoostedFromConfig).function("*").role("default").map,
      granted(executeBoostedFromConfig).procedure("*").role("default").map,
      denied(executeBoostedFromConfig).function("db.property*").role("default").map,
      denied(executeBoostedFromConfig).procedure("db.property*").role("default").map,
      denied(executeBoostedFromConfig).function("dbms.security.listUsers").role("default").map,
      denied(executeBoostedFromConfig).procedure("dbms.security.listUsers").role("default").map
    ))
  }

  test("should show execute boosted privileges for user from config settings") {
    // GIVEN
    setupUserWithCustomRole(rolename = "funcRole")

    // THEN
    execute("SHOW USER joe PRIVILEGES").toSet should be(Set(
      granted(executeBoostedFromConfig).function("db.labels").role("funcRole").user("joe").map,
      granted(executeBoostedFromConfig).procedure("db.labels").role("funcRole").user("joe").map,
      granted(executeBoostedFromConfig).function("db.property*").role("funcRole").user("joe").map,
      granted(executeBoostedFromConfig).procedure("db.property*").role("funcRole").user("joe").map,
      granted(access).role("funcRole").user("joe").map,

      granted(executeBoostedFromConfig).function("dbms.security.listUsers").role("PUBLIC").user("joe").map,
      granted(executeBoostedFromConfig).procedure("dbms.security.listUsers").role("PUBLIC").user("joe").map
    ))

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT EXECUTE FUNCTION dbms.* ON DBMS TO funcRole")
    execute("DENY EXECUTE BOOSTED FUNCTION apoc.* ON DBMS TO funcRole")

    // THEN
    execute("SHOW USER joe PRIVILEGES").toSet should be(Set(
      granted(executeBoostedFromConfig).function("db.labels").role("funcRole").user("joe").map,
      granted(executeBoostedFromConfig).procedure("db.labels").role("funcRole").user("joe").map,
      granted(executeBoostedFromConfig).function("db.property*").role("funcRole").user("joe").map,
      granted(executeBoostedFromConfig).procedure("db.property*").role("funcRole").user("joe").map,
      granted(access).role("funcRole").user("joe").map,
      granted(executeFunction).function("dbms.*").role("funcRole").user("joe").map,
      denied(executeBoostedFunction).function("apoc.*").role("funcRole").user("joe").map,
      denied(executeBoostedFunction).procedure("apoc.*").role("funcRole").user("joe").map,

      granted(executeBoostedFromConfig).function("dbms.security.listUsers").role("PUBLIC").user("joe").map,
      granted(executeBoostedFromConfig).procedure("dbms.security.listUsers").role("PUBLIC").user("joe").map
    ))
  }

  test("should show execute boosted privileges for multiple users from config settings") {
    // GIVEN
    setupUserWithCustomRole(rolename = "funcRole")
    setupUserWithCustomRole("alice", rolename = "default", access = false)
    setupUserWithCustomRole("bob")

    execute("CREATE USER charlie SET PASSWORD 'abc123'")
    execute("GRANT ROLE funcRole, default TO charlie")

    // THEN
    execute("SHOW USER joe, $user, bob, charlie PRIVILEGES", Map("user" -> "alice")).toSet should be(Set(
      granted(executeBoostedFromConfig).function("db.labels").role("funcRole").user("joe").map,
      granted(executeBoostedFromConfig).procedure("db.labels").role("funcRole").user("joe").map,
      granted(executeBoostedFromConfig).function("db.property*").role("funcRole").user("joe").map,
      granted(executeBoostedFromConfig).procedure("db.property*").role("funcRole").user("joe").map,
      granted(access).role("funcRole").user("joe").map,
      granted(executeBoostedFromConfig).function("dbms.security.listUsers").role("PUBLIC").user("joe").map,
      granted(executeBoostedFromConfig).procedure("dbms.security.listUsers").role("PUBLIC").user("joe").map,

      granted(executeBoostedFromConfig).function("db.labels").role("default").user("alice").map,
      granted(executeBoostedFromConfig).procedure("db.labels").role("default").user("alice").map,
      granted(executeBoostedFromConfig).function("*").role("default").user("alice").map,
      granted(executeBoostedFromConfig).procedure("*").role("default").user("alice").map,
      denied(executeBoostedFromConfig).function("db.property*").role("default").user("alice").map,
      denied(executeBoostedFromConfig).procedure("db.property*").role("default").user("alice").map,
      denied(executeBoostedFromConfig).function("dbms.security.listUsers").role("default").user("alice").map,
      denied(executeBoostedFromConfig).procedure("dbms.security.listUsers").role("default").user("alice").map,
      granted(executeBoostedFromConfig).function("dbms.security.listUsers").role("PUBLIC").user("alice").map,
      granted(executeBoostedFromConfig).procedure("dbms.security.listUsers").role("PUBLIC").user("alice").map,

      granted(access).role("custom").user("bob").map,
      granted(executeBoostedFromConfig).function("dbms.security.listUsers").role("PUBLIC").user("bob").map,
      granted(executeBoostedFromConfig).procedure("dbms.security.listUsers").role("PUBLIC").user("bob").map,

      granted(executeBoostedFromConfig).function("db.labels").role("funcRole").user("charlie").map,
      granted(executeBoostedFromConfig).procedure("db.labels").role("funcRole").user("charlie").map,
      granted(executeBoostedFromConfig).function("db.property*").role("funcRole").user("charlie").map,
      granted(executeBoostedFromConfig).procedure("db.property*").role("funcRole").user("charlie").map,
      granted(access).role("funcRole").user("charlie").map,
      granted(executeBoostedFromConfig).function("db.labels").role("default").user("charlie").map,
      granted(executeBoostedFromConfig).procedure("db.labels").role("default").user("charlie").map,
      granted(executeBoostedFromConfig).function("*").role("default").user("charlie").map,
      granted(executeBoostedFromConfig).procedure("*").role("default").user("charlie").map,
      denied(executeBoostedFromConfig).function("db.property*").role("default").user("charlie").map,
      denied(executeBoostedFromConfig).procedure("db.property*").role("default").user("charlie").map,
      denied(executeBoostedFromConfig).function("dbms.security.listUsers").role("default").user("charlie").map,
      denied(executeBoostedFromConfig).procedure("dbms.security.listUsers").role("default").user("charlie").map,
      granted(executeBoostedFromConfig).function("dbms.security.listUsers").role("PUBLIC").user("charlie").map,
      granted(executeBoostedFromConfig).procedure("dbms.security.listUsers").role("PUBLIC").user("charlie").map
    ))
  }

  test("should show execute boosted privileges for current user from config settings") {
    // GIVEN
    setupUserWithCustomRole(rolename = "funcRole")

    val expected = Set(
      granted(executeBoostedFromConfig).function("db.labels").role("funcRole").user("joe").map,
      granted(executeBoostedFromConfig).procedure("db.labels").role("funcRole").user("joe").map,
      granted(executeBoostedFromConfig).function("db.property*").role("funcRole").user("joe").map,
      granted(executeBoostedFromConfig).procedure("db.property*").role("funcRole").user("joe").map,
      granted(access).role("funcRole").user("joe").map,
      granted(executeBoostedFromConfig).function("dbms.security.listUsers").role("PUBLIC").user("joe").map,
      granted(executeBoostedFromConfig).procedure("dbms.security.listUsers").role("PUBLIC").user("joe").map
    )

    val result = new mutable.HashSet[Map[String, AnyRef]]
    executeOnSystem("joe", "soap", "SHOW USER PRIVILEGES", resultHandler = (row, _) => {
      result.add(asPrivilegesResult(row))
    })
    result should be(expected)
  }

  test("should show all privileges including from config settings") {
    // GIVEN
    setupUserWithCustomRole(rolename = "funcRole")
    setupUserWithCustomRole("alice", rolename = "default", access = false)

    // THEN
    execute("SHOW ALL PRIVILEGES WHERE role IN ['funcRole', 'default', 'custom']").toSet should be(Set(
      granted(executeBoostedFromConfig).function("db.labels").role("funcRole").map,
      granted(executeBoostedFromConfig).function("db.property*").role("funcRole").map,
      granted(access).role("funcRole").map,

      granted(executeBoostedFromConfig).function("db.labels").role("default").map,
      granted(executeBoostedFromConfig).function("*").role("default").map,
      denied(executeBoostedFromConfig).function("db.property*").role("default").map,
      denied(executeBoostedFromConfig).function("dbms.security.listUsers").role("default").map
    ))

    // WHEN restore PUBLIC role
    execute("GRANT EXECUTE FUNCTION * ON DBMS TO PUBLIC")
    execute("GRANT ACCESS ON DEFAULT DATABASE TO PUBLIC")

    // THEN
    execute("SHOW ALL PRIVILEGES").toSet should be(defaultRolePrivileges ++ Set(
      granted(executeBoostedFromConfig).function("db.labels").role("funcRole").map,
      granted(executeBoostedFromConfig).function("db.property*").role("funcRole").map,
      granted(access).role("funcRole").map,

      granted(executeBoostedFromConfig).function("db.labels").role("default").map,
      granted(executeBoostedFromConfig).function("*").role("default").map,
      denied(executeBoostedFromConfig).function("db.property*").role("default").map,
      denied(executeBoostedFromConfig).function("dbms.security.listUsers").role("default").map,

      granted(executeBoostedFromConfig).function("dbms.security.listUsers").role("PUBLIC").map
    ))
  }

  test("should filter privileges from config settings") {
    // GIVEN
    setupUserWithCustomRole(rolename = "funcRole")
    setupUserWithCustomRole("alice", rolename = "default", access = false)
    setupUserWithCustomRole("bob")

    // THEN
    execute("SHOW ALL PRIVILEGES YIELD segment, action, role, access WHERE role IN ['funcRole', 'default', 'custom']").toSet should be(Set(
      Map("segment" -> "FUNCTION(db.labels)", "action" -> EXECUTE_BOOSTED_FROM_CONFIG, "role" -> "funcRole", "access" -> "GRANTED"),
      Map("segment" -> "FUNCTION(db.property*)", "action" -> EXECUTE_BOOSTED_FROM_CONFIG, "role" -> "funcRole", "access" -> "GRANTED"),
      Map("segment" -> "database", "action" -> "access", "role" -> "funcRole", "access" -> "GRANTED"),

      Map("segment" -> "FUNCTION(db.labels)", "action" -> EXECUTE_BOOSTED_FROM_CONFIG, "role" -> "default", "access" -> "GRANTED"),
      Map("segment" -> "FUNCTION(*)", "action" -> EXECUTE_BOOSTED_FROM_CONFIG, "role" -> "default", "access" -> "GRANTED"),
      Map("segment" -> "FUNCTION(db.property*)", "action" -> EXECUTE_BOOSTED_FROM_CONFIG, "role" -> "default", "access" -> "DENIED"),
      Map("segment" -> "FUNCTION(dbms.security.listUsers)", "action" -> EXECUTE_BOOSTED_FROM_CONFIG, "role" -> "default", "access" -> "DENIED"),

      Map("segment" -> "database", "action" -> "access", "role" -> "custom", "access" -> "GRANTED")
    ))
  }

  test("should allow grant/revoke of function privileges to role in dbms.security.functions.roles/default_allowed") {
    // GIVEN
    execute("CREATE ROLE funcRole")
    execute("CREATE ROLE default")

    // WHEN
    execute("GRANT EXECUTE BOOSTED FUNCTION dbms.showCurrentUser ON DBMS TO funcRole, default")

    // THEN
    execute("SHOW ROLE funcRole, default PRIVILEGES").toSet should be(Set(
      granted(executeBoostedFromConfig).function("db.labels").role("funcRole").map,
      granted(executeBoostedFromConfig).function("db.property*").role("funcRole").map,
      granted(executeBoostedFunction).function("dbms.showCurrentUser").role("funcRole").map,

      granted(executeBoostedFromConfig).function("*").role("default").map,
      granted(executeBoostedFromConfig).function("db.labels").role("default").map,
      denied(executeBoostedFromConfig).function("db.property*").role("default").map,
      denied(executeBoostedFromConfig).function("dbms.security.listUsers").role("default").map,
      granted(executeBoostedFunction).function("dbms.showCurrentUser").role("default").map
    ))

    // WHEN
    execute("REVOKE GRANT EXECUTE BOOSTED FUNCTION dbms.showCurrentUser ON DBMS FROM funcRole, default")

    // THEN
    execute("SHOW ROLE funcRole, default PRIVILEGES").toSet should be(Set(
      granted(executeBoostedFromConfig).function("db.labels").role("funcRole").map,
      granted(executeBoostedFromConfig).function("db.property*").role("funcRole").map,

      granted(executeBoostedFromConfig).function("*").role("default").map,
      granted(executeBoostedFromConfig).function("db.labels").role("default").map,
      denied(executeBoostedFromConfig).function("db.property*").role("default").map,
      denied(executeBoostedFromConfig).function("dbms.security.listUsers").role("default").map
    ))
  }

  test("should allow grant/revoke of same function privileges to role in dbms.security.functions.roles/default_allowed") {
    // GIVEN
    execute("CREATE ROLE funcRole")
    execute("CREATE ROLE default")

    // WHEN
    execute("GRANT EXECUTE BOOSTED FUNCTION db.labels ON DBMS TO funcRole, default")

    // THEN
    execute("SHOW ROLE funcRole, default PRIVILEGES").toSet should be(Set(
      granted(executeBoostedFromConfig).function("db.labels").role("funcRole").map,
      granted(executeBoostedFromConfig).function("db.property*").role("funcRole").map,
      granted(executeBoostedFunction).function("db.labels").role("funcRole").map,

      granted(executeBoostedFromConfig).function("*").role("default").map,
      granted(executeBoostedFromConfig).function("db.labels").role("default").map,
      denied(executeBoostedFromConfig).function("db.property*").role("default").map,
      denied(executeBoostedFromConfig).function("dbms.security.listUsers").role("default").map,
      granted(executeBoostedFunction).function("db.labels").role("default").map
    ))

    // WHEN
    execute("REVOKE GRANT EXECUTE BOOSTED FUNCTION db.labels ON DBMS FROM funcRole, default")

    // THEN
    execute("SHOW ROLE funcRole, default PRIVILEGES").toSet should be(Set(
      granted(executeBoostedFromConfig).function("db.labels").role("funcRole").map,
      granted(executeBoostedFromConfig).function("db.property*").role("funcRole").map,

      granted(executeBoostedFromConfig).function("*").role("default").map,
      granted(executeBoostedFromConfig).function("db.labels").role("default").map,
      denied(executeBoostedFromConfig).function("db.property*").role("default").map,
      denied(executeBoostedFromConfig).function("dbms.security.listUsers").role("default").map
    ))
  }

  test("should have nice error message when trying to revoke privilege from dbms.security.functions.roles") {
    // GIVEN
    execute("CREATE ROLE funcRole")

    // WHEN .. THEN
    (the[InvalidArgumentsException] thrownBy {
      execute("REVOKE EXECUTE BOOSTED FUNCTION db.property* ON DBMS FROM funcRole")
    }).getMessage should be
    """Failed to revoke execute_boosted privilege from role 'funcRole': This privilege was granted through the config.
      |Altering the settings 'dbms.security.functions.roles' and 'dbms.security.functions.default_allowed' will affect this privilege after restart.""".stripMargin

    // THEN
    execute("SHOW ROLE funcRole PRIVILEGES").toSet should be(Set(
      granted(executeBoostedFromConfig).function("db.labels").role("funcRole").map,
      granted(executeBoostedFromConfig).function("db.property*").role("funcRole").map,
    ))
  }

  test("should have nice error message when trying to revoke privilege from dbms.security.functions.default_allowed") {
    // GIVEN
    execute("CREATE ROLE default")

    // WHEN .. THEN
    (the[InvalidArgumentsException] thrownBy {
      execute("REVOKE EXECUTE BOOSTED FUNCTION * ON DBMS FROM default")
    }).getMessage should be
    """Failed to revoke execute_boosted privilege from role 'default': This privilege was granted through the config.
      |Altering the settings 'dbms.security.functions.roles' and 'dbms.security.functions.default_allowed' will affect this privilege after restart.""".stripMargin

    // THEN
    execute("SHOW ROLE default PRIVILEGES").toSet should be(Set(
      granted(executeBoostedFromConfig).function("*").role("default").map,
      granted(executeBoostedFromConfig).function("db.labels").role("default").map,
      denied(executeBoostedFromConfig).function("db.property*").role("default").map,
      denied(executeBoostedFromConfig).function("dbms.security.listUsers").role("default").map
    ))
  }
}
