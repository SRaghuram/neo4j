/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.internal.cypher.acceptance

import java.lang.Boolean.TRUE

import com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME
import org.neo4j.graphdb.config.Setting
import org.neo4j.graphdb.security.AuthorizationViolationException

import scala.collection.mutable

class ExecuteProcedurePrivilegeAcceptanceTest extends AdministrationCommandAcceptanceTestBase {

  override protected def onNewGraphDatabase(): Unit = {
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute(s"REVOKE ACCESS ON DEFAULT DATABASE FROM ${PredefinedRoles.PUBLIC}")
    execute(s"REVOKE EXECUTE PROCEDURES * ON DBMS FROM ${PredefinedRoles.PUBLIC}")
    execute("SHOW ROLE PUBLIC PRIVILEGES").toList should be(List(granted(adminAction("execute_boosted_temp")).procedure("dbms.security.listUsers").role("PUBLIC").map))
  }

  //noinspection ScalaDeprecation
  override def databaseConfig(): Map[Setting[_], Object] = super.databaseConfig() ++ Map(
    GraphDatabaseSettings.auth_enabled -> TRUE,
    GraphDatabaseSettings.procedure_roles -> "db.labels:procRole,default;db.property*:procRole;dbms.security.listUsers:PUBLIC",
    GraphDatabaseSettings.default_allowed -> "default"
  )

  // Privilege tests

  test("should grant execute procedure privileges") {
    // GIVEN
    execute("CREATE ROLE custom")

    executePrivileges.foreach {
      case (command, action) =>
        withClue(s"$command: \n") {
          // WHEN
          execute(s"GRANT $command * ON DBMS TO custom")
          execute(s"GRANT $command test.proc, math.*, apoc.*.math.co? ON DBMS TO custom")

          // THEN
          execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
            granted(action).procedure("*").role("custom").map,
            granted(action).procedure("test.proc").role("custom").map,
            granted(action).procedure("math.*").role("custom").map,
            granted(action).procedure("apoc.*.math.co?").role("custom").map
          ))

          // WHEN
          execute(s"REVOKE GRANT $command * ON DBMS FROM custom")
          execute(s"REVOKE GRANT $command test.proc, math.*, apoc.*.math.co? ON DBMS FROM custom")

          // THEN
          execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set.empty)
        }
    }

    withClue(s"EXECUTE ADMIN PROCEDURES: \n") {
      // WHEN
      execute(s"GRANT EXECUTE ADMIN PROCEDURES ON DBMS TO custom")

      // THEN
      execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
        granted(adminAction("execute_admin")).role("custom").map
      ))

      // WHEN
      execute(s"REVOKE GRANT EXECUTE ADMIN PROCEDURES ON DBMS FROM custom")

      // THEN
      execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set.empty)
    }
  }

  test("should deny execute procedure privileges") {
    // GIVEN
    execute("CREATE ROLE custom")

    executePrivileges.foreach {
      case (command, action) =>
        withClue(s"$command: \n") {
          // WHEN
          execute(s"DENY $command * ON DBMS TO custom")
          execute(s"DENY $command test.proc, math.*, apoc.*.math.co? ON DBMS TO custom")

          // THEN
          execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
            denied(action).procedure("*").role("custom").map,
            denied(action).procedure("test.proc").role("custom").map,
            denied(action).procedure("math.*").role("custom").map,
            denied(action).procedure("apoc.*.math.co?").role("custom").map
          ))

          // WHEN
          execute(s"REVOKE DENY $command * ON DBMS FROM custom")
          execute(s"REVOKE DENY $command test.proc, math.*, apoc.*.math.co? ON DBMS FROM custom")

          // THEN
          execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set.empty)
        }
    }

    withClue(s"EXECUTE ADMIN PROCEDURES: \n") {
      // WHEN
      execute(s"DENY EXECUTE ADMIN PROCEDURES ON DBMS TO custom")

      // THEN
      execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
        denied(adminAction("execute_admin")).role("custom").map
      ))

      // WHEN
      execute(s"REVOKE DENY EXECUTE ADMIN PROCEDURES ON DBMS FROM custom")

      // THEN
      execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set.empty)
    }

  }

  test("should show execute boosted privileges for roles from config settings") {
    // GIVEN
    execute("CREATE ROLE procRole")
    execute("CREATE ROLE default")

    // THEN
    execute("SHOW ROLE procRole, default PRIVILEGES").toSet should be(Set(
      granted(adminAction("execute_boosted_temp")).procedure("db.labels").role("procRole").map,
      granted(adminAction("execute_boosted_temp")).procedure("db.labels").role("default").map,
      granted(adminAction("execute_boosted_temp")).procedure("db.property*").role("procRole").map,
      granted(adminAction("execute_boosted_temp")).procedure("*").role("default").map,
      denied(adminAction("execute_boosted_temp")).procedure("db.property*").role("default").map,
      denied(adminAction("execute_boosted_temp")).procedure("dbms.security.listUsers").role("default").map
    ))
  }

  test("should show execute boosted privileges for user from config settings") {
    // GIVEN
    setupUserWithCustomRole(rolename = "procRole")

    // THEN
    execute("SHOW USER joe PRIVILEGES").toSet should be(Set(
      granted(adminAction("execute_boosted_temp")).procedure("db.labels").role("procRole").user("joe").map,
      granted(adminAction("execute_boosted_temp")).procedure("db.property*").role("procRole").user("joe").map,
      granted(access).role("procRole").user("joe").map,

      granted(adminAction("execute_boosted_temp")).procedure("dbms.security.listUsers").role("PUBLIC").user("joe").map
    ))

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT EXECUTE PROCEDURE dbms.* ON DBMS TO procRole")
    execute("DENY EXECUTE BOOSTED PROCEDURE apoc.* ON DBMS TO procRole")

    // THEN
    execute("SHOW USER joe PRIVILEGES").toSet should be(Set(
      granted(adminAction("execute_boosted_temp")).procedure("db.labels").role("procRole").user("joe").map,
      granted(adminAction("execute_boosted_temp")).procedure("db.property*").role("procRole").user("joe").map,
      granted(access).role("procRole").user("joe").map,
      granted(executeProcedure).procedure("dbms.*").role("procRole").user("joe").map,
      denied(executeBoosted).procedure("apoc.*").role("procRole").user("joe").map,

      granted(adminAction("execute_boosted_temp")).procedure("dbms.security.listUsers").role("PUBLIC").user("joe").map
    ))
  }

  test("should show execute boosted privileges for multiple users from config settings") {
    // GIVEN
    setupUserWithCustomRole(rolename = "procRole")
    setupUserWithCustomRole("alice", rolename = "default", access = false)
    setupUserWithCustomRole("bob")

    execute("CREATE USER charlie SET PASSWORD 'abc123'")
    execute("GRANT ROLE procRole, default TO charlie")

    // THEN
    execute("SHOW USER joe, $user, bob, charlie PRIVILEGES", Map("user" -> "alice")).toSet should be(Set(
      granted(adminAction("execute_boosted_temp")).procedure("db.labels").role("procRole").user("joe").map,
      granted(adminAction("execute_boosted_temp")).procedure("db.property*").role("procRole").user("joe").map,
      granted(access).role("procRole").user("joe").map,
      granted(adminAction("execute_boosted_temp")).procedure("dbms.security.listUsers").role("PUBLIC").user("joe").map,

      granted(adminAction("execute_boosted_temp")).procedure("db.labels").role("default").user("alice").map,
      granted(adminAction("execute_boosted_temp")).procedure("*").role("default").user("alice").map,
      denied(adminAction("execute_boosted_temp")).procedure("db.property*").role("default").user("alice").map,
      denied(adminAction("execute_boosted_temp")).procedure("dbms.security.listUsers").role("default").user("alice").map,
      granted(adminAction("execute_boosted_temp")).procedure("dbms.security.listUsers").role("PUBLIC").user("alice").map,

      granted(access).role("custom").user("bob").map,
      granted(adminAction("execute_boosted_temp")).procedure("dbms.security.listUsers").role("PUBLIC").user("bob").map,

      granted(adminAction("execute_boosted_temp")).procedure("db.labels").role("procRole").user("charlie").map,
      granted(adminAction("execute_boosted_temp")).procedure("db.property*").role("procRole").user("charlie").map,
      granted(access).role("procRole").user("charlie").map,
      granted(adminAction("execute_boosted_temp")).procedure("db.labels").role("default").user("charlie").map,
      granted(adminAction("execute_boosted_temp")).procedure("*").role("default").user("charlie").map,
      denied(adminAction("execute_boosted_temp")).procedure("db.property*").role("default").user("charlie").map,
      denied(adminAction("execute_boosted_temp")).procedure("dbms.security.listUsers").role("default").user("charlie").map,
      granted(adminAction("execute_boosted_temp")).procedure("dbms.security.listUsers").role("PUBLIC").user("charlie").map
    ))
  }

  test("should show execute boosted privileges for current user from config settings") {
    // GIVEN
    setupUserWithCustomRole(rolename = "procRole")

    val expected = Set(
      granted(adminAction("execute_boosted_temp")).procedure("db.labels").role("procRole").user("joe").map,
      granted(adminAction("execute_boosted_temp")).procedure("db.property*").role("procRole").user("joe").map,
      granted(access).role("procRole").user("joe").map,
      granted(adminAction("execute_boosted_temp")).procedure("dbms.security.listUsers").role("PUBLIC").user("joe").map
    )

    val result = new mutable.HashSet[Map[String, AnyRef]]
    executeOnSystem("joe", "soap", "SHOW USER PRIVILEGES", resultHandler = (row, _) => {
      result.add(asPrivilegesResult(row))
    })
    result should be(expected)
  }

  test("should show all privileges including from config settings") {
    // GIVEN
    setupUserWithCustomRole(rolename = "procRole")
    setupUserWithCustomRole("alice", rolename = "default", access = false)

    // THEN
    execute("SHOW ALL PRIVILEGES WHERE role IN ['procRole', 'default', 'custom']").toSet should be(Set(
      granted(adminAction("execute_boosted_temp")).procedure("db.labels").role("procRole").map,
      granted(adminAction("execute_boosted_temp")).procedure("db.property*").role("procRole").map,
      granted(access).role("procRole").map,

      granted(adminAction("execute_boosted_temp")).procedure("db.labels").role("default").map,
      granted(adminAction("execute_boosted_temp")).procedure("*").role("default").map,
      denied(adminAction("execute_boosted_temp")).procedure("db.property*").role("default").map,
      denied(adminAction("execute_boosted_temp")).procedure("dbms.security.listUsers").role("default").map
    ))

    // WHEN restore PUBLIC role
    execute("GRANT EXECUTE PROCEDURE * ON DBMS TO PUBLIC")
    execute("GRANT ACCESS ON DEFAULT DATABASE TO PUBLIC")

    // THEN
    execute("SHOW ALL PRIVILEGES").toSet should be(defaultRolePrivileges ++ Set(
      granted(adminAction("execute_boosted_temp")).procedure("db.labels").role("procRole").map,
      granted(adminAction("execute_boosted_temp")).procedure("db.property*").role("procRole").map,
      granted(access).role("procRole").map,

      granted(adminAction("execute_boosted_temp")).procedure("db.labels").role("default").map,
      granted(adminAction("execute_boosted_temp")).procedure("*").role("default").map,
      denied(adminAction("execute_boosted_temp")).procedure("db.property*").role("default").map,
      denied(adminAction("execute_boosted_temp")).procedure("dbms.security.listUsers").role("default").map,

      granted(adminAction("execute_boosted_temp")).procedure("dbms.security.listUsers").role("PUBLIC").map
    ))
  }

  test("should filter privileges from config settings") {
    // GIVEN
    setupUserWithCustomRole(rolename = "procRole")
    setupUserWithCustomRole("alice", rolename = "default", access = false)
    setupUserWithCustomRole("bob")

    // THEN
    execute("SHOW ALL PRIVILEGES YIELD segment, action, role, access WHERE role IN ['procRole', 'default', 'custom']").toSet should be(Set(
      Map("segment" -> "PROCEDURE(db.labels)", "action" -> "execute_boosted_temp", "role" -> "procRole", "access" -> "GRANTED"),
      Map("segment" -> "PROCEDURE(db.property*)", "action" -> "execute_boosted_temp", "role" -> "procRole", "access" -> "GRANTED"),
      Map("segment" -> "database", "action" -> "access", "role" -> "procRole", "access" -> "GRANTED"),

      Map("segment" -> "PROCEDURE(db.labels)", "action" -> "execute_boosted_temp", "role" -> "default", "access" -> "GRANTED"),
      Map("segment" -> "PROCEDURE(*)", "action" -> "execute_boosted_temp", "role" -> "default", "access" -> "GRANTED"),
      Map("segment" -> "PROCEDURE(db.property*)", "action" -> "execute_boosted_temp", "role" -> "default", "access" -> "DENIED"),
      Map("segment" -> "PROCEDURE(dbms.security.listUsers)", "action" -> "execute_boosted_temp", "role" -> "default", "access" -> "DENIED"),

      Map("segment" -> "database", "action" -> "access", "role" -> "custom", "access" -> "GRANTED")
    ))
  }

  // Enforcement tests

  // EXECUTE PROCEDURE

  test("should execute procedure with execute procedure *") {
    // GIVEN
    setupUserAndGraph("foo", "bar")

    // WHEN
    execute("GRANT TRAVERSE ON GRAPH * NODES A TO custom")
    execute("GRANT EXECUTE PROCEDURE * ON DBMS TO custom")

    // THEN
    executeOnDefault("foo", "bar", "CALL db.labels", resultHandler = (row, _) => {
      row.get("label") should equal("A")
    }) should be(1)
  }

  test("should execute dbms procedure when granted execution") {
    // GIVEN
    setupUserAndGraph("foo", "bar")

    // WHEN
    execute("GRANT EXECUTE PROCEDURE dbms.showCurrentUser ON DBMS TO custom")

    // THEN
    executeOnDefault("foo", "bar", "CALL dbms.showCurrentUser()", resultHandler = (row, _) => {
      row.get("username") should equal("foo")
    }) should be(1)
  }

  test("should execute procedures when granted execution through globbing") {
    // GIVEN
    setupUserAndGraph("foo", "bar")

    // WHEN
    execute("GRANT EXECUTE PROCEDURE dbms.show* ON DBMS TO custom")

    // THEN
    executeOnDefault("foo", "bar", "CALL dbms.showCurrentUser()", resultHandler = (row, _) => {
      row.get("username") should equal("foo")
    }) should be(1)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT TRAVERSE ON GRAPH * NODES A TO custom")
    execute("GRANT EXECUTE PROCEDURE d?.l?bels ON DBMS TO custom")

    // THEN
    executeOnDefault("foo", "bar", "CALL db.labels", resultHandler = (row, _) => {
      row.get("label") should equal("A")
    }) should be(1)
  }

  test("should fail execute procedure with no privileges") {
    // GIVEN
    setupUserAndGraph("foo", "bar")

    // WHEN
    execute("GRANT TRAVERSE ON GRAPH * NODES A TO custom")

    // THEN
    (the[AuthorizationViolationException] thrownBy {
      executeOnDefault("foo", "bar", "CALL db.labels")
    }).getMessage should include(FAIL_EXECUTE_PROC)
  }

  test("should fail execute dbms procedure with no privileges") {
    // GIVEN
    setupUserAndGraph("foo", "bar")

    // THEN
    (the[AuthorizationViolationException] thrownBy {
      executeOnDefault("foo", "bar", "CALL dbms.showCurrentUser()")
    }).getMessage should include(FAIL_EXECUTE_PROC)
  }

  test("should fail execute procedure with deny procedure") {
    // GIVEN
    setupUserAndGraph("foo", "bar")

    // WHEN
    execute("GRANT TRAVERSE ON GRAPH * NODES A TO custom")
    execute("GRANT EXECUTE PROCEDURE * ON DBMS TO custom")
    execute("DENY EXECUTE PROCEDURE db.labels ON DBMS TO custom")

    // THEN
    (the[AuthorizationViolationException] thrownBy {
      executeOnDefault("foo", "bar", "CALL db.labels")
    }).getMessage should include(FAIL_EXECUTE_PROC)
  }

  test("should fail execute dbms procedure with deny procedure") {
    // GIVEN
    setupUserAndGraph("foo", "bar")

    // WHEN
    execute("GRANT EXECUTE PROCEDURE * ON DBMS TO custom")
    execute("DENY EXECUTE PROCEDURE dbms.showCurrentUser ON DBMS TO custom")

    // THEN
    (the[AuthorizationViolationException] thrownBy {
      executeOnDefault("foo", "bar", "CALL dbms.showCurrentUser()")
    }).getMessage should include(FAIL_EXECUTE_PROC)
  }

  // EXECUTE BOOSTED PROCEDURE

  test("should execute procedure with execute boosted procedure *") {
    // GIVEN
    setupUserAndGraph("foo", "bar")

    // WHEN
    execute("GRANT TRAVERSE ON GRAPH * NODES A TO custom")
    execute("GRANT EXECUTE BOOSTED PROCEDURE * ON DBMS TO custom")

    val expected = Seq("A", "B")
    // THEN
    executeOnDefault("foo", "bar", "CALL db.labels() YIELD label RETURN label ORDER BY label ASC", resultHandler = (row, idx) => {
      row.get("label") should equal(expected(idx))
    }) should be(2)
  }

  test("should execute procedure with execute boosted procedure through globbing") {
    // GIVEN
    setupUserAndGraph("foo", "bar")

    // WHEN
    execute("GRANT TRAVERSE ON GRAPH * NODES A TO custom")
    execute("GRANT EXECUTE BOOSTED PROCEDURE *.labels ON DBMS TO custom")

    val expected = Seq("A", "B")
    // THEN
    executeOnDefault("foo", "bar", "CALL db.labels() YIELD label RETURN label ORDER BY label ASC", resultHandler = (row, idx) => {
      row.get("label") should equal(expected(idx))
    }) should be(2)
  }

  test("should execute procedure without boosting when denied execute boosted") {
    // GIVEN
    setupUserAndGraph("foo", "bar")

    // WHEN
    execute("GRANT TRAVERSE ON GRAPH * NODES A TO custom")
    execute("GRANT EXECUTE PROCEDURE * ON DBMS TO custom")
    execute("DENY EXECUTE BOOSTED PROCEDURE db.labels ON DBMS TO custom")

    // THEN
    executeOnDefault("foo", "bar", "CALL db.labels", resultHandler = (row, _) => {
      row.get("label") should equal("A")
    }) should be(1)
  }

  test("should execute procedure without boosting when denied execute boosted through globbing") {
    // GIVEN
    setupUserAndGraph("foo", "bar")

    // WHEN
    execute("GRANT TRAVERSE ON GRAPH * NODES A TO custom")
    execute("GRANT EXECUTE PROCEDURE * ON DBMS TO custom")
    execute("DENY EXECUTE BOOSTED PROCEDURE db.la?els ON DBMS TO custom")

    // THEN
    executeOnDefault("foo", "bar", "CALL db.labels", resultHandler = (row, _) => {
      row.get("label") should equal("A")
    }) should be(1)
  }

  test("should fail execute procedure when denied execute granted execute boosted") {
    // GIVEN
    setupUserAndGraph("foo", "bar")

    // WHEN
    execute("GRANT TRAVERSE ON GRAPH * NODES A TO custom")
    execute("GRANT EXECUTE BOOSTED PROCEDURE * ON DBMS TO custom")
    execute("DENY EXECUTE PROCEDURE db.labels ON DBMS TO custom")

    // THEN
    (the[AuthorizationViolationException] thrownBy {
      executeOnDefault("foo", "bar", "CALL db.labels")
    }).getMessage should include(FAIL_EXECUTE_PROC)
  }

  // EXECUTE ADMIN PROCEDURES

  test("should execute admin procedure with EXECUTE ADMIN PROCEDURES") {
    // GIVEN
    setupUserAndGraph("foo", "bar")

    // WHEN
    execute("GRANT EXECUTE ADMIN PROCEDURES ON DBMS TO custom")

    // THEN
    executeOnDefault("foo", "bar", "CALL dbms.listConfig('dbms.security.auth_enabled')") should be(1)
  }

  test("should fail execute admin procedure with EXECUTE ADMIN PROCEDURES and DENIED BOOSTED specific") {
    // GIVEN
    setupUserAndGraph("foo", "bar")

    // WHEN
    execute("GRANT EXECUTE ADMIN PROCEDURES ON DBMS TO custom")
    execute("DENY EXECUTE BOOSTED PROCEDURE dbms.listConfig ON DBMS TO custom")

    // THEN
    (the[AuthorizationViolationException] thrownBy {
      executeOnDefault("foo", "bar", "CALL dbms.listConfig('dbms.security.auth_enabled')")
    }).getMessage should include(FAIL_EXECUTE_PROC)
  }

  test("should fail execute admin procedure with EXECUTE ADMIN PROCEDURES and DENIED EXECUTE specific") {
    // GIVEN
    setupUserAndGraph("foo", "bar")

    // WHEN
    execute("GRANT EXECUTE ADMIN PROCEDURES ON DBMS TO custom")
    execute("DENY EXECUTE PROCEDURE dbms.listConfig ON DBMS TO custom")

    // THEN
    (the[AuthorizationViolationException] thrownBy {
      executeOnDefault("foo", "bar", "CALL dbms.listConfig('dbms.security.auth_enabled')")
    }).getMessage should include(FAIL_EXECUTE_PROC)
  }

  test("should fail execute admin procedure with EXECUTE ADMIN PROCEDURES and DENIED BOOSTED specific and GRANT EXECUTE") {
    // GIVEN
    setupUserAndGraph("foo", "bar")

    // WHEN
    execute("GRANT EXECUTE ADMIN PROCEDURES ON DBMS TO custom")
    execute("DENY EXECUTE BOOSTED PROCEDURE dbms.listConfig ON DBMS TO custom")
    execute("GRANT EXECUTE PROCEDURE dbms.listConfig ON DBMS TO custom")

    // THEN
    (the[AuthorizationViolationException] thrownBy {
      executeOnDefault("foo", "bar", "CALL dbms.listConfig('dbms.security.auth_enabled')")
    }).getMessage should include(FAIL_EXECUTE_ADMIN_PROC)
  }

  test("should fail execute admin procedure with EXECUTE BOOSTED specific and DENIED ADMIN PROCEDURES") {
    // GIVEN
    setupUserAndGraph("foo", "bar")

    // WHEN
    execute("GRANT EXECUTE BOOSTED PROCEDURE dbms.listConfig ON DBMS TO custom")
    execute("DENY EXECUTE ADMIN PROCEDURES ON DBMS TO custom")

    // THEN
    (the[AuthorizationViolationException] thrownBy {
      executeOnDefault("foo", "bar", "CALL dbms.listConfig('dbms.security.auth_enabled')")
    }).getMessage should include(FAIL_EXECUTE_PROC)
  }

  test("should fail execute admin procedure with EXECUTE BOOSTED specific and DENIED ADMIN PROCEDURES and GRANT EXECUTE") {
    // GIVEN
    setupUserAndGraph("foo", "bar")

    // WHEN
    execute("GRANT EXECUTE BOOSTED PROCEDURE dbms.listConfig ON DBMS TO custom")
    execute("DENY EXECUTE ADMIN PROCEDURES ON DBMS TO custom")
    execute("GRANT EXECUTE PROCEDURE dbms.listConfig ON DBMS TO custom")

    // THEN
    (the[AuthorizationViolationException] thrownBy {
      executeOnDefault("foo", "bar", "CALL dbms.listConfig('dbms.security.auth_enabled')")
    }).getMessage should include(FAIL_EXECUTE_ADMIN_PROC)
  }

  test("should execute security procedure with EXECUTE ADMIN PROCEDURES") {
    // GIVEN
    setupUserAndGraph("foo", "bar")

    // WHEN
    execute("GRANT EXECUTE ADMIN PROCEDURES ON DBMS TO custom")

    // THEN
    executeOnSystem("foo", "bar", "CALL dbms.security.listUsersForRole('PUBLIC')") should be(2)
  }

  test("should execute list users for roles procedure with EXECUTE ADMIN PROCEDURES even if DENIED SHOW ROLES") {
    // GIVEN
    setupUserAndGraph("foo", "bar")

    // WHEN
    execute("GRANT EXECUTE ADMIN PROCEDURES ON DBMS TO custom")
    execute("DENY SHOW ROLE ON DBMS TO custom")

    // THEN
    executeOnSystem("foo", "bar", "CALL dbms.security.listUsersForRole('PUBLIC')") should be(2)
  }

  test("should fail execute non-admin procedure with only EXECUTE ADMIN PROCEDURES") {
    // GIVEN
    setupUserAndGraph("foo", "bar")

    // WHEN
    execute("GRANT EXECUTE ADMIN PROCEDURES ON DBMS TO custom")

    // THEN
    (the[AuthorizationViolationException] thrownBy {
      executeOnDefault("foo", "bar", "CALL db.labels")
    }).getMessage should include(FAIL_EXECUTE_PROC)

    // THEN
    (the[AuthorizationViolationException] thrownBy {
      executeOnSystem("foo", "bar", "CALL db.labels")
    }).getMessage should include(FAIL_EXECUTE_PROC)
  }

  // Executing @Admin procedures

  test("should execute admin procedure with ALL ON DBMS") {
    // GIVEN
    setupUserAndGraph("foo", "bar")

    // WHEN
    execute("GRANT ALL ON DBMS TO custom")

    // THEN
    executeOnDefault("foo", "bar", "CALL dbms.listConfig('dbms.security.auth_enabled')") should be(1)
  }

  test("should fail execute admin procedure with execute procedure") {
    // GIVEN
    setupUserAndGraph("foo", "bar")

    // WHEN
    execute("GRANT EXECUTE PROCEDURE dbms.listConfig ON DBMS TO custom")

    // THEN
    (the[AuthorizationViolationException] thrownBy {
      executeOnDefault("foo", "bar", "CALL dbms.listConfig('dbms.security.auth_enabled')")
    }).getMessage should include(FAIL_EXECUTE_ADMIN_PROC)
  }

  test("should execute admin procedure with execute boosted procedure") {
    // GIVEN
    setupUserAndGraph("foo", "bar")

    // WHEN
    execute("GRANT EXECUTE BOOSTED PROCEDURE dbms.listConfig ON DBMS TO custom")

    // THEN
    executeOnDefault("foo", "bar", "CALL dbms.listConfig('dbms.security.auth_enabled')") should be(1)
  }

  test("should fail execute admin procedure with execute boosted and denied execute procedure") {
    // GIVEN
    setupUserAndGraph("foo", "bar")

    // WHEN
    execute("GRANT EXECUTE BOOSTED PROCEDURE dbms.listConfig ON DBMS TO custom")
    execute("DENY EXECUTE PROCEDURE dbms.listConfig ON DBMS TO custom")

    // THEN
    (the[AuthorizationViolationException] thrownBy {
      executeOnDefault("foo", "bar", "CALL dbms.listConfig('dbms.security.auth_enabled')")
    }).getMessage should include(FAIL_EXECUTE_PROC)
  }

  // EXECUTE BOOSTED PROCEDURE from config settings

  test("executing procedure with boosted privileges from procedure_roles config") {
    // GIVEN
    setupUserAndGraph(rolename = "procRole")

    // THEN
    val expected = Seq("A", "B")

    executeOnDefault("joe", "soap", "CALL db.labels() YIELD label RETURN label ORDER BY label ASC", resultHandler = (row, idx) => {
      row.get("label") should equal(expected(idx))
    }) should be(2)
  }

  test("should not be boosted when not matching procedure_roles config") {
    // GIVEN
    setupUserAndGraph(rolename = "procRole")

    // THEN
    withClue("Without EXECUTE privilege") {
      (the[AuthorizationViolationException] thrownBy {
        executeOnDefault("joe", "soap", "CALL db.relationshipTypes()")
      }).getMessage should include(FAIL_EXECUTE_PROC)
    }

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT EXECUTE PROCEDURE * ON DBMS TO procRole")

    // THEN
    withClue("With EXECUTE privilege") {
      executeOnDefault("joe", "soap", "CALL db.relationshipTypes()") should be(0)
    }
  }

  test("executing procedure with boosted privileges from procedure_roles and default_allowed config") {
    // GIVEN
    setupUserAndGraph(rolename = "default")

    // THEN
    val expected = Seq("A", "B")

    executeOnDefault("joe", "soap", "CALL db.labels() YIELD label RETURN label ORDER BY label ASC", resultHandler = (row, idx) => {
      row.get("label") should equal(expected(idx))
    }) should be(2)
  }

  test("executing procedure with boosted privileges from default_allowed config") {
    // GIVEN
    setupUserAndGraph(rolename = "default")

    // THEN
    executeOnDefault("joe", "soap", "CALL db.relationshipTypes()", resultHandler = (row, _) => {
      row.get("relationshipType") should equal("REL")
    }) should be(1)
  }

  test("should not be boosted for default_allowed when procedure matching procedure_roles config") {
    // GIVEN
    setupUserAndGraph(rolename = "default")

    // THEN
    withClue("Without EXECUTE privilege") {
      (the[AuthorizationViolationException] thrownBy {
        executeOnDefault("joe", "soap", "CALL db.propertyKeys()")
      }).getMessage should include(FAIL_EXECUTE_PROC)
    }

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT EXECUTE PROCEDURE * ON DBMS TO default")

    // THEN
    withClue("With EXECUTE privilege") {
      executeOnDefault("joe", "soap", "CALL db.propertyKeys()") should be(0)
    }
  }

  test("should respect combined privileges from config and system graph") {
    // GIVEN
    setupUserAndGraph(rolename = "procRole")
    execute("DENY EXECUTE PROCEDURE * ON DBMS TO procRole")

    // THEN
    withClue("With DENY EXECUTE privilege") {
      (the[AuthorizationViolationException] thrownBy {
        executeOnDefault("joe", "soap", "CALL db.propertyKeys()")
      }).getMessage should include(FAIL_EXECUTE_PROC)
    }

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("REVOKE DENY EXECUTE PROCEDURE * ON DBMS FROM procRole")
    execute("DENY EXECUTE BOOSTED PROCEDURE * ON DBMS TO procRole")

    // THEN
    withClue("With DENY EXECUTE BOOSTED privilege") {
      (the[AuthorizationViolationException] thrownBy {
        executeOnDefault("joe", "soap", "CALL db.propertyKeys()")
      }).getMessage should include(FAIL_EXECUTE_PROC)
    }

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT EXECUTE PROCEDURE * ON DBMS TO procRole")

    // THEN
    withClue("With GRANT EXECUTE and DENY EXECUTE BOOSTED privilege") {
      executeOnDefault("joe", "soap", "CALL db.propertyKeys()") should be(0)
    }
  }

  test("should get privilege for PUBLIC from config") {
    // GIVEN
    execute("CREATE USER joe SET PASSWORD 'soap' CHANGE NOT REQUIRED")

    val expected = Seq("joe", "neo4j")

    // WHEN
    executeOnSystem("joe", "soap", "CALL dbms.security.listUsers() YIELD username RETURN username ORDER BY username", resultHandler = (row, idx) => {
      row.get("username") should be(expected(idx))
    }) should be(2)
  }

  // Helper methods

  def setupUserAndGraph( username: String = "joe", password: String = "soap", rolename: String = "custom" ): Unit = {
    super.setupUserWithCustomRole( username, password, rolename )

    selectDatabase(GraphDatabaseSettings.DEFAULT_DATABASE_NAME)
    execute(
      """
        |CREATE (:A)-[:REL]->(:B {a: 1})
        |""".stripMargin)

    selectDatabase(SYSTEM_DATABASE_NAME)
  }
}
