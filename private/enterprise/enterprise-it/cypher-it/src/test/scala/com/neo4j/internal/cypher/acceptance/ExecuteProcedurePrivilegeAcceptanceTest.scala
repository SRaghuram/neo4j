/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.internal.cypher.acceptance

import java.lang.Boolean.TRUE

import com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.PUBLIC
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME
import org.neo4j.graphdb.config.Setting
import org.neo4j.graphdb.security.AuthorizationViolationException

class ExecuteProcedurePrivilegeAcceptanceTest extends ExecutePrivilegeAcceptanceTestBase {

  override protected def onNewGraphDatabase(): Unit = {
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute(s"REVOKE ACCESS ON HOME DATABASE FROM $PUBLIC")
    execute(s"REVOKE EXECUTE PROCEDURES * ON DBMS FROM $PUBLIC")
    execute(s"REVOKE EXECUTE FUNCTIONS * ON DBMS FROM $PUBLIC")
    execute(s"SHOW ROLE $PUBLIC PRIVILEGES").toSet should be(grantedFromConfig("dbms.security.listUsers", PUBLIC))
  }

  //noinspection ScalaDeprecation
  override def databaseConfig(): Map[Setting[_], Object] = super.databaseConfig() ++ Map(
    GraphDatabaseSettings.auth_enabled -> TRUE,
    GraphDatabaseSettings.procedure_roles -> s"$labelsProcGlob:$roleName2,$defaultRole;$propertyProcGlob:$roleName2;$listUsersProcGlob:$PUBLIC",
    GraphDatabaseSettings.default_allowed -> defaultRole
  )

  // Privilege tests

  test("should grant execute procedure privileges") {
    // GIVEN
    execute(s"CREATE ROLE $roleName")

    executeProcedurePrivileges.foreach {
      case (command, action) =>
        withClue(s"$command: \n") {
          // WHEN
          execute(s"GRANT $command * ON DBMS TO $roleName")
          execute(s"GRANT $command test.proc, math.*, apoc.*.math.co? ON DBMS TO $roleName")

          // THEN
          execute(s"SHOW ROLE $roleName PRIVILEGES").toSet should be(Set(
            granted(action).procedure("*").role(roleName).map,
            granted(action).procedure("test.proc").role(roleName).map,
            granted(action).procedure("math.*").role(roleName).map,
            granted(action).procedure("apoc.*.math.co?").role(roleName).map
          ))

          // WHEN
          execute(s"REVOKE GRANT $command * ON DBMS FROM $roleName")
          execute(s"REVOKE GRANT $command test.proc, math.*, apoc.*.math.co? ON DBMS FROM $roleName")

          // THEN
          execute(s"SHOW ROLE $roleName PRIVILEGES").toSet should be(Set.empty)
        }
    }

    withClue(s"EXECUTE ADMIN PROCEDURES: \n") {
      // WHEN
      execute(s"GRANT EXECUTE ADMIN PROCEDURES ON DBMS TO $roleName")

      // THEN
      execute(s"SHOW ROLE $roleName PRIVILEGES").toSet should be(Set(
        granted(adminAction("execute_admin")).role(roleName).map
      ))

      // WHEN
      execute(s"REVOKE GRANT EXECUTE ADMIN PROCEDURES ON DBMS FROM $roleName")

      // THEN
      execute(s"SHOW ROLE $roleName PRIVILEGES").toSet should be(Set.empty)
    }
  }

  test("should deny execute procedure privileges") {
    // GIVEN
    execute(s"CREATE ROLE $roleName")

    executeProcedurePrivileges.foreach {
      case (command, action) =>
        withClue(s"$command: \n") {
          // WHEN
          execute(s"DENY $command * ON DBMS TO $roleName")
          execute(s"DENY $command test.proc, math.*, apoc.*.math.co? ON DBMS TO $roleName")

          // THEN
          execute(s"SHOW ROLE $roleName PRIVILEGES").toSet should be(Set(
            denied(action).procedure("*").role(roleName).map,
            denied(action).procedure("test.proc").role(roleName).map,
            denied(action).procedure("math.*").role(roleName).map,
            denied(action).procedure("apoc.*.math.co?").role(roleName).map
          ))

          // WHEN
          execute(s"REVOKE DENY $command * ON DBMS FROM $roleName")
          execute(s"REVOKE DENY $command test.proc, math.*, apoc.*.math.co? ON DBMS FROM $roleName")

          // THEN
          execute(s"SHOW ROLE $roleName PRIVILEGES").toSet should be(Set.empty)
        }
    }

    withClue(s"EXECUTE ADMIN PROCEDURES: \n") {
      // WHEN
      execute(s"DENY EXECUTE ADMIN PROCEDURES ON DBMS TO $roleName")

      // THEN
      execute(s"SHOW ROLE $roleName PRIVILEGES").toSet should be(Set(
        denied(adminAction("execute_admin")).role(roleName).map
      ))

      // WHEN
      execute(s"REVOKE DENY EXECUTE ADMIN PROCEDURES ON DBMS FROM $roleName")

      // THEN
      execute(s"SHOW ROLE $roleName PRIVILEGES").toSet should be(Set.empty)
    }

  }

  // Enforcement tests

  // EXECUTE PROCEDURE

  test("should execute procedure with execute procedure *") {
    // GIVEN
    setupUserAndGraph()

    // WHEN
    execute(s"GRANT TRAVERSE ON GRAPH * NODES A TO $roleName")
    execute(s"GRANT EXECUTE PROCEDURE * ON DBMS TO $roleName")

    // THEN
    executeOnDBMSDefault(username, password, "CALL db.labels", resultHandler = (row, _) => {
      row.get("label") should equal("A")
    }) should be(1)
  }

  test("should execute dbms procedure when granted execution") {
    // GIVEN
    setupUserAndGraph()

    // WHEN
    execute(s"GRANT EXECUTE PROCEDURE dbms.showCurrentUser ON DBMS TO $roleName")

    // THEN
    executeOnDBMSDefault(username, password, "CALL dbms.showCurrentUser()", resultHandler = (row, _) => {
      row.get("username") should equal(username)
    }) should be(1)
  }

  test("should execute procedures when granted execution through globbing") {
    // GIVEN
    setupUserAndGraph()

    // WHEN
    execute(s"GRANT EXECUTE PROCEDURE dbms.show* ON DBMS TO $roleName")

    // THEN
    executeOnDBMSDefault(username, password, "CALL dbms.showCurrentUser()", resultHandler = (row, _) => {
      row.get("username") should equal(username)
    }) should be(1)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute(s"GRANT TRAVERSE ON GRAPH * NODES A TO $roleName")
    execute(s"GRANT EXECUTE PROCEDURE d?.l?bels ON DBMS TO $roleName")

    // THEN
    executeOnDBMSDefault(username, password, "CALL db.labels", resultHandler = (row, _) => {
      row.get("label") should equal("A")
    }) should be(1)
  }

  test("should fail execute procedure with no privileges") {
    // GIVEN
    setupUserAndGraph()

    // WHEN
    execute(s"GRANT TRAVERSE ON GRAPH * NODES A TO $roleName")

    // THEN
    (the[AuthorizationViolationException] thrownBy {
      executeOnDBMSDefault(username, password, "CALL db.labels")
    }).getMessage should include(FAIL_EXECUTE_PROC)
  }

  test("should fail execute dbms procedure with no privileges") {
    // GIVEN
    setupUserAndGraph()

    // THEN
    (the[AuthorizationViolationException] thrownBy {
      executeOnDBMSDefault(username, password, "CALL dbms.showCurrentUser()")
    }).getMessage should include(FAIL_EXECUTE_PROC)
  }

  test("should fail execute procedure with deny procedure") {
    // GIVEN
    setupUserAndGraph()

    // WHEN
    execute(s"GRANT TRAVERSE ON GRAPH * NODES A TO $roleName")
    execute(s"GRANT EXECUTE PROCEDURE * ON DBMS TO $roleName")
    execute(s"DENY EXECUTE PROCEDURE db.labels ON DBMS TO $roleName")

    // THEN
    (the[AuthorizationViolationException] thrownBy {
      executeOnDBMSDefault(username, password, "CALL db.labels")
    }).getMessage should include(FAIL_EXECUTE_PROC)
  }

  test("should fail execute dbms procedure with deny procedure") {
    // GIVEN
    setupUserAndGraph()

    // WHEN
    execute(s"GRANT EXECUTE PROCEDURE * ON DBMS TO $roleName")
    execute(s"DENY EXECUTE PROCEDURE dbms.showCurrentUser ON DBMS TO $roleName")

    // THEN
    (the[AuthorizationViolationException] thrownBy {
      executeOnDBMSDefault(username, password, "CALL dbms.showCurrentUser()")
    }).getMessage should include(FAIL_EXECUTE_PROC)
  }

  // EXECUTE BOOSTED PROCEDURE

  test("should execute procedure with execute boosted procedure *") {
    // GIVEN
    setupUserAndGraph()

    // WHEN
    execute(s"GRANT TRAVERSE ON GRAPH * NODES A TO $roleName")
    execute(s"GRANT EXECUTE BOOSTED PROCEDURE * ON DBMS TO $roleName")

    val expected = Seq("A", "B")
    // THEN
    executeOnDBMSDefault(username, password, "CALL db.labels() YIELD label RETURN label ORDER BY label ASC", resultHandler = (row, idx) => {
      row.get("label") should equal(expected(idx))
    }) should be(2)
  }

  test("should execute procedure with execute boosted procedure through globbing") {
    // GIVEN
    setupUserAndGraph()

    // WHEN
    execute(s"GRANT TRAVERSE ON GRAPH * NODES A TO $roleName")
    execute(s"GRANT EXECUTE BOOSTED PROCEDURE *.labels ON DBMS TO $roleName")

    val expected = Seq("A", "B")
    // THEN
    executeOnDBMSDefault(username, password, "CALL db.labels() YIELD label RETURN label ORDER BY label ASC", resultHandler = (row, idx) => {
      row.get("label") should equal(expected(idx))
    }) should be(2)
  }

  test("should execute procedure without boosting when denied execute boosted") {
    // GIVEN
    setupUserAndGraph()

    // WHEN
    execute(s"GRANT TRAVERSE ON GRAPH * NODES A TO $roleName")
    execute(s"GRANT EXECUTE PROCEDURE * ON DBMS TO $roleName")
    execute(s"DENY EXECUTE BOOSTED PROCEDURE db.labels ON DBMS TO $roleName")

    // THEN
    executeOnDBMSDefault(username, password, "CALL db.labels", resultHandler = (row, _) => {
      row.get("label") should equal("A")
    }) should be(1)
  }

  test("should execute procedure without boosting when denied execute boosted through globbing") {
    // GIVEN
    setupUserAndGraph()

    // WHEN
    execute(s"GRANT TRAVERSE ON GRAPH * NODES A TO $roleName")
    execute(s"GRANT EXECUTE PROCEDURE * ON DBMS TO $roleName")
    execute(s"DENY EXECUTE BOOSTED PROCEDURE db.la?els ON DBMS TO $roleName")

    // THEN
    executeOnDBMSDefault(username, password, "CALL db.labels", resultHandler = (row, _) => {
      row.get("label") should equal("A")
    }) should be(1)
  }

  test("should fail execute procedure when denied execute granted execute boosted") {
    // GIVEN
    setupUserAndGraph()

    // WHEN
    execute(s"GRANT TRAVERSE ON GRAPH * NODES A TO $roleName")
    execute(s"GRANT EXECUTE BOOSTED PROCEDURE * ON DBMS TO $roleName")
    execute(s"DENY EXECUTE PROCEDURE db.labels ON DBMS TO $roleName")

    // THEN
    (the[AuthorizationViolationException] thrownBy {
      executeOnDBMSDefault(username, password, "CALL db.labels")
    }).getMessage should include(FAIL_EXECUTE_PROC)
  }

  test("should fail execute procedure when denied and granted execute boosted") {
    // GIVEN
    setupUserAndGraph()

    // WHEN
    execute(s"GRANT TRAVERSE ON GRAPH * NODES A TO $roleName")
    execute(s"GRANT EXECUTE BOOSTED PROCEDURE * ON DBMS TO $roleName")
    execute(s"DENY EXECUTE BOOSTED PROCEDURE db.labels ON DBMS TO $roleName")

    // THEN
    (the[AuthorizationViolationException] thrownBy {
      executeOnDBMSDefault(username, password, "CALL db.labels")
    }).getMessage should include(FAIL_EXECUTE_PROC)
  }

  // EXECUTE ADMIN PROCEDURES

  test("should execute admin procedure with EXECUTE ADMIN PROCEDURES") {
    // GIVEN
    setupUserAndGraph()

    // WHEN
    execute(s"GRANT EXECUTE ADMIN PROCEDURES ON DBMS TO $roleName")

    // THEN
    executeOnDBMSDefault(username, password, "CALL dbms.listConfig('dbms.security.auth_enabled')") should be(1)
  }

  test("should fail execute admin procedure with EXECUTE ADMIN PROCEDURES and DENIED BOOSTED specific") {
    // GIVEN
    setupUserAndGraph()

    // WHEN
    execute(s"GRANT EXECUTE ADMIN PROCEDURES ON DBMS TO $roleName")
    execute(s"DENY EXECUTE BOOSTED PROCEDURE dbms.listConfig ON DBMS TO $roleName")

    // THEN
    (the[AuthorizationViolationException] thrownBy {
      executeOnDBMSDefault(username, password, "CALL dbms.listConfig('dbms.security.auth_enabled')")
    }).getMessage should include(FAIL_EXECUTE_PROC)
  }

  test("should fail execute admin procedure with EXECUTE ADMIN PROCEDURES and DENIED EXECUTE specific") {
    // GIVEN
    setupUserAndGraph()

    // WHEN
    execute(s"GRANT EXECUTE ADMIN PROCEDURES ON DBMS TO $roleName")
    execute(s"DENY EXECUTE PROCEDURE dbms.listConfig ON DBMS TO $roleName")

    // THEN
    (the[AuthorizationViolationException] thrownBy {
      executeOnDBMSDefault(username, password, "CALL dbms.listConfig('dbms.security.auth_enabled')")
    }).getMessage should include(FAIL_EXECUTE_PROC)
  }

  test("should fail execute admin procedure with EXECUTE ADMIN PROCEDURES and DENIED BOOSTED specific and GRANT EXECUTE") {
    // GIVEN
    setupUserAndGraph()

    // WHEN
    execute(s"GRANT EXECUTE ADMIN PROCEDURES ON DBMS TO $roleName")
    execute(s"DENY EXECUTE BOOSTED PROCEDURE dbms.listConfig ON DBMS TO $roleName")
    execute(s"GRANT EXECUTE PROCEDURE dbms.listConfig ON DBMS TO $roleName")

    // THEN
    (the[AuthorizationViolationException] thrownBy {
      executeOnDBMSDefault(username, password, "CALL dbms.listConfig('dbms.security.auth_enabled')")
    }).getMessage should include(FAIL_EXECUTE_ADMIN_PROC)
  }

  test("should fail execute admin procedure with EXECUTE BOOSTED specific and DENIED ADMIN PROCEDURES") {
    // GIVEN
    setupUserAndGraph()

    // WHEN
    execute(s"GRANT EXECUTE BOOSTED PROCEDURE dbms.listConfig ON DBMS TO $roleName")
    execute(s"DENY EXECUTE ADMIN PROCEDURES ON DBMS TO $roleName")

    // THEN
    (the[AuthorizationViolationException] thrownBy {
      executeOnDBMSDefault(username, password, "CALL dbms.listConfig('dbms.security.auth_enabled')")
    }).getMessage should include(FAIL_EXECUTE_PROC)
  }

  test("should fail execute admin procedure with EXECUTE BOOSTED specific and DENIED ADMIN PROCEDURES and GRANT EXECUTE") {
    // GIVEN
    setupUserAndGraph()

    // WHEN
    execute(s"GRANT EXECUTE BOOSTED PROCEDURE dbms.listConfig ON DBMS TO $roleName")
    execute(s"DENY EXECUTE ADMIN PROCEDURES ON DBMS TO $roleName")
    execute(s"GRANT EXECUTE PROCEDURE dbms.listConfig ON DBMS TO $roleName")

    // THEN
    (the[AuthorizationViolationException] thrownBy {
      executeOnDBMSDefault(username, password, "CALL dbms.listConfig('dbms.security.auth_enabled')")
    }).getMessage should include(FAIL_EXECUTE_ADMIN_PROC)
  }

  test("should execute security procedure with EXECUTE ADMIN PROCEDURES") {
    // GIVEN
    setupUserAndGraph()

    // WHEN
    execute(s"GRANT EXECUTE ADMIN PROCEDURES ON DBMS TO $roleName")

    // THEN
    executeOnSystem(username, password, s"CALL dbms.security.listUsersForRole('$PUBLIC')") should be(2)
  }

  test("should execute list users for roles procedure with EXECUTE ADMIN PROCEDURES even if DENIED SHOW ROLES") {
    // GIVEN
    setupUserAndGraph()

    // WHEN
    execute(s"GRANT EXECUTE ADMIN PROCEDURES ON DBMS TO $roleName")
    execute(s"DENY SHOW ROLE ON DBMS TO $roleName")

    // THEN
    executeOnSystem(username, password, s"CALL dbms.security.listUsersForRole('$PUBLIC')") should be(2)
  }

  test("should fail execute non-admin procedure with only EXECUTE ADMIN PROCEDURES") {
    // GIVEN
    setupUserAndGraph()

    // WHEN
    execute(s"GRANT EXECUTE ADMIN PROCEDURES ON DBMS TO $roleName")

    // THEN
    (the[AuthorizationViolationException] thrownBy {
      executeOnDBMSDefault(username, password, "CALL db.labels")
    }).getMessage should include(FAIL_EXECUTE_PROC)

    // THEN
    (the[AuthorizationViolationException] thrownBy {
      executeOnSystem(username, password, "CALL db.labels")
    }).getMessage should include(FAIL_EXECUTE_PROC)
  }

  // Executing @Admin procedures

  test("should fail execute admin procedure with execute procedure") {
    // GIVEN
    setupUserAndGraph()

    // WHEN
    execute(s"GRANT EXECUTE PROCEDURE dbms.listConfig ON DBMS TO $roleName")

    // THEN
    (the[AuthorizationViolationException] thrownBy {
      executeOnDBMSDefault(username, password, "CALL dbms.listConfig('dbms.security.auth_enabled')")
    }).getMessage should include(FAIL_EXECUTE_ADMIN_PROC)
  }

  test("should execute admin procedure with execute boosted procedure") {
    // GIVEN
    setupUserAndGraph()

    // WHEN
    execute(s"GRANT EXECUTE BOOSTED PROCEDURE dbms.listConfig ON DBMS TO $roleName")

    // THEN
    executeOnDBMSDefault(username, password, "CALL dbms.listConfig('dbms.security.auth_enabled')") should be(1)
  }

  test("should fail execute admin procedure with execute boosted and denied execute procedure") {
    // GIVEN
    setupUserAndGraph()

    // WHEN
    execute(s"GRANT EXECUTE BOOSTED PROCEDURE dbms.listConfig ON DBMS TO $roleName")
    execute(s"DENY EXECUTE PROCEDURE dbms.listConfig ON DBMS TO $roleName")

    // THEN
    (the[AuthorizationViolationException] thrownBy {
      executeOnDBMSDefault(username, password, "CALL dbms.listConfig('dbms.security.auth_enabled')")
    }).getMessage should include(FAIL_EXECUTE_PROC)
  }

  // ALL ON DBMS

  test("should execute any procedure boosted with ALL ON DBMS") {
    // GIVEN
    setupUserAndGraph()

    // WHEN
    execute(s"GRANT TRAVERSE ON GRAPH * NODES A TO $roleName")
    execute(s"GRANT ALL ON DBMS TO $roleName")

    // THEN
    val expected = Seq("A", "B")
    // THEN
    executeOnDBMSDefault(username, password, "CALL db.labels() YIELD label RETURN label ORDER BY label ASC", resultHandler = (row, idx) => {
      row.get("label") should equal(expected(idx))
    }) should be(2)
  }

  test("should execute admin procedure with ALL ON DBMS") {
    // GIVEN
    setupUserAndGraph()

    // WHEN
    execute(s"GRANT ALL ON DBMS TO $roleName")

    // THEN
    executeOnDBMSDefault(username, password, "CALL dbms.listConfig('dbms.security.auth_enabled')") should be(1)
  }

  test("should execute procedure without boosting when granted all on dbms denied execute boosted") {
    // GIVEN
    setupUserAndGraph()

    // WHEN
    execute(s"GRANT TRAVERSE ON GRAPH * NODES A TO $roleName")
    execute(s"GRANT ALL ON DBMS TO $roleName")
    execute(s"DENY EXECUTE BOOSTED PROCEDURE db.labels ON DBMS TO $roleName")

    // THEN
    executeOnDBMSDefault(username, password, "CALL db.labels", resultHandler = (row, _) => {
      row.get("label") should equal("A")
    }) should be(1)
  }

  test("should fail execute procedure when denied all on dbms granted execute") {
    // GIVEN
    setupUserAndGraph()

    // WHEN
    execute(s"GRANT TRAVERSE ON GRAPH * NODES A TO $roleName")
    execute(s"GRANT EXECUTE PROCEDURE db.labels ON DBMS TO $roleName")
    execute(s"DENY ALL ON DBMS TO $roleName")

    // THEN
    (the[AuthorizationViolationException] thrownBy {
      executeOnDBMSDefault(username, password, "CALL db.labels")
    }).getMessage should include(FAIL_EXECUTE_PROC)
  }

  // EXECUTE BOOSTED PROCEDURE from config settings

  test("executing procedure with boosted privileges from procedure_roles config") {
    // GIVEN
    setupUserAndGraph(rolename = roleName2)

    // THEN
    val expected = Seq("A", "B")

    executeOnDBMSDefault(username, password, "CALL db.labels() YIELD label RETURN label ORDER BY label ASC", resultHandler = (row, idx) => {
      row.get("label") should equal(expected(idx))
    }) should be(2)
  }

  test("should not be boosted when not matching procedure_roles config") {
    // GIVEN
    setupUserAndGraph(rolename = roleName2)

    // THEN
    withClue("Without EXECUTE privilege") {
      (the[AuthorizationViolationException] thrownBy {
        executeOnDBMSDefault(username, password, "CALL db.relationshipTypes()")
      }).getMessage should include(FAIL_EXECUTE_PROC)
    }

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute(s"GRANT EXECUTE PROCEDURE * ON DBMS TO $roleName2")

    // THEN
    withClue("With EXECUTE privilege") {
      executeOnDBMSDefault(username, password, "CALL db.relationshipTypes()") should be(0)
    }
  }

  test("executing procedure with boosted privileges from procedure_roles and default_allowed config") {
    // GIVEN
    setupUserAndGraph(rolename = defaultRole)

    // THEN
    val expected = Seq("A", "B")

    executeOnDBMSDefault(username, password, "CALL db.labels() YIELD label RETURN label ORDER BY label ASC", resultHandler = (row, idx) => {
      row.get("label") should equal(expected(idx))
    }) should be(2)
  }

  test("executing procedure with boosted privileges from default_allowed config") {
    // GIVEN
    setupUserAndGraph(rolename = defaultRole)

    // THEN
    executeOnDBMSDefault(username, password, "CALL db.relationshipTypes()", resultHandler = (row, _) => {
      row.get("relationshipType") should equal("REL")
    }) should be(1)
  }

  test("should not be boosted for default_allowed when procedure matching procedure_roles config") {
    // GIVEN
    setupUserAndGraph(rolename = defaultRole)

    // THEN
    withClue("Without EXECUTE privilege") {
      (the[AuthorizationViolationException] thrownBy {
        executeOnDBMSDefault(username, password, "CALL db.propertyKeys()")
      }).getMessage should include(FAIL_EXECUTE_PROC)
    }

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute(s"GRANT EXECUTE PROCEDURE * ON DBMS TO $defaultRole")

    // THEN
    withClue("With EXECUTE privilege") {
      executeOnDBMSDefault(username, password, "CALL db.propertyKeys()") should be(0)
    }
  }

  test("should respect combined privileges from config and system graph") {
    // GIVEN
    setupUserAndGraph(rolename = roleName2)
    execute(s"DENY EXECUTE PROCEDURE * ON DBMS TO $roleName2")

    // THEN
    withClue("With DENY EXECUTE privilege") {
      (the[AuthorizationViolationException] thrownBy {
        executeOnDBMSDefault(username, password, "CALL db.propertyKeys()")
      }).getMessage should include(FAIL_EXECUTE_PROC)
    }

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute(s"REVOKE DENY EXECUTE PROCEDURE * ON DBMS FROM $roleName2")
    execute(s"DENY EXECUTE BOOSTED PROCEDURE * ON DBMS TO $roleName2")

    // THEN
    withClue("With DENY EXECUTE BOOSTED privilege") {
      (the[AuthorizationViolationException] thrownBy {
        executeOnDBMSDefault(username, password, "CALL db.propertyKeys()")
      }).getMessage should include(FAIL_EXECUTE_PROC)
    }

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute(s"GRANT EXECUTE PROCEDURE * ON DBMS TO $roleName2")

    // THEN
    withClue("With GRANT EXECUTE and DENY EXECUTE BOOSTED privilege") {
      executeOnDBMSDefault(username, password, "CALL db.propertyKeys()") should be(0)
    }
  }

  test("should get privilege for PUBLIC from config") {
    // GIVEN
    execute(s"CREATE USER $username SET PASSWORD '$password' CHANGE NOT REQUIRED")

    val expected = Seq(username, defaultUsername)

    // WHEN
    executeOnSystem(username, password, "CALL dbms.security.listUsers() YIELD username RETURN username ORDER BY username", resultHandler = (row, idx) => {
      row.get("username") should be(expected(idx))
    }) should be(2)
  }

  // Helper methods

  def setupUserAndGraph( username: String = username, password: String = password, rolename: String = roleName ): Unit = {
    super.setupUserWithCustomRole( username, password, rolename )

    selectDatabase(GraphDatabaseSettings.DEFAULT_DATABASE_NAME)
    execute(
      """
        |CREATE (:A)-[:REL]->(:B {a: 1})
        |""".stripMargin)

    selectDatabase(SYSTEM_DATABASE_NAME)
  }
}
