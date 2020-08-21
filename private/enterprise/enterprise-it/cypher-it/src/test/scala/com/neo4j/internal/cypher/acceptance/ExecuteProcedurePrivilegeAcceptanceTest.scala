/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.internal.cypher.acceptance

import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.graphdb.QueryExecutionException
import org.neo4j.graphdb.security.AuthorizationViolationException

class ExecuteProcedurePrivilegeAcceptanceTest extends AdministrationCommandAcceptanceTestBase {

  override protected def onNewGraphDatabase(): Unit = clearPublicRole()

  // Privilege tests
  test("should grant execute procedure privileges") {
    // GIVEN
    execute("CREATE ROLE custom")

    executePrivileges.foreach {
      case (command, action) =>
        withClue(s"$command: \n") {
          // WHEN
          execute(s"GRANT $command * ON DBMS TO custom")
          execute(s"GRANT $command test.proc, math.* ON DBMS TO custom")

          // THEN
          execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
            granted(action).procedure("*").role("custom").map,
            granted(action).procedure("test.proc").role("custom").map,
            granted(action).procedure("math.*").role("custom").map
          ))

          // WHEN
          execute(s"REVOKE GRANT $command * ON DBMS FROM custom")
          execute(s"REVOKE GRANT $command test.proc, math.* ON DBMS FROM custom")

          // THEN
          execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set.empty)
        }
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
          execute(s"DENY $command test.proc, math.* ON DBMS TO custom")

          // THEN
          execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
            denied(action).procedure("*").role("custom").map,
            denied(action).procedure("test.proc").role("custom").map,
            denied(action).procedure("math.*").role("custom").map
          ))

          // WHEN
          execute(s"REVOKE DENY $command * ON DBMS FROM custom")
          execute(s"REVOKE DENY $command test.proc, math.* ON DBMS FROM custom")

          // THEN
          execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set.empty)
        }
    }
  }

  // Enforcement tests

  def setupUserAndGraph( username: String = "joe", password: String = "soap" ): Unit = {
    super.setupUserWithCustomRole( username, password )
    execute("REVOKE EXECUTE PROCEDURE * ON DBMS FROM PUBLIC")

    selectDatabase(GraphDatabaseSettings.DEFAULT_DATABASE_NAME)
    execute(
      """
        |CREATE (:A)
        |CREATE (:B)
        |""".stripMargin)

    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
  }

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
    the[QueryExecutionException] thrownBy {
      executeOnDefault("foo", "bar", "CALL dbms.listConfig('dbms.security.auth_enabled')")
    } should have message "Permission denied."
  }

  test("should fail execute procedure with no privileges") {
    // GIVEN
    setupUserAndGraph("foo", "bar")

    // WHEN
    execute("GRANT TRAVERSE ON GRAPH * NODES A TO custom")

    // THEN
    the[AuthorizationViolationException] thrownBy {
      executeOnDefault("foo", "bar", "CALL db.labels")
    } should have message "Permission denied."
  }

  test("should fail execute dbms procedure with no privileges") {
    // GIVEN
    setupUserAndGraph("foo", "bar")

    // THEN
    the[AuthorizationViolationException] thrownBy {
      executeOnDefault("foo", "bar", "CALL dbms.showCurrentUser()")
    } should have message "Permission denied."
  }

  test("should fail execute procedure with deny procedure") {
    // GIVEN
    setupUserAndGraph("foo", "bar")

    // WHEN
    execute("GRANT TRAVERSE ON GRAPH * NODES A TO custom")
    execute("GRANT EXECUTE PROCEDURE * ON DBMS TO custom")
    execute("DENY EXECUTE PROCEDURE db.labels ON DBMS TO custom")

    // THEN
    the[AuthorizationViolationException] thrownBy {
      executeOnDefault("foo", "bar", "CALL db.labels")
    } should have message "Permission denied."
  }

  test("should fail execute dbms procedure with deny procedure") {
    // GIVEN
    setupUserAndGraph("foo", "bar")

    // WHEN
    execute("GRANT EXECUTE PROCEDURE * ON DBMS TO custom")
    execute("DENY EXECUTE PROCEDURE dbms.showCurrentUser ON DBMS TO custom")

    // THEN
    the[AuthorizationViolationException] thrownBy {
      executeOnDefault("foo", "bar", "CALL dbms.showCurrentUser()")
    } should have message "Permission denied."
  }
}
