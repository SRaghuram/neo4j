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
import org.neo4j.graphdb.Node
import org.neo4j.graphdb.QueryExecutionException
import org.neo4j.graphdb.config.Setting
import org.neo4j.graphdb.security.AuthorizationViolationException
import org.neo4j.kernel.api.procedure.GlobalProcedures
import org.neo4j.kernel.internal.GraphDatabaseAPI
import org.neo4j.procedure.Name
import org.neo4j.procedure.UserAggregationFunction
import org.neo4j.procedure.UserAggregationResult
import org.neo4j.procedure.UserAggregationUpdate
import org.neo4j.procedure.UserFunction

class ExecuteFunctionPrivilegeAcceptanceTest extends AdministrationCommandAcceptanceTestBase {

  override protected def onNewGraphDatabase(): Unit = {
    val globalProcedures: GlobalProcedures = graphOps.asInstanceOf[GraphDatabaseAPI].getDependencyResolver.resolveDependency(classOf[GlobalProcedures])
    globalProcedures.registerFunction(classOf[TestFunction])
    globalProcedures.registerAggregationFunction(classOf[TestFunction])
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute(s"REVOKE EXECUTE FUNCTION * ON DBMS FROM ${PredefinedRoles.PUBLIC}")
    //    execute("SHOW ROLE PUBLIC PRIVILEGES").toList should be(List(granted(executeBoostedFromConfig).procedure("dbms.security.listUsers").role("PUBLIC").map))
  }

  //noinspection ScalaDeprecation
  override def databaseConfig(): Map[Setting[_], Object] = super.databaseConfig() ++ Map(
    GraphDatabaseSettings.auth_enabled -> TRUE,
    GraphDatabaseSettings.procedure_roles -> "db.labels:funcRole,default;db.property*:funcRole;dbms.security.listUsers:PUBLIC",
    GraphDatabaseSettings.default_allowed -> "default"
  )

  // Privilege tests

  test("should grant execute function privileges") {
    // GIVEN
    execute("CREATE ROLE custom")

    executeFunctionPrivileges.foreach {
      case (command, action) =>
        withClue(s"$command: \n") {
          // WHEN
          execute(s"GRANT $command * ON DBMS TO custom")
          execute(s"GRANT $command test.func, math.*, apoc.*.math.co? ON DBMS TO custom")

          // THEN
          execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
            granted(action).function("*").role("custom").map,
            granted(action).function("test.func").role("custom").map,
            granted(action).function("math.*").role("custom").map,
            granted(action).function("apoc.*.math.co?").role("custom").map
          ))

          // WHEN
          execute(s"REVOKE GRANT $command * ON DBMS FROM custom")
          execute(s"REVOKE GRANT $command test.func, math.*, apoc.*.math.co? ON DBMS FROM custom")

          // THEN
          execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set.empty)
        }
    }
  }

  test("should deny execute function privileges") {
    // GIVEN
    execute("CREATE ROLE custom")

    executeFunctionPrivileges.foreach {
      case (command, action) =>
        withClue(s"$command: \n") {
          // WHEN
          execute(s"DENY $command * ON DBMS TO custom")
          execute(s"DENY $command test.func, math.*, apoc.*.math.co? ON DBMS TO custom")

          // THEN
          execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
            denied(action).function("*").role("custom").map,
            denied(action).function("test.func").role("custom").map,
            denied(action).function("math.*").role("custom").map,
            denied(action).function("apoc.*.math.co?").role("custom").map
          ))

          // WHEN
          execute(s"REVOKE DENY $command * ON DBMS FROM custom")
          execute(s"REVOKE DENY $command test.func, math.*, apoc.*.math.co? ON DBMS FROM custom")

          // THEN
          execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set.empty)
        }
    }
  }

  // Enforcement tests

  // EXECUTE NON AGGREGATION FUNCTIONS

  test("should execute builtin function without any function privileges") {
    // GIVEN
    setupUserAndGraph("foo", "bar")

    // THEN
    executeOnDefault("foo", "bar", "RETURN toLower('A')", resultHandler = (row, _) => {
      row.get("toLower('A')") should equal("a")
    }) should be(1)
  }

  test("should execute builtin (user defined) function without any function privileges") {
    // GIVEN
    setupUserAndGraph("foo", "bar")

    // THEN
    executeOnDefault("foo", "bar", "RETURN date('2020-09-25').year AS year", resultHandler = (row, _) => {
      row.get("year") should equal(2020)
    }) should be(1)
  }

  test("should fail execute user defined function without any function privileges") {
    // GIVEN
    setupUserAndGraph("foo", "bar")

    // THEN
    (the[AuthorizationViolationException] thrownBy {
      executeOnDefault("foo", "bar", "RETURN test.function() AS result")
    }).getMessage should include(FAIL_EXECUTE_FUNC)
  }

  Seq("*", "test.function", "????.func*").foreach(
    function => {
      test(s"should execute user defined function with execute function $function") {
        // GIVEN
        setupUserAndGraph("foo", "bar")

        // WHEN
        execute(s"GRANT EXECUTE FUNCTION $function ON DBMS TO custom")

        // THEN
        executeOnDefault("foo", "bar", "RETURN test.function() AS result", resultHandler = (row, _) => {
          row.get("result") should equal("OK")
        }) should be(1)
      }

      test(s"should execute user defined function with execute boosted function $function") {
        // GIVEN
        setupUserAndGraph("foo", "bar")

        // WHEN
        execute(s"GRANT EXECUTE BOOSTED FUNCTION $function ON DBMS TO custom")

        // THEN
        executeOnDefault("foo", "bar", "RETURN test.function() AS result", resultHandler = (row, _) => {
          row.get("result") should equal("OK")
        }) should be(1)
      }
    }
  )

  Seq(
    ("EXECUTE", "EXECUTE"),
    ("EXECUTE BOOSTED", "EXECUTE"),
    ("EXECUTE BOOSTED", "EXECUTE BOOSTED")
  ).foreach {
    case (granted, denied) =>
      test(s"should fail execute user defined function with granted $granted and denied $denied *") {
        // GIVEN
        setupUserAndGraph("foo", "bar")

        // WHEN
        execute(s"GRANT $granted FUNCTION * ON DBMS TO custom")
        execute(s"DENY $denied FUNCTION * ON DBMS TO custom")

        // THEN
        (the[AuthorizationViolationException] thrownBy {
          executeOnDefault("foo", "bar", "RETURN test.function() AS result")
        }).getMessage should include(FAIL_EXECUTE_FUNC)
      }

      test(s"should fail execute user defined function with granted $granted and denied $denied specific function") {
        // GIVEN
        setupUserAndGraph("foo", "bar")

        // WHEN
        execute(s"GRANT $granted FUNCTION * ON DBMS TO custom")
        execute(s"DENY $denied FUNCTION test.function ON DBMS TO custom")

        // THEN
        (the[AuthorizationViolationException] thrownBy {
          executeOnDefault("foo", "bar", "RETURN test.function() AS result")
        }).getMessage should include(FAIL_EXECUTE_FUNC)
      }

  }

  test("should get default result when executing user defined function without privilege required inside") {
    // GIVEN
    setupUserAndGraph("foo", "bar")

    // WHEN
    execute("GRANT EXECUTE FUNCTION * ON DBMS TO custom")
    execute("GRANT TRAVERSE ON GRAPH * NODES A TO custom")

    // THEN
    executeOnDefault("foo", "bar", "MATCH (a:A) RETURN test.safe.read.property(a, 'prop', 'N/A') AS result", resultHandler = (row, _) => {
      row.get("result") should equal("N/A")
    }) should be(1)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT EXECUTE BOOSTED FUNCTION * ON DBMS TO custom")
    execute("DENY EXECUTE BOOSTED FUNCTION test.safe.* ON DBMS TO custom")

    // THEN
    executeOnDefault("foo", "bar", "MATCH (a:A) RETURN test.safe.read.property(a, 'prop', 'N/A') AS result", resultHandler = (row, _) => {
      row.get("result") should equal("N/A")
    }) should be(1)
  }

  test("should get actual result when executing user defined function with privilege required inside") {
    // GIVEN
    setupUserAndGraph("foo", "bar")

    // WHEN
    execute("GRANT EXECUTE FUNCTION * ON DBMS TO custom")
    execute("GRANT MATCH {prop} ON GRAPH * NODES A TO custom")

    // THEN
    executeOnDefault("foo", "bar", "MATCH (a:A) RETURN test.safe.read.property(a, 'prop', 'N/A') AS result", resultHandler = (row, _) => {
      row.get("result") should equal(1)
    }) should be(1)
  }

  test("should get actual result when executing boosted user defined function without privilege required inside") {
    // GIVEN
    setupUserAndGraph("foo", "bar")

    // WHEN
    execute("GRANT EXECUTE BOOSTED FUNCTION * ON DBMS TO custom")
    execute("GRANT TRAVERSE ON GRAPH * NODES A TO custom")

    // THEN
    executeOnDefault("foo", "bar", "MATCH (a:A) RETURN test.safe.read.property(a, 'prop', 'N/A') AS result", resultHandler = (row, _) => {
      row.get("result") should equal(1)
    }) should be(1)
  }

  test("should fail execute user defined function without privilege required inside") {
    // GIVEN
    setupUserAndGraph("foo", "bar")

    // WHEN
    execute("GRANT EXECUTE FUNCTION * ON DBMS TO custom")
    execute("GRANT TRAVERSE ON GRAPH * NODES A TO custom")

    // THEN
    (the[QueryExecutionException] thrownBy {
      executeOnDefault("foo", "bar", "MATCH (a:A) RETURN test.read.property(a, 'prop') AS result")
    }).getMessage should include("No such property")

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT EXECUTE BOOSTED FUNCTION * ON DBMS TO custom")
    execute("DENY EXECUTE BOOSTED FUNCTION test.* ON DBMS TO custom")

    // THEN
    (the[QueryExecutionException] thrownBy {
      executeOnDefault("foo", "bar", "MATCH (a:A) RETURN test.read.property(a, 'prop') AS result")
    }).getMessage should include("No such property")
  }

  test("should get result when executing user defined function with privilege required inside") {
    // GIVEN
    setupUserAndGraph("foo", "bar")

    // WHEN
    execute("GRANT EXECUTE FUNCTION * ON DBMS TO custom")
    execute("GRANT MATCH {prop} ON GRAPH * NODES A TO custom")

    // THEN
    executeOnDefault("foo", "bar", "MATCH (a:A) RETURN test.read.property(a, 'prop') AS result", resultHandler = (row, _) => {
      row.get("result") should equal(1)
    }) should be(1)
  }

  test("should get result when executing boosted user defined function without privilege required inside") {
    // GIVEN
    setupUserAndGraph("foo", "bar")

    // WHEN
    execute("GRANT EXECUTE BOOSTED FUNCTION * ON DBMS TO custom")
    execute("GRANT TRAVERSE ON GRAPH * NODES A TO custom")

    // THEN
    executeOnDefault("foo", "bar", "MATCH (a:A) RETURN test.read.property(a, 'prop') AS result", resultHandler = (row, _) => {
      row.get("result") should equal(1)
    }) should be(1)
  }

  // EXECUTE AGGREGATION FUNCTIONS

  test("should execute builtin aggregation function without any function privileges") {
    // GIVEN
    setupUserAndGraph("foo", "bar")

    // THEN
    executeOnDefault("foo", "bar", "UNWIND [1,2,3] AS x RETURN sum(x)", resultHandler = (row, _) => {
      row.get("sum(x)") should equal(6)
    }) should be(1)
  }

  test("should fail execute user defined aggregation function without any function privileges") {
    // GIVEN
    setupUserAndGraph("foo", "bar")

    // THEN
    (the[AuthorizationViolationException] thrownBy {
      executeOnDefault("foo", "bar", "UNWIND [1,2,3] AS l RETURN test.return.latest(l) AS result")
    }).getMessage should include(FAIL_EXECUTE_AGG_FUNC)
  }

  Seq("*", "test.return.latest", "????.return.*").foreach(
    function => {
      test(s"should execute user defined aggregation function with execute function $function") {
        // GIVEN
        setupUserAndGraph("foo", "bar")

        // WHEN
        execute(s"GRANT EXECUTE FUNCTION $function ON DBMS TO custom")

        // THEN
        executeOnDefault("foo", "bar", "UNWIND [1,2,3] AS l RETURN test.return.latest(l) AS result", resultHandler = (row, _) => {
          row.get("result") should equal(3)
        }) should be(1)
      }

      test(s"should execute user defined aggregation function with execute boosted function $function") {
        // GIVEN
        setupUserAndGraph("foo", "bar")

        // WHEN
        execute(s"GRANT EXECUTE BOOSTED FUNCTION $function ON DBMS TO custom")

        // THEN
        executeOnDefault("foo", "bar", "UNWIND [1,2,3] AS l RETURN test.return.latest(l) AS result", resultHandler = (row, _) => {
          row.get("result") should equal(3)
        }) should be(1)
      }
    }
  )

  Seq(
    ("EXECUTE", "EXECUTE"),
    ("EXECUTE BOOSTED", "EXECUTE"),
    ("EXECUTE BOOSTED", "EXECUTE BOOSTED")
  ).foreach {
    case (granted, denied) =>
      test(s"should fail execute user defined aggregation function with granted $granted and denied $denied *") {
        // GIVEN
        setupUserAndGraph("foo", "bar")

        // WHEN
        execute(s"GRANT $granted FUNCTION * ON DBMS TO custom")
        execute(s"DENY $denied FUNCTION * ON DBMS TO custom")

        // THEN
        (the[AuthorizationViolationException] thrownBy {
          executeOnDefault("foo", "bar", "UNWIND [1,2,3] AS l RETURN test.return.latest(l) AS result")
        }).getMessage should include(FAIL_EXECUTE_AGG_FUNC)
      }

      test(s"should fail execute user defined aggregation function with granted $granted and denied $denied specific function") {
        // GIVEN
        setupUserAndGraph("foo", "bar")

        // WHEN
        execute(s"GRANT $granted FUNCTION * ON DBMS TO custom")
        execute(s"DENY $denied FUNCTION test.return.latest ON DBMS TO custom")

        // THEN
        (the[AuthorizationViolationException] thrownBy {
          executeOnDefault("foo", "bar", "UNWIND [1,2,3] AS l RETURN test.return.latest(l) AS result")
        }).getMessage should include(FAIL_EXECUTE_AGG_FUNC)
      }
  }

  test("should get default result when executing user defined aggregation function without privilege required inside") {
    // GIVEN
    setupUserAndGraph("foo", "bar")

    // WHEN
    execute("GRANT EXECUTE FUNCTION * ON DBMS TO custom")
    execute("GRANT TRAVERSE ON GRAPH * NODES B TO custom")

    // THEN
    executeOnDefault("foo", "bar", "MATCH (a:B) RETURN test.safe.read.sum.prop(a) AS result", resultHandler = (row, _) => {
      row.get("result") should equal(0)
    }) should be(1)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT EXECUTE BOOSTED FUNCTION * ON DBMS TO custom")
    execute("DENY EXECUTE BOOSTED FUNCTION test.safe.* ON DBMS TO custom")

    // THEN
    executeOnDefault("foo", "bar", "MATCH (a:B) RETURN test.safe.read.sum.prop(a) AS result", resultHandler = (row, _) => {
      row.get("result") should equal(0)
    }) should be(1)
  }

  test("should get actual result when executing user defined aggregation function with privilege required inside") {
    // GIVEN
    setupUserAndGraph("foo", "bar")

    // WHEN
    execute("GRANT EXECUTE FUNCTION * ON DBMS TO custom")
    execute("GRANT MATCH {prop} ON GRAPH * NODES B TO custom")

    // THEN
    executeOnDefault("foo", "bar", "MATCH (a:B) RETURN test.safe.read.sum.prop(a) AS result", resultHandler = (row, _) => {
      row.get("result") should equal(6)
    }) should be(1)
  }

  test("should get actual result when executing boosted user defined aggregation function without privilege required inside") {
    // GIVEN
    setupUserAndGraph("foo", "bar")

    // WHEN
    execute("GRANT EXECUTE BOOSTED FUNCTION * ON DBMS TO custom")
    execute("GRANT TRAVERSE ON GRAPH * NODES B TO custom")

    // THEN
    executeOnDefault("foo", "bar", "MATCH (a:B) RETURN test.safe.read.sum.prop(a) AS result", resultHandler = (row, _) => {
      row.get("result") should equal(6)
    }) should be(1)
  }

  test("should fail execute user defined aggregation function without privilege required inside") {
    // GIVEN
    setupUserAndGraph("foo", "bar")

    // WHEN
    execute("GRANT EXECUTE FUNCTION * ON DBMS TO custom")
    execute("GRANT TRAVERSE ON GRAPH * NODES B TO custom")

    // THEN
    (the[QueryExecutionException] thrownBy {
      executeOnDefault("foo", "bar", "MATCH (a:B) RETURN test.read.sum.prop(a) AS result")
    }).getMessage should include("No such property")

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT EXECUTE BOOSTED FUNCTION * ON DBMS TO custom")
    execute("DENY EXECUTE BOOSTED FUNCTION test.read.* ON DBMS TO custom")

    // THEN
    (the[QueryExecutionException] thrownBy {
      executeOnDefault("foo", "bar", "MATCH (a:B) RETURN test.read.sum.prop(a) AS result")
    }).getMessage should include("No such property")
  }

  test("should get result when executing user defined aggregation function with privilege required inside") {
    // GIVEN
    setupUserAndGraph("foo", "bar")

    // WHEN
    execute("GRANT EXECUTE FUNCTION * ON DBMS TO custom")
    execute("GRANT MATCH {prop} ON GRAPH * NODES B TO custom")

    // THEN
    executeOnDefault("foo", "bar", "MATCH (a:B) RETURN test.read.sum.prop(a) AS result", resultHandler = (row, _) => {
      row.get("result") should equal(6)
    }) should be(1)
  }

  test("should get result when executing boosted user defined aggregation function without privilege required inside") {
    // GIVEN
    setupUserAndGraph("foo", "bar")

    // WHEN
    execute("GRANT EXECUTE BOOSTED FUNCTION * ON DBMS TO custom")
    execute("GRANT TRAVERSE ON GRAPH * NODES B TO custom")

    // THEN
    executeOnDefault("foo", "bar", "MATCH (a:B) RETURN test.read.sum.prop(a) AS result", resultHandler = (row, _) => {
      row.get("result") should equal(6)
    }) should be(1)
  }

  // ALL ON DBMS



  // Helper methods

  def setupUserAndGraph( username: String = "joe", password: String = "soap", rolename: String = "custom" ): Unit = {
    super.setupUserWithCustomRole( username, password, rolename )

    selectDatabase(GraphDatabaseSettings.DEFAULT_DATABASE_NAME)
    execute("CREATE (:A:B {prop: 1}), (:B {prop: 2}), (:B {prop: 3})")

    selectDatabase(SYSTEM_DATABASE_NAME)
  }
}

class TestFunction {
  @UserFunction( "test.function" )
  def function(): String = "OK"

  @UserFunction( "test.read.property" )
  def readProperty(@Name("node") node: Node, @Name("propertyKey") propertyKey: String): AnyRef = node.getProperty(propertyKey)

  @UserFunction( "test.safe.read.property" )
  def readProperty(@Name("node") node: Node, @Name("propertyKey") propertyKey: String, @Name("defaultValue") defaultValue: AnyRef ): AnyRef =
    node.getProperty(propertyKey, defaultValue)

  @UserAggregationFunction( "test.return.latest" )
  def myAggFunc: ReturnLatest = new ReturnLatest

  @UserAggregationFunction( "test.read.sum.prop" )
  def myAggregator: Aggregator = new Aggregator

  @UserAggregationFunction( "test.safe.read.sum.prop" )
  def mySafeAggregator: SafeAggregator = new SafeAggregator
}

object TestFunction {
  def apply(): TestFunction = new TestFunction()
}

class ReturnLatest {
  var latest: Long = 0

  @UserAggregationUpdate
  def update(@Name("value") value: Long): Unit = latest = value

  @UserAggregationResult
  def result: Long = latest
}

object ReturnLatest {
  def apply(): Aggregator = new Aggregator()
}

class Aggregator {
  var sum: Long = 0

  @UserAggregationUpdate
  def update(@Name("node") node: Node): Unit = {
    sum = sum + node.getProperty("prop").asInstanceOf[Long]
  }

  @UserAggregationResult
  def result: Long = sum
}

object Aggregator {
  def apply(): Aggregator = new Aggregator()
}

class SafeAggregator {
  var sum: Long = 0

  @UserAggregationUpdate
  def update(@Name("node") node: Node): Unit = {
    sum = sum + node.getProperty("prop", 0L).asInstanceOf[Long]
  }

  @UserAggregationResult
  def result: Long = sum
}

object SafeAggregator {
  def apply(): SafeAggregator = new SafeAggregator()
}
