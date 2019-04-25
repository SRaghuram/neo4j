/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.internal.cypher.acceptance

import java.util.Optional

import com.neo4j.cypher.CommercialGraphDatabaseTestSupport
import com.neo4j.server.security.enterprise.systemgraph._
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.cypher._
import org.neo4j.cypher.internal.javacompat.GraphDatabaseCypherService
import org.neo4j.dbms.database.{DatabaseContext, DatabaseManager}
import org.neo4j.kernel.database.DatabaseId
import org.neo4j.logging.Log
import org.neo4j.server.security.auth.SecureHasher
import org.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles
import org.neo4j.server.security.systemgraph.ContextSwitchingSystemGraphQueryExecutor

class SecurityCypherAcceptanceTest extends ExecutionEngineFunSuite with CommercialGraphDatabaseTestSupport {
  private val defaultRoles = Set(
    Map("role" -> PredefinedRoles.ADMIN, "is_built_in" -> true),
    Map("role" -> PredefinedRoles.ARCHITECT, "is_built_in" -> true),
    Map("role" -> PredefinedRoles.PUBLISHER, "is_built_in" -> true),
    Map("role" -> PredefinedRoles.EDITOR, "is_built_in" -> true),
    Map("role" -> PredefinedRoles.READER, "is_built_in" -> true)
  )
  private val defaultRolesWithUsers = Set(
    Map("role" -> PredefinedRoles.ADMIN, "is_built_in" -> true, "member" -> "neo4j"),
    Map("role" -> PredefinedRoles.ARCHITECT, "is_built_in" -> true, "member" -> null),
    Map("role" -> PredefinedRoles.PUBLISHER, "is_built_in" -> true, "member" -> null),
    Map("role" -> PredefinedRoles.EDITOR, "is_built_in" -> true, "member" -> null),
    Map("role" -> PredefinedRoles.READER, "is_built_in" -> true, "member" -> null)
  )
  private val foo = Map("role" -> "foo", "is_built_in" -> false)
  private val bar = Map("role" -> "bar", "is_built_in" -> false)

  test("should list all roles") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE foo")

    // WHEN
    val result = execute("SHOW ROLES")

    // THEN
    result.toSet should be(defaultRoles ++ Set(foo))
  }

  test("should list populated default roles") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)

    // WHEN
    val result = execute("SHOW POPULATED ROLES")

    // THEN
    result.toSet should be(Set(Map("role" -> PredefinedRoles.ADMIN, "is_built_in" -> true)))
  }

  test("should list populated roles") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    // TODO update to use actual DDL when available for creating users and assigning to roles
    systemGraphInnerQueryExecutor.executeQueryLong(
      """CREATE (r:Role {name:'foo'})
        |CREATE (u1:User {name:'Bar',credentials:'neo',passwordChangeRequired:false,suspended:false})-[:HAS_ROLE]->(r)
        |CREATE (u2:User {name:'Baz',credentials:'NEO',passwordChangeRequired:false,suspended:false})-[:HAS_ROLE]->(r)
      """.stripMargin)

    // WHEN
    val result = execute("SHOW POPULATED ROLES")

    // THEN
    result.toSet should be(Set(Map("role" -> PredefinedRoles.ADMIN, "is_built_in" -> true), foo))
  }

  test("should list default roles with users") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)

    // WHEN
    val result = execute("SHOW ALL ROLES WITH USERS")

    // THEN
    result.toSet should be(defaultRolesWithUsers)
  }

  test("should list populated roles with users") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE foo")

    // WHEN
    val result = execute("SHOW POPULATED ROLES WITH USERS")

    // THEN
    result.toSet should be(Set(Map("role" -> PredefinedRoles.ADMIN, "is_built_in" -> true, "member" -> "neo4j")))
  }

  test("should list populated roles with several users") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    // TODO update to use actual DDL when available for creating users and assigning to roles
    systemGraphInnerQueryExecutor.executeQueryLong(
      """CREATE (r:Role {name:'foo'})
        |CREATE (u1:User {name:'Bar',credentials:'neo',passwordChangeRequired:false,suspended:false})-[:HAS_ROLE]->(r)
        |CREATE (u2:User {name:'Baz',credentials:'NEO',passwordChangeRequired:false,suspended:false})-[:HAS_ROLE]->(r)
      """.stripMargin)

    // WHEN
    val result = execute("SHOW POPULATED ROLES WITH USERS")

    // THEN
    result.toSet should be(Set(Map("role" -> PredefinedRoles.ADMIN, "is_built_in" -> true, "member" -> "neo4j"), foo ++ Map("member" -> "Bar"), foo ++ Map("member" -> "Baz")))
  }

  test("should create role") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)

    // WHEN
    execute("CREATE ROLE foo")

    // THEN
    val result = execute("SHOW ROLES")
    result.toSet should be(defaultRoles ++ Set(foo))
  }

  test("should fail on creating already existing role") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE foo")
    val result = execute("SHOW ROLES")
    result.toSet should be(defaultRoles ++ Set(foo))

    try{
      // WHEN
      execute("CREATE ROLE foo")

      fail("Expected error \"Cannot create already existing role\" but succeeded.")
    } catch {
      // THEN
      case e :Exception if e.getMessage.equals("Cannot create already existing role") =>
    }

    val result2 = execute("SHOW ROLES")
    result2.toSet should be(defaultRoles ++ Set(foo))
  }

  test("should create role from existing role") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE foo")
    val result = execute("SHOW ROLES")
    result.toSet should be(defaultRoles ++ Set(foo))

    // WHEN
    execute("CREATE ROLE bar AS COPY OF foo")

    // THEN
    val result2 = execute("SHOW ROLES")
    result2.toSet should be(defaultRoles ++ Set(foo, bar))
  }

  test("should fail on creating from non-existing role") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)

    try{
      // WHEN
      execute("CREATE ROLE bar AS COPY OF foo")

      fail("Expected error \"Cannot create role 'bar' from non-existent role 'foo'\" but succeeded.")
    } catch {
      // THEN
      case e :Exception if e.getMessage.equals("Cannot create role 'bar' from non-existent role 'foo'") =>
    }

    val result2 = execute("SHOW ROLES")
    result2.toSet should be(defaultRoles ++ Set.empty)
  }

  test("should fail on creating already existing role from other role") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE foo")
    execute("CREATE ROLE bar")
    val result = execute("SHOW ROLES")
    result.toSet should be(defaultRoles ++ Set(foo, bar))

    try{
      // WHEN
      execute("CREATE ROLE bar AS COPY OF foo")

      fail("Expected error \"Cannot create already existing role\" but succeeded.")
    } catch {
      // THEN
      case e :Exception if e.getMessage.equals("Cannot create already existing role") =>
    }

    val result2 = execute("SHOW ROLES")
    result2.toSet should be(defaultRoles ++ Set(foo, bar))
  }

  test("should fail on creating existing role from non-existing role") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE bar")
    val result = execute("SHOW ROLES")
    result.toSet should be(defaultRoles ++ Set(bar))

    try{
      // WHEN
      execute("CREATE ROLE bar AS COPY OF foo")

      fail("Expected error \"Cannot create role 'bar' from non-existent role 'foo'\" but succeeded.")
    } catch {
      // THEN
      case e :Exception if e.getMessage.equals("Cannot create role 'bar' from non-existent role 'foo'") =>
    }

    val result2 = execute("SHOW ROLES")
    result2.toSet should be(defaultRoles ++ Set(bar))
  }

  test("should create and drop role") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE foo")
    val result = execute("SHOW ROLES")
    result.toSet should be(defaultRoles ++ Set(foo))

    // WHEN
    execute("DROP ROLE foo")

    // THEN
    val result2 = execute("SHOW ROLES")
    result2.toSet should be(defaultRoles ++ Set.empty)
  }

  test("should fail on dropping non-existing role") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)

    try {
      // WHEN
      execute("DROP ROLE foo")

      fail("Expected error \"Cannot drop non-existent role 'foo'\"")
    } catch {
      // THEN
      case e :Exception if e.getMessage.equals("Cannot drop non-existent role 'foo'") =>
    }

    // THEN
    val result2 = execute("SHOW ROLES")
    result2.toSet should be(defaultRoles ++ Set.empty)
  }

  // The systemGraphInnerQueryExecutor is needed for test setup with multiple users
  // But it can't be initialized until after super.initTest()
  private var systemGraphInnerQueryExecutor: ContextSwitchingSystemGraphQueryExecutor = _

  protected override def initTest(): Unit = {
    super.initTest()
    systemGraphInnerQueryExecutor = new ContextSwitchingSystemGraphQueryExecutor(databaseManager(), "neo4j")
    val secureHasher: SecureHasher = new SecureHasher
    val systemGraphOperations: SystemGraphOperations = new SystemGraphOperations(systemGraphInnerQueryExecutor, secureHasher)
    val importOptions = new SystemGraphImportOptions(false, false, false, false, null, null, null, null, null, null)
    val systemGraphInitializer = new SystemGraphInitializer(systemGraphInnerQueryExecutor, systemGraphOperations, importOptions, secureHasher, mock[Log])
    systemGraphInitializer.initializeSystemGraph()
  }

  private def databaseManager() = graph.getDependencyResolver.resolveDependency(classOf[DatabaseManager[DatabaseContext]])

  private def selectDatabase(name: String): Unit = {
    val manager = databaseManager()
    val maybeCtx: Optional[DatabaseContext] = manager.getDatabaseContext(new DatabaseId(name))
    val dbCtx: DatabaseContext = maybeCtx.orElseGet(() => throw new RuntimeException(s"No such database: $name"))
    graphOps = dbCtx.databaseFacade()
    graph = new GraphDatabaseCypherService(graphOps)
    eengine = ExecutionEngineHelper.createEngine(graph)
  }
}
