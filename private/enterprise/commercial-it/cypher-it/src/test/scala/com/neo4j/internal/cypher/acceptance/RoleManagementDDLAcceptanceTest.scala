/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.internal.cypher.acceptance

import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles

class RoleManagementDDLAcceptanceTest extends DDLAcceptanceTestBase {
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

  test("should list all default roles") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)

    // WHEN
    val result = execute("SHOW ALL ROLES")

    // THEN
    result.toSet should be(defaultRoles)
  }

  test("should fail on listing roles when not on system database") {
    try {
      // WHEN
      execute("SHOW ROLES")

      fail("Expected error \"Trying to run `CATALOG SHOW ALL ROLES` against non-system database.\" but succeeded.")
    } catch {
      // THEN
      case e :Exception => e.getMessage should startWith("Trying to run `CATALOG SHOW ALL ROLES` against non-system database")
    }
  }

  test("should list populated default roles") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)

    // WHEN
    val result = execute("SHOW POPULATED ROLES")

    // THEN
    result.toSet should be(Set(Map("role" -> PredefinedRoles.ADMIN, "is_built_in" -> true)))
  }

  test("should create and list roles") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)

    // WHEN
    execute("CREATE ROLE foo")
    val result = execute("SHOW ROLES")

    // THEN
    result.toSet should be(defaultRoles ++ Set(foo))
  }

  test("should list populated roles") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE USER Bar SET PASSWORD 'neo'")
    execute("CREATE USER Baz SET PASSWORD 'NEO'")
    execute("CREATE ROLE foo")
    execute("GRANT ROLE foo TO Bar")
    execute("GRANT ROLE foo TO Baz")

    // WHEN
    val result = execute("SHOW POPULATED ROLES")

    // THEN
    result.toSet should be(Set(Map("role" -> PredefinedRoles.ADMIN, "is_built_in" -> true), foo))
  }

  test("should list default roles with users") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)

    // WHEN
    val result = execute("SHOW ROLES WITH USERS")

    // THEN
    result.toSet should be(defaultRolesWithUsers)
  }

  test("should list all default roles with users") {
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
    execute("CREATE USER Bar SET PASSWORD 'neo'")
    execute("CREATE USER Baz SET PASSWORD 'NEO'")
    execute("CREATE ROLE foo")
    execute("GRANT ROLE foo TO Bar")
    execute("GRANT ROLE foo TO Baz")

    // WHEN
    val result = execute("SHOW POPULATED ROLES WITH USERS")

    // THEN
    result.toSet should be(Set(
      Map("role" -> PredefinedRoles.ADMIN, "is_built_in" -> true, "member" -> "neo4j"),
      foo ++ Map("member" -> "Bar"),
      foo ++ Map("member" -> "Baz")
    ))
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

    try {
      // WHEN
      execute("CREATE ROLE foo")

      fail("Expected error \"The specified role 'foo' already exists.\" but succeeded.")
    } catch {
      // THEN
      case e: Exception => e.getMessage should be("The specified role 'foo' already exists.")
    }

    val result2 = execute("SHOW ROLES")
    result2.toSet should be(defaultRoles ++ Set(foo))
  }

  test("should fail on creating role when not on system database") {
    try {
      // WHEN
      execute("CREATE ROLE foo")

      fail("Expected error \"Trying to run `CATALOG CREATE ROLE` against non-system database.\" but succeeded.")
    } catch {
      // THEN
      case e :Exception => e.getMessage should startWith("Trying to run `CATALOG CREATE ROLE` against non-system database")
    }
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

    try {
      // WHEN
      execute("CREATE ROLE bar AS COPY OF foo")

      fail("Expected error \"Cannot create role 'bar' from non-existent role 'foo'.\" but succeeded.")
    } catch {
      // THEN
      case e: Exception => e.getMessage should be("Cannot create role 'bar' from non-existent role 'foo'.")
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

    try {
      // WHEN
      execute("CREATE ROLE bar AS COPY OF foo")

      fail("Expected error \"The specified role 'bar' already exists.\" but succeeded.")
    } catch {
      // THEN
      case e: Exception => e.getMessage should be("The specified role 'bar' already exists.")
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

    try {
      // WHEN
      execute("CREATE ROLE bar AS COPY OF foo")

      fail("Expected error \"Cannot create role 'bar' from non-existent role 'foo'.\" but succeeded.")
    } catch {
      // THEN
      case e: Exception => e.getMessage should be("Cannot create role 'bar' from non-existent role 'foo'.")
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

  test("should fail on dropping default role") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("SHOW ROLES").toSet should be(defaultRoles)

    try {
      // WHEN
      execute(s"DROP ROLE ${PredefinedRoles.READER}")

      fail("Expected error \"'%s' is a predefined role and can not be deleted or modified.\" but succeeded.".format(PredefinedRoles.READER))
    } catch {
      // THEN
      case e: Exception => e.getMessage should be("'%s' is a predefined role and can not be deleted or modified.".format(PredefinedRoles.READER))
    }

    // THEN
    execute("SHOW ROLES").toSet should be(defaultRoles)
  }

  test("should fail on dropping non-existing role") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)

    try {
      // WHEN
      execute("DROP ROLE foo")

      fail("Expected error \"Role 'foo' does not exist.\" but succeeded.")
    } catch {
      // THEN
      case e: Exception => e.getMessage should be("Role 'foo' does not exist.")
    }

    // THEN
    val result2 = execute("SHOW ROLES")
    result2.toSet should be(defaultRoles ++ Set.empty)
  }

  test("should fail on dropping role when not on system database") {
    try {
      // WHEN
      execute("DROP ROLE foo")

      fail("Expected error \"Trying to run `CATALOG DROP ROLE` against non-system database.\" but succeeded.")
    } catch {
      // THEN
      case e :Exception => e.getMessage should startWith("Trying to run `CATALOG DROP ROLE` against non-system database")
    }
  }
}
