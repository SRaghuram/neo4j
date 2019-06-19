/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.internal.cypher.acceptance

import com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.cypher.{DatabaseManagementException, InvalidArgumentException}
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException

class RoleManagementDDLAcceptanceTest extends DDLAcceptanceTestBase {
  private val defaultRoles = Set(
    Map("role" -> PredefinedRoles.ADMIN, "isBuiltIn" -> true),
    Map("role" -> PredefinedRoles.ARCHITECT, "isBuiltIn" -> true),
    Map("role" -> PredefinedRoles.PUBLISHER, "isBuiltIn" -> true),
    Map("role" -> PredefinedRoles.EDITOR, "isBuiltIn" -> true),
    Map("role" -> PredefinedRoles.READER, "isBuiltIn" -> true)
  )
  private val foo = Map("role" -> "foo", "isBuiltIn" -> false)
  private val bar = Map("role" -> "bar", "isBuiltIn" -> false)

  // Tests for showing roles

  test("should show all default roles") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)

    // WHEN
    val result = execute("SHOW ALL ROLES")

    // THEN
    result.toSet should be(defaultRoles)
  }

  test("should show populated default roles") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)

    // WHEN
    val result = execute("SHOW POPULATED ROLES")

    // THEN
    result.toSet should be(Set(Map("role" -> PredefinedRoles.ADMIN, "isBuiltIn" -> true)))
  }

  test("should create and show roles") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)

    // WHEN
    execute("CREATE ROLE foo")
    val result = execute("SHOW ROLES")

    // THEN
    result.toSet should be(defaultRoles ++ Set(foo))
  }

  test("should show populated roles") {
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
    result.toSet should be(Set(Map("role" -> PredefinedRoles.ADMIN, "isBuiltIn" -> true), foo))
  }

  test("should show default roles with users") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)

    // WHEN
    val result = execute("SHOW ROLES WITH USERS")

    // THEN
    result.toSet should be(defaultRolesWithUsers)
  }

  test("should show all default roles with users") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)

    // WHEN
    val result = execute("SHOW ALL ROLES WITH USERS")

    // THEN
    result.toSet should be(defaultRolesWithUsers)
  }

  test("should show populated roles with users") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE foo")

    // WHEN
    val result = execute("SHOW POPULATED ROLES WITH USERS")

    // THEN
    result.toSet should be(Set(Map("role" -> PredefinedRoles.ADMIN, "isBuiltIn" -> true, "member" -> "neo4j")))
  }

  test("should show populated roles with several users") {
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
      Map("role" -> PredefinedRoles.ADMIN, "isBuiltIn" -> true, "member" -> "neo4j"),
      foo ++ Map("member" -> "Bar"),
      foo ++ Map("member" -> "Baz")
    ))
  }

  test("should fail when showing roles when not on system database") {
    the[DatabaseManagementException] thrownBy {
      // WHEN
      execute("SHOW ROLES")
      // THEN
    } should have message
      "This is a DDL command and it should be executed against the system database: SHOW ALL ROLES"
  }

  // Tests for creating roles

  test("should create role") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)

    // WHEN
    execute("CREATE ROLE foo")

    execute("SHOW ROLES").toSet should be(defaultRoles ++ Set(foo))
  }

  test("should fail when creating already existing role") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE foo")
    execute("SHOW ROLES").toSet should be(defaultRoles ++ Set(foo))

    the[InvalidArgumentsException] thrownBy {
      // WHEN
      execute("CREATE ROLE foo")
      // THEN
    } should have message "The specified role 'foo' already exists."

    execute("SHOW ROLES").toSet should be(defaultRoles ++ Set(foo))
  }

  test("should fail when creating role with invalid name") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)

    the[InvalidArgumentException] thrownBy {
      // WHEN
      execute("CREATE ROLE ``")
      // THEN
    } should have message "The provided role name is empty."

    the[InvalidArgumentException] thrownBy {
      // WHEN
      execute("CREATE ROLE `my%role`")
      // THEN
    } should have message
      """Role name 'my%role' contains illegal characters.
        |Use simple ascii characters, numbers and underscores.""".stripMargin

    execute("SHOW ROLES").toSet should be(defaultRoles ++ Set.empty)
  }

  test("should create role from existing role") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE foo")
    execute("SHOW ROLES").toSet should be(defaultRoles ++ Set(foo))

    // WHEN
    execute("CREATE ROLE bar AS COPY OF foo")

    execute("SHOW ROLES").toSet should be(defaultRoles ++ Set(foo, bar))
    execute("SHOW ROLE bar PRIVILEGES").toSet should be(Set.empty)
  }

  test("should create role from existing role and copy privileges") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE foo")
    execute("SHOW ROLES").toSet should be(defaultRoles ++ Set(foo))
    execute("GRANT TRAVERSE ON GRAPH * NODES * (*) TO foo")
    execute("GRANT READ (a,b,c) ON GRAPH * NODES A (*) TO foo")
    val expected = Set(grantTraverse().map,
      grantRead().property("a").node("A").map,
      grantRead().property("b").node("A").map,
      grantRead().property("c").node("A").map
    )
    val expectedFoo = expected.map(_ ++ Map("role" -> "foo"))
    val expectedBar = expected.map(_ ++ Map("role" -> "bar"))

    // WHEN
    execute("CREATE ROLE bar AS COPY OF foo")

    // THEN
    execute("SHOW ROLES").toSet should be(defaultRoles ++ Set(foo, bar))
    execute("SHOW ROLE foo PRIVILEGES").toSet should be(expectedFoo)
    execute("SHOW ROLE bar PRIVILEGES").toSet should be(expectedBar)
  }

  test("should fail when creating from non-existing role") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)

    the[InvalidArgumentsException] thrownBy {
      // WHEN
      execute("CREATE ROLE bar AS COPY OF foo")
      // THEN
    } should have message "Role 'foo' does not exist."

    // and an invalid (non-existing) one
    the[InvalidArgumentsException] thrownBy {
      // WHEN
      execute("CREATE ROLE bar AS COPY OF ``")
      // THEN
    } should have message "Role '' does not exist."

    execute("SHOW ROLES").toSet should be(defaultRoles ++ Set.empty)
  }

  test("should fail when creating role with invalid name from role") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE foo")
    execute("SHOW ROLES").toSet should be(defaultRoles ++ Set(foo))

    the[InvalidArgumentException] thrownBy {
      // WHEN
      execute("CREATE ROLE `` AS COPY OF foo")
      // THEN
    } should have message "The provided role name is empty."

    the[InvalidArgumentException] thrownBy {
      // WHEN
      execute("CREATE ROLE `my%role` AS COPY OF foo")
      // THEN
    } should have message
      """Role name 'my%role' contains illegal characters.
        |Use simple ascii characters, numbers and underscores.""".stripMargin

    execute("SHOW ROLES").toSet should be(defaultRoles ++ Set(foo))
  }

  test("should fail when creating already existing role from other role") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE foo")
    execute("CREATE ROLE bar")
    execute("SHOW ROLES").toSet should be(defaultRoles ++ Set(foo, bar))

    the[InvalidArgumentsException] thrownBy {
      // WHEN
      execute("CREATE ROLE bar AS COPY OF foo")
      // THEN
    } should have message "The specified role 'bar' already exists."

    execute("SHOW ROLES").toSet should be(defaultRoles ++ Set(foo, bar))
  }

  test("should fail when creating existing role from non-existing role") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE bar")
    execute("SHOW ROLES").toSet should be(defaultRoles ++ Set(bar))

    the[InvalidArgumentsException] thrownBy {
      // WHEN
      execute("CREATE ROLE bar AS COPY OF foo")
      // THEN
    } should have message "Role 'foo' does not exist."

    execute("SHOW ROLES").toSet should be(defaultRoles ++ Set(bar))
  }

  test("should fail when creating role when not on system database") {
    the[DatabaseManagementException] thrownBy {
      // WHEN
      execute("CREATE ROLE foo")
      // THEN
    } should have message
      "This is a DDL command and it should be executed against the system database: CREATE ROLE"
  }

  // Tests for dropping roles

  test("should drop role") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE foo")
    execute("SHOW ROLES").toSet should be(defaultRoles ++ Set(foo))

    // WHEN
    execute("DROP ROLE foo")

    execute("SHOW ROLES").toSet should be(defaultRoles ++ Set.empty)
  }

  test("should drop built-in role") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("SHOW ROLES").toSet should be(defaultRoles)

    // WHEN
    execute(s"DROP ROLE ${PredefinedRoles.READER}")

    // THEN
    execute("SHOW ROLES").toSet should be(defaultRoles -- Set(Map("role" -> PredefinedRoles.READER, "isBuiltIn" -> true)))
  }

  test("should fail when dropping non-existing role") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)

    the[InvalidArgumentsException] thrownBy {
      // WHEN
      execute("DROP ROLE foo")
      // THEN
    } should have message "Role 'foo' does not exist."

    // and an invalid (non-existing) one
    the[InvalidArgumentsException] thrownBy {
      // WHEN
      execute("DROP ROLE ``")
      // THEN
    } should have message "Role '' does not exist."

    execute("SHOW ROLES").toSet should be(defaultRoles ++ Set.empty)
  }

  test("should fail when dropping role when not on system database") {
    the[DatabaseManagementException] thrownBy {
      // WHEN
      execute("DROP ROLE foo")
      // THEN
    } should have message
      "This is a DDL command and it should be executed against the system database: DROP ROLE"
  }
}
