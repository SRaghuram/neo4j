/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.internal.cypher.acceptance

import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException
import org.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles
import org.parboiled.errors.ParserRuntimeException

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

  // SHOW ROLES

  test("should show all default roles") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)

    // WHEN
    val result = execute("SHOW ALL ROLES")

    // THEN
    result.toSet should be(defaultRoles)
  }

  test("should fail when showing roles when not on system database") {
    the[IllegalStateException] thrownBy {
      // WHEN
      execute("SHOW ROLES")
      // THEN
    } should have message "Trying to run `CATALOG SHOW ALL ROLES` against non-system database."
  }

  test("should show populated default roles") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)

    // WHEN
    val result = execute("SHOW POPULATED ROLES")

    // THEN
    result.toSet should be(Set(Map("role" -> PredefinedRoles.ADMIN, "is_built_in" -> true)))
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
    result.toSet should be(Set(Map("role" -> PredefinedRoles.ADMIN, "is_built_in" -> true), foo))
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
    result.toSet should be(Set(Map("role" -> PredefinedRoles.ADMIN, "is_built_in" -> true, "member" -> "neo4j")))
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
      Map("role" -> PredefinedRoles.ADMIN, "is_built_in" -> true, "member" -> "neo4j"),
      foo ++ Map("member" -> "Bar"),
      foo ++ Map("member" -> "Baz")
    ))
  }

  // CREATE ROLE

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

    val exception = the[ParserRuntimeException] thrownBy {
      // WHEN
      execute("CREATE ROLE ``")
      // THEN
    }
    exception.getMessage should include("The provided role name is empty.")

    execute("SHOW ROLES").toSet should be(defaultRoles ++ Set.empty)
  }

  test("should fail when creating role when not on system database") {
    the[IllegalStateException] thrownBy {
      // WHEN
      execute("CREATE ROLE foo")
      // THEN
    } should have message "Trying to run `CATALOG CREATE ROLE` against non-system database."
  }

  test("should create role from existing role") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE foo")
    execute("SHOW ROLES").toSet should be(defaultRoles ++ Set(foo))

    // WHEN
    execute("CREATE ROLE bar AS COPY OF foo")

    execute("SHOW ROLES").toSet should be(defaultRoles ++ Set(foo, bar))
  }

  test("should fail when creating from non-existing role") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)

    the[InvalidArgumentsException] thrownBy {
      // WHEN
      execute("CREATE ROLE bar AS COPY OF foo")
      // THEN
    } should have message "Cannot create role 'bar' from non-existent role 'foo'."

    // and an invalid (non-existing) one
    the[InvalidArgumentsException] thrownBy {
      // WHEN
      execute("CREATE ROLE bar AS COPY OF ``")
      // THEN
    } should have message "Cannot create role 'bar' from non-existent role ''."

    execute("SHOW ROLES").toSet should be(defaultRoles ++ Set.empty)
  }

  test("should fail when creating role with invalid name from role") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE foo")
    execute("SHOW ROLES").toSet should be(defaultRoles ++ Set(foo))

    val exception = the[ParserRuntimeException] thrownBy {
      // WHEN
      execute("CREATE ROLE `` AS COPY OF foo")
      // THEN
    }
    exception.getMessage should include("The provided role name is empty.")

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
    } should have message "Cannot create role 'bar' from non-existent role 'foo'."

    execute("SHOW ROLES").toSet should be(defaultRoles ++ Set(bar))
  }

  // DROP ROLE

  test("should drop role") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE foo")
    execute("SHOW ROLES").toSet should be(defaultRoles ++ Set(foo))

    // WHEN
    execute("DROP ROLE foo")

    execute("SHOW ROLES").toSet should be(defaultRoles ++ Set.empty)
  }

  test("should fail when dropping default role") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("SHOW ROLES").toSet should be(defaultRoles)

    the[InvalidArgumentsException] thrownBy {
      // WHEN
      execute(s"DROP ROLE ${PredefinedRoles.READER}")
      // THEN
    } should have message "'%s' is a predefined role and can not be deleted or modified.".format(PredefinedRoles.READER)

    // THEN
    execute("SHOW ROLES").toSet should be(defaultRoles)
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
    the[IllegalStateException] thrownBy {
      // WHEN
      execute("DROP ROLE foo")
      // THEN
    } should have message "Trying to run `CATALOG DROP ROLE` against non-system database."
  }
}
