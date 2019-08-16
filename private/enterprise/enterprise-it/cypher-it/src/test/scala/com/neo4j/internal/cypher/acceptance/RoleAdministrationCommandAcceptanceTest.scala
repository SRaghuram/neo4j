/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.internal.cypher.acceptance

import com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.exceptions.{DatabaseAdministrationException, InvalidArgumentException}
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException
import org.scalatest.enablers.Messaging.messagingNatureOfThrowable

class RoleAdministrationCommandAcceptanceTest extends AdministrationCommandAcceptanceTestBase {
  private val defaultRoles = Set(
    Map("role" -> PredefinedRoles.ADMIN, "isBuiltIn" -> true),
    Map("role" -> PredefinedRoles.ARCHITECT, "isBuiltIn" -> true),
    Map("role" -> PredefinedRoles.PUBLISHER, "isBuiltIn" -> true),
    Map("role" -> PredefinedRoles.EDITOR, "isBuiltIn" -> true),
    Map("role" -> PredefinedRoles.READER, "isBuiltIn" -> true)
  )
  private val foo = Map("role" -> "foo", "isBuiltIn" -> false)
  private val bar = Map("role" -> "bar", "isBuiltIn" -> false)

  test("should return empty counts to the outside for commands that update the system graph internally") {
    //TODO: ADD ANY NEW UPDATING COMMANDS HERE

    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE USER Bar SET PASSWORD 'neo'")
    Seq("x", "y", "z").foreach(user => execute(s"CREATE USER $user SET PASSWORD 'neo'"))
    Seq("a", "b", "c").foreach(role => execute(s"CREATE ROLE $role"))

    // Notice: They are executed in succession so they have to make sense in that order
    assertQueriesAndSubQueryCounts(List(
      "CREATE ROLE foo" -> 1,
      "GRANT ROLE foo TO Bar" -> 1,
      "REVOKE ROLE foo FROM Bar" -> 1,
      "DROP ROLE foo" -> 1,
      "GRANT ROLE a,b,c TO x,y,z" -> 9
    ))
  }

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
    the[DatabaseAdministrationException] thrownBy {
      // WHEN
      execute("SHOW ROLES")
      // THEN
    } should have message
      "This is an administration command and it should be executed against the system database: SHOW ALL ROLES"
  }

  // Tests for creating roles

  test("should create role") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)

    // WHEN
    execute("CREATE ROLE foo")

    execute("SHOW ROLES").toSet should be(defaultRoles ++ Set(foo))
  }

  test("should create role using if not exists") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)

    // WHEN
    execute("CREATE ROLE IF NOT EXISTS foo")

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
    } should have message "Failed to create the specified role 'foo': Role already exists."

    execute("SHOW ROLES").toSet should be(defaultRoles ++ Set(foo))
  }

  test("should do nothing when creating already existing role using if not exists") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE foo")
    execute("SHOW ROLES").toSet should be(defaultRoles ++ Set(foo))

    // WHEN
    execute("CREATE ROLE IF NOT EXISTS foo")

    // THEN
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

  test("should create role from existing role using if not exists") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE foo")
    execute("SHOW ROLES").toSet should be(defaultRoles ++ Set(foo))

    // WHEN
    execute("CREATE ROLE IF NOT EXISTS bar AS COPY OF foo")

    execute("SHOW ROLES").toSet should be(defaultRoles ++ Set(foo, bar))
    execute("SHOW ROLE bar PRIVILEGES").toSet should be(Set.empty)
  }

  test("should create role from existing role and copy privileges") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE foo")
    execute("SHOW ROLES").toSet should be(defaultRoles ++ Set(foo))
    execute("GRANT TRAVERSE ON GRAPH * NODES * (*) TO foo")
    execute("GRANT READ {a,b,c} ON GRAPH * NODES A (*) TO foo")
    val expected = Set(traverse().node("*").map,
      read().property("a").node("A").map,
      read().property("b").node("A").map,
      read().property("c").node("A").map
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
    } should have message "Failed to create a role as copy of 'foo': Role does not exist."

    // and an invalid (non-existing) one
    the[InvalidArgumentsException] thrownBy {
      // WHEN
      execute("CREATE ROLE bar AS COPY OF ``")
      // THEN
    } should have message "Failed to create a role as copy of '': Role does not exist."

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
    } should have message "Failed to create the specified role 'bar': Role already exists."

    execute("SHOW ROLES").toSet should be(defaultRoles ++ Set(foo, bar))
  }

  test("should do nothing when creating already existing role from other role using if not exists") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE foo")
    execute("GRANT TRAVERSE ON GRAPH * NODES * TO foo")
    execute("CREATE ROLE bar")
    execute("SHOW ROLES").toSet should be(defaultRoles ++ Set(foo, bar))
    execute("SHOW ROLE foo PRIVILEGES").toSet should be(Set(traverse().role("foo").node("*").map))
    execute("SHOW ROLE bar PRIVILEGES").toSet should be(Set.empty)

    // WHEN
    execute("CREATE ROLE IF NOT EXISTS bar AS COPY OF foo")

    // THEN
    execute("SHOW ROLES").toSet should be(defaultRoles ++ Set(foo, bar))
    execute("SHOW ROLE foo PRIVILEGES").toSet should be(Set(traverse().role("foo").node("*").map))
    execute("SHOW ROLE bar PRIVILEGES").toSet should be(Set.empty)
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
    } should have message "Failed to create a role as copy of 'foo': Role does not exist."

    execute("SHOW ROLES").toSet should be(defaultRoles ++ Set(bar))
  }

  test("should fail when creating role when not on system database") {
    the[DatabaseAdministrationException] thrownBy {
      // WHEN
      execute("CREATE ROLE foo")
      // THEN
    } should have message
      "This is an administration command and it should be executed against the system database: CREATE ROLE"
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

  test("should drop existing role using if exists") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE foo")
    execute("SHOW ROLES").toSet should be(defaultRoles ++ Set(foo))

    // WHEN
    execute("DROP ROLE IF EXISTS foo")

    // THEN
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
    execute("SHOW ROLES").toSet should be(defaultRoles)

    the[InvalidArgumentsException] thrownBy {
      // WHEN
      execute("DROP ROLE foo")
      // THEN
    } should have message "Failed to delete the specified role 'foo': Role does not exist."

    // THEN
    execute("SHOW ROLES").toSet should be(defaultRoles)

    // and an invalid (non-existing) one
    the[InvalidArgumentsException] thrownBy {
      // WHEN
      execute("DROP ROLE ``")
      // THEN
    } should have message "Failed to delete the specified role '': Role does not exist."

    // THEN
    execute("SHOW ROLES").toSet should be(defaultRoles)
  }

  test("should do nothing when dropping non-existing role using if exists") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("SHOW ROLES").toSet should be(defaultRoles)

    // WHEN
    execute("DROP ROLE IF EXISTS foo")

    // THEN
    execute("SHOW ROLES").toSet should be(defaultRoles)

    // and an invalid (non-existing) one

    // WHEN
    execute("DROP ROLE IF EXISTS ``")

    // THEN
    execute("SHOW ROLES").toSet should be(defaultRoles)
  }

  test("should fail when dropping role when not on system database") {
    the[DatabaseAdministrationException] thrownBy {
      // WHEN
      execute("DROP ROLE foo")
      // THEN
    } should have message
      "This is an administration command and it should be executed against the system database: DROP ROLE"
  }

  // Tests for granting roles to users

  test("should grant role to user") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    execute("CREATE USER user SET PASSWORD 'neo'")

    // WHEN
    execute("GRANT ROLE custom TO user")

    // THEN
    execute("SHOW ROLES WITH USERS").toSet should be(defaultRolesWithUsers + Map("role" -> "custom", "isBuiltIn" -> false, "member" -> "user"))
  }

  test("should grant roles and list users with roles") {
    // GIVEN
    // User  : Roles
    // neo4j : admin
    // Bar   : dragon, fairy
    // Baz   :
    // Zet   : fairy
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE USER Bar SET PASSWORD 'neo'")
    execute("CREATE USER Baz SET PASSWORD 'NEO'")
    execute("CREATE USER Zet SET PASSWORD 'NeX'")
    execute("CREATE ROLE dragon")
    execute("CREATE ROLE fairy")

    // WHEN
    execute("GRANT ROLE dragon TO Bar")
    execute("GRANT ROLE fairy TO Bar")
    execute("GRANT ROLE fairy TO Zet")

    // THEN
    val result = execute("SHOW USERS")
    result.toSet shouldBe Set(
      neo4jUser,
      user("Bar", Seq("fairy", "dragon")),
      user("Baz"),
      user("Zet", Seq("fairy"))
    )
  }

  test("should grant role to several users") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    execute("CREATE USER userA SET PASSWORD 'neo'")
    execute("CREATE USER userB SET PASSWORD 'neo'")

    // WHEN
    execute("GRANT ROLE custom TO userA, userB")

    // THEN
    execute("SHOW ROLES WITH USERS").toSet should be(defaultRolesWithUsers + Map("role" -> "custom", "isBuiltIn" -> false, "member" -> "userA")
      + Map("role" -> "custom", "isBuiltIn" -> false, "member" -> "userB"))
  }

  test("should grant multiple roles to user") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom1")
    execute("CREATE ROLE custom2")
    execute("CREATE USER userA SET PASSWORD 'neo'")

    // WHEN
    execute("GRANT ROLE custom1, custom2 TO userA")

    // THEN
    execute("SHOW ROLES WITH USERS").toSet should be(defaultRolesWithUsers + Map("role" -> "custom1", "isBuiltIn" -> false, "member" -> "userA")
      + Map("role" -> "custom2", "isBuiltIn" -> false, "member" -> "userA"))
  }

  test("should grant multiple roles to several users") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom1")
    execute("CREATE ROLE custom2")
    execute("CREATE USER userA SET PASSWORD 'neo'")
    execute("CREATE USER userB SET PASSWORD 'neo'")

    // WHEN
    execute("GRANT ROLE custom1, custom2 TO userA, userB")

    // THEN
    execute("SHOW ROLES WITH USERS").toSet should be(defaultRolesWithUsers
      + Map("role" -> "custom1", "isBuiltIn" -> false, "member" -> "userA")
      + Map("role" -> "custom1", "isBuiltIn" -> false, "member" -> "userB")
      + Map("role" -> "custom2", "isBuiltIn" -> false, "member" -> "userA")
      + Map("role" -> "custom2", "isBuiltIn" -> false, "member" -> "userB"))
  }

  test("should be able to grant already granted role to user") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE USER Bar SET PASSWORD 'neo'")
    execute("CREATE ROLE dragon")
    execute("GRANT ROLE dragon TO Bar")
    execute("SHOW USERS").toSet shouldBe Set(neo4jUser, user("Bar", Seq("dragon")))

    // WHEN
    execute("GRANT ROLE dragon TO Bar")

    // THEN
    execute("SHOW USERS").toSet shouldBe Set(neo4jUser, user("Bar", Seq("dragon")))
  }

  test("should fail when granting non-existing role to user") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE USER Bar SET PASSWORD 'neo'")
    execute("SHOW USERS").toSet shouldBe Set(neo4jUser, user("Bar"))
    execute("SHOW ROLES WITH USERS").toSet shouldBe defaultRolesWithUsers

    the[InvalidArgumentsException] thrownBy {
      // WHEN
      execute("GRANT ROLE dragon TO Bar")
      // THEN
    } should have message "Failed to grant role 'dragon' to user 'Bar': Role does not exist."

    // THEN
    execute("SHOW USERS").toSet shouldBe Set(neo4jUser, user("Bar"))
    execute("SHOW ROLES WITH USERS").toSet shouldBe defaultRolesWithUsers

    // and an invalid (non-existing) one
    the[InvalidArgumentsException] thrownBy {
      // WHEN
      execute("GRANT ROLE `` TO Bar")
      // THEN
    } should have message "Failed to grant role '' to user 'Bar': Role does not exist."

    // AND
    execute("SHOW USERS").toSet shouldBe Set(neo4jUser, user("Bar"))
    execute("SHOW ROLES WITH USERS").toSet shouldBe defaultRolesWithUsers
  }

  test("should fail when granting role to non-existing user") {
    // GIVEN
    val rolesWithUsers = defaultRolesWithUsers ++ Set(Map("role" -> "dragon", "isBuiltIn" -> false, "member" -> null))
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE dragon")
    execute("SHOW USERS").toSet shouldBe Set(neo4jUser)
    execute("SHOW ROLES WITH USERS").toSet shouldBe rolesWithUsers

    the[InvalidArgumentsException] thrownBy {
      // WHEN
      execute("GRANT ROLE dragon TO Bar")
      // THEN
    } should have message "Failed to grant role 'dragon' to user 'Bar': User does not exist."

    // THEN
    execute("SHOW USERS").toSet shouldBe Set(neo4jUser)
    execute("SHOW ROLES WITH USERS").toSet shouldBe rolesWithUsers

    // and an invalid (non-existing) one

    the[InvalidArgumentsException] thrownBy {
      // WHEN
      execute("GRANT ROLE dragon TO ``")
      // THEN
    } should have message "Failed to grant role 'dragon' to user '': User does not exist."

    // THEN
    execute("SHOW USERS").toSet shouldBe Set(neo4jUser)
    execute("SHOW ROLES WITH USERS").toSet shouldBe rolesWithUsers
  }

  test("should fail when granting non-existing role to non-existing user") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("SHOW USERS").toSet shouldBe Set(neo4jUser)
    execute("SHOW ROLES WITH USERS").toSet shouldBe defaultRolesWithUsers

    the[InvalidArgumentsException] thrownBy {
      // WHEN
      execute("GRANT ROLE dragon TO Bar")
      // THEN
    } should have message "Failed to grant role 'dragon' to user 'Bar': Role does not exist."

    execute("SHOW USERS").toSet shouldBe Set(neo4jUser)
    execute("SHOW ROLES WITH USERS").toSet shouldBe defaultRolesWithUsers

    // and an invalid (non-existing) ones
    the[InvalidArgumentsException] thrownBy {
      // WHEN
      execute("GRANT ROLE `` TO ``")
      // THEN
    } should have message "Failed to grant role '' to user '': Role does not exist."

    execute("SHOW USERS").toSet shouldBe Set(neo4jUser)
    execute("SHOW ROLES WITH USERS").toSet shouldBe defaultRolesWithUsers
  }

  test("should fail when granting role to user when not on system database") {
    the[DatabaseAdministrationException] thrownBy {
      // WHEN
      execute("GRANT ROLE dragon TO Bar")
      // THEN
    } should have message
      "This is an administration command and it should be executed against the system database: GRANT ROLE"

    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE USER Bar SET PASSWORD 'neo'")
    execute("CREATE ROLE dragon")
    selectDatabase(GraphDatabaseSettings.DEFAULT_DATABASE_NAME)

    the[DatabaseAdministrationException] thrownBy {
      // WHEN
      execute("GRANT ROLE dragon TO Bar")
      // THEN
    } should have message
      "This is an administration command and it should be executed against the system database: GRANT ROLE"
  }

  // Tests for revoking roles from users

  test("should revoke role from user") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    execute("CREATE USER user SET PASSWORD 'neo'")
    execute("GRANT ROLE custom TO user")

    // WHEN
    execute("REVOKE ROLE custom FROM user")

    // THEN
    execute("SHOW ROLES WITH USERS").toSet should be(defaultRolesWithUsers + Map("role" -> "custom", "isBuiltIn" -> false, "member" -> null))
  }

  test("should revoke role from several users") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    execute("CREATE USER userA SET PASSWORD 'neo'")
    execute("CREATE USER userB SET PASSWORD 'neo'")
    execute("GRANT ROLE custom TO userA")
    execute("GRANT ROLE custom TO userB")

    // WHEN
    execute("REVOKE ROLE custom FROM userA, userB")

    // THEN
    execute("SHOW ROLES WITH USERS").toSet should be(defaultRolesWithUsers + Map("role" -> "custom", "isBuiltIn" -> false, "member" -> null))
  }

  test("should revoke multiple roles from user") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom1")
    execute("CREATE ROLE custom2")
    execute("CREATE USER userA SET PASSWORD 'neo'")
    execute("GRANT ROLE custom1 TO userA")
    execute("GRANT ROLE custom2 TO userA")

    // WHEN
    execute("REVOKE ROLE custom1, custom2 FROM userA")

    // THEN
    execute("SHOW ROLES WITH USERS").toSet should be(defaultRolesWithUsers + Map("role" -> "custom1", "isBuiltIn" -> false, "member" -> null)
      + Map("role" -> "custom2", "isBuiltIn" -> false, "member" -> null))
  }

  test("should revoke multiple roles from several users") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom1")
    execute("CREATE ROLE custom2")
    execute("CREATE USER userA SET PASSWORD 'neo'")
    execute("CREATE USER userB SET PASSWORD 'neo'")
    execute("CREATE USER userC SET PASSWORD 'neo'")
    execute("CREATE USER userD SET PASSWORD 'neo'")
    execute("GRANT ROLE custom1 TO userA")
    execute("GRANT ROLE custom1 TO userB")
    execute("GRANT ROLE custom1 TO userC")
    execute("GRANT ROLE custom2 TO userA")
    execute("GRANT ROLE custom2 TO userB")
    execute("GRANT ROLE custom2 TO userD")

    // WHEN
    execute("REVOKE ROLE custom1, custom2 FROM userA, userB, userC, userD")

    // THEN
    execute("SHOW ROLES WITH USERS").toSet should be(defaultRolesWithUsers + Map("role" -> "custom1", "isBuiltIn" -> false, "member" -> null)
      + Map("role" -> "custom2", "isBuiltIn" -> false, "member" -> null))
  }

  test("should be able to revoke already revoked role from user") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    execute("CREATE USER user SET PASSWORD 'neo'")
    execute("GRANT ROLE custom TO user")
    execute("REVOKE ROLE custom FROM user")

    // WHEN
    execute("REVOKE ROLE custom FROM user").toSet should be(Set.empty)

    // THEN
    execute("SHOW ROLES WITH USERS").toSet should be(defaultRolesWithUsers + Map("role" -> "custom", "isBuiltIn" -> false, "member" -> null))
  }

  test("should grant and revoke multiple roles to multiple users") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE USER Bar SET PASSWORD 'neo'")
    execute("CREATE USER Baz SET PASSWORD 'NEO'")
    execute("CREATE ROLE foo")
    execute("CREATE ROLE fum")
    val admin = Map("role" -> PredefinedRoles.ADMIN, "isBuiltIn" -> true, "member" -> "neo4j")

    def role(r: String, u: String) = Map("role" -> r, "member" -> u, "isBuiltIn" -> false)

    // WHEN using single user and role version of GRANT
    execute("GRANT ROLE foo TO Bar")
    execute("GRANT ROLE foo TO Baz")

    // THEN
    execute("SHOW POPULATED ROLES WITH USERS").toSet should be(Set(admin, role("foo", "Bar"), role("foo", "Baz")))

    // WHEN using single user and role version of REVOKE
    execute("REVOKE ROLE foo FROM Bar")
    execute("REVOKE ROLE foo FROM Baz")

    // THEN
    execute("SHOW POPULATED ROLES WITH USERS").toSet should be(Set(admin))

    // WHEN granting with multiple users and roles version
    execute("GRANT ROLE foo, fum TO Bar, Baz")

    // THEN
    execute("SHOW POPULATED ROLES WITH USERS").toSet should be(Set(admin, role("foo", "Bar"), role("foo", "Baz"), role("fum", "Bar"), role("fum", "Baz")))

    // WHEN revoking only one of many
    execute("REVOKE ROLE foo FROM Bar")

    // THEN
    execute("SHOW POPULATED ROLES WITH USERS").toSet should be(Set(admin, role("foo", "Baz"), role("fum", "Bar"), role("fum", "Baz")))

    // WHEN revoking with multiple users and roles version
    execute("REVOKE ROLE foo, fum FROM Bar, Baz")

    // THEN
    execute("SHOW POPULATED ROLES WITH USERS").toSet should be(Set(admin))
  }

  test("should do nothing when revoking non-existent role from (existing) user") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE USER user SET PASSWORD 'neo'")
    execute("SHOW ROLES WITH USERS").toSet should be(defaultRolesWithUsers)

    // WHEN
    execute("REVOKE ROLE custom FROM user")

    // THEN
    execute("SHOW ROLES WITH USERS").toSet should be(defaultRolesWithUsers)
  }

  test("should do nothing when revoking (existing) role from non-existing user") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    val roles = defaultRolesWithUsers + Map("role" -> "custom", "isBuiltIn" -> false, "member" -> null)
    execute("SHOW ROLES WITH USERS").toSet should be(roles)

    // WHEN
    execute("REVOKE ROLE custom FROM user")

    // THEN
    execute("SHOW ROLES WITH USERS").toSet should be(roles)
  }

  test("should do nothing when revoking non-existing role from non-existing user") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("SHOW ROLES WITH USERS").toSet should be(defaultRolesWithUsers)

    // WHEN
    execute("REVOKE ROLE custom FROM user")

    // THEN
    execute("SHOW ROLES WITH USERS").toSet should be(defaultRolesWithUsers)
  }

  test("should fail when revoking role from user when not on system database") {
    the[DatabaseAdministrationException] thrownBy {
      // WHEN
      execute("REVOKE ROLE dragon FROM Bar")
      // THEN
    } should have message
      "This is an administration command and it should be executed against the system database: REVOKE ROLE"

    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE USER Bar SET PASSWORD 'neo'")
    execute("CREATE ROLE dragon")
    execute("GRANT ROLE dragon TO Bar")
    selectDatabase(GraphDatabaseSettings.DEFAULT_DATABASE_NAME)

    the[DatabaseAdministrationException] thrownBy {
      // WHEN
      execute("REVOKE ROLE dragon FROM Bar")
      // THEN
    } should have message
      "This is an administration command and it should be executed against the system database: REVOKE ROLE"
  }
}
