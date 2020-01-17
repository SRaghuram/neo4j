/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.internal.cypher.acceptance

import com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.cypher.internal.v4_0.ast.semantics.SemanticError
import org.neo4j.exceptions.{DatabaseAdministrationException, InvalidArgumentException, SyntaxException}
import org.neo4j.graphdb.security.AuthorizationViolationException
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException
import org.scalatest.enablers.Messaging.messagingNatureOfThrowable

class RoleAdministrationCommandAcceptanceTest extends AdministrationCommandAcceptanceTestBase {

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
      "CREATE ROLE foo2 IF NOT EXISTS" -> 1,
      "CREATE OR REPLACE ROLE foo" -> 2,
      "CREATE OR REPLACE ROLE foo3" -> 1,
      "CREATE ROLE bar AS COPY OF foo" -> 1,
      "CREATE ROLE bar2 IF NOT EXISTS AS COPY OF foo" -> 1,
      "CREATE OR REPLACE ROLE bar AS COPY OF foo" -> 2,
      "CREATE OR REPLACE ROLE bar3 AS COPY OF foo" -> 1,
      "GRANT ROLE foo TO Bar" -> 1,
      "REVOKE ROLE foo FROM Bar" -> 1,
      "DROP ROLE foo" -> 1,
      "DROP ROLE foo2 IF EXISTS" -> 1,
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
    result.toSet should be(Set(role(PredefinedRoles.ADMIN).builtIn().map))
  }

  test("should create and show roles") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)

    // WHEN
    execute("CREATE ROLE foo")
    val result = execute("SHOW ROLES")

    // THEN
    result.toSet should be(defaultRoles ++ Set(role("foo").map))
  }

  test("should not create role with reserved name") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)

    // WHEN
    val exception = the[SyntaxException] thrownBy execute("CREATE ROLE PUBLIC")
    exception.getMessage should startWith("Failed to create the specified role 'PUBLIC': 'PUBLIC' is a reserved role name and cannot be created.")

    // THEN
    val result = execute("SHOW ROLES")
    result.toSet should be(defaultRoles)
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
    result.toSet should be(Set(role(PredefinedRoles.ADMIN).builtIn().map, role("foo").map))
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
    result.toSet should be(Set(role(PredefinedRoles.ADMIN).builtIn().member("neo4j").map))
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
      role(PredefinedRoles.ADMIN).builtIn().member("neo4j").map,
      role("foo").member("Bar").map,
      role("foo").member("Baz").map
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

    execute("SHOW ROLES").toSet should be(defaultRoles ++ Set(role("foo").map))
  }

  test("should create role using if not exists") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)

    // WHEN
    execute("CREATE ROLE foo IF NOT EXISTS")

    execute("SHOW ROLES").toSet should be(defaultRoles ++ Set(role("foo").map))
  }

  test("should fail when creating already existing role") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE foo")
    execute("SHOW ROLES").toSet should be(defaultRoles ++ Set(role("foo").map))

    the[InvalidArgumentsException] thrownBy {
      // WHEN
      execute("CREATE ROLE foo")
      // THEN
    } should have message "Failed to create the specified role 'foo': Role already exists."

    execute("SHOW ROLES").toSet should be(defaultRoles ++ Set(role("foo").map))
  }

  test("should do nothing when creating already existing role using if not exists") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE foo")
    execute("SHOW ROLES").toSet should be(defaultRoles ++ Set(role("foo").map))

    // WHEN
    execute("CREATE ROLE foo IF NOT EXISTS")

    // THEN
    execute("SHOW ROLES").toSet should be(defaultRoles ++ Set(role("foo").map))
  }

  test("should replace already existing role") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("SHOW ROLES").toSet should be(defaultRoles)

    // WHEN: creation
    execute("CREATE OR REPLACE ROLE foo")
    execute("GRANT ROLE foo TO neo4j")

    // THEN
    execute("SHOW ROLES WITH USERS").toSet should be(defaultRolesWithUsers ++ Set(role("foo").member("neo4j").map))

    // WHEN: replacing
    execute("CREATE OR REPLACE ROLE foo")

    // THEN
    execute("SHOW ROLES WITH USERS").toSet should be(defaultRolesWithUsers ++ Set(role("foo").noMember().map))
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
    execute("SHOW ROLES").toSet should be(defaultRoles ++ Set(role("foo").map))

    // WHEN
    execute("CREATE ROLE bar AS COPY OF foo")

    execute("SHOW ROLES").toSet should be(defaultRoles ++ Set(role("foo").map, role("bar").map))
    execute("SHOW ROLE bar PRIVILEGES").toSet should be(Set.empty)
  }

  test("should create role from existing role using if not exists") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE foo")
    execute("SHOW ROLES").toSet should be(defaultRoles ++ Set(role("foo").map))

    // WHEN
    execute("CREATE ROLE bar IF NOT EXISTS AS COPY OF foo")

    execute("SHOW ROLES").toSet should be(defaultRoles ++ Set(role("foo").map, role("bar").map))
    execute("SHOW ROLE bar PRIVILEGES").toSet should be(Set.empty)
  }

  test("should create role from existing role and copy privileges") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE foo")
    execute("SHOW ROLES").toSet should be(defaultRoles ++ Set(role("foo").map))
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
    execute("SHOW ROLES").toSet should be(defaultRoles ++ Set(role("foo").map, role("bar").map))
    execute("SHOW ROLE foo PRIVILEGES").toSet should be(expectedFoo)
    execute("SHOW ROLE bar PRIVILEGES").toSet should be(expectedBar)
  }

  test("should replace role and copy privileges from existing role") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE base1")
    execute("CREATE ROLE base2")
    val baseRoles = Set(Map("role" -> "base1", "isBuiltIn" -> false), Map("role" -> "base2", "isBuiltIn" -> false))
    execute("SHOW ROLES").toSet should be(defaultRoles ++ baseRoles)
    execute("GRANT TRAVERSE ON GRAPH * NODES A TO base1")
    execute("GRANT TRAVERSE ON GRAPH * NODES B TO base2")

    // WHEN: creation
    execute("CREATE OR REPLACE ROLE bar AS COPY OF base1")

    // THEN
    execute("SHOW ROLES").toSet should be(defaultRoles ++ baseRoles ++ Set(role("bar").map))
    execute("SHOW ROLE bar PRIVILEGES").toSet should be(Set(traverse().role("bar").node("A").map))

    // WHEN: replacing
    execute("CREATE OR REPLACE ROLE bar AS COPY OF base2")

    // THEN
    execute("SHOW ROLES").toSet should be(defaultRoles ++ baseRoles ++ Set(role("bar").map))
    execute("SHOW ROLE bar PRIVILEGES").toSet should be(Set(traverse().role("bar").node("B").map))
  }

  test("should fail when creating from non-existing role") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)

    the[InvalidArgumentsException] thrownBy {
      // WHEN
      execute("CREATE ROLE bar AS COPY OF foo")
      // THEN
    } should have message "Failed to create a role as copy of 'foo': Role does not exist."

    // THEN
    execute("SHOW ROLES").toSet should be(defaultRoles ++ Set.empty)

    the[InvalidArgumentsException] thrownBy {
      // WHEN
      execute("CREATE ROLE bar IF NOT EXISTS AS COPY OF foo")
      // THEN
    } should have message "Failed to create a role as copy of 'foo': Role does not exist."

    // THEN
    execute("SHOW ROLES").toSet should be(defaultRoles ++ Set.empty)

    // and an invalid (non-existing) one
    the[InvalidArgumentsException] thrownBy {
      // WHEN
      execute("CREATE ROLE bar AS COPY OF ``")
      // THEN
    } should have message "Failed to create a role as copy of '': Role does not exist."

    // THEN
    execute("SHOW ROLES").toSet should be(defaultRoles ++ Set.empty)
  }

  test("should fail when creating role with invalid name from role") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE foo")
    execute("SHOW ROLES").toSet should be(defaultRoles ++ Set(role("foo").map))

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

    execute("SHOW ROLES").toSet should be(defaultRoles ++ Set(role("foo").map))
  }

  test("should fail when creating already existing role from other role") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE foo")
    execute("CREATE ROLE bar")
    execute("SHOW ROLES").toSet should be(defaultRoles ++ Set(role("foo").map, role("bar").map))

    the[InvalidArgumentsException] thrownBy {
      // WHEN
      execute("CREATE ROLE bar AS COPY OF foo")
      // THEN
    } should have message "Failed to create the specified role 'bar': Role already exists."

    execute("SHOW ROLES").toSet should be(defaultRoles ++ Set(role("foo").map, role("bar").map))
  }

  test("should do nothing when creating already existing role from other role using if not exists") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE foo")
    execute("GRANT TRAVERSE ON GRAPH * NODES * TO foo")
    execute("CREATE ROLE bar")
    execute("SHOW ROLES").toSet should be(defaultRoles ++ Set(role("foo").map, role("bar").map))
    execute("SHOW ROLE foo PRIVILEGES").toSet should be(Set(traverse().role("foo").node("*").map))
    execute("SHOW ROLE bar PRIVILEGES").toSet should be(Set.empty)

    // WHEN
    execute("CREATE ROLE bar IF NOT EXISTS AS COPY OF foo")

    // THEN
    execute("SHOW ROLES").toSet should be(defaultRoles ++ Set(role("foo").map, role("bar").map))
    execute("SHOW ROLE foo PRIVILEGES").toSet should be(Set(traverse().role("foo").node("*").map))
    execute("SHOW ROLE bar PRIVILEGES").toSet should be(Set.empty)
  }

  test("should fail when creating existing role from non-existing role") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE bar")
    execute("SHOW ROLES").toSet should be(defaultRoles ++ Set(role("bar").map))

    the[InvalidArgumentsException] thrownBy {
      // WHEN
      execute("CREATE ROLE bar AS COPY OF foo")
      // THEN
    } should have message "Failed to create a role as copy of 'foo': Role does not exist."

    execute("SHOW ROLES").toSet should be(defaultRoles ++ Set(role("bar").map))
  }

  test("should do nothing when creating existing role from non-existing role using if exists") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE bar")
    execute("GRANT ROLE bar TO neo4j")
    execute("SHOW ROLES WITH USERS").toSet should be(defaultRolesWithUsers ++ Set(role("bar").member("neo4j").map))

    // WHEN
    execute("CREATE ROLE bar IF NOT EXISTS AS COPY OF foo")

    // THEN
    execute("SHOW ROLES WITH USERS").toSet should be(defaultRolesWithUsers ++ Set(role("bar").member("neo4j").map))
  }

  test("should get syntax exception when using both replace and if not exists") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)

    // WHEN
    val exceptionCreate = the[SyntaxException] thrownBy {
      execute("CREATE OR REPLACE ROLE foo IF NOT EXISTS")
    }

    // THEN
    exceptionCreate.getMessage should include("Failed to create the specified role 'foo': cannot have both `OR REPLACE` and `IF NOT EXISTS`.")

    // WHEN
    val exceptionCopy = the[SyntaxException] thrownBy {
      execute("CREATE OR REPLACE ROLE foo IF NOT EXISTS AS COPY OF bar")
    }

    // THEN
    exceptionCopy.getMessage should include("Failed to create the specified role 'foo': cannot have both `OR REPLACE` and `IF NOT EXISTS`.")
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
    execute("SHOW ROLES").toSet should be(defaultRoles ++ Set(role("foo").map))

    // WHEN
    execute("DROP ROLE foo")

    execute("SHOW ROLES").toSet should be(defaultRoles ++ Set.empty)
  }

  test("should not drop role with reserved name") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)

    // WHEN
    val exception = the[SyntaxException] thrownBy execute("DROP ROLE PUBLIC")
    exception.getMessage should startWith("Failed to drop the specified role 'PUBLIC': 'PUBLIC' is a reserved role and cannot be dropped.")

    // THEN
    val result = execute("SHOW ROLES")
    result.toSet should be(defaultRoles)
  }

  test("should drop existing role using if exists") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE foo")
    execute("SHOW ROLES").toSet should be(defaultRoles ++ Set(role("foo").map))

    // WHEN
    execute("DROP ROLE foo IF EXISTS")

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
    execute("SHOW ROLES").toSet should be(defaultRoles -- Set(role(PredefinedRoles.READER).builtIn().map))
  }

  test("should not drop admin role") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("SHOW ROLES").toSet should be(defaultRoles)

    // WHEN
    the[AuthorizationViolationException] thrownBy {
      execute(s"DROP ROLE ${PredefinedRoles.ADMIN}")
    } should have message "Permission denied."

    // THEN
    execute("SHOW ROLES").toSet should be(defaultRoles)
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
    execute("DROP ROLE foo IF EXISTS")

    // THEN
    execute("SHOW ROLES").toSet should be(defaultRoles)

    // and an invalid (non-existing) one

    // WHEN
    execute("DROP ROLE `` IF EXISTS")

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

  test("should not grant reserved role to user") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE USER user SET PASSWORD 'neo'")

    // WHEN
    val exception = the[SyntaxException] thrownBy execute("GRANT ROLE PUBLIC TO user")
    // THEN
    exception.getMessage should startWith("Failed to grant the specified role 'PUBLIC': 'PUBLIC' is a reserved role and cannot be granted.")

    execute("SHOW USERS").toSet shouldBe Set(neo4jUser, user("user"))
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

  test("should not revoke reserved role from user") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE USER user SET PASSWORD 'neo'")

    // WHEN
    val exception = the[SyntaxException] thrownBy execute("REVOKE ROLE PUBLIC FROM user")
    // THEN
    exception.getMessage should startWith("Failed to revoke the specified role 'PUBLIC': 'PUBLIC' is a reserved role and cannot be revoked.")
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
    val admin = role(PredefinedRoles.ADMIN).builtIn().member("neo4j").map

    // WHEN using single user and role version of GRANT
    execute("GRANT ROLE foo TO Bar")
    execute("GRANT ROLE foo TO Baz")

    // THEN
    execute("SHOW POPULATED ROLES WITH USERS").toSet should be(Set(admin, role("foo").member("Bar").map, role("foo").member("Baz").map))

    // WHEN using single user and role version of REVOKE
    execute("REVOKE ROLE foo FROM Bar")
    execute("REVOKE ROLE foo FROM Baz")

    // THEN
    execute("SHOW POPULATED ROLES WITH USERS").toSet should be(Set(admin))

    // WHEN granting with multiple users and roles version
    execute("GRANT ROLE foo, fum TO Bar, Baz")

    // THEN
    execute("SHOW POPULATED ROLES WITH USERS").toSet should be(Set(admin, role("foo").member("Bar").map, role("foo").member("Baz").map, role("fum").member("Bar").map, role("fum").member("Baz").map))

    // WHEN revoking only one of many
    execute("REVOKE ROLE foo FROM Bar")

    // THEN
    execute("SHOW POPULATED ROLES WITH USERS").toSet should be(Set(admin, role("foo").member("Baz").map, role("fum").member("Bar").map, role("fum").member("Baz").map))

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
