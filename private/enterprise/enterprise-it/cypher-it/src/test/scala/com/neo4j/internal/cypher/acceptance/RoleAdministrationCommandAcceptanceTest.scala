/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.internal.cypher.acceptance

import com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles
import com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.ADMIN
import com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.PUBLIC
import com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.READER
import org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME
import org.neo4j.exceptions.DatabaseAdministrationException
import org.neo4j.exceptions.InvalidArgumentException
import org.neo4j.exceptions.SyntaxException
import org.neo4j.graphdb.security.AuthorizationViolationException
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException

class RoleAdministrationCommandAcceptanceTest extends AdministrationCommandAcceptanceTestBase {

  test("should return empty counts to the outside for commands that update the system graph internally") {
    //TODO: ADD ANY NEW UPDATING COMMANDS HERE

    // GIVEN
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
    // WHEN
    val result = execute("SHOW ALL ROLES")

    // THEN
    result.toSet should be(defaultRoles)
  }

  test("should show all default roles with yield") {
    // WHEN
    val result = execute("SHOW ALL ROLES YIELD role")

    // THEN
    result.toSet should be(defaultRoles)
  }

  test("should show all default roles with where") {
    // WHEN
    val result = execute(s"SHOW ALL ROLES WHERE role='${PredefinedRoles.PUBLIC}'")

    // THEN
    result.toSet should be(Set(public))
  }

  test("should show populated default roles") {
    // WHEN
    val result = execute("SHOW POPULATED ROLES")

    // THEN
    result.toSet should be(Set(admin, public))
  }

  test("should not accept EXISTS subclause in show commands") {
    val exception = the[SyntaxException] thrownBy {
      execute("SHOW ROLES WHERE true AND EXISTS { MATCH (n) }")
    }
    exception.getMessage should startWith("The EXISTS clause is not valid on SHOW commands. (line 1, column 27 (offset: 26))")
  }

  test("should not accept EXISTS subclause in show commands with RETURN") {
    val exception = the[SyntaxException] thrownBy {
      execute("SHOW ROLES YIELD role RETURN role, EXISTS { MATCH (n) }")
    }
    exception.getMessage should startWith("The EXISTS subclause is not valid inside a WITH or RETURN clause. (line 1, column 36 (offset: 35))")
  }

  test("should create and show roles") {
    // WHEN
    execute("CREATE ROLE foo")
    val result = execute("SHOW ROLES")

    // THEN
    result.toSet should be(defaultRoles ++ Set(role("foo").map))
  }

  test("should show populated roles") {
    execute("CREATE USER Bar SET PASSWORD 'neo'")
    execute("CREATE USER Baz SET PASSWORD 'NEO'")
    execute("CREATE ROLE foo")
    execute("GRANT ROLE foo TO Bar")
    execute("GRANT ROLE foo TO Baz")

    // WHEN
    val result = execute("SHOW POPULATED ROLES")

    // THEN
    result.toSet should be(Set(admin, public, role("foo").map))
  }

  test("should show default roles with users") {
    // WHEN
    val result = execute("SHOW ROLES WITH USERS")

    // THEN
    result.toSet should be(defaultRolesWithUsers)
  }

  test("should show all default roles with users") {
    // WHEN
    val result = execute("SHOW ALL ROLES WITH USERS")

    // THEN
    result.toSet should be(defaultRolesWithUsers)
  }

  test("should show all default roles with users with yield") {
    // WHEN
    val result = execute("SHOW ALL ROLES WITH USERS YIELD member ")

    // THEN
    val justMembers = defaultRolesWithUsers.toList.map(_.filterKeys(_ == "member"))
    result.toList should contain theSameElementsAs justMembers
  }

  test("should show all default roles with users with order by") {
    // WHEN
    val result = execute("SHOW ALL ROLES WITH USERS YIELD role, member ORDER BY role")

    // THEN
    val sortedByRole = defaultRolesWithUsers.toList.sortBy(_.apply("role").asInstanceOf[String])
    result.toList should be(sortedByRole)
  }

  test("should show all default roles with users with order by limit / skip ") {
    // WHEN
    // Order should be PUBLIC, admin, architect, editor, publisher, reader
    val result = execute("SHOW ALL ROLES WITH USERS YIELD role, member ORDER BY role SKIP 1 LIMIT 2")

    // THEN
    // sort is A-Za-z so we skip PUBLIC and start at admin
    result.toList should be(List(Map("role" -> "admin", "member" -> "neo4j"), Map("role" -> "architect", "member" -> null)))
  }

  test("should show all default roles with users with order by limit / skip in return") {
    // WHEN
    val result = execute("SHOW ALL ROLES WITH USERS YIELD role, member WHERE true RETURN role, member ORDER BY role SKIP 1 LIMIT 2")

    // THEN
    // sort is A-Za-z so we skip PUBLIC and start at admin
    result.toList should be(List(Map("role" -> "admin", "member" -> "neo4j"), Map("role" -> "architect", "member" -> null)))
  }

  test("should count roles using aggregation and alias") {
    // WHEN
    // Roles are PUBLIC, admin, architect, editor, publisher, reader
    val result = execute("SHOW ALL ROLES WITH USERS YIELD role RETURN count(role) as rolecount")

    // THEN
    result.toList should be(List(Map("rolecount" -> 6)))
  }

  test("should count roles using aggregation and no alias") {
    // WHEN
    // Roles are PUBLIC, admin, architect, editor, publisher, reader
    val result = execute("SHOW ALL ROLES WITH USERS YIELD role RETURN count(role)")

    // THEN
    result.toList should be(List(Map("count(role)" -> 6)))
  }

  test("should show populated roles with users") {
    // GIVEN
    execute("CREATE ROLE foo")

    // WHEN
    val result = execute("SHOW POPULATED ROLES WITH USERS")

    // THEN
    result.toSet should be(Set(adminWithDefaultUser) ++ publicRole("neo4j"))
  }

  test("should show populated roles with several users") {
    // GIVEN
    execute("CREATE USER Bar SET PASSWORD 'neo'")
    execute("CREATE USER Baz SET PASSWORD 'NEO'")
    execute("CREATE ROLE foo")
    execute("GRANT ROLE foo TO Bar")
    execute("GRANT ROLE foo TO Baz")

    // WHEN
    val result = execute("SHOW POPULATED ROLES WITH USERS")

    // THEN
    result.toSet should be(Set(
      adminWithDefaultUser,
      role("foo").member("Bar").map,
      role("foo").member("Baz").map,
    ) ++ publicRole("neo4j", "Bar", "Baz"))
  }

  test("should show populated roles with users with YIELD with aliases") {
    // GIVEN
    execute("CREATE ROLE foo")

    // WHEN
    val result = execute("SHOW POPULATED ROLES WITH USERS YIELD role as foo WHERE foo = 'admin' RETURN foo")

    // THEN
    result.toSet should be(Set(Map("foo" -> "admin")))
  }

  test("should show populated roles with users with YIELD and RETURN with aliases") {
    // GIVEN
    execute("CREATE ROLE foo")

    // WHEN
    val result = execute("SHOW POPULATED ROLES WITH USERS YIELD role WHERE role = 'admin' RETURN role as foo")

    // THEN
    result.toSet should be(Set(Map("foo" -> "admin")))
  }

  test("should not show roles when not using the system database") {
    // WHEN
    selectDatabase(DEFAULT_DATABASE_NAME)
    val exception = the[DatabaseAdministrationException] thrownBy {
      execute("SHOW ALL ROLES WITH USERS YIELD * WHERE member = $username", ("username"-> "neo4j"))
    }

    // THEN
    exception.getMessage shouldBe("This is an administration command and it should be executed against the system database: SHOW ALL ROLES")
  }

  test("should not show role with invalid yield") {
    // GIVEN
    setup()

    // WHEN
    val exception = the[SyntaxException] thrownBy {
      execute("SHOW ALL ROLES YIELD foo, bar, baz")
    }

    // THEN
    exception.getMessage should startWith("Variable `foo` not defined")
    exception.getMessage should include("(line 1, column 22 (offset: 21))")

  }

  test("should not show role with return without yield") {
    // GIVEN
    setup()

    // WHEN
    val exception = the[SyntaxException] thrownBy {
      execute("SHOW ALL ROLES RETURN role")
    }

    // THEN
    exception.getMessage should startWith("Invalid input 'R': expected whitespace, comment, WITH, YIELD, WHERE, ';' or end of input")
    exception.getMessage should include("(line 1, column 16 (offset: 15))")
  }

  test("should not show role with return with where without yield") {
    // GIVEN
    setup()

    // WHEN
    val exception = the[SyntaxException] thrownBy {
      execute("SHOW ALL ROLES WHERE role='PUBLIC' RETURN role")
    }

    // THEN
    exception.getMessage should startWith("Invalid input 'R'")
    exception.getMessage should include("(line 1, column 36 (offset: 35))")
  }

  test("should not show role with invalid return") {
    // GIVEN
    setup()

    // WHEN
    val exception = the[SyntaxException] thrownBy {
      execute("SHOW ALL ROLES YIELD role RETURN foo")
    }
    // THEN
    exception.getMessage should startWith("Variable `foo` not defined")
    exception.getMessage should include("(line 1, column 34 (offset: 33))")

  }

  test("should not count roles using invalid aggregation") {
    // WHEN
    val exception = the[SyntaxException] thrownBy {
      execute("SHOW ALL ROLES YIELD role RETURN count(foo)")
    }

    // THEN
    exception.getMessage should startWith("Variable `foo` not defined")
    exception.getMessage should include("(line 1, column 40 (offset: 39))")
  }

  test("should not show role with invalid where") {
    // GIVEN
    setup()

    // WHEN
    val exception = the[SyntaxException] thrownBy {
      execute("SHOW POPULATED ROLES WHERE foo = 'bar'")
    }

    // THEN
    exception.getMessage should startWith("Variable `foo` not defined")
    exception.getMessage should include("(line 1, column 28 (offset: 27))")
  }

  test("should not show role with yield and invalid where") {
    // GIVEN
    setup()

    // WHEN
    val exception = the[SyntaxException] thrownBy {
      execute("SHOW ALL ROLES YIELD role WHERE foo = 'bar'")
    }

    // THEN
    exception.getMessage should startWith("Variable `foo` not defined")
    exception.getMessage should include("(line 1, column 33 (offset: 32))")
  }

  test("should not show role with yield and invalid skip") {
    // GIVEN
    setup()

    // WHEN
    val exception = the[SyntaxException] thrownBy {
      execute("SHOW POPULATED ROLES YIELD role ORDER BY role SKIP -1")
    }

    // THEN
    exception.getMessage should startWith("Invalid input. '-1' is not a valid value. Must be a non-negative integer")
    exception.getMessage should include("(line 1, column 52 (offset: 51))")
  }

  test("should not show role with yield and invalid limit") {
    // GIVEN
    setup()

    // WHEN
    val exception = the[SyntaxException] thrownBy {
      execute("SHOW ALL ROLES YIELD role ORDER BY role LIMIT -1")
    }

    // THEN
    exception.getMessage should startWith("Invalid input. '-1' is not a valid value. Must be a non-negative integer")
    exception.getMessage should include("(line 1, column 47 (offset: 46))")
  }

  test("should not show role with invalid order by") {
    // GIVEN
    setup()

    // WHEN
    val exception = the[SyntaxException] thrownBy {
      execute("SHOW ALL ROLES YIELD role ORDER BY bar")
    }

    // THEN
    exception.getMessage should startWith("Variable `bar` not defined")
    exception.getMessage should include("(line 1, column 36 (offset: 35))")
  }

  // Tests for creating roles

  test("should create role") {
    // WHEN
    execute("CREATE ROLE foo")

    execute("SHOW ROLES").toSet should be(defaultRoles ++ Set(role("foo").map))
  }

  test("should create role with parameter") {
    // WHEN
    execute("CREATE ROLE $role", Map("role" -> "foo"))

    execute("SHOW ROLES").toSet should be(defaultRoles ++ Set(role("foo").map))
  }

  test("should create role using if not exists") {
    // WHEN
    execute("CREATE ROLE foo IF NOT EXISTS")

    execute("SHOW ROLES").toSet should be(defaultRoles ++ Set(role("foo").map))
  }

  test("should not create role with reserved name") {
    // WHEN
    val exception = the[InvalidArgumentException] thrownBy execute("CREATE ROLE PUBLIC")
    exception.getMessage should startWith("Failed to create the specified role 'PUBLIC': 'PUBLIC' is a reserved role.")

    // THEN
    val result = execute("SHOW ROLES")
    result.toSet should be(defaultRoles)
  }

  test("should not create role with reserved name using parameter") {
    // WHEN
    val exception = the[InvalidArgumentException] thrownBy execute("CREATE ROLE $role", Map("role" -> PUBLIC))
    exception.getMessage should startWith("Failed to create the specified role 'PUBLIC': 'PUBLIC' is a reserved role.")

    // THEN
    val result = execute("SHOW ROLES")
    result.toSet should be(defaultRoles)
  }

  test("should allow create role with parameter that looks like reserved name") {
    // WHEN
    execute(s"CREATE ROLE $$$PUBLIC", Map(PUBLIC -> "allowed"))

    // THEN
    val result = execute("SHOW ROLES")
    result.toSet should be(defaultRoles ++ Set(role("allowed").map))
  }

  test("should fail when creating already existing role") {
    // GIVEN
    execute("CREATE ROLE foo")

    the[InvalidArgumentsException] thrownBy {
      // WHEN
      execute("CREATE ROLE foo")
      // THEN
    } should have message "Failed to create the specified role 'foo': Role already exists."

    the[InvalidArgumentsException] thrownBy {
      // WHEN
      execute("CREATE ROLE $r", Map("r" -> "foo"))
      // THEN
    } should have message "Failed to create the specified role 'foo': Role already exists."

    execute("SHOW ROLES").toSet should be(defaultRoles ++ Set(role("foo").map))
  }

  test("should do nothing when creating already existing role using if not exists") {
    // GIVEN
    execute("CREATE ROLE foo")

    // WHEN
    execute("CREATE ROLE foo IF NOT EXISTS")

    // THEN
    execute("SHOW ROLES").toSet should be(defaultRoles ++ Set(role("foo").map))
  }

  test("should replace already existing role") {
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
    the[InvalidArgumentException] thrownBy {
      // WHEN
      execute("CREATE ROLE ``")
      // THEN
    } should have message "The provided role name is empty."

    the[InvalidArgumentException] thrownBy {
      // WHEN
      execute("CREATE ROLE $r", Map("r" -> ""))
      // THEN
    } should have message "The provided role name is empty."

    the[InvalidArgumentException] thrownBy {
      // WHEN
      execute("CREATE ROLE `my%role`")
      // THEN
    } should have message
      """Role name 'my%role' contains illegal characters.
        |Use simple ascii characters, numbers and underscores.""".stripMargin

    the[InvalidArgumentException] thrownBy {
      // WHEN
      execute("CREATE ROLE $r", Map("r" -> "my%role"))
      // THEN
    } should have message
      """Role name 'my%role' contains illegal characters.
        |Use simple ascii characters, numbers and underscores.""".stripMargin

    execute("SHOW ROLES").toSet should be(defaultRoles)
  }

  test("should create role from existing role") {
    // GIVEN
    execute("CREATE ROLE foo")

    // WHEN
    execute("CREATE ROLE bar AS COPY OF foo")

    // THEN
    execute("SHOW ROLES").toSet should be(defaultRoles ++ Set(role("foo").map, role("bar").map))
    execute("SHOW ROLE bar PRIVILEGES").toSet should be(Set.empty)
  }

  test("should create role from existing role with parameter") {
    // GIVEN
    execute("CREATE ROLE foo")

    // WHEN
    execute("CREATE ROLE bar AS COPY OF $other", Map("other" -> "foo"))

    // THEN
    execute("SHOW ROLES").toSet should be(defaultRoles ++ Set(role("foo").map, role("bar").map))
    execute("SHOW ROLE bar PRIVILEGES").toSet should be(Set.empty)
  }

  test("should create role with parameter from existing role with parameter") {
    // GIVEN
    execute("CREATE ROLE foo")

    // WHEN
    execute("CREATE ROLE $role AS COPY OF $other", Map("role" -> "bar", "other" -> "foo"))

    // THEN
    execute("SHOW ROLES").toSet should be(defaultRoles ++ Set(role("foo").map, role("bar").map))
    execute("SHOW ROLE bar PRIVILEGES").toSet should be(Set.empty)
  }

  test("should create role from existing role using if not exists") {
    // GIVEN
    execute("CREATE ROLE foo")

    // WHEN
    execute("CREATE ROLE bar IF NOT EXISTS AS COPY OF foo")

    // THEN
    execute("SHOW ROLES").toSet should be(defaultRoles ++ Set(role("foo").map, role("bar").map))
    execute("SHOW ROLE bar PRIVILEGES").toSet should be(Set.empty)
  }

  test("should create role from existing role and copy privileges") {
    // GIVEN
    execute("CREATE ROLE foo")
    execute("GRANT TRAVERSE ON GRAPH * NODES * (*) TO foo")
    execute("GRANT READ {a,b,c} ON GRAPH * NODES A (*) TO foo")
    val expected = Set(granted(traverse).node("*").map,
      granted(read).property("a").node("A").map,
      granted(read).property("b").node("A").map,
      granted(read).property("c").node("A").map
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
    execute("CREATE ROLE base1")
    execute("CREATE ROLE base2")
    execute("GRANT TRAVERSE ON GRAPH * NODES A TO base1")
    execute("GRANT TRAVERSE ON GRAPH * NODES B TO base2")
    val baseRoles = Set(role("base1").map, role("base2").map)

    // WHEN: creation
    execute("CREATE OR REPLACE ROLE bar AS COPY OF base1")

    // THEN
    execute("SHOW ROLES").toSet should be(defaultRoles ++ baseRoles ++ Set(role("bar").map))
    execute("SHOW ROLE bar PRIVILEGES").toSet should be(Set(granted(traverse).role("bar").node("A").map))

    // WHEN: replacing
    execute("CREATE OR REPLACE ROLE bar AS COPY OF base2")

    // THEN
    execute("SHOW ROLES").toSet should be(defaultRoles ++ baseRoles ++ Set(role("bar").map))
    execute("SHOW ROLE bar PRIVILEGES").toSet should be(Set(granted(traverse).role("bar").node("B").map))

    // WHEN: replacing with parameters
    execute("CREATE OR REPLACE ROLE $role1 AS COPY OF $role2", Map("role1" -> "bar", "role2" -> "base1"))

    // THEN
    execute("SHOW ROLES").toSet should be(defaultRoles ++ baseRoles ++ Set(role("bar").map))
    execute("SHOW ROLE bar PRIVILEGES").toSet should be(Set(granted(traverse).role("bar").node("A").map))
  }

  test("should fail when creating from non-existing role") {
    the[InvalidArgumentsException] thrownBy {
      // WHEN
      execute("CREATE ROLE bar AS COPY OF foo")
      // THEN
    } should have message "Failed to create a role as copy of 'foo': Role does not exist."

    // THEN
    execute("SHOW ROLES").toSet should be(defaultRoles)

    the[InvalidArgumentsException] thrownBy {
      // WHEN
      execute("CREATE ROLE $newRole IF NOT EXISTS AS COPY OF $oldRole", Map("newRole" -> "bar", "oldRole" -> "foo"))
      // THEN
    } should have message "Failed to create a role as copy of 'foo': Role does not exist."

    // THEN
    execute("SHOW ROLES").toSet should be(defaultRoles)

    // and an invalid (non-existing) one
    the[InvalidArgumentsException] thrownBy {
      // WHEN
      execute("CREATE ROLE bar AS COPY OF ``")
      // THEN
    } should have message "Failed to create a role as copy of '': Role does not exist."

    // THEN
    execute("SHOW ROLES").toSet should be(defaultRoles)
  }

  test("should fail when creating role with invalid name from role") {
    // GIVEN
    execute("CREATE ROLE foo")

    the[InvalidArgumentException] thrownBy {
      // WHEN
      execute("CREATE ROLE `` AS COPY OF foo")
      // THEN
    } should have message "The provided role name is empty."

    // and with parameter
    the[InvalidArgumentException] thrownBy {
      // WHEN
      execute("CREATE ROLE $role AS COPY OF foo", Map("role" -> ""))
      // THEN
    } should have message "The provided role name is empty."

    // and using illegal characters
    the[InvalidArgumentException] thrownBy {
      // WHEN
      execute("CREATE ROLE `my%role` AS COPY OF foo")
      // THEN
    } should have message
      """Role name 'my%role' contains illegal characters.
        |Use simple ascii characters, numbers and underscores.""".stripMargin

    // and using illegal characters with parameter
    the[InvalidArgumentException] thrownBy {
      // WHEN
      execute("CREATE ROLE $role AS COPY OF foo", Map("role" -> "my%role"))
      // THEN
    } should have message
      """Role name 'my%role' contains illegal characters.
        |Use simple ascii characters, numbers and underscores.""".stripMargin

    execute("SHOW ROLES").toSet should be(defaultRoles ++ Set(role("foo").map))
  }

  test("should fail when creating already existing role from other role") {
    // GIVEN
    execute("CREATE ROLE foo")
    execute("CREATE ROLE bar")

    the[InvalidArgumentsException] thrownBy {
      // WHEN
      execute("CREATE ROLE bar AS COPY OF foo")
      // THEN
    } should have message "Failed to create the specified role 'bar': Role already exists."

    the[InvalidArgumentsException] thrownBy {
      // WHEN
      execute("CREATE ROLE $newRole AS COPY OF $oldRole", Map("newRole" -> "bar", "oldRole" -> "foo"))
      // THEN
    } should have message "Failed to create the specified role 'bar': Role already exists."

    execute("SHOW ROLES").toSet should be(defaultRoles ++ Set(role("foo").map, role("bar").map))
  }

  test("should do nothing when creating already existing role from other role using if not exists") {
    // GIVEN
    execute("CREATE ROLE foo")
    execute("GRANT TRAVERSE ON GRAPH * NODES * TO foo")
    execute("CREATE ROLE bar")

    // WHEN
    execute("CREATE ROLE bar IF NOT EXISTS AS COPY OF foo")

    // THEN
    execute("SHOW ROLES").toSet should be(defaultRoles ++ Set(role("foo").map, role("bar").map))
    execute("SHOW ROLE foo PRIVILEGES").toSet should be(Set(granted(traverse).role("foo").node("*").map))
    execute("SHOW ROLE bar PRIVILEGES").toSet should be(Set.empty)
  }

  test("should fail when creating existing role from non-existing role") {
    // GIVEN
    execute("CREATE ROLE bar")

    the[InvalidArgumentsException] thrownBy {
      // WHEN
      execute("CREATE ROLE bar AS COPY OF foo")
      // THEN
    } should have message "Failed to create a role as copy of 'foo': Role does not exist."

    execute("SHOW ROLES").toSet should be(defaultRoles ++ Set(role("bar").map))
  }

  test("should do nothing when creating existing role from non-existing role using if exists") {
    // GIVEN
    execute("CREATE ROLE bar")
    execute("GRANT ROLE bar TO neo4j")

    // WHEN
    execute("CREATE ROLE bar IF NOT EXISTS AS COPY OF foo")

    // THEN
    execute("SHOW ROLES WITH USERS").toSet should be(defaultRolesWithUsers ++ Set(role("bar").member("neo4j").map))
  }

  test("should get syntax exception when using both replace and if not exists") {
    // WHEN
    val exceptionCreate = the[SyntaxException] thrownBy {
      execute("CREATE OR REPLACE ROLE $role IF NOT EXISTS", Map("role" -> "foo"))
    }

    // THEN
    exceptionCreate.getMessage should include("Failed to create the specified role '$role': cannot have both `OR REPLACE` and `IF NOT EXISTS`.")

    // WHEN
    val exceptionCopy = the[SyntaxException] thrownBy {
      execute("CREATE OR REPLACE ROLE foo IF NOT EXISTS AS COPY OF bar")
    }

    // THEN
    exceptionCopy.getMessage should include("Failed to create the specified role 'foo': cannot have both `OR REPLACE` and `IF NOT EXISTS`.")
  }

  // Tests for dropping roles

  test("should drop role") {
    // GIVEN
    execute("CREATE ROLE foo")

    // WHEN
    execute("DROP ROLE foo")

    // THEN
    execute("SHOW ROLES").toSet should be(defaultRoles)
  }

  test("should drop role with parameter") {
    // GIVEN
    execute("CREATE ROLE foo")

    // WHEN
    execute("DROP ROLE $role", Map("role" -> "foo"))

    // THEN
    execute("SHOW ROLES").toSet should be(defaultRoles)
  }

  test("should not drop role with reserved name") {
    // WHEN
    val exception = the[InvalidArgumentException] thrownBy execute("DROP ROLE PUBLIC")
    exception.getMessage should startWith("Failed to delete the specified role 'PUBLIC': 'PUBLIC' is a reserved role.")

    // THEN
    val result = execute("SHOW ROLES")
    result.toSet should be(defaultRoles)
  }

  test("should not drop role with reserved name using parameter") {
    // WHEN
    val exception = the[InvalidArgumentException] thrownBy execute("DROP ROLE $role", Map("role" -> PUBLIC))
    exception.getMessage should startWith("Failed to delete the specified role 'PUBLIC': 'PUBLIC' is a reserved role.")

    // THEN
    val result = execute("SHOW ROLES")
    result.toSet should be(defaultRoles)
  }

  test("should allow drop role with parameter that looks like reserved name") {
    // GIVEN
    execute("CREATE ROLE foo")

    // WHEN
    execute(s"DROP ROLE $$$PUBLIC", Map(PUBLIC -> "foo"))

    // THEN
    execute("SHOW ROLES").toSet should be(defaultRoles)
  }

  test("should drop existing role using if exists") {
    // GIVEN
    execute("CREATE ROLE foo")

    // WHEN
    execute("DROP ROLE foo IF EXISTS")

    // THEN
    execute("SHOW ROLES").toSet should be(defaultRoles)
  }

  test("should drop built-in role") {
    // WHEN
    execute(s"DROP ROLE $READER")

    // THEN
    execute("SHOW ROLES").toSet should be(defaultRoles - reader)
  }

  test("should lose admin rights when dropping the admin role") {
    // GIVEN
    execute(s"CREATE USER alice SET PASSWORD 'secret' CHANGE NOT REQUIRED")
    execute(s"GRANT ROLE $ADMIN TO alice")

    //WHEN
    executeOnSystem("alice", "secret", s"DROP ROLE $ADMIN")

    // WHEN
    the[AuthorizationViolationException] thrownBy {
      executeOnSystem("alice", "secret",  s"DROP ROLE $READER")
    } should have message PERMISSION_DENIED_DROP_ROLE

    // THEN
    execute("SHOW ROLES").toSet should be(defaultRoles - admin)
  }

  test("should fail when dropping non-existing role") {
    the[InvalidArgumentsException] thrownBy {
      // WHEN
      execute("DROP ROLE foo")
      // THEN
    } should have message "Failed to delete the specified role 'foo': Role does not exist."

    // and with parameter
    the[InvalidArgumentsException] thrownBy {
      // WHEN
      execute("DROP ROLE $role", Map("role" -> "foo"))
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

    // and an invalid (non-existing) one with parameter
    the[InvalidArgumentsException] thrownBy {
      // WHEN
      execute("DROP ROLE $role", Map("role" -> ""))
      // THEN
    } should have message "Failed to delete the specified role '': Role does not exist."

    // THEN
    execute("SHOW ROLES").toSet should be(defaultRoles)
  }

  test("should do nothing when dropping non-existing role using if exists") {
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

  // Tests for granting roles to users

  test("should grant role to user") {
    // GIVEN
    execute("CREATE ROLE custom")
    execute("CREATE USER user SET PASSWORD 'neo'")

    // WHEN
    execute("GRANT ROLE custom TO user")

    // THEN
    execute("SHOW ROLES WITH USERS").toSet should be(
      defaultRolesWithUsers +
        role("custom").member("user").map ++
        publicRole("user")
    )
  }

  test("should grant role to user using parameters") {
    // GIVEN
    execute("CREATE ROLE custom")
    execute("CREATE USER user SET PASSWORD 'neo'")

    // WHEN
    execute("GRANT ROLE $role TO $name", Map("role" -> "custom", "name" -> "user"))

    // THEN
    execute("SHOW ROLES WITH USERS").toSet should be(
      defaultRolesWithUsers +
        role("custom").member("user").map ++
        publicRole("user")
    )
  }

  test("should not fail granting reserved role to user") {
    // WHEN
    execute("GRANT ROLE PUBLIC TO neo4j")

    // THEN
    execute("SHOW USERS").toSet shouldBe Set(defaultUser)
  }

  test("should not fail granting reserved role as parameter to user") {
    // WHEN
    execute("GRANT ROLE $role TO neo4j", Map("role" -> PUBLIC))

    // THEN
    execute("SHOW USERS").toSet shouldBe Set(defaultUser)
  }

  test("should grant reserved role together with other role to user") {
    // GIVEN
    execute("CREATE ROLE custom")

    // WHEN
    execute("GRANT ROLE PUBLIC, custom TO neo4j")

    // THEN
    execute("SHOW USERS").toSet shouldBe Set(user("neo4j", Seq(ADMIN, "custom")))
  }

  test("should grant roles and list users with roles") {
    // GIVEN
    // User  : Roles
    // neo4j : admin
    // Bar   : dragon, fairy
    // Baz   :
    // Zet   : fairy
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
      defaultUser,
      user("Bar", Seq("fairy", "dragon")),
      user("Baz"),
      user("Zet", Seq("fairy"))
    )
  }

  test("should grant role to several users") {
    // GIVEN
    execute("CREATE ROLE custom")
    execute("CREATE USER userA SET PASSWORD 'neo'")
    execute("CREATE USER userB SET PASSWORD 'neo'")

    // WHEN
    execute("GRANT ROLE custom TO userA, userB")

    // THEN
    execute("SHOW ROLES WITH USERS").toSet should be(
      publicRole("userA", "userB") ++ defaultRolesWithUsers +
        role("custom").member("userA").map +
        role("custom").member("userB").map
    )
  }

  test("should grant multiple roles to user") {
    // GIVEN
    execute("CREATE ROLE custom1")
    execute("CREATE ROLE custom2")
    execute("CREATE USER userA SET PASSWORD 'neo'")

    // WHEN
    execute("GRANT ROLE custom1, custom2 TO userA")

    // THEN
    execute("SHOW ROLES WITH USERS").toSet should be(
      publicRole("userA") ++ defaultRolesWithUsers +
        role("custom1").member("userA").map +
        role("custom2").member("userA").map
    )
  }

  test("should grant multiple roles to several users") {
    // GIVEN
    execute("CREATE ROLE custom1")
    execute("CREATE ROLE custom2")
    execute("CREATE USER userA SET PASSWORD 'neo'")
    execute("CREATE USER userB SET PASSWORD 'neo'")

    // WHEN
    execute("GRANT ROLE custom1, custom2 TO userA, userB")

    // THEN
    execute("SHOW ROLES WITH USERS").toSet should be(
      publicRole("userA", "userB") ++ defaultRolesWithUsers +
        role("custom1").member("userA").map +
        role("custom1").member("userB").map +
        role("custom2").member("userA").map +
        role("custom2").member("userB").map
    )
  }

  test("should grant multiple roles to several users using parameters") {
    // GIVEN
    execute("CREATE ROLE custom1")
    execute("CREATE ROLE custom2")
    execute("CREATE USER userA SET PASSWORD 'neo'")
    execute("CREATE USER userB SET PASSWORD 'neo'")

    // WHEN
    execute("GRANT ROLE $a, $b TO $x, $y", Map("a" -> "custom1", "b" -> "custom2", "x" -> "userA", "y" -> "userB"))

    // THEN
    execute("SHOW ROLES WITH USERS").toSet should be(
      publicRole("userA", "userB") ++ defaultRolesWithUsers +
        role("custom1").member("userA").map +
        role("custom1").member("userB").map +
        role("custom2").member("userA").map +
        role("custom2").member("userB").map
    )
  }

  test("should be able to grant already granted role to user") {
    // GIVEN
    setupUserWithCustomRole("Bar", "neo", "dragon")

    // WHEN
    execute("GRANT ROLE dragon TO Bar")

    // THEN
    execute("SHOW USERS").toSet shouldBe Set(defaultUser, user("Bar", Seq("dragon"), passwordChangeRequired = false))
  }

  test("should fail when granting non-existing role to user") {
    // GIVEN
    execute("CREATE USER Bar SET PASSWORD 'neo'")

    the[InvalidArgumentsException] thrownBy {
      // WHEN
      execute("GRANT ROLE dragon TO Bar")
      // THEN
    } should have message "Failed to grant role 'dragon' to user 'Bar': Role does not exist."

    // THEN
    execute("SHOW USERS").toSet shouldBe Set(defaultUser, user("Bar"))
    execute("SHOW ROLES WITH USERS").toSet shouldBe defaultRolesWithUsers ++ publicRole("Bar")

    // and with parameters
    the[InvalidArgumentsException] thrownBy {
      // WHEN
      execute("GRANT ROLE $role TO Bar", Map("role" -> "dragon"))
      // THEN
    } should have message "Failed to grant role 'dragon' to user 'Bar': Role does not exist."

    // THEN
    execute("SHOW USERS").toSet shouldBe Set(defaultUser, user("Bar"))
    execute("SHOW ROLES WITH USERS").toSet shouldBe defaultRolesWithUsers ++ publicRole("Bar")

    // and an invalid (non-existing) one
    the[InvalidArgumentsException] thrownBy {
      // WHEN
      execute("GRANT ROLE `` TO Bar")
      // THEN
    } should have message "Failed to grant role '' to user 'Bar': Role does not exist."

    // AND
    execute("SHOW USERS").toSet shouldBe Set(defaultUser, user("Bar"))
    execute("SHOW ROLES WITH USERS").toSet shouldBe defaultRolesWithUsers ++ publicRole("Bar")

    // and an invalid (non-existing) one with parameter
    the[InvalidArgumentsException] thrownBy {
      // WHEN
      execute("GRANT ROLE $role TO Bar", Map("role" -> ""))
      // THEN
    } should have message "Failed to grant role '' to user 'Bar': Role does not exist."

    // AND
    execute("SHOW USERS").toSet shouldBe Set(defaultUser, user("Bar"))
    execute("SHOW ROLES WITH USERS").toSet shouldBe defaultRolesWithUsers ++ publicRole("Bar")
  }

  test("should fail when granting role to non-existing user") {
    // GIVEN
    val rolesWithUsers = defaultRolesWithUsers + role("dragon").noMember().map
    execute("CREATE ROLE dragon")

    the[InvalidArgumentsException] thrownBy {
      // WHEN
      execute("GRANT ROLE dragon TO Bar")
      // THEN
    } should have message "Failed to grant role 'dragon' to user 'Bar': User does not exist."

    // THEN
    execute("SHOW USERS").toSet shouldBe Set(defaultUser)
    execute("SHOW ROLES WITH USERS").toSet shouldBe rolesWithUsers

    // and with parameters
    the[InvalidArgumentsException] thrownBy {
      // WHEN
      execute("GRANT ROLE dragon TO $user", Map("user" -> "Bar"))
      // THEN
    } should have message "Failed to grant role 'dragon' to user 'Bar': User does not exist."

    // THEN
    execute("SHOW USERS").toSet shouldBe Set(defaultUser)
    execute("SHOW ROLES WITH USERS").toSet shouldBe rolesWithUsers

    // and an invalid (non-existing) one
    the[InvalidArgumentsException] thrownBy {
      // WHEN
      execute("GRANT ROLE dragon TO ``")
      // THEN
    } should have message "Failed to grant role 'dragon' to user '': User does not exist."

    // THEN
    execute("SHOW USERS").toSet shouldBe Set(defaultUser)
    execute("SHOW ROLES WITH USERS").toSet shouldBe rolesWithUsers

    // and an invalid (non-existing) one with parameters
    the[InvalidArgumentsException] thrownBy {
      // WHEN
      execute("GRANT ROLE dragon TO $user", Map("user" -> ""))
      // THEN
    } should have message "Failed to grant role 'dragon' to user '': User does not exist."

    // THEN
    execute("SHOW USERS").toSet shouldBe Set(defaultUser)
    execute("SHOW ROLES WITH USERS").toSet shouldBe rolesWithUsers
  }

  test("should fail when granting non-existing role to non-existing user") {
    the[InvalidArgumentsException] thrownBy {
      // WHEN
      execute("GRANT ROLE dragon TO Bar")
      // THEN
    } should have message "Failed to grant role 'dragon' to user 'Bar': Role does not exist."

    execute("SHOW USERS").toSet shouldBe Set(defaultUser)
    execute("SHOW ROLES WITH USERS").toSet shouldBe defaultRolesWithUsers

    // and an invalid (non-existing) ones
    the[InvalidArgumentsException] thrownBy {
      // WHEN
      execute("GRANT ROLE `` TO ``")
      // THEN
    } should have message "Failed to grant role '' to user '': Role does not exist."

    execute("SHOW USERS").toSet shouldBe Set(defaultUser)
    execute("SHOW ROLES WITH USERS").toSet shouldBe defaultRolesWithUsers
  }

  // Tests for revoking roles from users

  test("should revoke role from user") {
    // GIVEN
    setupUserWithCustomRole("user", "neo")

    // WHEN
    execute("REVOKE ROLE custom FROM user")

    // THEN
    execute("SHOW ROLES WITH USERS").toSet should be(
      publicRole("user") ++ defaultRolesWithUsers +
        role("custom").noMember().map
    )
  }

  test("should revoke role from user with parameters") {
    // GIVEN
    setupUserWithCustomRole("user", "neo")

    // WHEN
    execute("REVOKE ROLE $role FROM $user", Map("role" -> "custom", "user" -> "user"))

    // THEN
    execute("SHOW ROLES WITH USERS").toSet should be(
      publicRole("user") ++ defaultRolesWithUsers +
        role("custom").noMember().map
    )
  }

  test("should fail revoking reserved role") {
    // GIVEN
    execute("CREATE USER user SET PASSWORD 'neo'")

    // WHEN
    val exception = the[InvalidArgumentException] thrownBy execute("REVOKE ROLE PUBLIC FROM user")
    // THEN
    exception.getMessage should startWith("Failed to revoke the specified role 'PUBLIC': 'PUBLIC' is a reserved role.")

    // THEN
    execute("SHOW POPULATED ROLES WITH USERS").toSet should be(
      Set(adminWithDefaultUser) ++ publicRole("neo4j","user")
    )
  }

  test("should fail revoking reserved role using parameter") {
    // GIVEN
    execute("CREATE USER user SET PASSWORD 'neo'")

    // WHEN
    val exception = the[InvalidArgumentException] thrownBy execute("REVOKE ROLE $role FROM user", Map("role" -> PUBLIC))
    // THEN
    exception.getMessage should startWith("Failed to revoke the specified role 'PUBLIC': 'PUBLIC' is a reserved role.")

    // THEN
    execute("SHOW POPULATED ROLES WITH USERS").toSet should be(
      Set(adminWithDefaultUser) ++ publicRole("neo4j","user")
    )
  }

  test("should fail revoking reserved role together with other role") {
    // GIVEN
    setupUserWithCustomRole("user", "neo")

    // WHEN
    val exception = the[InvalidArgumentException] thrownBy execute("REVOKE ROLE PUBLIC, custom FROM user")
    // THEN
    exception.getMessage should startWith("Failed to revoke the specified role 'PUBLIC': 'PUBLIC' is a reserved role.")

    // THEN
    execute("SHOW ROLES WITH USERS").toSet should be(publicRole("user") ++ defaultRolesWithUsers + role("custom").member("user").map)
  }

  test("should revoke role from several users") {
    // GIVEN
    execute("CREATE ROLE custom")
    execute("CREATE USER userA SET PASSWORD 'neo'")
    execute("CREATE USER userB SET PASSWORD 'neo'")
    execute("GRANT ROLE custom TO userA")
    execute("GRANT ROLE custom TO userB")

    // WHEN
    execute("REVOKE ROLE custom FROM userA, userB")

    // THEN
    execute("SHOW ROLES WITH USERS").toSet should be(
      publicRole("userA", "userB") ++ defaultRolesWithUsers +
        role("custom").noMember().map
    )
  }

  test("should revoke multiple roles from user") {
    // GIVEN
    execute("CREATE ROLE custom1")
    execute("CREATE ROLE custom2")
    execute("CREATE USER userA SET PASSWORD 'neo'")
    execute("GRANT ROLE custom1 TO userA")
    execute("GRANT ROLE custom2 TO userA")

    // WHEN
    execute("REVOKE ROLE custom1, custom2 FROM userA")

    // THEN
    execute("SHOW ROLES WITH USERS").toSet should be(
      publicRole("userA") ++ defaultRolesWithUsers +
        role("custom1").noMember().map +
        role("custom2").noMember().map
    )
  }

  test("should revoke multiple roles from several users") {
    // GIVEN
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
    execute("SHOW ROLES WITH USERS").toSet should be(
      publicRole("userA", "userB", "userC", "userD") ++ defaultRolesWithUsers +
        role("custom1").noMember().map +
        role("custom2").noMember().map
    )
  }

  test("should revoke multiple roles from several users using parameters") {
    // GIVEN
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
    execute("REVOKE ROLE $one, $two FROM $a, $b, $c, $d",
      Map("one" -> "custom1", "two" -> "custom2", "a" -> "userA", "b" -> "userB", "c" -> "userC", "d" -> "userD"))

    // THEN
    execute("SHOW ROLES WITH USERS").toSet should be(
      publicRole("userA", "userB", "userC", "userD") ++ defaultRolesWithUsers +
        role("custom1").noMember().map +
        role("custom2").noMember().map
    )
  }

  test("should be able to revoke already revoked role from user") {
    // GIVEN
    setupUserWithCustomRole("user", "neo")
    execute("REVOKE ROLE custom FROM user")

    // WHEN
    execute("REVOKE ROLE custom FROM user").toSet should be(Set.empty)

    // THEN
    execute("SHOW ROLES WITH USERS").toSet should be(
      publicRole("user") ++ defaultRolesWithUsers +
        role("custom").noMember().map
    )
  }

  test("should grant and revoke multiple roles to multiple users") {
    // GIVEN
    execute("CREATE USER Bar SET PASSWORD 'neo'")
    execute("CREATE USER Baz SET PASSWORD 'NEO'")
    execute("CREATE ROLE foo")
    execute("CREATE ROLE fum")
    val publicRoles = publicRole("Bar", "Baz", "neo4j")

    // WHEN using single user and role version of GRANT
    execute("GRANT ROLE foo TO Bar")
    execute("GRANT ROLE foo TO Baz")

    // THEN
    execute("SHOW POPULATED ROLES WITH USERS").toSet should be(publicRoles ++ Set(adminWithDefaultUser, role("foo").member("Bar").map, role("foo").member("Baz").map))

    // WHEN using single user and role version of REVOKE
    execute("REVOKE ROLE foo FROM Bar")
    execute("REVOKE ROLE foo FROM Baz")

    // THEN
    execute("SHOW POPULATED ROLES WITH USERS").toSet should be(publicRoles ++ Set(adminWithDefaultUser))

    // WHEN granting with multiple users and roles version
    execute("GRANT ROLE foo, $fum TO Bar, $Baz", Map("fum" -> "fum", "Baz" -> "Baz"))

    // THEN
    execute("SHOW POPULATED ROLES WITH USERS").toSet should be(publicRoles ++ Set(adminWithDefaultUser, role("foo").member("Bar").map, role("foo").member("Baz").map, role("fum").member("Bar").map, role("fum").member("Baz").map))

    // WHEN revoking only one of many
    execute("REVOKE ROLE foo FROM Bar")

    // THEN
    execute("SHOW POPULATED ROLES WITH USERS").toSet should be(publicRoles ++ Set(adminWithDefaultUser, role("foo").member("Baz").map, role("fum").member("Bar").map, role("fum").member("Baz").map))

    // WHEN revoking with multiple users and roles version
    execute("REVOKE ROLE foo, $fum FROM Bar, $Baz", Map("fum" -> "fum", "Baz" -> "Baz"))

    // THEN
    execute("SHOW POPULATED ROLES WITH USERS").toSet should be(publicRoles ++ Set(adminWithDefaultUser))
  }

  test("should do nothing when revoking non-existent role from (existing) user") {
    // GIVEN
    execute("CREATE USER user SET PASSWORD 'neo'")

    // WHEN
    execute("REVOKE ROLE custom FROM user")

    // THEN
    execute("SHOW ROLES WITH USERS").toSet should be(defaultRolesWithUsers ++ publicRole("user"))
  }

  test("should do nothing when revoking (existing) role from non-existing user") {
    // GIVEN
    execute("CREATE ROLE custom")
    val roles = defaultRolesWithUsers + role("custom").noMember().map

    // WHEN
    execute("REVOKE ROLE custom FROM user")

    // THEN
    execute("SHOW ROLES WITH USERS").toSet should be(roles)
  }

  test("should do nothing when revoking non-existing role from non-existing user") {
    // WHEN
    execute("REVOKE ROLE custom FROM user")

    // THEN
    execute("SHOW ROLES WITH USERS").toSet should be(defaultRolesWithUsers)
  }
}
