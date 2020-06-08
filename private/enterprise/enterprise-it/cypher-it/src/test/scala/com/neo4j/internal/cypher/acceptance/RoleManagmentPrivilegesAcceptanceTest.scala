/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.internal.cypher.acceptance

import org.neo4j.graphdb.security.AuthorizationViolationException

import scala.collection.mutable

class RoleManagmentPrivilegesAcceptanceTest extends AdministrationCommandAcceptanceTestBase {

  // Privilege tests

  test("should not revoke other role management privileges when revoking role management") {
    // GIVEN
    createRoleWithOnlyAdminPrivilege()
    execute("CREATE ROLE custom AS COPY OF adminOnly")
    execute("GRANT CREATE ROLE ON DBMS TO custom")
    execute("GRANT DROP ROLE ON DBMS TO custom")
    execute("GRANT ASSIGN ROLE ON DBMS TO custom")
    execute("GRANT REMOVE ROLE ON DBMS TO custom")
    execute("GRANT SHOW ROLE ON DBMS TO custom")
    execute("GRANT ROLE MANAGEMENT ON DBMS TO custom")

    // WHEN
    execute("REVOKE ROLE MANAGEMENT ON DBMS FROM custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      granted(adminPrivilege).role("custom").map,
      granted(adminAction("create_role")).role("custom").map,
      granted(adminAction("drop_role")).role("custom").map,
      granted(adminAction("assign_role")).role("custom").map,
      granted(adminAction("remove_role")).role("custom").map,
      granted(adminAction("show_role")).role("custom").map
    ))
  }

  test("Should revoke sub-privilege even if role management exists") {
    // Given
    createRoleWithOnlyAdminPrivilege()
    execute("CREATE ROLE custom AS COPY OF adminOnly")
    execute("GRANT CREATE ROLE ON DBMS TO custom")
    execute("GRANT DROP ROLE ON DBMS TO custom")
    execute("GRANT ASSIGN ROLE ON DBMS TO custom")
    execute("GRANT REMOVE ROLE ON DBMS TO custom")
    execute("GRANT SHOW ROLE ON DBMS TO custom")
    execute("GRANT ROLE MANAGEMENT ON DBMS TO custom")

    // When
    // Now revoke each sub-privilege in turn
    Seq(
      "CREATE ROLE",
      "DROP ROLE",
      "SHOW ROLE",
      "ASSIGN ROLE",
      "REMOVE ROLE"
    ).foreach(privilege => execute(s"REVOKE $privilege ON DBMS FROM custom"))

    // Then
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      granted(adminPrivilege).role("custom").map,
      granted(adminAction("role_management")).role("custom").map
    ))
  }

  // Enforcement tests

  // CREATE ROLE

  test("should enforce create role privilege") {
    // GIVEN
    setupUserWithCustomRole("foo", "bar")

    // WHEN
    execute("GRANT CREATE ROLE ON DBMS TO custom")

    // THEN
    executeOnSystem("foo", "bar", "CREATE ROLE role")
    execute("SHOW ROLES").toSet should be(defaultRoles ++ Set(role("custom").map, role("role").map))

    // WHEN
    execute("DROP ROLE role")
    execute("REVOKE CREATE ROLE ON DBMS FROM custom")

    // THEN
    the[AuthorizationViolationException] thrownBy {
      executeOnSystem("foo", "bar", "CREATE ROLE role")
    } should have message "Permission denied."
  }

  test("should fail when creating role when denied create role privilege") {
    // GIVEN
    setupUserWithCustomAdminRole("foo", "bar")

    // WHEN
    execute("DENY CREATE ROLE ON DBMS TO custom")

    // THEN
    the[AuthorizationViolationException] thrownBy {
      executeOnSystem("foo", "bar", "CREATE ROLE role")
    } should have message "Permission denied."
  }

  test("should fail when replacing role with denied create role privilege") {
    // GIVEN
    setupUserWithCustomAdminRole("foo", "bar")

    // WHEN
    execute("DENY CREATE ROLE ON DBMS TO custom")

    // THEN
    the[AuthorizationViolationException] thrownBy {
      executeOnSystem("foo", "bar", "CREATE OR REPLACE ROLE myRole")
    } should have message "Permission denied."
  }

  test("should fail when replacing role with denied drop role privilege") {
    // GIVEN
    setupUserWithCustomAdminRole("foo", "bar")

    // WHEN
    execute("DENY DROP ROLE ON DBMS TO custom")

    // THEN
    the[AuthorizationViolationException] thrownBy {
      executeOnSystem("foo", "bar", "CREATE OR REPLACE ROLE myRole")
    } should have message "Permission denied."
  }

  test("should fail when replacing role without create role privilege") {
    // GIVEN
    setupUserWithCustomRole("foo", "bar")

    // WHEN
    execute("GRANT DROP ROLE ON DBMS TO custom")

    // THEN
    the[AuthorizationViolationException] thrownBy {
      executeOnSystem("foo", "bar", "CREATE OR REPLACE ROLE myRole")
    } should have message "Permission denied."
  }

  test("should fail when replacing role without drop role privilege") {
    // GIVEN
    setupUserWithCustomRole("foo", "bar")

    // WHEN
    execute("GRANT CREATE ROLE ON DBMS TO custom")

    // THEN
    the[AuthorizationViolationException] thrownBy {
      executeOnSystem("foo", "bar", "CREATE OR REPLACE ROLE myRole")
    } should have message "Permission denied."
  }

  // DROP ROLE

  test("should enforce drop role privilege") {
    // GIVEN
    setupUserWithCustomRole("foo", "bar")
    execute("CREATE ROLE role")

    // WHEN
    execute("GRANT DROP ROLE ON DBMS TO custom")

    // THEN
    executeOnSystem("foo", "bar", "DROP ROLE role")
    execute("SHOW ROLES").toSet should be(defaultRoles ++ Set(role("custom").map))

    // WHEN
    execute("CREATE ROLE role")
    execute("REVOKE DROP ROLE ON DBMS FROM custom")

    // THEN
    the[AuthorizationViolationException] thrownBy {
      executeOnSystem("foo", "bar", "DROP ROLE role")
    } should have message "Permission denied."
  }

  test("should fail when dropping role when denied drop role privilege") {
    // GIVEN
    setupUserWithCustomAdminRole("foo", "bar")

    // WHEN
    execute("CREATE ROLE role")
    execute("DENY DROP ROLE ON DBMS TO custom")

    // THEN
    the[AuthorizationViolationException] thrownBy {
      executeOnSystem("foo", "bar", "DROP ROLE role")
    } should have message "Permission denied."
  }

  // ASSIGN ROLE

  test("should enforce assign role privilege") {
    // GIVEN
    setupUserWithCustomRole("foo", "bar")
    execute("CREATE ROLE role")

    // WHEN
    execute("GRANT ASSIGN ROLE ON DBMS TO custom")

    // THEN
    executeOnSystem("foo", "bar", "GRANT ROLE role TO foo")
    execute("SHOW ROLES WITH USERS").toSet should be(publicRole("foo") ++ defaultRolesWithUsers ++ Set(role("custom").member("foo").map, role("role").member("foo").map))

    // WHEN
    execute("REVOKE ROLE role FROM foo")
    execute("REVOKE ASSIGN ROLE ON DBMS FROM custom")

    // THEN
    the[AuthorizationViolationException] thrownBy {
      executeOnSystem("foo", "bar", "GRANT ROLE role TO foo")
    } should have message "Permission denied."
  }

  test("should fail when granting role when denied assign role privilege") {
    // GIVEN
    setupUserWithCustomAdminRole("foo", "bar")

    // WHEN
    execute("CREATE ROLE role")
    execute("DENY ASSIGN ROLE ON DBMS TO custom")

    // THEN
    the[AuthorizationViolationException] thrownBy {
      executeOnSystem("foo", "bar", "GRANT ROLE role TO foo")
    } should have message "Permission denied."
  }

  // REMOVE ROLE

  test("should enforce remove role privilege") {
    // GIVEN
    setupUserWithCustomRole("foo", "bar")

    // WHEN
    execute("CREATE ROLE role")
    execute("GRANT ROLE role TO foo")
    execute("GRANT REMOVE ROLE ON DBMS TO custom")

    // THEN
    executeOnSystem("foo", "bar", "REVOKE ROLE role FROM foo")
    execute("SHOW ROLES WITH USERS").toSet should be(publicRole("foo") ++ defaultRolesWithUsers ++ Set(role("custom").member("foo").map, role("role").noMember().map))

    // WHEN
    execute("GRANT ROLE role TO foo")
    execute("REVOKE REMOVE ROLE ON DBMS FROM custom")

    // THEN
    the[AuthorizationViolationException] thrownBy {
      executeOnSystem("foo", "bar", "REVOKE ROLE role FROM foo")
    } should have message "Permission denied."
  }

  test("should fail when revoking role when denied remove role privilege") {
    // GIVEN
    setupUserWithCustomAdminRole("foo", "bar")

    // WHEN
    execute("CREATE ROLE role")
    execute("DENY REMOVE ROLE ON DBMS TO custom")

    // THEN
    the[AuthorizationViolationException] thrownBy {
      executeOnSystem("foo", "bar", "REVOKE ROLE role FROM foo")
    } should have message "Permission denied."
  }

  // SHOW ROLE

  test("should enforce show role privilege") {
    // GIVEN
    setupUserWithCustomRole("foo", "bar")

    // WHEN
    execute("GRANT SHOW ROLE ON DBMS TO custom")

    // THEN
    executeOnSystem("foo", "bar", "SHOW ROLES") should be(defaultRoles.size + 1)

    // WHEN
    execute("REVOKE SHOW ROLE ON DBMS FROM custom")

    // THEN
    the[AuthorizationViolationException] thrownBy {
      executeOnSystem("foo", "bar", "SHOW ROLES")
    } should have message "Permission denied."
  }

  test("should fail showing roles when denied show role privilege") {
    // GIVEN
    setupUserWithCustomAdminRole("foo", "bar")

    // WHEN
    execute("DENY SHOW ROLE ON DBMS TO custom")

    // THEN
    the[AuthorizationViolationException] thrownBy {
      executeOnSystem("foo", "bar", "SHOW ROLES")
    } should have message "Permission denied."
  }

  test("should show roles with users with correct privileges") {
    // GIVEN
    setupUserWithCustomRole("foo", "bar")

    // WHEN
    execute("GRANT SHOW ROLE ON DBMS TO custom")
    execute("GRANT SHOW USER ON DBMS TO custom")

    // THEN
    val result = new mutable.HashSet[Map[String, AnyRef]]
    executeOnSystem("foo", "bar", "SHOW ROLES WITH USERS", resultHandler = (row, _) => {
      val role = Map(
        "role" -> row.get("role"),
        "member" -> row.get("member")
      )
      result.add(role)
    })
    result should be(defaultRolesWithUsers ++ publicRole("foo") ++ Set(role("custom").member("foo").map))
  }

  test("should fail to show roles with users without show user privilege") {
    // GIVEN
    setupUserWithCustomRole("foo", "bar")

    // WHEN
    execute("GRANT SHOW ROLE ON DBMS TO custom")

    // THEN
    the[AuthorizationViolationException] thrownBy {
      executeOnSystem("foo", "bar", "SHOW ROLES WITH USERS")
    } should have message "Permission denied."
  }

  test("should fail to show roles with users without show role privilege") {
    // GIVEN
    setupUserWithCustomRole("foo", "bar")

    // WHEN
    execute("GRANT SHOW USER ON DBMS TO custom")

    // THEN
    the[AuthorizationViolationException] thrownBy {
      executeOnSystem("foo", "bar", "SHOW ROLES WITH USERS")
    } should have message "Permission denied."
  }

  test("should show populated roles with only show role privilege") {
    // GIVEN
    setupUserWithCustomRole("foo", "bar")

    // WHEN
    execute("GRANT SHOW ROLE ON DBMS TO custom")

    // THEN
    val result = new mutable.HashSet[Map[String, AnyRef]]
    executeOnSystem("foo", "bar", "SHOW POPULATED ROLES", resultHandler = (row, _) => {
      val role = Map("role" -> row.get("role"))
      result.add(role)
    })

    result should be(Set(role("custom").map, public, admin))
  }

  test("should fail showing populated roles when denied show role privilege") {
    // GIVEN
    setupUserWithCustomAdminRole("foo", "bar")

    // WHEN
    execute("DENY SHOW ROLE ON DBMS TO custom")

    // THEN
    the[AuthorizationViolationException] thrownBy {
      executeOnSystem("foo", "bar", "SHOW POPULATED ROLES")
    } should have message "Permission denied."
  }

  test("should show populated roles with users with correct privileges") {
    // GIVEN
    setupUserWithCustomRole("foo", "bar")

    // WHEN
    execute("GRANT SHOW ROLE ON DBMS TO custom")
    execute("GRANT SHOW USER ON DBMS TO custom")

    // THEN
    val result = new mutable.HashSet[Map[String, AnyRef]]
    executeOnSystem("foo", "bar", "SHOW POPULATED ROLES WITH USERS", resultHandler = (row, _) => {
      val role = Map(
        "role" -> row.get("role"),
        "member" -> row.get("member")
      )
      result.add(role)
    })

    result should be(
      publicRole("foo", "neo4j") ++ Set(role("custom").member("foo").map, adminWithDefaultUser)
    )
  }

  test("should fail to show populated roles with users without show user privilege") {
    // GIVEN
    setupUserWithCustomRole("foo", "bar")

    // WHEN
    execute("GRANT SHOW ROLE ON DBMS TO custom")

    // THEN
    the[AuthorizationViolationException] thrownBy {
      executeOnSystem("foo", "bar", "SHOW POPULATED ROLES WITH USERS")
    } should have message "Permission denied."
  }

  test("should fail to show populated roles with users without show role privilege") {
    // GIVEN
    setupUserWithCustomRole("foo", "bar")

    // WHEN
    execute("GRANT SHOW USER ON DBMS TO custom")

    // THEN
    the[AuthorizationViolationException] thrownBy {
      executeOnSystem("foo", "bar", "SHOW POPULATED ROLES WITH USERS")
    } should have message "Permission denied."
  }

  // ROLE MANAGEMENT

  test("should be able to create role with role management privilege") {
    // GIVEN
    setupUserWithCustomAdminRole("foo", "bar")

    // WHEN
    execute("GRANT ROLE MANAGEMENT ON DBMS TO custom")

    // THEN
    executeOnSystem("foo", "bar", "CREATE ROLE role")
  }

  test("should deny create role when denied role management privilege") {
    // GIVEN
    setupUserWithCustomAdminRole("foo", "bar")

    // WHEN
    execute("GRANT CREATE ROLE ON DBMS TO custom")
    execute("DENY ROLE MANAGEMENT ON DBMS TO custom")

    // THEN
    the[AuthorizationViolationException] thrownBy {
      executeOnSystem("foo", "bar", "CREATE ROLE role")
    } should have message "Permission denied."
  }
}
