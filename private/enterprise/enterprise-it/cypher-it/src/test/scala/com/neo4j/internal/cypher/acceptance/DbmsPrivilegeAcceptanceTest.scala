/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.internal.cypher.acceptance

import org.neo4j.graphdb.security.AuthorizationViolationException

import scala.collection.mutable

class DbmsPrivilegeAcceptanceTest extends AdministrationCommandAcceptanceTestBase {

  // Privilege tests

  Seq(("GRANT", granted: privilegeFunction), ("DENY", denied: privilegeFunction)).foreach {
    case (grantOrDeny, grantedOrDenied) =>
      test(s"should $grantOrDeny create role privilege") {
        // GIVEN
        execute("CREATE ROLE custom")

        // WHEN
        execute(s"$grantOrDeny CREATE ROLE ON DBMS TO custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
         grantedOrDenied(adminAction("create_role")).role("custom").map
        ))
      }

      test(s"should $grantOrDeny drop role privilege") {
        // GIVEN
        execute("CREATE ROLE custom")

        // WHEN
        execute(s"$grantOrDeny DROP ROLE ON DBMS TO $$role", Map("role" -> "custom"))

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          grantedOrDenied(adminAction("drop_role")).role("custom").map
        ))
      }

      test(s"should $grantOrDeny assign role privilege") {
        // GIVEN
        execute("CREATE ROLE custom")

        // WHEN
        execute(s"$grantOrDeny ASSIGN ROLE ON DBMS TO custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          grantedOrDenied(adminAction("assign_role")).role("custom").map
        ))
      }

      test(s"should $grantOrDeny remove role privilege") {
        // GIVEN
        execute("CREATE ROLE custom")

        // WHEN
        execute(s"$grantOrDeny REMOVE ROLE ON DBMS TO custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          grantedOrDenied(adminAction("remove_role")).role("custom").map
        ))
      }

      test(s"should $grantOrDeny show role privilege") {
        // GIVEN
        execute("CREATE ROLE custom")

        // WHEN
        execute(s"$grantOrDeny SHOW ROLE ON DBMS TO custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          grantedOrDenied(adminAction("show_role")).role("custom").map
        ))
      }

      test(s"should $grantOrDeny role management privilege") {
        // GIVEN
        execute("CREATE ROLE custom")

        // WHEN
        execute(s"$grantOrDeny ROLE MANAGEMENT ON DBMS TO custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          grantedOrDenied(adminAction("role_management")).role("custom").map
        ))
      }

      test(s"should $grantOrDeny create user privilege") {
        // GIVEN
        execute("CREATE ROLE custom")

        // WHEN
        execute(s"$grantOrDeny CREATE USER ON DBMS TO custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          grantedOrDenied(adminAction("create_user")).role("custom").map
        ))
      }

      test(s"should $grantOrDeny drop user privilege") {
        // GIVEN
        execute("CREATE ROLE custom")

        // WHEN
        execute(s"$grantOrDeny DROP USER ON DBMS TO custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          grantedOrDenied(adminAction("drop_user")).role("custom").map
        ))
      }

      test(s"should $grantOrDeny show user privilege") {
        // GIVEN
        execute("CREATE ROLE custom")

        // WHEN
        execute(s"$grantOrDeny SHOW USER ON DBMS TO $$role", Map("role" -> "custom"))

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          grantedOrDenied(adminAction("show_user")).role("custom").map
        ))
      }

      test(s"should $grantOrDeny alter user privilege") {
        // GIVEN
        execute("CREATE ROLE custom")

        // WHEN
        execute(s"$grantOrDeny ALTER USER ON DBMS TO custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          grantedOrDenied(adminAction("alter_user")).role("custom").map
        ))
      }

      test(s"should $grantOrDeny user management privilege") {
        // GIVEN
        execute("CREATE ROLE custom")

        // WHEN
        execute(s"$grantOrDeny USER MANAGEMENT ON DBMS TO custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          grantedOrDenied(adminAction("user_management")).role("custom").map
        ))
      }

      test(s"should $grantOrDeny create database privilege") {
        // GIVEN
        execute("CREATE ROLE custom")

        // WHEN
        execute(s"$grantOrDeny CREATE DATABASE ON DBMS TO custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          grantedOrDenied(adminAction("create_database")).role("custom").map
        ))
      }

      test(s"should $grantOrDeny drop database privilege") {
        // GIVEN
        execute("CREATE ROLE custom")

        // WHEN
        execute(s"$grantOrDeny DROP DATABASE ON DBMS TO custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          grantedOrDenied(adminAction("drop_database")).role("custom").map
        ))
      }

      test(s"should $grantOrDeny database management privilege") {
        // GIVEN
        execute("CREATE ROLE custom")

        // WHEN
        execute(s"$grantOrDeny DATABASE MANAGEMENT ON DBMS TO $$role", Map("role" -> "custom"))

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          grantedOrDenied(adminAction("database_management")).role("custom").map
        ))
      }

      test(s"should $grantOrDeny show privilege privilege") {
        // GIVEN
        execute("CREATE ROLE custom")

        // WHEN
        execute(s"$grantOrDeny SHOW PRIVILEGE ON DBMS TO custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          grantedOrDenied(adminAction("show_privilege")).role("custom").map
        ))
      }

      test(s"should $grantOrDeny assign privilege privilege") {
        // GIVEN
        execute("CREATE ROLE custom")

        // WHEN
        execute(s"$grantOrDeny ASSIGN PRIVILEGE ON DBMS TO custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          grantedOrDenied(adminAction("assign_privilege")).role("custom").map
        ))
      }

      test(s"should $grantOrDeny remove privilege privilege") {
        // GIVEN
        execute("CREATE ROLE custom")

        // WHEN
        execute(s"$grantOrDeny REMOVE PRIVILEGE ON DBMS TO $$role", Map("role" -> "custom"))

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          grantedOrDenied(adminAction("remove_privilege")).role("custom").map
        ))
      }

      test(s"should $grantOrDeny privilege management privilege") {
        // GIVEN
        execute("CREATE ROLE custom")

        // WHEN
        execute(s"$grantOrDeny PRIVILEGE MANAGEMENT ON DBMS TO custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          grantedOrDenied(adminAction("privilege_management")).role("custom").map
        ))
      }

      test(s"should $grantOrDeny all dbms privilege privilege") {
        // GIVEN
        execute("CREATE ROLE custom")

        // WHEN
        execute(s"$grantOrDeny ALL DBMS PRIVILEGES ON DBMS TO custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          grantedOrDenied(adminAction("dbms_actions")).role("custom").map
        ))
      }
  }

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
      granted(admin).role("custom").map,
      granted(adminAction("create_role")).role("custom").map,
      granted(adminAction("drop_role")).role("custom").map,
      granted(adminAction("assign_role")).role("custom").map,
      granted(adminAction("remove_role")).role("custom").map,
      granted(adminAction("show_role")).role("custom").map
    ))
  }

  test("should not revoke other user management privileges when revoking user management") {
    // GIVEN
    createRoleWithOnlyAdminPrivilege()
    execute("CREATE ROLE custom AS COPY OF adminOnly")
    execute("GRANT CREATE USER ON DBMS TO custom")
    execute("GRANT DROP USER ON DBMS TO custom")
    execute("GRANT SHOW USER ON DBMS TO custom")
    execute("GRANT ALTER USER ON DBMS TO custom")
    execute("GRANT USER MANAGEMENT ON DBMS TO custom")

    // WHEN
    execute("REVOKE USER MANAGEMENT ON DBMS FROM custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      granted(admin).role("custom").map,
      granted(adminAction("create_user")).role("custom").map,
      granted(adminAction("drop_user")).role("custom").map,
      granted(adminAction("alter_user")).role("custom").map,
      granted(adminAction("show_user")).role("custom").map
    ))
  }

  test("should not revoke other database management privileges when revoking database management") {
    // GIVEN
    createRoleWithOnlyAdminPrivilege()
    execute("CREATE ROLE custom AS COPY OF adminOnly")
    execute("GRANT CREATE DATABASE ON DBMS TO custom")
    execute("GRANT DROP DATABASE ON DBMS TO custom")
    execute("GRANT DATABASE MANAGEMENT ON DBMS TO custom")

    // WHEN
    execute("REVOKE DATABASE MANAGEMENT ON DBMS FROM custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      granted(admin).role("custom").map,
      granted(adminAction("create_database")).role("custom").map,
      granted(adminAction("drop_database")).role("custom").map
    ))
  }

  test("should not revoke other privilege management privileges when revoking privilege management") {
    // GIVEN
    createRoleWithOnlyAdminPrivilege()
    execute("CREATE ROLE custom AS COPY OF adminOnly")
    execute("GRANT SHOW PRIVILEGE ON DBMS TO custom")
    execute("GRANT ASSIGN PRIVILEGE ON DBMS TO custom")
    execute("GRANT REMOVE PRIVILEGE ON DBMS TO custom")
    execute("GRANT PRIVILEGE MANAGEMENT ON DBMS TO custom")

    // WHEN
    execute("REVOKE PRIVILEGE MANAGEMENT ON DBMS FROM custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      granted(admin).role("custom").map,
      granted(adminAction("show_privilege")).role("custom").map,
      granted(adminAction("assign_privilege")).role("custom").map,
      granted(adminAction("remove_privilege")).role("custom").map
    ))
  }

  test("should not revoke other dbms privileges when revoking all dbms privileges") {
    // GIVEN
    createRoleWithOnlyAdminPrivilege()
    execute("CREATE ROLE custom AS COPY OF adminOnly")
    allDbmsPrivileges("GRANT", includingCompound = true)

    // WHEN
    execute("REVOKE ALL DBMS PRIVILEGES ON DBMS FROM $role", Map("role" -> "custom"))

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      granted(admin).role("custom").map,
      granted(adminAction("create_role")).role("custom").map,
      granted(adminAction("drop_role")).role("custom").map,
      granted(adminAction("assign_role")).role("custom").map,
      granted(adminAction("remove_role")).role("custom").map,
      granted(adminAction("show_role")).role("custom").map,
      granted(adminAction("role_management")).role("custom").map,
      granted(adminAction("create_user")).role("custom").map,
      granted(adminAction("drop_user")).role("custom").map,
      granted(adminAction("alter_user")).role("custom").map,
      granted(adminAction("show_user")).role("custom").map,
      granted(adminAction("user_management")).role("custom").map,
      granted(adminAction("create_database")).role("custom").map,
      granted(adminAction("drop_database")).role("custom").map,
      granted(adminAction("database_management")).role("custom").map,
      granted(adminAction("show_privilege")).role("custom").map,
      granted(adminAction("assign_privilege")).role("custom").map,
      granted(adminAction("remove_privilege")).role("custom").map,
      granted(adminAction("privilege_management")).role("custom").map
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
      granted(admin).role("custom").map,
      granted(adminAction("role_management")).role("custom").map
    ))
  }

  test("Should revoke sub-privilege even if user management exists") {
    // Given
    createRoleWithOnlyAdminPrivilege()
    execute("CREATE ROLE custom AS COPY OF adminOnly")
    execute("GRANT CREATE USER ON DBMS TO custom")
    execute("GRANT DROP USER ON DBMS TO custom")
    execute("GRANT SHOW USER ON DBMS TO custom")
    execute("GRANT ALTER USER ON DBMS TO custom")
    execute("GRANT USER MANAGEMENT ON DBMS TO custom")

    // When
    // Now revoke each sub-privilege in turn
    Seq(
      "CREATE USER",
      "DROP USER",
      "SHOW USER",
      "ALTER USER"
    ).foreach(privilege => execute(s"REVOKE $privilege ON DBMS FROM custom"))

    // Then
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      granted(admin).role("custom").map,
      granted(adminAction("user_management")).role("custom").map
    ))
  }

  test("Should revoke sub-privilege even if database management exists") {
    // Given
    execute("CREATE ROLE custom")
    execute("GRANT CREATE DATABASE ON DBMS TO custom")
    execute("GRANT DROP DATABASE ON DBMS TO custom")
    execute("GRANT DATABASE MANAGEMENT ON DBMS TO custom")

    // When
    // Now revoke each sub-privilege in turn
    Seq(
      "CREATE DATABASE",
      "DROP DATABASE"
    ).foreach(privilege => execute(s"REVOKE $privilege ON DBMS FROM custom"))

    // Then
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      granted(adminAction("database_management")).role("custom").map
    ))
  }

  test("Should revoke sub-privilege even if privilege management exists") {
    // Given
    execute("CREATE ROLE custom")
    execute("GRANT SHOW PRIVILEGE ON DBMS TO custom")
    execute("GRANT ASSIGN PRIVILEGE ON DBMS TO custom")
    execute("GRANT REMOVE PRIVILEGE ON DBMS TO custom")
    execute("GRANT PRIVILEGE MANAGEMENT ON DBMS TO custom")

    // When
    // Now revoke each sub-privilege in turn
    Seq(
      "SHOW PRIVILEGE",
      "ASSIGN PRIVILEGE",
      "REMOVE PRIVILEGE"
    ).foreach(privilege => execute(s"REVOKE $privilege ON DBMS FROM custom"))

    // Then
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      granted(adminAction("privilege_management")).role("custom").map
    ))
  }

  test("Should revoke sub-privilege even if all dbms privilege exists") {
    // Given
    createRoleWithOnlyAdminPrivilege()
    execute("CREATE ROLE custom AS COPY OF adminOnly")
    allDbmsPrivileges("GRANT", includingCompound = true)

    // When
    // Now revoke each sub-privilege in turn
    allDbmsPrivileges("REVOKE", includingCompound = false)

    // Then
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      granted(admin).role("custom").map,
      granted(adminAction("dbms_actions")).role("custom").map
    ))
  }

  test("Should do nothing when revoking a non-existing subset of a compound (mostly dbms) admin privilege") {
    // Given
    setup()
    createRoleWithOnlyAdminPrivilege("custom")

    // When
    // Now try to revoke each sub-privilege (that we have syntax for) in turn
    //TODO: ADD ANY NEW SUB-PRIVILEGES HERE
    Seq(
      "CREATE ROLE ON DBMS",
      "DROP ROLE ON DBMS",
      "SHOW ROLE ON DBMS",
      "ASSIGN ROLE ON DBMS",
      "REMOVE ROLE ON DBMS",
      "ROLE MANAGEMENT ON DBMS",
      "CREATE USER ON DBMS",
      "DROP USER ON DBMS",
      "SHOW USER ON DBMS",
      "ALTER USER ON DBMS",
      "USER MANAGEMENT ON DBMS",
      "CREATE DATABASE ON DBMS",
      "DROP DATABASE ON DBMS",
      "DATABASE MANAGEMENT ON DBMS",
      "SHOW PRIVILEGE ON DBMS",
      "ASSIGN PRIVILEGE ON DBMS",
      "REMOVE PRIVILEGE ON DBMS",
      "PRIVILEGE MANAGEMENT ON DBMS",
      "ALL DBMS PRIVILEGES ON DBMS",
      "SHOW TRANSACTION (*) ON DATABASES *",
      "TERMINATE TRANSACTION (*) ON DATABASES *",
      "TRANSACTION MANAGEMENT ON DATABASES *",
      "START ON DATABASES *",
      "STOP ON DATABASES *"
    ).foreach(queryPart => execute(s"REVOKE $queryPart FROM custom"))

    // Then
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(granted(admin).role("custom").map))
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
        "member" -> row.get("member"),
        "isBuiltIn" -> row.get("isBuiltIn")
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
        "member" -> row.get("member"),
        "isBuiltIn" -> row.get("isBuiltIn")
      )
      result.add(role)
    })

    result should be(
      publicRole("foo", "neo4j") ++
      Set(role("custom").member("foo").map, role("admin").member("neo4j").builtIn().map)
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

  // CREATE USER

  test("should enforce create user privilege") {
    // GIVEN
    setupUserWithCustomRole("foo", "bar")

    // WHEN
    execute("GRANT CREATE USER ON DBMS TO custom")

    // THEN
    executeOnSystem("foo", "bar", "CREATE USER user SET PASSWORD 'abc'")
    execute("SHOW USERS").toSet should be(Set(neo4jUser, user("foo", passwordChangeRequired = false, roles = Seq("custom")), user("user")))

    // WHEN
    execute("DROP USER user")
    execute("REVOKE CREATE USER ON DBMS FROM custom")

    // THEN
    the[AuthorizationViolationException] thrownBy {
      executeOnSystem("foo", "bar", "CREATE USER user SET PASSWORD 'abc'")
    } should have message "Permission denied."
  }

  test("should fail when creating user when denied create user privilege") {
    // GIVEN
    setupUserWithCustomAdminRole("foo", "bar")

    // WHEN
    execute("DENY CREATE USER ON DBMS TO custom")

    // THEN
    the[AuthorizationViolationException] thrownBy {
      executeOnSystem("foo", "bar", "CREATE USER user SET PASSWORD 'abc'")
    } should have message "Permission denied."
  }

  test("should fail when replacing user when denied create user privilege") {
    // GIVEN
    setupUserWithCustomAdminRole("foo", "bar")

    // WHEN
    execute("DENY CREATE USER ON DBMS TO custom")

    // THEN
    the[AuthorizationViolationException] thrownBy {
      executeOnSystem("foo", "bar", "CREATE OR REPLACE USER user SET PASSWORD 'abc'")
    } should have message "Permission denied."
  }

  test("should fail when replacing user when denied drop user privilege") {
    // GIVEN
    setupUserWithCustomAdminRole("foo", "bar")

    // WHEN
    execute("DENY DROP USER ON DBMS TO custom")

    // THEN
    the[AuthorizationViolationException] thrownBy {
      executeOnSystem("foo", "bar", "CREATE OR REPLACE USER user SET PASSWORD 'abc'")
    } should have message "Permission denied."
  }

  test("should fail when replacing user without create user privilege") {
    // GIVEN
    setupUserWithCustomRole("foo", "bar")

    // WHEN
    execute("GRANT DROP USER ON DBMS TO custom")

    // THEN
    the[AuthorizationViolationException] thrownBy {
      executeOnSystem("foo", "bar", "CREATE OR REPLACE USER bar SET PASSWORD 'firstPassword'")
    } should have message "Permission denied."
  }

  test("should fail when replacing user without drop user privilege") {
    // GIVEN
    setupUserWithCustomRole("foo", "bar")

    // WHEN
    execute("GRANT CREATE USER ON DBMS TO custom")

    // THEN
    the[AuthorizationViolationException] thrownBy {
      executeOnSystem("foo", "bar", "CREATE OR REPLACE USER bar SET PASSWORD 'firstPassword'")
    } should have message "Permission denied."
  }

  // DROP USER

  test("should enforce drop user privilege") {
    // GIVEN
    setupUserWithCustomRole("foo", "bar")

    // WHEN
    execute("CREATE USER user SET PASSWORD 'abc'")
    execute("GRANT DROP USER ON DBMS TO custom")

    // THEN
    executeOnSystem("foo", "bar", "DROP USER user")
    execute("SHOW USERS").toSet should be(Set(neo4jUser, user("foo", passwordChangeRequired = false, roles = Seq("custom"))))

    // WHEN
    execute("CREATE USER user SET PASSWORD 'abc'")
    execute("REVOKE DROP USER ON DBMS FROM custom")

    // THEN
    the[AuthorizationViolationException] thrownBy {
      executeOnSystem("foo", "bar", "DROP USER user")
    } should have message "Permission denied."
  }

  test("should fail when dropping user when denied drop user privilege") {
    // GIVEN
    setupUserWithCustomAdminRole("foo", "bar")

    // WHEN
    execute("CREATE USER user SET PASSWORD 'abc'")
    execute("DENY DROP USER ON DBMS TO custom")

    // THEN
    the[AuthorizationViolationException] thrownBy {
      executeOnSystem("foo", "bar", "DROP USER user")
    } should have message "Permission denied."
  }

  // ALTER USER

  test("should enforce alter user privilege") {
    // GIVEN
    setupUserWithCustomRole("foo", "bar")

    // WHEN
    execute("CREATE USER user SET PASSWORD 'abc' CHANGE REQUIRED")
    execute("GRANT ALTER USER ON DBMS TO custom")

    // THEN
    executeOnSystem("foo", "bar", "ALTER USER user SET PASSWORD CHANGE NOT REQUIRED")
    execute("SHOW USERS").toSet should be(Set(
      neo4jUser,
      user("foo", passwordChangeRequired = false, roles = Seq("custom")),
      user("user", passwordChangeRequired = false)
    ))

    // WHEN
    execute("REVOKE ALTER USER ON DBMS FROM custom")

    // THEN
    the[AuthorizationViolationException] thrownBy {
      executeOnSystem("foo", "bar", "ALTER USER user SET PASSWORD CHANGE REQUIRED")
    } should have message "Permission denied."
  }

  test("should fail when altering user when denied alter user privilege") {
    // GIVEN
    setupUserWithCustomAdminRole("foo", "bar")

    // WHEN
    execute("CREATE USER user SET PASSWORD 'abc'")
    execute("DENY ALTER USER ON DBMS TO custom")

    // THEN
    the[AuthorizationViolationException] thrownBy {
      executeOnSystem("foo", "bar", "ALTER USER user SET PASSWORD CHANGE NOT REQUIRED")
    } should have message "Permission denied."
  }

  // SHOW USER

  test("should enforce show user privilege") {
    // GIVEN
    setupUserWithCustomRole("foo", "bar")

    // WHEN
    execute("GRANT SHOW USER ON DBMS TO custom")

    // THEN
    executeOnSystem("foo", "bar", "SHOW USERS") should be(2)

    // WHEN
    execute("REVOKE SHOW USER ON DBMS FROM custom")

    // THEN
    the[AuthorizationViolationException] thrownBy {
      executeOnSystem("foo", "bar", "SHOW USERS")
    } should have message "Permission denied."
  }

  test("should fail when listing users when denied show user privilege") {
    // GIVEN
    setupUserWithCustomAdminRole("foo", "bar")

    // WHEN
    execute("DENY SHOW USER ON DBMS TO custom")

    // THEN
    the[AuthorizationViolationException] thrownBy {
      executeOnSystem("foo", "bar", "SHOW USERS")
    } should have message "Permission denied."
  }

  // USER MANAGEMENT

  test("should enforce user management privilege") {
    // GIVEN
    setupUserWithCustomRole("foo", "bar")

    // WHEN
    execute("GRANT USER MANAGEMENT ON DBMS TO custom")

    // THEN
    executeOnSystem("foo", "bar", "CREATE USER user SET PASSWORD 'abc'")
    executeOnSystem("foo", "bar", "ALTER USER user SET PASSWORD CHANGE NOT REQUIRED")
    executeOnSystem("foo", "bar", "SHOW USERS")
    executeOnSystem("foo", "bar", "DROP USER user")

    // WHEN
    execute("REVOKE USER MANAGEMENT ON DBMS FROM custom")
    execute("CREATE USER alice SET PASSWORD 'bar' CHANGE NOT REQUIRED")

    // THEN
    the[AuthorizationViolationException] thrownBy {
      executeOnSystem("foo", "bar", "CREATE USER user SET PASSWORD 'abc'")
    } should have message "Permission denied."
    the[AuthorizationViolationException] thrownBy {
      executeOnSystem("foo", "bar", "ALTER USER alice SET PASSWORD CHANGE NOT REQUIRED")
    } should have message "Permission denied."
    the[AuthorizationViolationException] thrownBy {
      executeOnSystem("foo", "bar", "SHOW USERS")
    } should have message "Permission denied."
    the[AuthorizationViolationException] thrownBy {
      executeOnSystem("foo", "bar", "DROP USER alice")
    } should have message "Permission denied."
  }

  test("should deny user management when denied user management privilege") {
    // GIVEN
    setupUserWithCustomAdminRole("foo", "bar")

    // WHEN
    execute("GRANT CREATE USER ON DBMS TO custom")
    execute("GRANT DROP USER ON DBMS TO custom")
    execute("GRANT ALTER USER ON DBMS TO custom")
    execute("GRANT SHOW USER ON DBMS TO custom")
    execute("DENY USER MANAGEMENT ON DBMS TO custom")

    // THEN
    the[AuthorizationViolationException] thrownBy {
      executeOnSystem("foo", "bar", "CREATE USER user SET PASSWORD 'abc'")
    } should have message "Permission denied."
    the[AuthorizationViolationException] thrownBy {
      executeOnSystem("foo", "bar", "DROP USER neo4j")
    } should have message "Permission denied."
    the[AuthorizationViolationException] thrownBy {
      executeOnSystem("foo", "bar", "ALTER USER foo SET PASSWORD 'abc'")
    } should have message "Permission denied."
    the[AuthorizationViolationException] thrownBy {
      executeOnSystem("foo", "bar", "SHOW USERS")
    } should have message "Permission denied."
  }

  // CREATE DATABASE

  test("should enforce create database privilege") {
    // GIVEN
    setupUserWithCustomRole("foo", "bar")

    // WHEN
    execute("GRANT CREATE DATABASE ON DBMS TO custom")

    // THEN
    executeOnSystem("foo", "bar", "CREATE DATABASE baz")
    execute("SHOW DATABASE baz").toSet should be(Set(db("baz")))

    // WHEN
    execute("DROP DATABASE baz")
    execute("REVOKE CREATE DATABASE ON DBMS FROM custom")

    // THEN
    the[AuthorizationViolationException] thrownBy {
      executeOnSystem("foo", "bar", "CREATE DATABASE baz")
    } should have message "Permission denied."

    execute("SHOW DATABASE baz").toSet should be(Set.empty)
  }

  test("should fail when creating database when denied database user privilege") {
    // GIVEN
    setupUserWithCustomAdminRole("foo", "bar")

    // WHEN
    execute("DENY CREATE DATABASE ON DBMS TO custom")

    // THEN
    the[AuthorizationViolationException] thrownBy {
      executeOnSystem("foo", "bar", "CREATE DATABASE baz")
    } should have message "Permission denied."

    execute("SHOW DATABASE baz").toSet should be(Set.empty)
  }

  test("should fail when replacing database with denied create database privilege") {
    // GIVEN
    setupUserWithCustomAdminRole("foo", "bar")

    // WHEN
    execute("DENY CREATE DATABASE ON DBMS TO custom")

    // THEN
    the[AuthorizationViolationException] thrownBy {
      executeOnSystem("foo", "bar", "CREATE OR REPLACE DATABASE myDb")
    } should have message "Permission denied."
  }

  test("should fail when replacing database with denied drop database privilege") {
    // GIVEN
    setupUserWithCustomAdminRole("foo", "bar")

    // WHEN
    execute("DENY DROP DATABASE ON DBMS TO custom")

    // THEN
    the[AuthorizationViolationException] thrownBy {
      executeOnSystem("foo", "bar", "CREATE OR REPLACE DATABASE myDb")
    } should have message "Permission denied."
  }

  test("should fail when replacing database without create database privilege") {
    // GIVEN
    setupUserWithCustomRole("foo", "bar")

    // WHEN
    execute("GRANT DROP DATABASE ON DBMS TO custom")

    // THEN
    the[AuthorizationViolationException] thrownBy {
      executeOnSystem("foo", "bar", "CREATE OR REPLACE DATABASE myDb")
    } should have message "Permission denied."
  }

  test("should fail when replacing database without drop database privilege") {
    // GIVEN
    setupUserWithCustomRole("foo", "bar")

    // WHEN
    execute("GRANT CREATE DATABASE ON DBMS TO custom")

    // THEN
    the[AuthorizationViolationException] thrownBy {
      executeOnSystem("foo", "bar", "CREATE OR REPLACE DATABASE myDb")
    } should have message "Permission denied."
  }

  // DROP DATABASE

  test("should enforce drop database privilege") {
    // GIVEN
    setupUserWithCustomRole("foo", "bar")

    // WHEN
    execute("CREATE DATABASE baz")
    execute("GRANT DROP DATABASE ON DBMS TO custom")

    // THEN
    executeOnSystem("foo", "bar", "DROP DATABASE baz")
    execute("SHOW DATABASE baz").toSet should be(Set.empty)

    // WHEN
    execute("CREATE DATABASE baz")
    execute("REVOKE DROP DATABASE ON DBMS FROM custom")

    // THEN
    the[AuthorizationViolationException] thrownBy {
      executeOnSystem("foo", "bar", "DROP DATABASE baz")
    } should have message "Permission denied."

    execute("SHOW DATABASE baz").toSet should be(Set(db("baz")))
  }

  test("should fail when dropping database when denied drop database privilege") {
    // GIVEN
    setupUserWithCustomAdminRole("foo", "bar")

    // WHEN
    execute("CREATE DATABASE baz")
    execute("DENY DROP DATABASE ON DBMS TO custom")

    // THEN
    the[AuthorizationViolationException] thrownBy {
      executeOnSystem("foo", "bar", "DROP DATABASE baz")
    } should have message "Permission denied."

    execute("SHOW DATABASE baz").toSet should be(Set(db("baz")))
  }

  // DATABASE MANAGEMENT

  test("should enforce database management privilege") {
    // GIVEN
    setupUserWithCustomRole("foo", "bar")

    // WHEN
    execute("GRANT DATABASE MANAGEMENT ON DBMS TO custom")

    // THEN
    executeOnSystem("foo", "bar", "CREATE DATABASE baz")
    executeOnSystem("foo", "bar", "DROP DATABASE baz")

    // WHEN
    execute("REVOKE DATABASE MANAGEMENT ON DBMS FROM custom")
    execute("CREATE DATABASE baz")

    // THEN
    the[AuthorizationViolationException] thrownBy {
      executeOnSystem("foo", "bar", "CREATE DATABASE userDb")
    } should have message "Permission denied."
    the[AuthorizationViolationException] thrownBy {
      executeOnSystem("foo", "bar", "DROP DATABASE baz")
    } should have message "Permission denied."
  }

  test("should fail database management when denied database management privilege") {
    // GIVEN
    setupUserWithCustomAdminRole("foo", "bar")

    // WHEN
    execute("GRANT CREATE DATABASE ON DBMS TO custom")
    execute("GRANT DROP DATABASE ON DBMS TO custom")
    execute("DENY DATABASE MANAGEMENT ON DBMS TO custom")

    execute("CREATE DATABASE baz")

    // THEN
    the[AuthorizationViolationException] thrownBy {
      executeOnSystem("foo", "bar", "CREATE DATABASE userDb")
    } should have message "Permission denied."
    the[AuthorizationViolationException] thrownBy {
      executeOnSystem("foo", "bar", "DROP DATABASE baz")
    } should have message "Permission denied."
  }

  // SHOW PRIVILEGE

  val showPrivilegeCommands = Seq(
    "SHOW PRIVILEGES",
    "SHOW ALL PRIVILEGES",
    "SHOW ROLE custom PRIVILEGES"
  )

  test("should enforce show privilege privilege") {
    // GIVEN
    setupUserWithCustomRole("foo", "bar")

    // WHEN
    execute("GRANT SHOW PRIVILEGE ON DBMS TO custom")

    // THEN
    showPrivilegeCommands.foreach(command =>
      withClue(command) {
        executeOnSystem("foo", "bar", command)
      }
    )

    // WHEN
    execute("REVOKE SHOW PRIVILEGE ON DBMS FROM custom")

    // THEN
    showPrivilegeCommands.foreach(command =>
      withClue(command) {
        the[AuthorizationViolationException] thrownBy {
          executeOnSystem("foo", "bar", command)
        } should have message "Permission denied."
      }
    )
  }

  test("should fail when showing privileges when denied show privilege privilege") {
    // GIVEN
    setupUserWithCustomAdminRole("foo", "bar")

    // WHEN
    execute("DENY SHOW PRIVILEGE ON DBMS TO custom")

    // THEN
    showPrivilegeCommands.foreach(command =>
      withClue(command) {
        the[AuthorizationViolationException] thrownBy {
          executeOnSystem("foo", "bar", command)
        } should have message "Permission denied."
      }
    )
  }

  test("should show user privileges with correct privileges") {
    // GIVEN
    setupUserWithCustomRole("foo", "bar")

    // WHEN
    execute("GRANT SHOW PRIVILEGE ON DBMS TO custom")
    execute("GRANT SHOW USER ON DBMS TO custom")

    // THEN
    val result = new mutable.HashSet[Map[String, AnyRef]]
    executeOnSystem("foo", "bar", "SHOW USER neo4j privileges", resultHandler = (row, _) => {
      result.add(asPrivilegesResult(row))
    })
    result should be(defaultUserPrivileges)
  }

  test("should fail to show user privileges without show user privilege") {
    // GIVEN
    setupUserWithCustomRole("foo", "bar")

    // WHEN
    execute("GRANT SHOW PRIVILEGE ON DBMS TO custom")

    // THEN
    the[AuthorizationViolationException] thrownBy {
      executeOnSystem("foo", "bar", "SHOW USER neo4j privileges")
    } should have message "Permission denied."
  }

  test("should fail to show user privileges without show privilege privilege") {
    // GIVEN
    setupUserWithCustomRole("foo", "bar")

    // WHEN
    execute("GRANT SHOW USER ON DBMS TO custom")

    // THEN
    the[AuthorizationViolationException] thrownBy {
      executeOnSystem("foo", "bar", "SHOW USER neo4j privileges")
    } should have message "Permission denied."
  }

  test("should always be able to show your own privileges") {
    // GIVEN
    setupUserWithCustomAdminRole("foo", "bar")

    // WHEN
    execute("DENY SHOW PRIVILEGE ON DBMS TO custom")
    execute("DENY SHOW USER ON DBMS TO custom")

    // THEN
    executeOnSystem("foo", "bar", "SHOW USER foo PRIVILEGES")
  }

  // ASSIGN & REMOVE PRIVILEGE

  Seq(
    // Graph commands
    ("TRAVERSE", "TRAVERSE ON GRAPH * NODES A", Set(traverse ++ Map("segment" -> "NODE(A)"))),
    ("READ", "READ {prop} ON GRAPH * NODES *", Set(read ++ Map("segment" -> "NODE(*)", "resource" -> "property(prop)"))),
    ("MATCH", "MATCH {prop} ON GRAPH * NODES A", Set(matchPrivilege ++ Map("segment" -> "NODE(A)", "resource" -> "property(prop)"))),
    ("WRITE", "WRITE ON GRAPH *", Set(write ++ Map("segment" -> "NODE(*)"), write ++ Map("segment" -> "RELATIONSHIP(*)"))),

    // Database commands
    ("ACCESS", "ACCESS ON DATABASE *", Set(access)),
    ("START", "START ON DATABASE *", Set(startDatabase)),
    ("STOP", "STOP ON DATABASE *", Set(stopDatabase)),
    ("CREATE INDEX", "CREATE INDEX ON DATABASE *", Set(createIndex)),
    ("DROP INDEX", "DROP INDEX ON DATABASE *", Set(dropIndex)),
    ("INDEX MANAGEMENT", "INDEX MANAGEMENT ON DATABASE *", Set(indexManagement)),
    ("CREATE CONSTRAINT", "CREATE CONSTRAINT ON DATABASE *", Set(createConstraint)),
    ("DROP CONSTRAINT", "DROP CONSTRAINT ON DATABASE *", Set(dropConstraint)),
    ("CONSTRAINT MANAGEMENT", "CONSTRAINT MANAGEMENT ON DATABASE *", Set(constraintManagement)),
    ("CREATE NEW LABEL", "CREATE NEW LABEL ON DATABASE *", Set(createNodeLabel)),
    ("CREATE NEW TYPE", "CREATE NEW TYPE ON DATABASE *", Set(createRelationshipType)),
    ("CREATE NEW NAME", "CREATE NEW NAME ON DATABASE *", Set(createPropertyKey)),
    ("NAME MANAGEMENT", "NAME MANAGEMENT ON DATABASE *", Set(nameManagement)),
    ("ALL DATABASE", "ALL ON DATABASE *", Set(allDatabasePrivilege)),
    ("SHOW TRANSACTION", "SHOW TRANSACTION ON DATABASE *", Set(showTransaction("*"))),
    ("TERMINATE TRANSACTION", "TERMINATE TRANSACTION ON DATABASE *", Set(terminateTransaction("*"))),
    ("TRANSACTION MANAGEMENT", "TRANSACTION MANAGEMENT ON DATABASE *", Set(transaction("*"))),

    // Dbms commands
    ("CREATE ROLE", "CREATE ROLE ON DBMS", Set(adminAction("create_role"))),
    ("DROP ROLE", "DROP ROLE ON DBMS", Set(adminAction("drop_role"))),
    ("ASSIGN ROLE", "ASSIGN ROLE ON DBMS", Set(adminAction("assign_role"))),
    ("REMOVE ROLE", "REMOVE ROLE ON DBMS", Set(adminAction("remove_role"))),
    ("SHOW ROLE", "SHOW ROLE ON DBMS", Set(adminAction("show_role"))),
    ("ROLE MANAGEMENT", "ROLE MANAGEMENT ON DBMS", Set(adminAction("role_management"))),
    ("CREATE USER", "CREATE USER ON DBMS", Set(adminAction("create_user"))),
    ("DROP USER", "DROP USER ON DBMS", Set(adminAction("drop_user"))),
    ("ALTER USER", "ALTER USER ON DBMS", Set(adminAction("alter_user"))),
    ("SHOW USER", "SHOW USER ON DBMS", Set(adminAction("show_user"))),
    ("USER MANAGEMENT", "USER MANAGEMENT ON DBMS", Set(adminAction("user_management"))),
    ("CREATE DATABASE", "CREATE DATABASE ON DBMS", Set(adminAction("create_database"))),
    ("DROP DATABASE", "DROP DATABASE ON DBMS", Set(adminAction("drop_database"))),
    ("DATABASE MANAGEMENT", "DATABASE MANAGEMENT ON DBMS", Set(adminAction("database_management"))),
    ("SHOW PRIVILEGE", "SHOW PRIVILEGE ON DBMS", Set(adminAction("show_privilege"))),
    ("ASSIGN PRIVILEGE", "ASSIGN PRIVILEGE ON DBMS", Set(adminAction("assign_privilege"))),
    ("REMOVE PRIVILEGE", "REMOVE PRIVILEGE ON DBMS", Set(adminAction("remove_privilege"))),
    ("PRIVILEGE MANAGEMENT", "PRIVILEGE MANAGEMENT ON DBMS", Set(adminAction("privilege_management"))),
  ).foreach {
    case (privilege, command, showPrivileges) =>

      test(s"should enforce assign privilege privilege for GRANT $privilege") {
        // GIVEN
        setupUserWithCustomRole("foo", "bar")
        execute("CREATE ROLE otherRole")

        // WHEN
        execute("GRANT ASSIGN PRIVILEGE ON DBMS TO custom")

        // THEN
        executeOnSystem("foo", "bar", s"GRANT $command TO otherRole")
        execute("SHOW ROLE otherRole PRIVILEGES").toSet should be(showPrivileges.map(p => granted(p).role("otherRole").map))

        // WHEN
        execute("REVOKE ASSIGN PRIVILEGE ON DBMS FROM custom")
        execute(s"REVOKE $command FROM otherRole")

        // THEN
        the[AuthorizationViolationException] thrownBy {
          executeOnSystem("foo", "bar", s"GRANT $command TO otherRole")
        } should have message "Permission denied."

        execute("SHOW ROLE otherRole PRIVILEGES").toSet should be(Set.empty)
      }

      test(s"should enforce assign privilege privilege for DENY $privilege") {
        // GIVEN
        setupUserWithCustomRole("foo", "bar")
        execute("CREATE ROLE otherRole")

        // WHEN
        execute("GRANT ASSIGN PRIVILEGE ON DBMS TO custom")

        // THEN
        executeOnSystem("foo", "bar", s"DENY $command TO otherRole")
        execute("SHOW ROLE otherRole PRIVILEGES").toSet should be(showPrivileges.map(p => denied(p).role("otherRole").map))

        // WHEN
        execute("REVOKE ASSIGN PRIVILEGE ON DBMS FROM custom")
        execute(s"REVOKE $command FROM otherRole")

        // THEN
        the[AuthorizationViolationException] thrownBy {
          executeOnSystem("foo", "bar", s"DENY $command TO otherRole")
        } should have message "Permission denied."

        execute("SHOW ROLE otherRole PRIVILEGES").toSet should be(Set.empty)
      }

      test(s"should fail when granting and denying $privilege privileges when denied assign privilege privilege") {
        // GIVEN
        setupUserWithCustomAdminRole("foo", "bar")
        execute("CREATE ROLE otherRole")

        // WHEN
        execute("DENY ASSIGN PRIVILEGE ON DBMS TO custom")

        // THEN
        the[AuthorizationViolationException] thrownBy {
          executeOnSystem("foo", "bar", s"GRANT $command TO otherRole")
        } should have message "Permission denied."

        the[AuthorizationViolationException] thrownBy {
          executeOnSystem("foo", "bar", s"DENY $command TO otherRole")
        } should have message "Permission denied."

        execute("SHOW ROLE otherRole PRIVILEGES").toSet should be(Set.empty)
      }

      test(s"should enforce remove privilege privilege for $privilege") {
        // GIVEN
        setupUserWithCustomRole("foo", "bar")
        execute("CREATE ROLE otherRole")

        // WHEN
        execute("GRANT REMOVE PRIVILEGE ON DBMS TO custom")
        execute(s"GRANT $command TO otherRole")

        // THEN
        executeOnSystem("foo", "bar", s"REVOKE $command FROM otherRole")
        execute("SHOW ROLE otherRole PRIVILEGES").toSet should be(Set.empty)

        // WHEN
        execute("REVOKE REMOVE PRIVILEGE ON DBMS FROM custom")
        execute(s"GRANT $command TO otherRole")

        // THEN
        the[AuthorizationViolationException] thrownBy {
          executeOnSystem("foo", "bar", s"REVOKE $command FROM otherRole")
        } should have message "Permission denied."

        execute("SHOW ROLE otherRole PRIVILEGES").toSet should be(showPrivileges.map(p => granted(p).role("otherRole").map))
      }

      test(s"should fail when revoking $privilege privileges when denied remove privilege privilege") {
        // GIVEN
        setupUserWithCustomAdminRole("foo", "bar")
        execute("CREATE ROLE otherRole")

        // WHEN
        execute("DENY REMOVE PRIVILEGE ON DBMS TO custom")
        execute(s"GRANT $command TO otherRole")

        // THEN
        the[AuthorizationViolationException] thrownBy {
          executeOnSystem("foo", "bar", s"REVOKE $command FROM otherRole")
        } should have message "Permission denied."

        execute("SHOW ROLE otherRole PRIVILEGES").toSet should be(showPrivileges.map(p => granted(p).role("otherRole").map))
      }
  }

  // PRIVILEGE MANAGEMENT

  test("should enforce privilege management privilege") {
    // GIVEN
    setupUserWithCustomRole("foo", "bar")
    execute("CREATE ROLE otherRole")

    // WHEN
    execute("GRANT PRIVILEGE MANAGEMENT ON DBMS TO custom")
    execute("GRANT TRAVERSE ON GRAPH * NODES A TO otherRole")

    // THEN
    executeOnSystem("foo", "bar", "SHOW ROLE otherRole PRIVILEGES", resultHandler = (row, _) => {
      val res = Map(
        "access" -> row.get("access"),
        "action" -> row.get("action"),
        "resource" -> row.get("resource"),
        "graph" -> row.get("graph"),
        "segment" -> row.get("segment"),
        "role" -> row.get("role"),
      )
      res should be(granted(traverse).node("A").role("otherRole").map)
    }) should be(1)

    executeOnSystem("foo", "bar", "GRANT TRAVERSE ON GRAPH * NODES B TO otherRole")
    executeOnSystem("foo", "bar", "DENY TRAVERSE ON GRAPH * NODES C TO otherRole")
    executeOnSystem("foo", "bar", "REVOKE TRAVERSE ON GRAPH * NODES A FROM otherRole")

    execute("SHOW ROLE otherRole PRIVILEGES").toSet should be(
      Set(
        granted(traverse).node("B").role("otherRole").database("*").map,
        denied(traverse).node("C").role("otherRole").database("*").map
      )
    )

    // WHEN
    execute("REVOKE PRIVILEGE MANAGEMENT ON DBMS FROM custom")
    execute("REVOKE TRAVERSE ON GRAPH * NODES B FROM otherRole")
    execute("REVOKE TRAVERSE ON GRAPH * NODES C FROM otherRole")
    execute("GRANT TRAVERSE ON GRAPH * NODES A TO otherRole")

    // THEN
    the[AuthorizationViolationException] thrownBy {
      executeOnSystem("foo", "bar", "SHOW ROLE otherRole PRIVILEGES")
    } should have message "Permission denied."

    the[AuthorizationViolationException] thrownBy {
      executeOnSystem("foo", "bar", "GRANT TRAVERSE ON GRAPH * NODES B TO otherRole")
    } should have message "Permission denied."

    the[AuthorizationViolationException] thrownBy {
      executeOnSystem("foo", "bar", "DENY TRAVERSE ON GRAPH * NODES C TO otherRole")
    } should have message "Permission denied."

    the[AuthorizationViolationException] thrownBy {
      executeOnSystem("foo", "bar", "REVOKE TRAVERSE ON GRAPH * NODES A FROM otherRole")
    } should have message "Permission denied."

    execute("SHOW ROLE otherRole PRIVILEGES").toSet should be(
      Set(granted(traverse).node("A").role("otherRole").database("*").map)
    )
  }

  test("should fail privilege management when denied privilege management privilege") {
    // GIVEN
    setupUserWithCustomAdminRole("foo", "bar")
    execute("CREATE ROLE otherRole")

    // WHEN
    execute("GRANT SHOW PRIVILEGE ON DBMS TO custom")
    execute("GRANT ASSIGN PRIVILEGE ON DBMS TO custom")
    execute("GRANT REMOVE PRIVILEGE ON DBMS TO custom")
    execute("DENY PRIVILEGE MANAGEMENT ON DBMS TO custom")

    execute("GRANT TRAVERSE ON GRAPH * NODES A TO otherRole")

    // THEN
    the[AuthorizationViolationException] thrownBy {
      executeOnSystem("foo", "bar", "SHOW ROLE otherRole PRIVILEGES")
    } should have message "Permission denied."

    the[AuthorizationViolationException] thrownBy {
      executeOnSystem("foo", "bar", "GRANT TRAVERSE ON GRAPH * NODES B TO otherRole")
    } should have message "Permission denied."

    the[AuthorizationViolationException] thrownBy {
      executeOnSystem("foo", "bar", "DENY TRAVERSE ON GRAPH * NODES C TO otherRole")
    } should have message "Permission denied."

    the[AuthorizationViolationException] thrownBy {
      executeOnSystem("foo", "bar", "REVOKE TRAVERSE ON GRAPH * NODES A FROM otherRole")
    } should have message "Permission denied."

    execute("SHOW ROLE otherRole PRIVILEGES").toSet should be(
      Set(granted(traverse).node("A").role("otherRole").database("*").map)
    )
  }

  // ALL DBMS PRIVILEGES

  test("should enforce all dbms privileges privilege") {
    // GIVEN
    setupUserWithCustomRole("foo", "bar")

    // WHEN
    execute("GRANT ALL DBMS PRIVILEGES ON DBMS TO custom")

    // THEN

    // Should be able to do role management
    executeOnSystem("foo", "bar", "CREATE ROLE otherRole")
    execute("SHOW ROLES").toSet should be(defaultRoles ++ Set(role("custom").map, role("otherRole").map))

    // Should be able to do user management
    executeOnSystem("foo", "bar", "DROP USER neo4j")
    execute("SHOW USERS").toSet should be(Set(user("foo", Seq("custom"), passwordChangeRequired = false)))

    // Should be able to do database management
    executeOnSystem("foo", "bar", "CREATE DATABASE baz")
    execute("SHOW DATABASE baz").toSet should be(Set(db("baz")))

    // Should be able to do privilege management
    executeOnSystem("foo", "bar", "GRANT TRAVERSE ON GRAPH * NODES A TO otherRole")
    execute("SHOW ROLE otherRole PRIVILEGES").toSet should be(Set(granted(traverse).node("A").role("otherRole").database("*").map))

    // WHEN
    execute("REVOKE ALL DBMS PRIVILEGES ON DBMS FROM custom")

    // THEN

    // Should not be able to do role management
    the[AuthorizationViolationException] thrownBy {
      executeOnSystem("foo", "bar", "CREATE ROLE role")
    } should have message "Permission denied."
    execute("SHOW ROLES").toSet should be(defaultRoles ++ Set(role("custom").map, role("otherRole").map))

    // Should not be able to do user management
    the[AuthorizationViolationException] thrownBy {
      executeOnSystem("foo", "bar", "CREATE USER alice SET PASSWORD 'secret'")
    } should have message "Permission denied."
    execute("SHOW USERS").toSet should be(Set(user("foo", Seq("custom"), passwordChangeRequired = false)))

    // Should not be able to do database management
    the[AuthorizationViolationException] thrownBy {
      executeOnSystem("foo", "bar", "DROP DATABASE baz")
    } should have message "Permission denied."
    execute("SHOW DATABASE baz").toSet should be(Set(db("baz")))

    // Should not be able to do privilege management
    the[AuthorizationViolationException] thrownBy {
      executeOnSystem("foo", "bar", "GRANT TRAVERSE ON GRAPH * NODES B TO otherRole")
    } should have message "Permission denied."
    execute("SHOW ROLE otherRole PRIVILEGES").toSet should be(Set(granted(traverse).node("A").role("otherRole").database("*").map))
  }

  test("should fail dbms management when denied all dbms privileges privilege") {
    // GIVEN
    setupUserWithCustomAdminRole("foo", "bar")
    execute("CREATE ROLE otherRole")

    // WHEN
    allDbmsPrivileges("GRANT", includingCompound = false)
    execute("DENY ALL DBMS PRIVILEGES ON DBMS TO custom")

    // THEN

    // Should not be able to do role management
    the[AuthorizationViolationException] thrownBy {
      executeOnSystem("foo", "bar", "CREATE ROLE role")
    } should have message "Permission denied."
    execute("SHOW ROLES").toSet should be(defaultRoles ++ Set(role("custom").map, role("otherRole").map))

    // Should not be able to do user management
    the[AuthorizationViolationException] thrownBy {
      executeOnSystem("foo", "bar", "DROP USER neo4j")
    } should have message "Permission denied."
    execute("SHOW USERS").toSet should be(Set(user("neo4j", Seq("admin")), user("foo", Seq("custom"), passwordChangeRequired = false)))

    // Should not be able to do database management
    the[AuthorizationViolationException] thrownBy {
      executeOnSystem("foo", "bar", "CREATE DATABASE baz")
    } should have message "Permission denied."
    execute("SHOW DATABASE baz").toSet should be(Set.empty)

    // Should not be able to do privilege management
    the[AuthorizationViolationException] thrownBy {
      executeOnSystem("foo", "bar", "GRANT TRAVERSE ON GRAPH * NODES A TO otherRole")
    } should have message "Permission denied."
    execute("SHOW ROLE otherRole PRIVILEGES").toSet should be(Set.empty)
  }

  // helper methods

  private def createRoleWithOnlyAdminPrivilege(name: String = "adminOnly"): Unit = {
    execute(s"CREATE ROLE $name AS COPY OF admin")
    execute(s"REVOKE MATCH {*} ON GRAPH * FROM $name")
    execute(s"REVOKE WRITE ON GRAPH * FROM $name")
    execute(s"REVOKE ACCESS ON DATABASE * FROM $name")
    execute(s"REVOKE ALL ON DATABASE * FROM $name")
    execute(s"REVOKE NAME ON DATABASE * FROM $name")
    execute(s"REVOKE INDEX ON DATABASE * FROM $name")
    execute(s"REVOKE CONSTRAINT ON DATABASE * FROM $name")
    execute(s"SHOW ROLE $name PRIVILEGES").toSet should be(Set(granted(admin).role(name).map))
  }

  private def allDbmsPrivileges(privType: String, includingCompound: Boolean): Unit = {

    val preposition = if (privType.equals("REVOKE")) "FROM" else "TO"

    execute(s"$privType CREATE ROLE ON DBMS $preposition custom")
    execute(s"$privType DROP ROLE ON DBMS $preposition custom")
    execute(s"$privType ASSIGN ROLE ON DBMS $preposition custom")
    execute(s"$privType REMOVE ROLE ON DBMS $preposition custom")
    execute(s"$privType SHOW ROLE ON DBMS $preposition custom")
    execute(s"$privType ROLE MANAGEMENT ON DBMS $preposition custom")
    execute(s"$privType CREATE USER ON DBMS $preposition custom")
    execute(s"$privType DROP USER ON DBMS $preposition custom")
    execute(s"$privType SHOW USER ON DBMS $preposition custom")
    execute(s"$privType ALTER USER ON DBMS $preposition custom")
    execute(s"$privType USER MANAGEMENT ON DBMS $preposition custom")
    execute(s"$privType CREATE DATABASE ON DBMS $preposition custom")
    execute(s"$privType DROP DATABASE ON DBMS $preposition custom")
    execute(s"$privType DATABASE MANAGEMENT ON DBMS $preposition custom")
    execute(s"$privType SHOW PRIVILEGE ON DBMS $preposition custom")
    execute(s"$privType ASSIGN PRIVILEGE ON DBMS $preposition custom")
    execute(s"$privType REMOVE PRIVILEGE ON DBMS $preposition custom")
    execute(s"$privType PRIVILEGE MANAGEMENT ON DBMS $preposition custom")

    if (includingCompound) {
      execute(s"$privType ALL DBMS PRIVILEGES ON DBMS $preposition custom")
    }
  }
}
