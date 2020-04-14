/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.internal.cypher.acceptance

import org.neo4j.graphdb.security.AuthorizationViolationException
import org.neo4j.internal.kernel.api.security.AuthenticationResult

class UserManagementPrivilegesAcceptanceTest extends AdministrationCommandAcceptanceTestBase {

  // Privilege tests

  test("should not revoke other user management privileges when revoking user management") {
    // GIVEN
    createRoleWithOnlyAdminPrivilege()
    execute("CREATE ROLE custom AS COPY OF adminOnly")
    execute("GRANT CREATE USER ON DBMS TO custom")
    execute("GRANT DROP USER ON DBMS TO custom")
    execute("GRANT SHOW USER ON DBMS TO custom")
    execute("GRANT SET USER STATUS ON DBMS TO custom")
    execute("GRANT SET PASSWORDS ON DBMS TO custom")
    execute("GRANT ALTER USER ON DBMS TO custom")
    execute("GRANT USER MANAGEMENT ON DBMS TO custom")

    // WHEN
    execute("REVOKE USER MANAGEMENT ON DBMS FROM custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      granted(adminPrivilege).role("custom").map,
      granted(adminAction("create_user")).role("custom").map,
      granted(adminAction("drop_user")).role("custom").map,
      granted(adminAction("show_user")).role("custom").map,
      granted(adminAction("set_user_status")).role("custom").map,
      granted(adminAction("set_passwords")).role("custom").map,
      granted(adminAction("alter_user")).role("custom").map
    ))
  }

    test("should not revoke sub parts when revoking alter user") {
    // GIVEN
    createRoleWithOnlyAdminPrivilege()
    execute("CREATE ROLE custom AS COPY OF adminOnly")
    execute("GRANT SET USER STATUS ON DBMS TO custom")
    execute("GRANT SET PASSWORDS ON DBMS TO custom")
    execute("GRANT ALTER USER ON DBMS TO custom")

    // WHEN
    execute("REVOKE ALTER USER ON DBMS FROM custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      granted(adminPrivilege).role("custom").map,
      granted(adminAction("set_user_status")).role("custom").map,
      granted(adminAction("set_passwords")).role("custom").map,
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
      granted(adminPrivilege).role("custom").map,
      granted(adminAction("user_management")).role("custom").map
    ))
  }

  // Enforcement tests

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
  //// SET PASSWORDS

  Seq( "alter user", "set passwords" ).foreach {
    privilege =>
      test(s"should enforce privilege for set password change required with $privilege") {
        // GIVEN
        setupUserWithCustomRole("foo", "bar")

        // WHEN
        execute("CREATE USER user SET PASSWORD 'abc' CHANGE REQUIRED")
        execute(s"GRANT $privilege ON DBMS TO custom")

        // THEN
        executeOnSystem("foo", "bar", "ALTER USER user SET PASSWORD CHANGE NOT REQUIRED")
        execute("SHOW USERS").toSet should be(Set(
          neo4jUser,
          user("foo", passwordChangeRequired = false, roles = Seq("custom")),
          user("user", passwordChangeRequired = false)
        ))

        // WHEN
        execute(s"REVOKE $privilege ON DBMS FROM custom")

        // THEN
        the[AuthorizationViolationException] thrownBy {
          executeOnSystem("foo", "bar", "ALTER USER user SET PASSWORD CHANGE REQUIRED")
        } should have message "Permission denied."
      }

      test(s"should fail when set password change required when denied $privilege") {
        // GIVEN
        setupUserWithCustomAdminRole("foo", "bar")

        // WHEN
        execute("CREATE USER user SET PASSWORD 'abc'")
        execute(s"DENY $privilege ON DBMS TO custom")

        // THEN
        the[AuthorizationViolationException] thrownBy {
          executeOnSystem("foo", "bar", "ALTER USER user SET PASSWORD CHANGE NOT REQUIRED")
        } should have message "Permission denied."
      }

      test(s"should enforce privilege for set password with $privilege") {
        // GIVEN
        setupUserWithCustomRole("foo", "bar")

        // WHEN
        execute("CREATE USER user SET PASSWORD 'abc' CHANGE REQUIRED")
        execute(s"GRANT $privilege ON DBMS TO custom")

        // THEN
        executeOnSystem("foo", "bar", "ALTER USER user SET PASSWORD 'cba'")
        testUserLogin("user", "cba", AuthenticationResult.PASSWORD_CHANGE_REQUIRED)
                // WHEN
        execute(s"REVOKE $privilege ON DBMS FROM custom")

        // THEN
        the[AuthorizationViolationException] thrownBy {
          executeOnSystem("foo", "bar", "ALTER USER user SET PASSWORD '123'")
        } should have message "Permission denied."
      }

      test(s"should fail when setting password when denied $privilege") {
        // GIVEN
        setupUserWithCustomAdminRole("foo", "bar")

        // WHEN
        execute("CREATE USER user SET PASSWORD 'abc'")
        execute(s"DENY $privilege ON DBMS TO custom")

        // THEN
        the[AuthorizationViolationException] thrownBy {
          executeOnSystem("foo", "bar", "ALTER USER user SET PASSWORD 'cba'")
        } should have message "Permission denied."
      }
  }

  //// SET USER STATUS

  Seq("alter user", "set user status").foreach {
    privilege =>
      test(s"should enforce privilege for setting user status with $privilege") {
        // GIVEN
        setupUserWithCustomRole("foo", "bar")

        // WHEN
        execute("CREATE USER user SET PASSWORD 'abc' CHANGE REQUIRED")
        execute(s"GRANT $privilege ON DBMS TO custom")

        // THEN
        executeOnSystem("foo", "bar", "ALTER USER user SET STATUS SUSPENDED")
        execute("SHOW USERS").toSet should be(Set(
          neo4jUser,
          user("foo", passwordChangeRequired = false, roles = Seq("custom")),
          user("user", suspended = true)
        ))

        // WHEN
        execute(s"REVOKE $privilege ON DBMS FROM custom")

        // THEN
        the[AuthorizationViolationException] thrownBy {
          executeOnSystem("foo", "bar", "ALTER USER user SET STATUS ACTIVE")
        } should have message "Permission denied."
      }

      test(s"should fail setting user status when denied $privilege") {
        // GIVEN
        setupUserWithCustomAdminRole("foo", "bar")

        // WHEN
        execute("CREATE USER user SET PASSWORD 'abc'")
        execute(s"DENY $privilege ON DBMS TO custom")

        // THEN
        the[AuthorizationViolationException] thrownBy {
          executeOnSystem("foo", "bar", "ALTER USER user SET STATUS SUSPENDED")
        } should have message "Permission denied."
      }
  }

  test("should enforce correct privileges when changing both password and status") {
    // GIVEN
    setupUserWithCustomRole("foo", "bar")

    // WHEN
    execute("CREATE USER user SET PASSWORD 'abc' CHANGE REQUIRED")
    execute(s"GRANT SET PASSWORDS ON DBMS TO custom")

    // THEN
    the[AuthorizationViolationException] thrownBy {
      executeOnSystem("foo", "bar", "ALTER USER user SET PASSWORD CHANGE NOT REQUIRED SET STATUS SUSPENDED")
    } should have message "Permission denied."

    // WHEN
    execute(s"GRANT SET USER STATUS ON DBMS TO custom")

    // THEN
    executeOnSystem("foo", "bar", "ALTER USER user SET PASSWORD CHANGE NOT REQUIRED SET STATUS SUSPENDED")
    execute("SHOW USERS").toSet should be(Set(
      neo4jUser,
      user("foo", passwordChangeRequired = false, roles = Seq("custom")),
      user("user", passwordChangeRequired = false, suspended = true)
    ))

    // WHEN
    execute(s"REVOKE SET PASSWORDS ON DBMS FROM custom")

    // THEN
    the[AuthorizationViolationException] thrownBy {
      executeOnSystem("foo", "bar", "ALTER USER user SET PASSWORD CHANGE REQUIRED SET STATUS ACTIVE")
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
}