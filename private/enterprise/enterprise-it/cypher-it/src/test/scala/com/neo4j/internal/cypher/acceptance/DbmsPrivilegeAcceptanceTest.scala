/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.internal.cypher.acceptance

import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.graphdb.security.AuthorizationViolationException

class DbmsPrivilegeAcceptanceTest extends AdministrationCommandAcceptanceTestBase {

  // Privilege tests

  Seq(("GRANT", "GRANTED"), ("DENY", "DENIED")).foreach {
    case (grant, relType) =>
      test(s"should $grant create role privilege") {
        // GIVEN
        selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
        execute("CREATE ROLE custom")

        // WHEN
        execute(s"$grant CREATE ROLE ON DBMS TO custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          adminAction("create_role", relType).role("custom").map
        ))
      }

      test(s"should $grant drop role privilege") {
        // GIVEN
        selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
        execute("CREATE ROLE custom")

        // WHEN
        execute(s"$grant DROP ROLE ON DBMS TO custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          adminAction("drop_role", relType).role("custom").map
        ))
      }

      test(s"should $grant assign role privilege") {
        // GIVEN
        selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
        execute("CREATE ROLE custom")

        // WHEN
        execute(s"$grant ASSIGN ROLE ON DBMS TO custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          adminAction("assign_role", relType).role("custom").map
        ))
      }

      test(s"should $grant remove role privilege") {
        // GIVEN
        selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
        execute("CREATE ROLE custom")

        // WHEN
        execute(s"$grant REMOVE ROLE ON DBMS TO custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          adminAction("remove_role", relType).role("custom").map
        ))
      }

      test(s"should $grant show role privilege") {
        // GIVEN
        selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
        execute("CREATE ROLE custom")

        // WHEN
        execute(s"$grant SHOW ROLE ON DBMS TO custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          adminAction("show_role", relType).role("custom").map
        ))
      }

      test(s"should $grant role management privilege") {
        // GIVEN
        selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
        execute("CREATE ROLE custom")

        // WHEN
        execute(s"$grant ROLE MANAGEMENT ON DBMS TO custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          adminAction("role_management", relType).role("custom").map
        ))
      }

      test(s"should $grant create user privilege") {
        // GIVEN
        selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
        execute("CREATE ROLE custom")

        // WHEN
        execute(s"$grant CREATE USER ON DBMS TO custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          adminAction("create_user", relType).role("custom").map
        ))
      }

      test(s"should $grant drop user privilege") {
        // GIVEN
        selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
        execute("CREATE ROLE custom")

        // WHEN
        execute(s"$grant DROP USER ON DBMS TO custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          adminAction("drop_user", relType).role("custom").map
        ))
      }

      test(s"should $grant show user privilege") {
        // GIVEN
        selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
        execute("CREATE ROLE custom")

        // WHEN
        execute(s"$grant SHOW USER ON DBMS TO custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          adminAction("show_user", relType).role("custom").map
        ))
      }

      test(s"should $grant alter user privilege") {
        // GIVEN
        selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
        execute("CREATE ROLE custom")

        // WHEN
        execute(s"$grant ALTER USER ON DBMS TO custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          adminAction("alter_user", relType).role("custom").map
        ))
      }

      test(s"should $grant user management privilege") {
        // GIVEN
        selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
        execute("CREATE ROLE custom")

        // WHEN
        execute(s"$grant USER MANAGEMENT ON DBMS TO custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          adminAction("user_management", relType).role("custom").map
        ))
      }
  }

  test("should revoke other role management privileges when revoking role management") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    execute("GRANT CREATE ROLE ON DBMS TO custom")
    execute("GRANT DROP ROLE ON DBMS TO custom")
    execute("GRANT ASSIGN ROLE ON DBMS TO custom")
    execute("GRANT REMOVE ROLE ON DBMS TO custom")
    execute("GRANT SHOW ROLE ON DBMS TO custom")
    execute("GRANT ROLE MANAGEMENT ON DBMS TO custom")

    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      adminAction("create_role").role("custom").map,
      adminAction("drop_role").role("custom").map,
      adminAction("assign_role").role("custom").map,
      adminAction("remove_role").role("custom").map,
      adminAction("show_role").role("custom").map,
      adminAction("role_management").role("custom").map
    ))

    // WHEN
    execute("REVOKE ROLE MANAGEMENT ON DBMS FROM custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(empty)
  }

  test("should revoke other user management privileges when revoking user management") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    execute("GRANT CREATE USER ON DBMS TO custom")
    execute("GRANT DROP USER ON DBMS TO custom")
    execute("GRANT SHOW USER ON DBMS TO custom")
    execute("GRANT ALTER USER ON DBMS TO custom")
    execute("GRANT USER MANAGEMENT ON DBMS TO custom")

    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      adminAction("create_user").role("custom").map,
      adminAction("drop_user").role("custom").map,
      adminAction("alter_user").role("custom").map,
      adminAction("show_user").role("custom").map,
      adminAction("user_management").role("custom").map
    ))

    // WHEN
    execute("REVOKE USER MANAGEMENT ON DBMS FROM custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(empty)
  }

  test("Should get error when revoking a subset of role management privilege") {
    // Given
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    execute("GRANT ROLE MANAGEMENT ON DBMS TO custom")
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(adminAction("role_management").role("custom").map))

    // Now try to revoke each sub-privilege in turn
    Seq(
      "CREATE ROLE",
      "DROP ROLE",
      "SHOW ROLE",
      "ASSIGN ROLE",
      "REMOVE ROLE"
    ).foreach { privilege =>
      // When && Then
      the[IllegalStateException] thrownBy {
        execute(s"REVOKE $privilege ON DBMS FROM custom")
      } should have message s"Unsupported to revoke a sub-privilege '$privilege' from a compound privilege 'ROLE MANAGEMENT', consider using DENY instead."
    }
  }

  test("Should get error when revoking a subset of user management privilege") {
    // Given
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    execute("GRANT USER MANAGEMENT ON DBMS TO custom")
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(adminAction("user_management").role("custom").map))

    // Now try to revoke each sub-privilege in turn
    Seq(
      "CREATE USER",
      "DROP USER",
      "SHOW USER",
      "ALTER USER"
    ).foreach { privilege =>
      // When && Then
      the[IllegalStateException] thrownBy {
        execute(s"REVOKE $privilege ON DBMS FROM custom")
      } should have message s"Unsupported to revoke a sub-privilege '$privilege' from a compound privilege 'USER MANAGEMENT', consider using DENY instead."
    }
  }

  // Enforcement tests

  // CREATE ROLE

  test("should enforce create role privilege") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    execute("CREATE USER foo SET PASSWORD 'bar' CHANGE NOT REQUIRED")
    execute("GRANT ROLE custom TO foo")

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
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom AS COPY OF admin")
    execute("CREATE USER foo SET PASSWORD 'bar' CHANGE NOT REQUIRED")
    execute("GRANT ROLE custom TO foo")

    // WHEN
    execute("DENY CREATE ROLE ON DBMS TO custom")

    // THEN
    the[AuthorizationViolationException] thrownBy {
      executeOnSystem("foo", "bar", "CREATE ROLE role")
    } should have message "Permission denied."
  }

  // DROP ROLE

  test("should enforce drop role privilege") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    execute("CREATE USER foo SET PASSWORD 'bar' CHANGE NOT REQUIRED")
    execute("GRANT ROLE custom TO foo")
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
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom AS COPY OF admin")
    execute("CREATE USER foo SET PASSWORD 'bar' CHANGE NOT REQUIRED")
    execute("GRANT ROLE custom TO foo")

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
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    execute("CREATE USER foo SET PASSWORD 'bar' CHANGE NOT REQUIRED")
    execute("GRANT ROLE custom TO foo")
    execute("CREATE ROLE role")

    // WHEN
    execute("GRANT ASSIGN ROLE ON DBMS TO custom")

    // THEN
    executeOnSystem("foo", "bar", "GRANT ROLE role TO foo")
    execute("SHOW ROLES WITH USERS").toSet should be(defaultRolesWithUsers ++ Set(role("custom").member("foo").map, role("role").member("foo").map))

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
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom AS COPY OF admin")
    execute("CREATE USER foo SET PASSWORD 'bar' CHANGE NOT REQUIRED")
    execute("GRANT ROLE custom TO foo")

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
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    execute("CREATE USER foo SET PASSWORD 'bar' CHANGE NOT REQUIRED")
    execute("GRANT ROLE custom TO foo")

    // WHEN
    execute("CREATE ROLE role")
    execute("GRANT ROLE role TO foo")
    execute("GRANT REMOVE ROLE ON DBMS TO custom")

    // THEN
    executeOnSystem("foo", "bar", "REVOKE ROLE role FROM foo")
    execute("SHOW ROLES WITH USERS").toSet should be(defaultRolesWithUsers ++ Set(role("custom").member("foo").map, role("role").noMember().map))

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
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom AS COPY OF admin")
    execute("CREATE USER foo SET PASSWORD 'bar' CHANGE NOT REQUIRED")
    execute("GRANT ROLE custom TO foo")

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
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    execute("CREATE USER foo SET PASSWORD 'bar' CHANGE NOT REQUIRED")
    execute("GRANT ROLE custom TO foo")

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
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom AS COPY OF admin")
    execute("CREATE USER foo SET PASSWORD 'bar' CHANGE NOT REQUIRED")
    execute("GRANT ROLE custom TO foo")

    // WHEN
    execute("DENY SHOW ROLE ON DBMS TO custom")

    // THEN
    the[AuthorizationViolationException] thrownBy {
      executeOnSystem("foo", "bar", "SHOW ROLES")
    } should have message "Permission denied."
  }

  // ROLE MANAGEMENT

  test("should be able to create role with role management privilege") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    execute("CREATE USER foo SET PASSWORD 'bar' CHANGE NOT REQUIRED")
    execute("GRANT ROLE custom TO foo")

    // WHEN
    execute("GRANT ROLE MANAGEMENT ON DBMS TO custom")

    // THEN
    executeOnSystem("foo", "bar", "CREATE ROLE role")
  }

  test("should deny create role when denied role management privilege") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom AS COPY OF admin")
    execute("CREATE USER foo SET PASSWORD 'bar' CHANGE NOT REQUIRED")
    execute("GRANT ROLE custom TO foo")

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
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    execute("CREATE USER foo SET PASSWORD 'bar' CHANGE NOT REQUIRED")
    execute("GRANT ROLE custom TO foo")

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
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom AS COPY OF admin")
    execute("CREATE USER foo SET PASSWORD 'bar' CHANGE NOT REQUIRED")
    execute("GRANT ROLE custom TO foo")

    // WHEN
    execute("DENY CREATE USER ON DBMS TO custom")

    // THEN
    the[AuthorizationViolationException] thrownBy {
      executeOnSystem("foo", "bar", "CREATE USER user SET PASSWORD 'abc'")
    } should have message "Permission denied."
  }

  // DROP USER

  test("should enforce drop user privilege") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    execute("CREATE USER foo SET PASSWORD 'bar' CHANGE NOT REQUIRED")
    execute("GRANT ROLE custom TO foo")

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
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom AS COPY OF admin")
    execute("CREATE USER foo SET PASSWORD 'bar' CHANGE NOT REQUIRED")
    execute("GRANT ROLE custom TO foo")

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
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    execute("CREATE USER foo SET PASSWORD 'bar' CHANGE NOT REQUIRED")
    execute("GRANT ROLE custom TO foo")

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
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom AS COPY OF admin")
    execute("CREATE USER foo SET PASSWORD 'bar' CHANGE NOT REQUIRED")
    execute("GRANT ROLE custom TO foo")

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
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    execute("CREATE USER foo SET PASSWORD 'bar' CHANGE NOT REQUIRED")
    execute("GRANT ROLE custom TO foo")

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
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom AS COPY OF admin")
    execute("CREATE USER foo SET PASSWORD 'bar' CHANGE NOT REQUIRED")
    execute("GRANT ROLE custom TO foo")

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
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    execute("CREATE USER foo SET PASSWORD 'bar' CHANGE NOT REQUIRED")
    execute("GRANT ROLE custom TO foo")

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
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom AS COPY OF admin")
    execute("CREATE USER foo SET PASSWORD 'bar' CHANGE NOT REQUIRED")
    execute("GRANT ROLE custom TO foo")

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
