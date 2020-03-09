/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.internal.cypher.acceptance

import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.graphdb.security.AuthorizationViolationException
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException

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

      test(s"should fail to $grant create role privilege to non-existing role") {
        // GIVEN
        selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)

        the[InvalidArgumentsException] thrownBy {
          // WHEN
          execute(s"$grant CREATE ROLE ON DBMS TO role")
          // THEN
        } should have message s"Failed to ${grant.toLowerCase} create_role privilege to role 'role': Role 'role' does not exist."
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

      test(s"should fail to $grant drop user privilege to non-existing role") {
        // GIVEN
        selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)

        the[InvalidArgumentsException] thrownBy {
          // WHEN
          execute(s"$grant DROP USER ON DBMS TO role")
          // THEN
        } should have message s"Failed to ${grant.toLowerCase} drop_user privilege to role 'role': Role 'role' does not exist."
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

      test(s"should $grant create database privilege") {
        // GIVEN
        selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
        execute("CREATE ROLE custom")

        // WHEN
        execute(s"$grant CREATE DATABASE ON DBMS TO custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          adminAction("create_database", relType).role("custom").map
        ))
      }

      test(s"should $grant drop database privilege") {
        // GIVEN
        selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
        execute("CREATE ROLE custom")

        // WHEN
        execute(s"$grant DROP DATABASE ON DBMS TO custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          adminAction("drop_database", relType).role("custom").map
        ))
      }

      test(s"should $grant database management privilege") {
        // GIVEN
        selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
        execute("CREATE ROLE custom")

        // WHEN
        execute(s"$grant DATABASE MANAGEMENT ON DBMS TO custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          adminAction("database_management", relType).role("custom").map
        ))
      }
  }

  test("should not revoke other role management privileges when revoking role management") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    createRoleWithOnlyAdminPrivilege()
    execute("CREATE ROLE custom AS COPY OF adminOnly")
    execute("GRANT CREATE ROLE ON DBMS TO custom")
    execute("GRANT DROP ROLE ON DBMS TO custom")
    execute("GRANT ASSIGN ROLE ON DBMS TO custom")
    execute("GRANT REMOVE ROLE ON DBMS TO custom")
    execute("GRANT SHOW ROLE ON DBMS TO custom")
    execute("GRANT ROLE MANAGEMENT ON DBMS TO custom")

    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantAdmin().role("custom").map,
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
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantAdmin().role("custom").map,
      adminAction("create_role").role("custom").map,
      adminAction("drop_role").role("custom").map,
      adminAction("assign_role").role("custom").map,
      adminAction("remove_role").role("custom").map,
      adminAction("show_role").role("custom").map
    ))
  }

  test("should not revoke other user management privileges when revoking user management") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    createRoleWithOnlyAdminPrivilege()
    execute("CREATE ROLE custom AS COPY OF adminOnly")
    execute("GRANT CREATE USER ON DBMS TO custom")
    execute("GRANT DROP USER ON DBMS TO custom")
    execute("GRANT SHOW USER ON DBMS TO custom")
    execute("GRANT ALTER USER ON DBMS TO custom")
    execute("GRANT USER MANAGEMENT ON DBMS TO custom")

    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantAdmin().role("custom").map,
      adminAction("create_user").role("custom").map,
      adminAction("drop_user").role("custom").map,
      adminAction("alter_user").role("custom").map,
      adminAction("show_user").role("custom").map,
      adminAction("user_management").role("custom").map
    ))

    // WHEN
    execute("REVOKE USER MANAGEMENT ON DBMS FROM custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantAdmin().role("custom").map,
      adminAction("create_user").role("custom").map,
      adminAction("drop_user").role("custom").map,
      adminAction("alter_user").role("custom").map,
      adminAction("show_user").role("custom").map
    ))
  }

    test("should not revoke other database management privileges when revoking database management") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    createRoleWithOnlyAdminPrivilege()
    execute("CREATE ROLE custom AS COPY OF adminOnly")
    execute("GRANT CREATE DATABASE ON DBMS TO custom")
    execute("GRANT DROP DATABASE ON DBMS TO custom")
    execute("GRANT DATABASE MANAGEMENT ON DBMS TO custom")

    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantAdmin().role("custom").map,
      adminAction("create_database").role("custom").map,
      adminAction("drop_database").role("custom").map,
      adminAction("database_management").role("custom").map
    ))

    // WHEN
    execute("REVOKE DATABASE MANAGEMENT ON DBMS FROM custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantAdmin().role("custom").map,
      adminAction("create_database").role("custom").map,
      adminAction("drop_database").role("custom").map
    ))
  }

  test("Should revoke sub-privilege even if role management exists") {
    // Given
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    createRoleWithOnlyAdminPrivilege()
    execute("CREATE ROLE custom AS COPY OF adminOnly")
    execute("GRANT CREATE ROLE ON DBMS TO custom")
    execute("GRANT DROP ROLE ON DBMS TO custom")
    execute("GRANT ASSIGN ROLE ON DBMS TO custom")
    execute("GRANT REMOVE ROLE ON DBMS TO custom")
    execute("GRANT SHOW ROLE ON DBMS TO custom")
    execute("GRANT ROLE MANAGEMENT ON DBMS TO custom")
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantAdmin().role("custom").map,
      adminAction("create_role").role("custom").map,
      adminAction("drop_role").role("custom").map,
      adminAction("assign_role").role("custom").map,
      adminAction("remove_role").role("custom").map,
      adminAction("show_role").role("custom").map,
      adminAction("role_management").role("custom").map
    ))

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
      grantAdmin().role("custom").map,
      adminAction("role_management").role("custom").map
    ))
  }

  test("Should revoke sub-privilege even if user management exists") {
    // Given
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    createRoleWithOnlyAdminPrivilege()
    execute("CREATE ROLE custom AS COPY OF adminOnly")
    execute("GRANT CREATE USER ON DBMS TO custom")
    execute("GRANT DROP USER ON DBMS TO custom")
    execute("GRANT SHOW USER ON DBMS TO custom")
    execute("GRANT ALTER USER ON DBMS TO custom")
    execute("GRANT USER MANAGEMENT ON DBMS TO custom")
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantAdmin().role("custom").map,
      adminAction("create_user").role("custom").map,
      adminAction("drop_user").role("custom").map,
      adminAction("alter_user").role("custom").map,
      adminAction("show_user").role("custom").map,
      adminAction("user_management").role("custom").map
    ))

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
      grantAdmin().role("custom").map,
      adminAction("user_management").role("custom").map
    ))
  }

    test("Should revoke sub-privilege even if database management exists") {
    // Given
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    execute("GRANT CREATE DATABASE ON DBMS TO custom")
    execute("GRANT DROP DATABASE ON DBMS TO custom")
    execute("GRANT DATABASE MANAGEMENT ON DBMS TO custom")

    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      adminAction("create_database").role("custom").map,
      adminAction("drop_database").role("custom").map,
      adminAction("database_management").role("custom").map
    ))

    // When
    // Now revoke each sub-privilege in turn
    Seq(
      "CREATE DATABASE",
      "DROP DATABASE"
    ).foreach(privilege => execute(s"REVOKE $privilege ON DBMS FROM custom"))

    // Then
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      adminAction("database_management").role("custom").map
    ))
  }

  test("should do nothing when revoking role management privilege from non-existing role") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE role")
    execute("GRANT ROLE MANAGEMENT ON DBMS TO role")

    // WHEN
    execute("REVOKE ROLE MANAGEMENT ON DBMS FROM wrongRole")
  }

  test("should do nothing when revoking user management privilege from non-existing role") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE role")
    execute("DENY USER MANAGEMENT ON DBMS TO role")

    // WHEN
    execute("REVOKE USER MANAGEMENT ON DBMS FROM wrongRole")
  }

  test("should do nothing when revoking database management privilege from non-existing role") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE role")
    execute("DENY DATABASE MANAGEMENT ON DBMS TO role")

    // WHEN
    execute("REVOKE DATABASE MANAGEMENT ON DBMS FROM wrongRole")
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
      "SHOW TRANSACTION (*) ON DATABASES *",
      "TERMINATE TRANSACTION (*) ON DATABASES *",
      "TRANSACTION MANAGEMENT ON DATABASES *",
      "START ON DATABASES *",
      "STOP ON DATABASES *"
    ).foreach(queryPart => execute(s"REVOKE $queryPart FROM custom"))

    // Then
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(grantAdmin().role("custom").map))
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

    // CREATE DATABASE

  test("should enforce create database privilege") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    execute("CREATE USER foo SET PASSWORD 'bar' CHANGE NOT REQUIRED")
    execute("GRANT ROLE custom TO foo")

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
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom AS COPY OF admin")
    execute("CREATE USER foo SET PASSWORD 'bar' CHANGE NOT REQUIRED")
    execute("GRANT ROLE custom TO foo")

    // WHEN
    execute("DENY CREATE DATABASE ON DBMS TO custom")

    // THEN
    the[AuthorizationViolationException] thrownBy {
      executeOnSystem("foo", "bar", "CREATE DATABASE baz")
    } should have message "Permission denied."

    execute("SHOW DATABASE baz").toSet should be(Set.empty)
  }

  // DROP DATABASE

  test("should enforce drop database privilege") {
    // GIVEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    execute("CREATE USER foo SET PASSWORD 'bar' CHANGE NOT REQUIRED")
    execute("GRANT ROLE custom TO foo")

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
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom AS COPY OF admin")
    execute("CREATE USER foo SET PASSWORD 'bar' CHANGE NOT REQUIRED")
    execute("GRANT ROLE custom TO foo")

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
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    execute("CREATE USER foo SET PASSWORD 'bar' CHANGE NOT REQUIRED")
    execute("GRANT ROLE custom TO foo")

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
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom AS COPY OF admin")
    execute("CREATE USER foo SET PASSWORD 'bar' CHANGE NOT REQUIRED")
    execute("GRANT ROLE custom TO foo")

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
    execute(s"SHOW ROLE $name PRIVILEGES").toSet should be(Set(grantAdmin().role(name).map))
  }
}
