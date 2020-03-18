/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.internal.cypher.acceptance

import org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME
import org.neo4j.dbms.api.DatabaseNotFoundException
import org.neo4j.exceptions.DatabaseAdministrationException
import org.neo4j.graphdb.security.AuthorizationViolationException
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException

class PrivilegeAdministrationCommandAcceptanceTest extends AdministrationCommandAcceptanceTestBase {

  test("should return empty counts to the outside for commands that update the system graph internally") {
    //TODO: ADD ANY NEW UPDATING COMMANDS HERE

    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    execute("CREATE DATABASE foo")
    Seq("a", "b", "c").foreach(role => execute(s"CREATE ROLE $role"))

    // Notice: They are executed in succession so they have to make sense in that order
    assertQueriesAndSubQueryCounts(List(
      "GRANT TRAVERSE ON GRAPH * NODES * TO custom" -> 1,
      "REVOKE GRANT TRAVERSE ON GRAPH * NODES * FROM custom" -> 1,
      "DENY TRAVERSE ON GRAPH * NODES * TO custom" -> 1,
      "REVOKE DENY TRAVERSE ON GRAPH * NODES * FROM custom" -> 1,

      "GRANT TRAVERSE ON GRAPH * NODES * TO custom" -> 1,
      "DENY TRAVERSE ON GRAPH * NODES * TO custom" -> 1,
      "REVOKE TRAVERSE ON GRAPH * NODES * FROM custom" -> 2,

      "GRANT TRAVERSE ON GRAPH * NODES * TO custom" -> 1,
      "REVOKE TRAVERSE ON GRAPH * NODES * FROM custom" -> 1,

      "GRANT READ {prop} ON GRAPH * NODES * TO custom" -> 1,
      "REVOKE GRANT READ {prop} ON GRAPH * NODES * FROM custom" -> 1,
      "DENY READ {prop} ON GRAPH * NODES * TO custom" -> 1,
      "REVOKE DENY READ {prop} ON GRAPH * NODES * FROM custom" -> 1,

      "GRANT READ {prop} ON GRAPH * NODES * TO custom" -> 1,
      "DENY READ {prop} ON GRAPH * NODES * TO custom" -> 1,
      "REVOKE READ {prop} ON GRAPH * NODES * FROM custom" -> 2,

      "DENY READ {prop} ON GRAPH * NODES * TO custom" -> 1,
      "REVOKE READ {prop} ON GRAPH * NODES * FROM custom" -> 1,

      "GRANT MATCH {prop} ON GRAPH * NODES * TO custom" -> 1,
      "REVOKE GRANT MATCH {prop} ON GRAPH * NODES * FROM custom" -> 1,
      "DENY MATCH {prop} ON GRAPH * NODES * TO custom" -> 1,
      "REVOKE DENY MATCH {prop} ON GRAPH * NODES * FROM custom" -> 1,
      "DENY MATCH {*} ON GRAPH * NODES * TO custom" -> 1,
      "GRANT MATCH {*} ON GRAPH * NODES * TO custom" -> 1,
      "REVOKE MATCH {*} ON GRAPH * NODES * FROM custom" -> 2,

      "GRANT READ {a,b,c} ON GRAPH foo ELEMENTS p, q TO a, b, c" -> 36  // 3 props * 3 labels * 2 labels/types * 2 elements(nodes,rels)
    ))
  }

  // Tests for showing privileges

  test("should not show privileges as non admin") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE USER foo SET PASSWORD 'bar' CHANGE NOT REQUIRED")
    execute("SHOW USERS").toSet shouldBe Set(neo4jUser, user("foo", passwordChangeRequired = false))

    // WHEN & THEN
    the[AuthorizationViolationException] thrownBy {
      executeOnSystem("foo", "bar", "SHOW PRIVILEGES")
    } should have message "Permission denied."
  }

  test("should show privileges for users") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)

    // WHEN
    execute("SHOW PRIVILEGES").toSet should be(defaultRolePrivileges)
  }

  test("should show all privileges") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)

    // WHEN
    execute("SHOW ALL PRIVILEGES").toSet should be(defaultRolePrivileges)
  }

  test("should not show privileges on a dropped database") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    execute("CREATE DATABASE foo")
    execute("GRANT ACCESS ON DATABASE foo TO custom")
    execute("GRANT TRAVERSE ON GRAPH foo NODES * TO custom")
    val grantOnFoo = Set(
      access().role("custom").database("foo").map,
      traverse().node("*").role("custom").database("foo").map
    )
    execute("SHOW PRIVILEGES").toSet should be(defaultRolePrivileges ++ grantOnFoo)

    // WHEN
    execute("DROP DATABASE foo")

    // THEN
    execute("SHOW PRIVILEGES").toSet should be(defaultRolePrivileges)
  }

  test("should not show privileges on a dropped role") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    execute("GRANT ACCESS ON DATABASE * TO custom")
    execute("GRANT TRAVERSE ON GRAPH * NODES * TO custom")
    val grantForCustom = Set(
      access().role("custom").map,
      traverse().node("*").role("custom").map
    )
    execute("SHOW PRIVILEGES").toSet should be(defaultRolePrivileges ++ grantForCustom)

    // WHEN
    execute("DROP ROLE custom")

    // THEN
    execute("SHOW PRIVILEGES").toSet should be(defaultRolePrivileges)
  }

  test("should fail when showing privileges for all users when not on system database") {
    the[DatabaseAdministrationException] thrownBy {
      // WHEN
      execute("SHOW ALL PRIVILEGES")
      // THEN
    } should have message
      "This is an administration command and it should be executed against the system database: SHOW PRIVILEGE"
  }

  test("should show privileges for specific role") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)

    // WHEN
    val result = execute("SHOW ROLE editor PRIVILEGES")

    // THEN
    val expected = Set(
      access().role("editor").map,
      matchPrivilege().role("editor").node("*").map,
      matchPrivilege().role("editor").relationship("*").map,
      write().role("editor").node("*").map,
      write().role("editor").relationship("*").map,
    )

    result.toSet should be(expected)
  }

  test("should not show role privileges as non admin") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    setupUserWithCustomRole()

    // WHEN & THEN
    the[AuthorizationViolationException] thrownBy {
      executeOnSystem("joe", "soap", "SHOW ROLE custom PRIVILEGES")
    } should have message "Permission denied."
  }

  test("should give nothing when showing privileges for non-existing role") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)

    // WHEN
    val resultFoo = execute("SHOW ROLE foo PRIVILEGES")

    // THEN
    resultFoo.toSet should be(Set.empty)

    // and an invalid (non-existing) one
    // WHEN
    val resultEmpty = execute("SHOW ROLE `` PRIVILEGES")

    // THEN
    resultEmpty.toSet should be(Set.empty)
  }

  test("should not show role privileges on a dropped database") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    execute("CREATE DATABASE foo")
    execute("GRANT TRAVERSE ON GRAPH foo NODES * TO custom")
    val grantOnFoo = Set(traverse().node("*").role("custom").database("foo").map)
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(grantOnFoo)

    // WHEN
    execute("DROP DATABASE foo")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set.empty)
  }

  test("should not show role privileges on a dropped role") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    execute("GRANT TRAVERSE ON GRAPH * NODES * TO custom")
    val grantForCustom = Set(traverse().node("*").role("custom").map)
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(grantForCustom)

    // WHEN
    execute("DROP ROLE custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set.empty)
  }

  test("should fail when showing privileges for roles when not on system database") {
    the[DatabaseAdministrationException] thrownBy {
      // WHEN
      execute("SHOW ROLE editor PRIVILEGES")
      // THEN
    } should have message
      "This is an administration command and it should be executed against the system database: SHOW PRIVILEGE"
  }

  test("should show privileges for specific user") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)

    // WHEN
    val result = execute("SHOW USER neo4j PRIVILEGES")

    // THEN
    val expected = Set(
      access().database(DEFAULT).role("PUBLIC").user("neo4j").map,
      access().role("admin").user("neo4j").map,
      matchPrivilege().role("admin").user("neo4j").node("*").map,
      matchPrivilege().role("admin").user("neo4j").relationship("*").map,
      write().role("admin").user("neo4j").node("*").map,
      write().role("admin").user("neo4j").relationship("*").map,
      nameManagement().role("admin").user("neo4j").map,
      indexManagement().role("admin").user("neo4j").map,
      constraintManagement().role("admin").user("neo4j").map,
      grantAdmin().role("admin").user("neo4j").map,
    )

    result.toSet should be(expected)
  }

  test("should show user privileges for current user as non admin") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE USER joe SET PASSWORD 'soap' CHANGE NOT REQUIRED")

    // WHEN
    executeOnSystem("joe", "soap", "SHOW USER joe PRIVILEGES", resultHandler = (row, _) => {
      // THEN
      val res = Map(
        "access" -> row.get("access"),
        "action" -> row.get("action"),
        "resource" -> row.get("resource"),
        "graph" -> row.get("graph"),
        "segment" -> row.get("segment"),
        "role" -> row.get("role"),
        "user" -> row.get("user")
      )
      res should be(access().database(DEFAULT).role("PUBLIC").user("joe").map)
    }) should be(1)
  }

  test("should show user privileges for current user as non admin without specifying the user name") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE USER joe SET PASSWORD 'soap' CHANGE NOT REQUIRED")

    // WHEN
    executeOnSystem("joe", "soap", "SHOW USER PRIVILEGES", resultHandler = (row, _) => {
      // THEN
      val res = Map(
        "access" -> row.get("access"),
        "action" -> row.get("action"),
        "resource" -> row.get("resource"),
        "graph" -> row.get("graph"),
        "segment" -> row.get("segment"),
        "role" -> row.get("role"),
        "user" -> row.get("user")
      )
      res should be(access().database(DEFAULT).role("PUBLIC").user("joe").map)
    }) should be(1)
  }

  test("should give nothing when showing privileges for non-existing user") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)

    // WHEN
    val resultFoo = execute("SHOW USER foo PRIVILEGES")

    // THEN
    resultFoo.toSet should be(Set.empty)

    // and an invalid (non-existing) one
    // WHEN
    val resultEmpty = execute("SHOW USER `` PRIVILEGES")

    // THEN
    resultEmpty.toSet should be(Set.empty)
  }

  test("should give nothing when showing privileges for a dropped user") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    execute("CREATE USER bar SET PASSWORD 'secret'")
    execute("GRANT ROLE custom TO bar")
    execute("GRANT TRAVERSE ON GRAPH * NODES * TO custom")
    val grantForCustom = Set(traverse().node("*").role("custom").user("bar").map)
    execute("SHOW USER bar PRIVILEGES").toSet should be(grantForCustom + access().role("PUBLIC").database(DEFAULT).user("bar").map)

    // WHEN
    execute("DROP USER bar")

    // THEN
    execute("SHOW USER bar PRIVILEGES").toSet should be(Set.empty)
  }

  test("should not show user privileges on a dropped database") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    execute("CREATE USER bar SET PASSWORD 'secret'")
    execute("GRANT ROLE custom TO bar")
    execute("CREATE DATABASE foo")
    execute("GRANT TRAVERSE ON GRAPH foo NODES * TO custom")
    val grantOnFoo = Set(traverse().node("*").role("custom").user("bar").database("foo").map)
    execute("SHOW USER bar PRIVILEGES").toSet should be(grantOnFoo + access().role("PUBLIC").database(DEFAULT).user("bar").map)

    // WHEN
    execute("DROP DATABASE foo")

    // THEN
    execute("SHOW USER bar PRIVILEGES").toSet should be(Set(access().role("PUBLIC").database(DEFAULT).user("bar").map))
  }

  test("should not show user privileges on a dropped role") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    execute("CREATE USER bar SET PASSWORD 'secret'")
    execute("GRANT ROLE custom TO bar")
    execute("GRANT TRAVERSE ON GRAPH * NODES * TO custom")
    val grantForCustom = Set(traverse().node("*").role("custom").user("bar").map)
    execute("SHOW USER bar PRIVILEGES").toSet should be(grantForCustom + access().role("PUBLIC").database(DEFAULT).user("bar").map)

    // WHEN
    execute("DROP ROLE custom")

    // THEN
    execute("SHOW USER bar PRIVILEGES").toSet should be(Set(access().role("PUBLIC").database(DEFAULT).user("bar").map))
  }

  test("should fail when showing privileges for users when not on system database") {
    the[DatabaseAdministrationException] thrownBy {
      // WHEN
      execute("SHOW USER neo4j PRIVILEGES")
      // THEN
    } should have message
      "This is an administration command and it should be executed against the system database: SHOW PRIVILEGE"
  }

  // Tests for granting and denying privileges

  Seq(
    ("grant", "GRANTED"),
    ("deny", "DENIED"),
  ).foreach {
    case (grantOrDeny, grantOrDenyRelType) =>
      val grantOrDenyCommand = grantOrDeny.toUpperCase

      Seq(
        ("traversal", "TRAVERSE", traverse(grantOrDenyRelType)),
        ("read", "READ {*}", read(grantOrDenyRelType)),
        ("match", "MATCH {*}", matchPrivilege(grantOrDenyRelType))
      ).foreach {
        case (actionName, actionCommand, startExpected) =>

          test(s"should $grantOrDeny $actionName privilege to custom role for all databases and all element types") {
            // GIVEN
            selectDatabase(SYSTEM_DATABASE_NAME)
            execute("CREATE ROLE custom")

            // WHEN
            execute(s"$grantOrDenyCommand $actionCommand ON GRAPH * TO custom")

            // THEN
            execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
              startExpected.role("custom").node("*").map,
              startExpected.role("custom").relationship("*").map
            ))
          }

          test(s"should fail ${grantOrDeny}ing $actionName privilege for all databases and all labels to non-existing role") {
            // GIVEN
            selectDatabase(SYSTEM_DATABASE_NAME)

            // WHEN
            val error1 = the[InvalidArgumentsException] thrownBy {
              // WHEN
              execute(s"$grantOrDenyCommand $actionCommand ON GRAPH * NODES * (*) TO custom")
              // THEN
            }
            error1.getMessage should be(s"Failed to $grantOrDeny $actionName privilege to role 'custom': Role 'custom' does not exist.")

            // WHEN
            val error2 = the[InvalidArgumentsException] thrownBy {
              // WHEN
              execute(s"$grantOrDenyCommand $actionCommand ON GRAPH * RELATIONSHIPS * (*) TO custom")
              // THEN
            }
            error2.getMessage should be(s"Failed to $grantOrDeny $actionName privilege to role 'custom': Role 'custom' does not exist.")

            // WHEN
            val error3 = the[InvalidArgumentsException] thrownBy {
              // WHEN
              execute(s"$grantOrDenyCommand $actionCommand ON GRAPH * TO custom")
              // THEN
            }
            error3.getMessage should be(s"Failed to $grantOrDeny $actionName privilege to role 'custom': Role 'custom' does not exist.")

            // THEN
            execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set())
          }

          test(s"should fail when ${grantOrDeny}ing $actionName privilege with missing database") {
            // GIVEN
            selectDatabase(SYSTEM_DATABASE_NAME)
            execute("CREATE ROLE custom")
            val error = the[DatabaseNotFoundException] thrownBy {
              execute(s"$grantOrDenyCommand $actionCommand ON GRAPH foo TO custom")
            }
            error.getMessage should be(s"Failed to $grantOrDeny $actionName privilege to role 'custom': Database 'foo' does not exist.")
          }

          test(s"should fail when ${grantOrDeny}ing $actionName privilege to custom role when not on system database") {
            val commandName = actionCommand.split(" ").head
            the[DatabaseAdministrationException] thrownBy {
              // WHEN
              execute(s"$grantOrDenyCommand $actionCommand ON GRAPH * TO custom")
              // THEN
            } should have message
              s"This is an administration command and it should be executed against the system database: $grantOrDenyCommand $commandName"
          }

          Seq(
            ("label", "NODES", addNode: builderType),
            ("relationship type", "RELATIONSHIPS", addRel: builderType)
          ).foreach {
            case (segmentName, segmentCommand, segmentFunction: builderType) =>

              test(s"should $grantOrDeny $actionName privilege to custom role for all databases and all ${segmentName}s") {
                // GIVEN
                selectDatabase(SYSTEM_DATABASE_NAME)
                execute("CREATE ROLE custom")

                // WHEN
                execute(s"$grantOrDenyCommand $actionCommand ON GRAPH * $segmentCommand * (*) TO custom")

                // THEN
                execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(segmentFunction(startExpected.role("custom"), "*").map))
              }

              test(s"should $grantOrDeny $actionName privilege to custom role for all databases but only a specific $segmentName (that does not need to exist)") {
                // GIVEN
                selectDatabase(SYSTEM_DATABASE_NAME)
                execute("CREATE ROLE custom")

                // WHEN
                execute(s"$grantOrDenyCommand $actionCommand ON GRAPH * $segmentCommand A (*) TO custom")

                // THEN
                execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(segmentFunction(startExpected.role("custom"), "A").map))
              }

              test(s"should $grantOrDeny $actionName privilege to custom role for a specific database and a specific $segmentName") {
                // GIVEN
                selectDatabase(SYSTEM_DATABASE_NAME)
                execute("CREATE ROLE custom")
                execute("CREATE DATABASE foo")

                // WHEN
                execute(s"$grantOrDenyCommand $actionCommand ON GRAPH foo $segmentCommand A (*) TO custom")

                // THEN
                execute("SHOW ROLE custom PRIVILEGES").toSet should
                  be(Set(segmentFunction(startExpected.role("custom").database("foo"), "A").map))
              }

              test(s"should $grantOrDeny $actionName privilege to custom role for a specific database and all ${segmentName}s") {
                // GIVEN
                selectDatabase(SYSTEM_DATABASE_NAME)
                execute("CREATE ROLE custom")
                execute("CREATE DATABASE foo")

                // WHEN
                execute(s"$grantOrDenyCommand $actionCommand ON GRAPH foo $segmentCommand * (*) TO custom")

                // THEN
                execute("SHOW ROLE custom PRIVILEGES").toSet should
                  be(Set(segmentFunction(startExpected.role("custom").database("foo"), "*").map))
              }

              test(s"should $grantOrDeny $actionName privilege to custom role for a specific database and multiple ${segmentName}s") {
                // GIVEN
                selectDatabase(SYSTEM_DATABASE_NAME)
                execute("CREATE ROLE custom")
                execute("CREATE DATABASE foo")

                // WHEN
                execute(s"$grantOrDenyCommand $actionCommand ON GRAPH foo $segmentCommand A (*) TO custom")
                execute(s"$grantOrDenyCommand $actionCommand ON GRAPH foo $segmentCommand B (*) TO custom")

                // THEN
                execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
                  segmentFunction(startExpected.role("custom").database("foo"), "A").map,
                  segmentFunction(startExpected.role("custom").database("foo"), "B").map
                ))
              }

              test(s"should $grantOrDeny $actionName privilege to custom role for a specific database and multiple ${segmentName}s in one grant") {
                // GIVEN
                selectDatabase(SYSTEM_DATABASE_NAME)
                execute("CREATE ROLE custom")
                execute("CREATE DATABASE foo")

                // WHEN
                execute(s"$grantOrDenyCommand $actionCommand ON GRAPH foo $segmentCommand A, B (*) TO custom")

                // THEN
                execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
                  segmentFunction(startExpected.role("custom").database("foo"), "A").map,
                  segmentFunction(startExpected.role("custom").database("foo"), "B").map
                ))
              }

              test(s"should $grantOrDeny $actionName privilege to multiple roles for a specific database and multiple ${segmentName}s in one grant") {
                // GIVEN
                selectDatabase(SYSTEM_DATABASE_NAME)
                execute("CREATE ROLE role1")
                execute("CREATE ROLE role2")
                execute("CREATE DATABASE foo")

                // WHEN
                execute(s"$grantOrDenyCommand $actionCommand ON GRAPH foo $segmentCommand A, B (*) TO role1, role2")

                // THEN
                execute("SHOW ROLE role1 PRIVILEGES").toSet should be(Set(
                  segmentFunction(startExpected.role("role1").database("foo"), "A").map,
                  segmentFunction(startExpected.role("role1").database("foo"), "B").map
                ))
                execute("SHOW ROLE role2 PRIVILEGES").toSet should be(Set(
                  segmentFunction(startExpected.role("role2").database("foo"), "A").map,
                  segmentFunction(startExpected.role("role2").database("foo"), "B").map
                ))
              }

          }

          test(s"should $grantOrDeny $actionName privilege to custom role for all databases and all elements") {
            // GIVEN
            selectDatabase(SYSTEM_DATABASE_NAME)
            execute("CREATE ROLE custom")

            // WHEN
            execute(s"$grantOrDenyCommand $actionCommand ON GRAPH * ELEMENTS * (*) TO custom")

            // THEN
            execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
              startExpected.role("custom").node("*").map,
              startExpected.role("custom").relationship("*").map
            ))
          }

          test(s"should $grantOrDeny $actionName privilege to custom role for all databases but only a specific element (that does not need to exist)") {
            // GIVEN
            selectDatabase(SYSTEM_DATABASE_NAME)
            execute("CREATE ROLE custom")

            // WHEN
            execute(s"$grantOrDenyCommand $actionCommand ON GRAPH * ELEMENTS A (*) TO custom")

            // THEN
            execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
              startExpected.role("custom").node("A").map,
              startExpected.role("custom").relationship("A").map
            ))
          }

          test(s"should $grantOrDeny $actionName privilege to custom role for a specific database and a specific element") {
            // GIVEN
            selectDatabase(SYSTEM_DATABASE_NAME)
            execute("CREATE ROLE custom")
            execute("CREATE DATABASE foo")

            // WHEN
            execute(s"$grantOrDenyCommand $actionCommand ON GRAPH foo ELEMENTS A (*) TO custom")

            // THEN
            execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
              startExpected.role("custom").database("foo").node("A").map,
              startExpected.role("custom").database("foo").relationship("A").map
            ))
          }

          test(s"should $grantOrDeny $actionName privilege to custom role for a specific database and all elements") {
            // GIVEN
            selectDatabase(SYSTEM_DATABASE_NAME)
            execute("CREATE ROLE custom")
            execute("CREATE DATABASE foo")

            // WHEN
            execute(s"$grantOrDenyCommand $actionCommand ON GRAPH foo ELEMENTS * (*) TO custom")

            // THEN
            execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
              startExpected.role("custom").database("foo").node("*").map,
              startExpected.role("custom").database("foo").relationship("*").map
            ))
          }

          test(s"should $grantOrDeny $actionName privilege to custom role for a specific database and multiple elements") {
            // GIVEN
            selectDatabase(SYSTEM_DATABASE_NAME)
            execute("CREATE ROLE custom")
            execute("CREATE DATABASE foo")

            // WHEN
            execute(s"$grantOrDenyCommand $actionCommand ON GRAPH foo ELEMENTS A (*) TO custom")
            execute(s"$grantOrDenyCommand $actionCommand ON GRAPH foo ELEMENTS B (*) TO custom")

            // THEN
            execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
              startExpected.role("custom").database("foo").node("A").map,
              startExpected.role("custom").database("foo").relationship("A").map,
              startExpected.role("custom").database("foo").node("B").map,
              startExpected.role("custom").database("foo").relationship("B").map
            ))
          }

          test(s"should $grantOrDeny $actionName privilege to custom role for a specific database and multiple elements in one grant") {
            // GIVEN
            selectDatabase(SYSTEM_DATABASE_NAME)
            execute("CREATE ROLE custom")
            execute("CREATE DATABASE foo")

            // WHEN
            execute(s"$grantOrDenyCommand $actionCommand ON GRAPH foo ELEMENTS A, B (*) TO custom")

            // THEN
            execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
              startExpected.role("custom").database("foo").node("A").map,
              startExpected.role("custom").database("foo").relationship("A").map,
              startExpected.role("custom").database("foo").node("B").map,
              startExpected.role("custom").database("foo").relationship("B").map
            ))
          }

          test(s"should $grantOrDeny $actionName privilege to multiple roles for a specific database and multiple elements in one grant") {
            // GIVEN
            selectDatabase(SYSTEM_DATABASE_NAME)
            execute("CREATE ROLE role1")
            execute("CREATE ROLE role2")
            execute("CREATE DATABASE foo")

            // WHEN
            execute(s"$grantOrDenyCommand $actionCommand ON GRAPH foo ELEMENTS A, B (*) TO role1, role2")

            // THEN
            execute("SHOW ROLE role1 PRIVILEGES").toSet should be(Set(
              startExpected.role("role1").database("foo").node("A").map,
              startExpected.role("role1").database("foo").relationship("A").map,
              startExpected.role("role1").database("foo").node("B").map,
              startExpected.role("role1").database("foo").relationship("B").map
            ))
            execute("SHOW ROLE role2 PRIVILEGES").toSet should be(Set(
              startExpected.role("role2").database("foo").node("A").map,
              startExpected.role("role2").database("foo").relationship("A").map,
              startExpected.role("role2").database("foo").node("B").map,
              startExpected.role("role2").database("foo").relationship("B").map
            ))
          }
      }
  }

  test("should not grant anything as non admin") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE USER foo SET PASSWORD 'bar' CHANGE NOT REQUIRED")
    execute("SHOW USERS").toSet shouldBe Set(neo4jUser, user("foo", passwordChangeRequired = false))

    // WHEN & THEN
    the[AuthorizationViolationException] thrownBy {
      executeOnSystem("foo", "bar", "GRANT MATCH {bar} ON GRAPH * TO custom")
    } should have message "Permission denied."
  }

  test("should not deny anything as non admin") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE USER foo SET PASSWORD 'bar' CHANGE NOT REQUIRED")
    execute("SHOW USERS").toSet shouldBe Set(neo4jUser, user("foo", passwordChangeRequired = false))

    // WHEN & THEN
    the[AuthorizationViolationException] thrownBy {
      executeOnSystem("foo", "bar", "DENY MATCH {bar} ON GRAPH * TO custom")
    } should have message "Permission denied."
  }

  // Tests for granting and denying privileges on properties
  Seq(
    ("grant", "GRANTED"),
    ("deny", "DENIED"),
  ).foreach {
    case (grantOrDeny, grantOrDenyRelType) =>
      val grantOrDenyCommand = grantOrDeny.toUpperCase

      Seq(
        ("read", "READ", read(grantOrDenyRelType)),
        ("match", "MATCH", matchPrivilege(grantOrDenyRelType))
      ).foreach {
        case (actionName, actionCommand, startExpected) =>

          test(s"should $grantOrDeny $actionName privilege for specific property to custom role for all databases and all element types") {
            // GIVEN
            selectDatabase(SYSTEM_DATABASE_NAME)
            execute("CREATE ROLE custom")

            // WHEN
            execute(s"$grantOrDenyCommand $actionCommand {bar} ON GRAPH * TO custom")

            // THEN
            execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
              startExpected.role("custom").property("bar").node("*").map,
              startExpected.role("custom").property("bar").relationship("*").map
            ))
          }

          Seq(
            ("label", "NODES", addNode: builderType),
            ("relationship type", "RELATIONSHIPS", addRel: builderType)
          ).foreach {
            case (segmentName, segmentCommand, segmentFunction: builderType) =>

              test(s"should $grantOrDeny $actionName privilege for specific property to custom role for all databases and all ${segmentName}s") {
                // GIVEN
                selectDatabase(SYSTEM_DATABASE_NAME)
                execute("CREATE ROLE custom")

                // WHEN
                execute(s"$grantOrDenyCommand $actionCommand {bar} ON GRAPH * $segmentCommand * (*) TO custom")

                // THEN
                execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
                  segmentFunction(startExpected.role("custom").property("bar"), "*").map
                ))
              }

              test(s"should $grantOrDeny $actionName privilege for specific property to custom role for all databases but only a specific $segmentName") {
                // GIVEN
                selectDatabase(SYSTEM_DATABASE_NAME)
                execute("CREATE ROLE custom")

                // WHEN
                execute(s"$grantOrDenyCommand $actionCommand {bar} ON GRAPH * $segmentCommand A (*) TO custom")

                // THEN
                execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
                  segmentFunction(startExpected.role("custom").property("bar"), "A").map
                ))
              }

              test(s"should $grantOrDeny $actionName privilege for specific property to custom role for a specific database and a specific $segmentName") {
                // GIVEN
                selectDatabase(SYSTEM_DATABASE_NAME)
                execute("CREATE ROLE custom")
                execute("CREATE DATABASE foo")

                // WHEN
                execute(s"$grantOrDenyCommand $actionCommand {bar} ON GRAPH foo $segmentCommand A (*) TO custom")

                // THEN
                execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
                  segmentFunction(startExpected.database("foo").role("custom").property("bar"), "A").map
                ))
              }

              test(s"should $grantOrDeny $actionName privilege for specific property to custom role for a specific database and all ${segmentName}s") {
                // GIVEN
                selectDatabase(SYSTEM_DATABASE_NAME)
                execute("CREATE ROLE custom")
                execute("CREATE DATABASE foo")

                // WHEN
                execute(s"$grantOrDenyCommand $actionCommand {bar} ON GRAPH foo $segmentCommand * (*) TO custom")

                // THEN
                execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
                  segmentFunction(startExpected.database("foo").role("custom").property("bar"), "*").map
                ))
              }

              test(s"should $grantOrDeny $actionName privilege for specific property to custom role for a specific database and multiple ${segmentName}s") {
                // GIVEN
                selectDatabase(SYSTEM_DATABASE_NAME)
                execute("CREATE ROLE custom")
                execute("CREATE DATABASE foo")

                // WHEN
                execute(s"$grantOrDenyCommand $actionCommand {bar} ON GRAPH foo $segmentCommand A (*) TO custom")
                execute(s"$grantOrDenyCommand $actionCommand {bar} ON GRAPH foo $segmentCommand B (*) TO custom")

                // THEN
                execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
                  segmentFunction(startExpected.database("foo").role("custom").property("bar"), "A").map,
                  segmentFunction(startExpected.database("foo").role("custom").property("bar"), "B").map
                ))
              }

              test(s"should $grantOrDeny $actionName privilege for multiple properties to custom role for a specific database and specific $segmentName") {
                // GIVEN
                selectDatabase(SYSTEM_DATABASE_NAME)
                execute("CREATE ROLE custom")
                execute("CREATE DATABASE foo")

                // WHEN
                execute(s"$grantOrDenyCommand $actionCommand {bar} ON GRAPH foo $segmentCommand A (*) TO custom")
                execute(s"$grantOrDenyCommand $actionCommand {baz} ON GRAPH foo $segmentCommand A (*) TO custom")

                // THEN
                execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
                  segmentFunction(startExpected.database("foo").role("custom").property("bar"), "A").map,
                  segmentFunction(startExpected.database("foo").role("custom").property("baz"), "A").map
                ))
              }

              test(s"should $grantOrDeny $actionName privilege for multiple properties to custom role for a specific database and multiple ${segmentName}s") {
                // GIVEN
                selectDatabase(SYSTEM_DATABASE_NAME)
                execute("CREATE ROLE custom")
                execute("CREATE DATABASE foo")

                // WHEN
                execute(s"$grantOrDenyCommand $actionCommand {bar} ON GRAPH foo $segmentCommand A (*) TO custom")
                execute(s"$grantOrDenyCommand $actionCommand {baz} ON GRAPH foo $segmentCommand B (*) TO custom")

                // THEN
                execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
                  segmentFunction(startExpected.database("foo").role("custom").property("bar"), "A").map,
                  segmentFunction(startExpected.database("foo").role("custom").property("baz"), "B").map
                ))
              }

              test(s"should $grantOrDeny $actionName privilege for multiple properties to multiple roles for a specific database and multiple ${segmentName}s in a single grant") {
                // GIVEN
                selectDatabase(SYSTEM_DATABASE_NAME)
                execute("CREATE ROLE role1")
                execute("CREATE ROLE role2")
                execute("CREATE ROLE role3")
                execute("CREATE DATABASE foo")

                // WHEN
                execute(s"$grantOrDenyCommand $actionCommand {a, b, c} ON GRAPH foo $segmentCommand A, B, C (*) TO role1, role2, role3")

                // THEN
                val expected = (for (l <- Seq("A", "B", "C")) yield {
                  for (p <- Seq("a", "b", "c")) yield {
                    segmentFunction(startExpected.database("foo").property(p), l)
                  }
                }).flatten

                execute("SHOW ROLE role1 PRIVILEGES").toSet should be(expected.map(_.role("role1").map).toSet)
                execute("SHOW ROLE role2 PRIVILEGES").toSet should be(expected.map(_.role("role2").map).toSet)
                execute("SHOW ROLE role3 PRIVILEGES").toSet should be(expected.map(_.role("role3").map).toSet)
              }

          }

          test(s"should $grantOrDeny $actionName privilege for specific property to custom role for all databases and all elements") {
            // GIVEN
            selectDatabase(SYSTEM_DATABASE_NAME)
            execute("CREATE ROLE custom")

            // WHEN
            execute(s"$grantOrDenyCommand $actionCommand {bar} ON GRAPH * ELEMENTS * (*) TO custom")

            // THEN
            execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
              startExpected.role("custom").property("bar").node("*").map,
              startExpected.role("custom").property("bar").relationship("*").map
            ))
          }

          test(s"should $grantOrDeny $actionName privilege for specific property to custom role for all databases but only a specific element") {
            // GIVEN
            selectDatabase(SYSTEM_DATABASE_NAME)
            execute("CREATE ROLE custom")

            // WHEN
            execute(s"$grantOrDenyCommand $actionCommand {bar} ON GRAPH * ELEMENTS A (*) TO custom")

            // THEN
            execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
              startExpected.role("custom").property("bar").node("A").map,
              startExpected.role("custom").property("bar").relationship("A").map
            ))
          }

          test(s"should $grantOrDeny $actionName privilege for specific property to custom role for a specific database and a specific element") {
            // GIVEN
            selectDatabase(SYSTEM_DATABASE_NAME)
            execute("CREATE ROLE custom")
            execute("CREATE DATABASE foo")

            // WHEN
            execute(s"$grantOrDenyCommand $actionCommand {bar} ON GRAPH foo ELEMENTS A (*) TO custom")

            // THEN
            execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
              startExpected.role("custom").database("foo").property("bar").node("A").map,
              startExpected.role("custom").database("foo").property("bar").relationship("A").map
            ))
          }

          test(s"should $grantOrDeny $actionName privilege for specific property to custom role for a specific database and all elements") {
            // GIVEN
            selectDatabase(SYSTEM_DATABASE_NAME)
            execute("CREATE ROLE custom")
            execute("CREATE DATABASE foo")

            // WHEN
            execute(s"$grantOrDenyCommand $actionCommand {bar} ON GRAPH foo ELEMENTS * (*) TO custom")

            // THEN
            execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
              startExpected.role("custom").database("foo").property("bar").node("*").map,
              startExpected.role("custom").database("foo").property("bar").relationship("*").map
            ))
          }

          test(s"should $grantOrDeny $actionName privilege for specific property to custom role for a specific database and multiple elements") {
            // GIVEN
            selectDatabase(SYSTEM_DATABASE_NAME)
            execute("CREATE ROLE custom")
            execute("CREATE DATABASE foo")

            // WHEN
            execute(s"$grantOrDenyCommand $actionCommand {bar} ON GRAPH foo ELEMENTS A (*) TO custom")
            execute(s"$grantOrDenyCommand $actionCommand {bar} ON GRAPH foo ELEMENTS B (*) TO custom")

            // THEN
            execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
              startExpected.role("custom").database("foo").property("bar").node("A").map,
              startExpected.role("custom").database("foo").property("bar").relationship("A").map,
              startExpected.role("custom").database("foo").property("bar").node("B").map,
              startExpected.role("custom").database("foo").property("bar").relationship("B").map
            ))
          }

          test(s"should $grantOrDeny $actionName privilege for multiple properties to custom role for a specific database and specific element") {
            // GIVEN
            selectDatabase(SYSTEM_DATABASE_NAME)
            execute("CREATE ROLE custom")
            execute("CREATE DATABASE foo")

            // WHEN
            execute(s"$grantOrDenyCommand $actionCommand {bar} ON GRAPH foo ELEMENTS A (*) TO custom")
            execute(s"$grantOrDenyCommand $actionCommand {baz} ON GRAPH foo ELEMENTS A (*) TO custom")

            // THEN
            execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
              startExpected.role("custom").database("foo").property("bar").node("A").map,
              startExpected.role("custom").database("foo").property("bar").relationship("A").map,
              startExpected.role("custom").database("foo").property("baz").node("A").map,
              startExpected.role("custom").database("foo").property("baz").relationship("A").map
            ))
          }

          test(s"should $grantOrDeny $actionName privilege for multiple properties to custom role for a specific database and multiple elements") {
            // GIVEN
            selectDatabase(SYSTEM_DATABASE_NAME)
            execute("CREATE ROLE custom")
            execute("CREATE DATABASE foo")

            // WHEN
            execute(s"$grantOrDenyCommand $actionCommand {bar} ON GRAPH foo ELEMENTS A (*) TO custom")
            execute(s"$grantOrDenyCommand $actionCommand {baz} ON GRAPH foo ELEMENTS B (*) TO custom")

            // THEN
            execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
              startExpected.role("custom").database("foo").property("bar").node("A").map,
              startExpected.role("custom").database("foo").property("bar").relationship("A").map,
              startExpected.role("custom").database("foo").property("baz").node("B").map,
              startExpected.role("custom").database("foo").property("baz").relationship("B").map
            ))
          }

          test(s"should $grantOrDeny $actionName privilege for multiple properties to multiple roles for a specific database and multiple elements in a single grant") {
            // GIVEN
            selectDatabase(SYSTEM_DATABASE_NAME)
            execute("CREATE ROLE role1")
            execute("CREATE ROLE role2")
            execute("CREATE ROLE role3")
            execute("CREATE DATABASE foo")

            // WHEN
            execute(s"$grantOrDenyCommand $actionCommand {a, b, c} ON GRAPH foo ELEMENTS A, B, C (*) TO role1, role2, role3")

            // THEN
            val expected = (for (l <- Seq("A", "B", "C")) yield {
              (for (p <- Seq("a", "b", "c")) yield {
                Seq(startExpected.database("foo").property(p).node(l),
                  startExpected.database("foo").property(p).relationship(l))
              }).flatten
            }).flatten

            execute("SHOW ROLE role1 PRIVILEGES").toSet should be(expected.map(_.role("role1").map).toSet)
            execute("SHOW ROLE role2 PRIVILEGES").toSet should be(expected.map(_.role("role2").map).toSet)
            execute("SHOW ROLE role3 PRIVILEGES").toSet should be(expected.map(_.role("role3").map).toSet)
          }
      }
  }
}
