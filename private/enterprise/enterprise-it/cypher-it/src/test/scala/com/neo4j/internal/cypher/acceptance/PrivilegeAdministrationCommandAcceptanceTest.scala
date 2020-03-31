/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.internal.cypher.acceptance

import com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.PUBLIC
import org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME
import org.neo4j.graphdb.QueryExecutionException
import org.neo4j.graphdb.security.AuthorizationViolationException
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException

import scala.collection.JavaConverters.mapAsJavaMapConverter

class PrivilegeAdministrationCommandAcceptanceTest extends AdministrationCommandAcceptanceTestBase {

  test("should return empty counts to the outside for commands that update the system graph internally") {
    //TODO: ADD ANY NEW UPDATING COMMANDS HERE

    // GIVEN
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
    execute("CREATE USER foo SET PASSWORD 'bar' CHANGE NOT REQUIRED")

    // WHEN & THEN
    the[AuthorizationViolationException] thrownBy {
      executeOnSystem("foo", "bar", "SHOW PRIVILEGES")
    } should have message "Permission denied."
  }

  test("should show privileges for users") {
    execute("SHOW PRIVILEGES").toSet should be(defaultRolePrivileges)
  }

  test("should show all privileges") {
    execute("SHOW ALL PRIVILEGES").toSet should be(defaultRolePrivileges)
  }

  test("should not show privileges on a dropped database") {
    // GIVEN
    execute("CREATE ROLE custom")
    execute("CREATE DATABASE foo")
    execute("GRANT ACCESS ON DATABASE foo TO custom")
    execute("GRANT TRAVERSE ON GRAPH foo NODES * TO custom")

    // WHEN
    execute("DROP DATABASE foo")

    // THEN
    execute("SHOW PRIVILEGES").toSet should be(defaultRolePrivileges)
  }

  test("should not show privileges on a dropped role") {
    // GIVEN
    execute("CREATE ROLE custom")
    execute("GRANT ACCESS ON DATABASE * TO custom")
    execute("GRANT TRAVERSE ON GRAPH * NODES * TO custom")

    // WHEN
    execute("DROP ROLE custom")

    // THEN
    execute("SHOW PRIVILEGES").toSet should be(defaultRolePrivileges)
  }

  test("should show privileges for specific role") {
    // WHEN
    val result = execute("SHOW ROLE editor PRIVILEGES")

    // THEN
    val expected = Set(
      granted(access).role("editor").map,
      granted(matchPrivilege).role("editor").node("*").map,
      granted(matchPrivilege).role("editor").relationship("*").map,
      granted(write).role("editor").node("*").map,
      granted(write).role("editor").relationship("*").map,
    )

    result.toSet should be(expected)
  }

  test("should show privileges for specific role as parameter") {
    // WHEN
    val result = execute("SHOW ROLE $role PRIVILEGES", Map("role" -> "editor"))

    // THEN
    val expected = Set(
      granted(access).role("editor").map,
      granted(matchPrivilege).role("editor").node("*").map,
      granted(matchPrivilege).role("editor").relationship("*").map,
      granted(write).role("editor").node("*").map,
      granted(write).role("editor").relationship("*").map,
    )

    result.toSet should be(expected)
  }

  test("should not show role privileges as non admin") {
    // GIVEN
    setupUserWithCustomRole()

    // WHEN & THEN
    the[AuthorizationViolationException] thrownBy {
      executeOnSystem("joe", "soap", "SHOW ROLE custom PRIVILEGES")
    } should have message "Permission denied."
  }

  test("should not show role privileges on a dropped database") {
    // GIVEN
    execute("CREATE ROLE custom")
    execute("CREATE DATABASE foo")
    execute("GRANT TRAVERSE ON GRAPH foo NODES * TO custom")

    // WHEN
    execute("DROP DATABASE foo")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set.empty)
  }

  test("should not show role privileges on a dropped role") {
    // GIVEN
    execute("CREATE ROLE custom")
    execute("GRANT TRAVERSE ON GRAPH * NODES * TO custom")

    // WHEN
    execute("DROP ROLE custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set.empty)
  }

  test("should show privileges for specific user") {
    // WHEN
    val result = execute("SHOW USER neo4j PRIVILEGES")

    // THEN
    result.toSet should be(defaultUserPrivileges)
  }

  test("should show privileges for specific user as parameter") {
    // WHEN
    val result = execute("SHOW USER $user PRIVILEGES", Map("user" -> "neo4j"))

    // THEN
    result.toSet should be(defaultUserPrivileges)
  }

  test("should show user privileges for current user as non admin") {
    // GIVEN
    execute("CREATE USER joe SET PASSWORD 'soap' CHANGE NOT REQUIRED")

    // WHEN
    executeOnSystem("joe", "soap", "SHOW USER joe PRIVILEGES", resultHandler = (row, _) => {
      // THEN
      asPrivilegesResult(row) should be(granted(access).database(DEFAULT).role(PUBLIC).user("joe").map)
    }) should be(1)
  }

  test("should show user privileges for current user as non admin without specifying the user name") {
    // GIVEN
    execute("CREATE USER joe SET PASSWORD 'soap' CHANGE NOT REQUIRED")

    // WHEN
    executeOnSystem("joe", "soap", "SHOW USER PRIVILEGES", resultHandler = (row, _) => {
      // THEN
      asPrivilegesResult(row) should be(granted(access).database(DEFAULT).role("PUBLIC").user("joe").map)
    }) should be(1)
  }

  test("should give nothing when showing privileges for non-existing user") {
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
    setupUserWithCustomRole("bar", "secret")
    execute("GRANT TRAVERSE ON GRAPH * NODES * TO custom")

    // WHEN
    execute("DROP USER bar")

    // THEN
    execute("SHOW USER bar PRIVILEGES").toSet should be(Set.empty)
  }

  test("should not show user privileges on a dropped database") {
    // GIVEN
    setupUserWithCustomRole("bar", "secret", access = false)
    execute("CREATE DATABASE foo")
    execute("GRANT TRAVERSE ON GRAPH foo NODES * TO custom")

    // WHEN
    execute("DROP DATABASE foo")

    // THEN
    execute("SHOW USER bar PRIVILEGES").toSet should be(Set(granted(access).role(PUBLIC).database(DEFAULT).user("bar").map))
  }

  test("should not show user privileges on a dropped role") {
    // GIVEN
    setupUserWithCustomRole("bar", "secret")
    execute("GRANT TRAVERSE ON GRAPH * NODES * TO custom")

    // WHEN
    execute("DROP ROLE custom")

    // THEN
    execute("SHOW USER bar PRIVILEGES").toSet should be(Set(granted(access).role(PUBLIC).database(DEFAULT).user("bar").map))
  }

  // Tests for granting and denying privileges

  Seq(
    ("grant", granted: privilegeFunction),
    ("deny", denied: privilegeFunction),
  ).foreach {
    case (grantOrDeny, grantedOrDenied: privilegeFunction) =>
      val grantOrDenyCommand = grantOrDeny.toUpperCase

      Seq(
        ("traversal", "TRAVERSE", grantedOrDenied(traverse)),
        ("read", "READ {*}", grantedOrDenied(read)),
        ("match", "MATCH {*}", grantedOrDenied(matchPrivilege))
      ).foreach {
        case (actionName, actionCommand, startExpected) =>

          test(s"should $grantOrDeny $actionName privilege to custom role for all databases and all element types") {
            // GIVEN
            execute("CREATE ROLE custom")

            // WHEN
            execute(s"$grantOrDenyCommand $actionCommand ON GRAPH * TO custom")

            // THEN
            execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
              startExpected.role("custom").node("*").map,
              startExpected.role("custom").relationship("*").map
            ))
          }

          test(s"should fail when ${grantOrDeny}ing $actionName privilege using * as parameter") {
            execute("CREATE ROLE custom")
            val error = the[InvalidArgumentsException] thrownBy {
              execute(s"$grantOrDenyCommand $actionCommand ON GRAPH $$db TO custom", Map("db" -> "*"))
            }
            error.getMessage should be(s"Failed to $grantOrDeny $actionName privilege to role 'custom': Parameterized database and graph names do not support wildcards.")
          }

          Seq(
            ("label", "NODES", addNode: builderType),
            ("relationship type", "RELATIONSHIPS", addRel: builderType)
          ).foreach {
            case (segmentName, segmentCommand, segmentFunction: builderType) =>

              test(s"should $grantOrDeny $actionName privilege to custom role for all databases and all ${segmentName}s") {
                // GIVEN
                execute("CREATE ROLE custom")

                // WHEN
                execute(s"$grantOrDenyCommand $actionCommand ON GRAPH * $segmentCommand * (*) TO custom")

                // THEN
                execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(segmentFunction(startExpected.role("custom"), "*").map))
              }

              test(s"should $grantOrDeny $actionName privilege to custom role for all databases but only a specific $segmentName (that does not need to exist)") {
                // GIVEN
                execute("CREATE ROLE custom")

                // WHEN
                execute(s"$grantOrDenyCommand $actionCommand ON GRAPH * $segmentCommand A (*) TO custom")

                // THEN
                execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(segmentFunction(startExpected.role("custom"), "A").map))
              }

              test(s"should $grantOrDeny $actionName privilege to custom role for a specific database and a specific $segmentName") {
                // GIVEN
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
            execute("CREATE ROLE custom")

            // WHEN
            execute(s"$grantOrDenyCommand $actionCommand ON GRAPH * ELEMENTS * (*) TO $$role", Map("role" -> "custom"))

            // THEN
            execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
              startExpected.role("custom").node("*").map,
              startExpected.role("custom").relationship("*").map
            ))
          }

          test(s"should $grantOrDeny $actionName privilege to custom role for all databases but only a specific element (that does not need to exist)") {
            // GIVEN
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

          test(s"should $grantOrDeny $actionName privilege to custom role for specific database and all element types using parameters") {
            // GIVEN
            execute("CREATE ROLE custom")

            // WHEN
            execute(s"$grantOrDenyCommand $actionCommand ON GRAPH $$db TO custom", Map("db" -> DEFAULT_DATABASE_NAME))

            // THEN
            execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
              startExpected.database(DEFAULT_DATABASE_NAME).role("custom").node("*").map,
              startExpected.database(DEFAULT_DATABASE_NAME).role("custom").relationship("*").map
            ))
          }

          test(s"should $grantOrDeny $actionName privilege to custom role for a specific database and multiple elements") {
            // GIVEN
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
    execute("CREATE USER foo SET PASSWORD 'bar' CHANGE NOT REQUIRED")

    // WHEN & THEN
    the[AuthorizationViolationException] thrownBy {
      executeOnSystem("foo", "bar", "GRANT MATCH {bar} ON GRAPH * TO custom")
    } should have message "Permission denied."
  }

  test("should not deny anything as non admin") {
    // GIVEN
    execute("CREATE USER foo SET PASSWORD 'bar' CHANGE NOT REQUIRED")

    // WHEN & THEN
    the[AuthorizationViolationException] thrownBy {
      executeOnSystem("foo", "bar", "DENY MATCH {bar} ON GRAPH * TO custom")
    } should have message "Permission denied."
  }

  test("should normalize graph name for graph privileges") {
    // GIVEN
    execute("CREATE DATABASE BaR")
    execute("CREATE ROLE custom")

    // WHEN
    execute("GRANT TRAVERSE ON GRAPH BaR NODES A TO custom")
    execute("DENY TRAVERSE ON GRAPH bar RELATIONSHIP B TO custom")
    execute("GRANT READ {prop} ON GRAPH BAR NODES A TO custom")
    execute("DENY READ {prop2} ON GRAPH bAR NODES B TO custom")
    execute("GRANT MATCH {prop3} ON GRAPH bAr NODES C TO custom")
    execute("DENY MATCH {prop4} ON GRAPH bAr NODES D TO custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      granted(traverse).role("custom").database("bar").node("A").map,
      denied(traverse).role("custom").database("bar").relationship("B").map,
      granted(read).role("custom").database("bar").property("prop").node("A").map,
      denied(read).role("custom").database("bar").property("prop2").node("B").map,
      granted(matchPrivilege).role("custom").database("bar").property("prop3").node("C").map,
      denied(matchPrivilege).role("custom").database("bar").property("prop4").node("D").map
    ))

    // WHEN
    execute("REVOKE GRANT TRAVERSE ON GRAPH bar NODES A FROM custom")
    execute("REVOKE DENY READ {prop2} ON GRAPH BAr NODES B FROM custom")
    execute("REVOKE MATCH {prop3} ON GRAPH BAr NODES C FROM custom")

    //THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      denied(traverse).role("custom").database("bar").relationship("B").map,
      granted(read).role("custom").database("bar").property("prop").node("A").map,
      denied(matchPrivilege).role("custom").database("bar").property("prop4").node("D").map
    ))
  }

  // Tests for granting and denying privileges on properties
  Seq(
    ("grant", granted: privilegeFunction),
    ("deny", denied: privilegeFunction),
  ).foreach {
    case (grantOrDeny, grantedOrDenied) =>
      val grantOrDenyCommand = grantOrDeny.toUpperCase

      Seq(
        ("read", "READ", grantedOrDenied(read)),
        ("match", "MATCH", grantedOrDenied(matchPrivilege))
      ).foreach {
        case (actionName, actionCommand, startExpected) =>

          test(s"should $grantOrDeny $actionName privilege for specific property to custom role for all databases and all element types") {
            // GIVEN
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

  test("should not be able to override internal parameters from the outside") {
    // GIVEN
    execute("CREATE USER joe SET PASSWORD 'soap' CHANGE NOT REQUIRED")

    // WHEN using a parameter name that used to be internal, but is not any more, it should work
    executeOnSystem("joe", "soap", "SHOW USER PRIVILEGES", resultHandler = (row, _) => {
      // THEN
      val res = asPrivilegesResult(row)
      println(res)
      res should be(granted(access).database(DEFAULT).role("PUBLIC").user("joe").map)
    }, params = Map[String, Object]("currentUser" -> "neo4j").asJava) should be(1)

    // WHEN using a parameter name that is the new internal name, an error should occur
    the[QueryExecutionException] thrownBy {
      executeOnSystem("joe", "soap", "SHOW USER PRIVILEGES",
        params = Map[String, Object]("__internal_currentUser" -> "neo4j").asJava)
    } should have message ("The query contains a parameter with an illegal name: '__internal_currentUser'")
  }
}
