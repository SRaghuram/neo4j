/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.internal.cypher.acceptance

import java.util

import org.neo4j.configuration.GraphDatabaseSettings.{DEFAULT_DATABASE_NAME, SYSTEM_DATABASE_NAME}
import org.neo4j.cypher.DatabaseManagementException
import org.neo4j.dbms.api.DatabaseNotFoundException
import org.neo4j.graphdb.security.AuthorizationViolationException
import org.neo4j.graphdb.{Node, QueryExecutionException, Result}
import org.neo4j.internal.kernel.api.Transaction
import org.neo4j.internal.kernel.api.security.LoginContext
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException

import scala.collection.Map

class PrivilegeDDLAcceptanceTest extends DDLAcceptanceTestBase {

  // Tests for showing privileges

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

  test("should fail when showing privileges for all users when not on system database") {
    the[DatabaseManagementException] thrownBy {
      // WHEN
      execute("SHOW ALL PRIVILEGES")
      // THEN
    } should have message
      "This is a DDL command and it should be executed against the system database: SHOW PRIVILEGE"
  }

  test("should show privileges for specific role") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)

    // WHEN
    val result = execute("SHOW ROLE editor PRIVILEGES")

    // THEN
    val expected = Set(
      grantTraverse().role("editor").node("*").map,
      grantTraverse().role("editor").relationship("*").map,
      grantRead().role("editor").node("*").map,
      grantRead().role("editor").relationship("*").map,
      grantWrite().role("editor").node("*").map,
      grantWrite().role("editor").relationship("*").map,
    )

    result.toSet should be(expected)
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

  test("should fail when showing privileges for roles when not on system database") {
    the[DatabaseManagementException] thrownBy {
      // WHEN
      execute("SHOW ROLE editor PRIVILEGES")
      // THEN
    } should have message
      "This is a DDL command and it should be executed against the system database: SHOW PRIVILEGE"
  }

  test("should show privileges for specific user") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)

    // WHEN
    val result = execute("SHOW USER neo4j PRIVILEGES")

    // THEN
    val expected = Set(
      grantTraverse().role("admin").user("neo4j").node("*").map,
      grantTraverse().role("admin").user("neo4j").relationship("*").map,
      grantRead().role("admin").user("neo4j").node("*").map,
      grantRead().role("admin").user("neo4j").relationship("*").map,
      grantWrite().role("admin").user("neo4j").node("*").map,
      grantWrite().role("admin").user("neo4j").relationship("*").map,
      grantToken().role("admin").user("neo4j").map,
      grantSchema().role("admin").user("neo4j").map,
      grantSystem().role("admin").user("neo4j").map,
    )

    result.toSet should be(expected)
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

  test("should fail when showing privileges for users when not on system database") {
    the[DatabaseManagementException] thrownBy {
      // WHEN
      execute("SHOW USER neo4j PRIVILEGES")
      // THEN
    } should have message
      "This is a DDL command and it should be executed against the system database: SHOW PRIVILEGE"
  }

  // Tests for granting privileges

  Seq(
    ("traversal", "TRAVERSE", Set(grantTraverse())),
    ("read", "READ (*)", Set(grantRead())),
    ("match", "MATCH (*)", Set(grantTraverse(), grantRead()))
  ).foreach {
    case (actionName, actionCommand, startExpected) =>

      test(s"should grant $actionName privilege to custom role for all databases and all element types") {
        // GIVEN
        selectDatabase(SYSTEM_DATABASE_NAME)
        execute("CREATE ROLE custom")

        // WHEN
        execute(s"GRANT $actionCommand ON GRAPH * TO custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(
          startExpected.map(_.role("custom").node("*").map) ++
            startExpected.map(_.role("custom").relationship("*").map)
        )
      }

      test(s"should fail granting $actionName privilege for all databases and all labels to non-existing role") {
        // GIVEN
        selectDatabase(SYSTEM_DATABASE_NAME)

        // WHEN
        the[InvalidArgumentsException] thrownBy {
          // WHEN
          execute(s"GRANT $actionCommand ON GRAPH * NODES * (*) TO custom")
          // THEN
        } should have message "Role 'custom' does not exist."

        // WHEN
        the[InvalidArgumentsException] thrownBy {
          // WHEN
          execute(s"GRANT $actionCommand ON GRAPH * RELATIONSHIPS * (*) TO custom")
          // THEN
        } should have message "Role 'custom' does not exist."

        // WHEN
        the[InvalidArgumentsException] thrownBy {
          // WHEN
          execute(s"GRANT $actionCommand ON GRAPH * TO custom")
          // THEN
        } should have message "Role 'custom' does not exist."

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set())
      }

      test(s"should fail when granting $actionName privilege with missing database") {
        // GIVEN
        selectDatabase(SYSTEM_DATABASE_NAME)
        execute("CREATE ROLE custom")
        the[DatabaseNotFoundException] thrownBy {
          execute(s"GRANT $actionCommand ON GRAPH foo TO custom")
        } should have message "Database 'foo' does not exist."
      }

      test(s"should fail when granting $actionName privilege to custom role when not on system database") {
        val commandName = actionCommand.split(" ").head
        the[DatabaseManagementException] thrownBy {
          // WHEN
          execute(s"GRANT $actionCommand ON GRAPH * TO custom")
          // THEN
        } should have message
          s"This is a DDL command and it should be executed against the system database: GRANT $commandName"
      }

      Seq(
        ("label", "NODES", addNode: builderType),
        ("relationship type", "RELATIONSHIPS", addRel: builderType)
      ).foreach {
        case (segmentName, segmentCommand, segmentFunction: builderType) =>

          test(s"should grant $actionName privilege to custom role for all databases and all ${segmentName}s") {
            // GIVEN
            selectDatabase(SYSTEM_DATABASE_NAME)
            execute("CREATE ROLE custom")

            // WHEN
            execute(s"GRANT $actionCommand ON GRAPH * $segmentCommand * (*) TO custom")

            // THEN
            execute("SHOW ROLE custom PRIVILEGES").toSet should be(startExpected.map(expected => segmentFunction(expected.role("custom"), "*").map))
          }

          test(s"should grant $actionName privilege to custom role for all databases but only a specific $segmentName (that does not need to exist)") {
            // GIVEN
            selectDatabase(SYSTEM_DATABASE_NAME)
            execute("CREATE ROLE custom")

            // WHEN
            execute(s"GRANT $actionCommand ON GRAPH * $segmentCommand A (*) TO custom")

            // THEN
            execute("SHOW ROLE custom PRIVILEGES").toSet should be(startExpected.map(expected => segmentFunction(expected.role("custom"), "A").map))
          }

          test(s"should grant $actionName privilege to custom role for a specific database and a specific $segmentName") {
            // GIVEN
            selectDatabase(SYSTEM_DATABASE_NAME)
            execute("CREATE ROLE custom")
            execute("CREATE DATABASE foo")

            // WHEN
            execute(s"GRANT $actionCommand ON GRAPH foo $segmentCommand A (*) TO custom")

            // THEN
            execute("SHOW ROLE custom PRIVILEGES").toSet should
              be(startExpected.map(expected => segmentFunction(expected.role("custom").database("foo"), "A").map))
          }

          test(s"should grant $actionName privilege to custom role for a specific database and all ${segmentName}s") {
            // GIVEN
            selectDatabase(SYSTEM_DATABASE_NAME)
            execute("CREATE ROLE custom")
            execute("CREATE DATABASE foo")

            // WHEN
            execute(s"GRANT $actionCommand ON GRAPH foo $segmentCommand * (*) TO custom")

            // THEN
            execute("SHOW ROLE custom PRIVILEGES").toSet should
              be(startExpected.map(expected => segmentFunction(expected.role("custom").database("foo"), "*").map))
          }

          test(s"should grant $actionName privilege to custom role for a specific database and multiple ${segmentName}s") {
            // GIVEN
            selectDatabase(SYSTEM_DATABASE_NAME)
            execute("CREATE ROLE custom")
            execute("CREATE DATABASE foo")

            // WHEN
            execute(s"GRANT $actionCommand ON GRAPH foo $segmentCommand A (*) TO custom")
            execute(s"GRANT $actionCommand ON GRAPH foo $segmentCommand B (*) TO custom")

            // THEN
            execute("SHOW ROLE custom PRIVILEGES").toSet should be(
              startExpected.map(expected => segmentFunction(expected.role("custom").database("foo"), "A").map) ++
                startExpected.map(expected => segmentFunction(expected.role("custom").database("foo"), "B").map)
            )
          }

          test(s"should grant $actionName privilege to custom role for a specific database and multiple ${segmentName}s in one grant") {
            // GIVEN
            selectDatabase(SYSTEM_DATABASE_NAME)
            execute("CREATE ROLE custom")
            execute("CREATE DATABASE foo")

            // WHEN
            execute(s"GRANT $actionCommand ON GRAPH foo $segmentCommand A, B (*) TO custom")

            // THEN
            execute("SHOW ROLE custom PRIVILEGES").toSet should be(
              startExpected.map(expected => segmentFunction(expected.role("custom").database("foo"), "A").map) ++
                startExpected.map(expected => segmentFunction(expected.role("custom").database("foo"), "B").map)
            )
          }

          test(s"should grant $actionName privilege to multiple roles for a specific database and multiple ${segmentName}s in one grant") {
            // GIVEN
            selectDatabase(SYSTEM_DATABASE_NAME)
            execute("CREATE ROLE role1")
            execute("CREATE ROLE role2")
            execute("CREATE DATABASE foo")

            // WHEN
            execute(s"GRANT $actionCommand ON GRAPH foo $segmentCommand A, B (*) TO role1, role2")

            // THEN
            execute("SHOW ROLE role1 PRIVILEGES").toSet should be(
              startExpected.map(expected => segmentFunction(expected.role("role1").database("foo"), "A").map) ++
                startExpected.map(expected => segmentFunction(expected.role("role1").database("foo"), "B").map)
            )
            execute("SHOW ROLE role2 PRIVILEGES").toSet should be(
              startExpected.map(expected => segmentFunction(expected.role("role2").database("foo"), "A").map) ++
                startExpected.map(expected => segmentFunction(expected.role("role2").database("foo"), "B").map)
            )
          }

      }
  }

  // Tests for granting privileges on properties

  Seq(
    ("read", "READ", Set.empty),
    ("match", "MATCH", Set(grantTraverse()))
  ).foreach {
    case (actionName, actionCommand, expectedTraverse) =>

      test(s"should grant $actionName privilege for specific property to custom role for all databases and all element types") {
        // GIVEN
        selectDatabase(SYSTEM_DATABASE_NAME)
        execute("CREATE ROLE custom")

        // WHEN
        execute(s"GRANT $actionCommand (bar) ON GRAPH * TO custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(
          Set(grantRead().role("custom").property("bar").node("*").map,
            grantRead().role("custom").property("bar").relationship("*").map) ++
            expectedTraverse.map(_.role("custom").node("*").map) ++
            expectedTraverse.map(_.role("custom").relationship("*").map)
        )
      }

      Seq(
        ("label", "NODES", addNode: builderType),
        ("relationship type", "RELATIONSHIPS", addRel: builderType)
      ).foreach {
        case (segmentName, segmentCommand, segmentFunction: builderType) =>

          test(s"should grant $actionName privilege for specific property to custom role for all databases and all ${segmentName}s") {
            // GIVEN
            selectDatabase(SYSTEM_DATABASE_NAME)
            execute("CREATE ROLE custom")

            // WHEN
            execute(s"GRANT $actionCommand (bar) ON GRAPH * $segmentCommand * (*) TO custom")

            // THEN
            execute("SHOW ROLE custom PRIVILEGES").toSet should be(
              Set(segmentFunction(grantRead().role("custom").property("bar"), "*").map) ++
                expectedTraverse.map(expected => segmentFunction(expected.role("custom"), "*").map)
            )
          }

          test(s"should grant $actionName privilege for specific property to custom role for all databases but only a specific $segmentName") {
            // GIVEN
            selectDatabase(SYSTEM_DATABASE_NAME)
            execute("CREATE ROLE custom")

            // WHEN
            execute(s"GRANT $actionCommand (bar) ON GRAPH * $segmentCommand A (*) TO custom")

            // THEN
            execute("SHOW ROLE custom PRIVILEGES").toSet should be(
              Set(segmentFunction(grantRead().role("custom").property("bar"), "A").map) ++
                expectedTraverse.map(expected => segmentFunction(expected.role("custom"), "A").map)
            )
          }

          test(s"should grant $actionName privilege for specific property to custom role for a specific database and a specific $segmentName") {
            // GIVEN
            selectDatabase(SYSTEM_DATABASE_NAME)
            execute("CREATE ROLE custom")
            execute("CREATE DATABASE foo")

            // WHEN
            execute(s"GRANT $actionCommand (bar) ON GRAPH foo $segmentCommand A (*) TO custom")

            // THEN
            execute("SHOW ROLE custom PRIVILEGES").toSet should be(
              Set(segmentFunction(grantRead().database("foo").role("custom").property("bar"), "A").map) ++
                expectedTraverse.map(expected => segmentFunction(expected.database("foo").role("custom"), "A").map)
            )
          }

          test(s"should grant $actionName privilege for specific property to custom role for a specific database and all ${segmentName}s") {
            // GIVEN
            selectDatabase(SYSTEM_DATABASE_NAME)
            execute("CREATE ROLE custom")
            execute("CREATE DATABASE foo")

            // WHEN
            execute(s"GRANT $actionCommand (bar) ON GRAPH foo $segmentCommand * (*) TO custom")

            // THEN
            execute("SHOW ROLE custom PRIVILEGES").toSet should be(
              Set(segmentFunction(grantRead().database("foo").role("custom").property("bar"), "*").map) ++
                expectedTraverse.map(expected => segmentFunction(expected.database("foo").role("custom"), "*").map)
            )
          }

          test(s"should grant $actionName privilege for specific property to custom role for a specific database and multiple ${segmentName}s") {
            // GIVEN
            selectDatabase(SYSTEM_DATABASE_NAME)
            execute("CREATE ROLE custom")
            execute("CREATE DATABASE foo")

            // WHEN
            execute(s"GRANT $actionCommand (bar) ON GRAPH foo $segmentCommand A (*) TO custom")
            execute(s"GRANT $actionCommand (bar) ON GRAPH foo $segmentCommand B (*) TO custom")

            // THEN
            execute("SHOW ROLE custom PRIVILEGES").toSet should be(
              Set(segmentFunction(grantRead().database("foo").role("custom").property("bar"), "A").map,
                segmentFunction(grantRead().database("foo").role("custom").property("bar"), "B").map) ++
                expectedTraverse.map(expected => segmentFunction(expected.database("foo").role("custom"), "A").map) ++
                expectedTraverse.map(expected => segmentFunction(expected.database("foo").role("custom"), "B").map)
            )
          }

          test(s"should grant $actionName privilege for multiple properties to custom role for a specific database and specific $segmentName") {
            // GIVEN
            selectDatabase(SYSTEM_DATABASE_NAME)
            execute("CREATE ROLE custom")
            execute("CREATE DATABASE foo")

            // WHEN
            execute(s"GRANT $actionCommand (bar) ON GRAPH foo $segmentCommand A (*) TO custom")
            execute(s"GRANT $actionCommand (baz) ON GRAPH foo $segmentCommand A (*) TO custom")

            // THEN
            execute("SHOW ROLE custom PRIVILEGES").toSet should be(
              Set(segmentFunction(grantRead().database("foo").role("custom").property("bar"), "A").map,
                segmentFunction(grantRead().database("foo").role("custom").property("baz"), "A").map) ++
                expectedTraverse.map(expected => segmentFunction(expected.database("foo").role("custom"), "A").map)
            )
          }

          test(s"should grant $actionName privilege for multiple properties to custom role for a specific database and multiple ${segmentName}s") {
            // GIVEN
            selectDatabase(SYSTEM_DATABASE_NAME)
            execute("CREATE ROLE custom")
            execute("CREATE DATABASE foo")

            // WHEN
            execute(s"GRANT $actionCommand (bar) ON GRAPH foo $segmentCommand A (*) TO custom")
            execute(s"GRANT $actionCommand (baz) ON GRAPH foo $segmentCommand B (*) TO custom")

            // THEN
            execute("SHOW ROLE custom PRIVILEGES").toSet should be(
              Set(segmentFunction(grantRead().database("foo").role("custom").property("bar"), "A").map,
                segmentFunction(grantRead().database("foo").role("custom").property("baz"), "B").map) ++
                expectedTraverse.map(expected => segmentFunction(expected.database("foo").role("custom"), "A").map) ++
                expectedTraverse.map(expected => segmentFunction(expected.database("foo").role("custom"), "B").map)
            )
          }

          test(s"should grant $actionName privilege for multiple properties to multiple roles for a specific database and multiple ${segmentName}s in a single grant") {
            // GIVEN
            selectDatabase(SYSTEM_DATABASE_NAME)
            execute("CREATE ROLE role1")
            execute("CREATE ROLE role2")
            execute("CREATE ROLE role3")
            execute("CREATE DATABASE foo")

            // WHEN
            execute(s"GRANT $actionCommand (a, b, c) ON GRAPH foo $segmentCommand A, B, C (*) TO role1, role2, role3")

            // THEN
            val expected = (for (l <- Seq("A", "B", "C")) yield {
              (for (p <- Seq("a", "b", "c")) yield {
                segmentFunction(grantRead().database("foo").property(p), l)
              }) ++ expectedTraverse.map(expected => segmentFunction(expected.database("foo"), l))
            }).flatten

            execute("SHOW ROLE role1 PRIVILEGES").toSet should be(expected.map(_.role("role1").map).toSet)
            execute("SHOW ROLE role2 PRIVILEGES").toSet should be(expected.map(_.role("role2").map).toSet)
            execute("SHOW ROLE role3 PRIVILEGES").toSet should be(expected.map(_.role("role3").map).toSet)
          }

      }
  }


  // Tests for REVOKE READ, TRAVERSE and MATCH

  test("should revoke correct read privilege different label qualifier") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    execute("CREATE DATABASE foo")
    execute("GRANT READ (bar) ON GRAPH foo NODES * (*) TO custom")
    execute("GRANT READ (bar) ON GRAPH foo NODES A (*) TO custom")
    execute("GRANT READ (bar) ON GRAPH foo NODES B (*) TO custom")
    execute("GRANT READ (bar) ON GRAPH foo RELATIONSHIPS * (*) TO custom")
    execute("GRANT READ (bar) ON GRAPH foo RELATIONSHIPS A (*) TO custom")
    execute("GRANT READ (bar) ON GRAPH foo RELATIONSHIPS B (*) TO custom")

    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantRead().database("foo").role("custom").property("bar").relationship("*").map,
      grantRead().database("foo").role("custom").property("bar").relationship("A").map,
      grantRead().database("foo").role("custom").property("bar").relationship("B").map,
      grantRead().database("foo").role("custom").property("bar").node("*").map,
      grantRead().database("foo").role("custom").property("bar").node("A").map,
      grantRead().database("foo").role("custom").property("bar").node("B").map
    ))

    // WHEN
    execute("REVOKE READ (bar) ON GRAPH foo NODES A (*) FROM custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantRead().database("foo").role("custom").property("bar").relationship("*").map,
      grantRead().database("foo").role("custom").property("bar").relationship("A").map,
      grantRead().database("foo").role("custom").property("bar").relationship("B").map,
      grantRead().database("foo").role("custom").property("bar").node("*").map,
      grantRead().database("foo").role("custom").property("bar").node("B").map
    ))

    // WHEN
    execute("REVOKE READ (bar) ON GRAPH foo NODES * (*) FROM custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantRead().database("foo").role("custom").property("bar").relationship("*").map,
      grantRead().database("foo").role("custom").property("bar").relationship("A").map,
      grantRead().database("foo").role("custom").property("bar").relationship("B").map,
      grantRead().database("foo").role("custom").property("bar").node("B").map
    ))
  }

  test("should revoke correct read privilege different relationship type qualifier") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    execute("CREATE DATABASE foo")
    execute("GRANT READ (bar) ON GRAPH foo NODES * (*) TO custom")
    execute("GRANT READ (bar) ON GRAPH foo NODES A (*) TO custom")
    execute("GRANT READ (bar) ON GRAPH foo NODES B (*) TO custom")
    execute("GRANT READ (bar) ON GRAPH foo RELATIONSHIPS * (*) TO custom")
    execute("GRANT READ (bar) ON GRAPH foo RELATIONSHIPS A (*) TO custom")
    execute("GRANT READ (bar) ON GRAPH foo RELATIONSHIPS B (*) TO custom")

    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantRead().database("foo").role("custom").property("bar").node("*").map,
      grantRead().database("foo").role("custom").property("bar").node("A").map,
      grantRead().database("foo").role("custom").property("bar").node("B").map,
      grantRead().database("foo").role("custom").property("bar").relationship("*").map,
      grantRead().database("foo").role("custom").property("bar").relationship("A").map,
      grantRead().database("foo").role("custom").property("bar").relationship("B").map
    ))

    // WHEN
    execute("REVOKE READ (bar) ON GRAPH foo RELATIONSHIPS A (*) FROM custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantRead().database("foo").role("custom").property("bar").node("*").map,
      grantRead().database("foo").role("custom").property("bar").node("A").map,
      grantRead().database("foo").role("custom").property("bar").node("B").map,
      grantRead().database("foo").role("custom").property("bar").relationship("*").map,
      grantRead().database("foo").role("custom").property("bar").relationship("B").map
    ))

    // WHEN
    execute("REVOKE READ (bar) ON GRAPH foo RELATIONSHIPS * (*) FROM custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantRead().database("foo").role("custom").property("bar").node("*").map,
      grantRead().database("foo").role("custom").property("bar").node("A").map,
      grantRead().database("foo").role("custom").property("bar").node("B").map,
      grantRead().database("foo").role("custom").property("bar").relationship("B").map
    ))
  }

  test("should revoke correct read privilege different property") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    execute("CREATE DATABASE foo")
    execute("GRANT READ (*) ON GRAPH foo TO custom")
    execute("GRANT READ (a) ON GRAPH foo TO custom")
    execute("GRANT READ (b) ON GRAPH foo TO custom")

    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantRead().database("foo").role("custom").node("*").property("a").map,
      grantRead().database("foo").role("custom").node("*").property("b").map,
      grantRead().database("foo").role("custom").node("*").map,
      grantRead().database("foo").role("custom").relationship("*").property("a").map,
      grantRead().database("foo").role("custom").relationship("*").property("b").map,
      grantRead().database("foo").role("custom").relationship("*").map
    ))

    // WHEN
    execute("REVOKE READ (a) ON GRAPH foo NODES * (*) FROM custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantRead().database("foo").role("custom").node("*").property("b").map,
      grantRead().database("foo").role("custom").node("*").map,
      grantRead().database("foo").role("custom").relationship("*").property("a").map,
      grantRead().database("foo").role("custom").relationship("*").property("b").map,
      grantRead().database("foo").role("custom").relationship("*").map
    ))

    // WHEN
    execute("REVOKE READ (*) ON GRAPH foo RELATIONSHIPS * (*) FROM custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantRead().database("foo").role("custom").node("*").property("b").map,
      grantRead().database("foo").role("custom").node("*").map,
      grantRead().database("foo").role("custom").relationship("*").property("a").map,
      grantRead().database("foo").role("custom").relationship("*").property("b").map
    ))

    // WHEN
    execute("REVOKE READ (b) ON GRAPH foo FROM custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantRead().database("foo").role("custom").node("*").map,
      grantRead().database("foo").role("custom").relationship("*").property("a").map
    ))
  }

  test("should revoke correct read privilege different databases") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    execute("CREATE DATABASE foo")
    execute("CREATE DATABASE bar")
    execute("GRANT READ (*) ON GRAPH * TO custom")
    execute("GRANT READ (*) ON GRAPH foo TO custom")
    execute("GRANT READ (*) ON GRAPH bar TO custom")

    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantRead().role("custom").node("*").map,
      grantRead().role("custom").node("*").database("foo").map,
      grantRead().role("custom").node("*").database("bar").map,
      grantRead().role("custom").relationship("*").map,
      grantRead().role("custom").relationship("*").database("foo").map,
      grantRead().role("custom").relationship("*").database("bar").map
    ))

    // WHEN
    execute("REVOKE READ (*) ON GRAPH foo FROM custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantRead().role("custom").node("*").map,
      grantRead().role("custom").node("*").database("bar").map,
      grantRead().role("custom").relationship("*").map,
      grantRead().role("custom").relationship("*").database("bar").map
    ))

    // WHEN
    execute("REVOKE READ (*) ON GRAPH * FROM custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantRead().role("custom").node("*").database("bar").map,
      grantRead().role("custom").relationship("*").database("bar").map
    ))
  }

  test("should revoke correct traverse node privilege different label qualifier") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    execute("CREATE DATABASE foo")
    execute("GRANT TRAVERSE ON GRAPH foo TO custom")
    execute("GRANT TRAVERSE ON GRAPH foo NODES A (*) TO custom")
    execute("GRANT TRAVERSE ON GRAPH foo NODES B (*) TO custom")

    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantTraverse().database("foo").role("custom").node("*").map,
      grantTraverse().database("foo").role("custom").relationship("*").map,
      grantTraverse().database("foo").role("custom").node("A").map,
      grantTraverse().database("foo").role("custom").node("B").map
    ))

    // WHEN
    execute("REVOKE TRAVERSE ON GRAPH foo NODES A (*) FROM custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantTraverse().database("foo").role("custom").node("*").map,
      grantTraverse().database("foo").role("custom").relationship("*").map,
      grantTraverse().database("foo").role("custom").node("B").map
    ))

    // WHEN
    execute("REVOKE TRAVERSE ON GRAPH foo NODES * (*) FROM custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantTraverse().database("foo").role("custom").relationship("*").map,
      grantTraverse().database("foo").role("custom").node("B").map
    ))
  }

  test("should revoke correct traverse node privilege different databases") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    execute("CREATE DATABASE foo")
    execute("CREATE DATABASE bar")
    execute("GRANT TRAVERSE ON GRAPH * TO custom")
    execute("GRANT TRAVERSE ON GRAPH foo NODES * (*) TO custom")
    execute("GRANT TRAVERSE ON GRAPH bar NODES * (*) TO custom")

    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantTraverse().role("custom").node("*").map,
      grantTraverse().role("custom").relationship("*").map,
      grantTraverse().role("custom").node("*").database("foo").map,
      grantTraverse().role("custom").node("*").database("bar").map
    ))

    // WHEN
    execute("REVOKE TRAVERSE ON GRAPH foo NODES * (*) FROM custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantTraverse().role("custom").node("*").map,
      grantTraverse().role("custom").relationship("*").map,
      grantTraverse().role("custom").node("*").database("bar").map
    ))

    // WHEN
    execute("REVOKE TRAVERSE ON GRAPH * NODES * (*) FROM custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantTraverse().role("custom").relationship("*").map,
      grantTraverse().role("custom").node("*").database("bar").map
    ))
  }

  test("should revoke correct traverse relationships privilege different type qualifier") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    execute("CREATE DATABASE foo")
    execute("GRANT TRAVERSE ON GRAPH foo TO custom")
    execute("GRANT TRAVERSE ON GRAPH foo RELATIONSHIPS A (*) TO custom")
    execute("GRANT TRAVERSE ON GRAPH foo RELATIONSHIPS B (*) TO custom")

    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantTraverse().database("foo").role("custom").node("*").map,
      grantTraverse().database("foo").role("custom").relationship("*").map,
      grantTraverse().database("foo").role("custom").relationship("A").map,
      grantTraverse().database("foo").role("custom").relationship("B").map
    ))

    // WHEN
    execute("REVOKE TRAVERSE ON GRAPH foo RELATIONSHIPS A (*) FROM custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantTraverse().database("foo").role("custom").node("*").map,
      grantTraverse().database("foo").role("custom").relationship("*").map,
      grantTraverse().database("foo").role("custom").relationship("B").map
    ))

    // WHEN
    execute("REVOKE TRAVERSE ON GRAPH foo RELATIONSHIPS * (*) FROM custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantTraverse().database("foo").role("custom").node("*").map,
      grantTraverse().database("foo").role("custom").relationship("B").map
    ))
  }

  test("should revoke correct traverse relationship privilege different databases") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    execute("CREATE DATABASE foo")
    execute("CREATE DATABASE bar")
    execute("GRANT TRAVERSE ON GRAPH * TO custom")
    execute("GRANT TRAVERSE ON GRAPH foo RELATIONSHIPS * (*) TO custom")
    execute("GRANT TRAVERSE ON GRAPH bar RELATIONSHIPS * (*) TO custom")

    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantTraverse().role("custom").node("*").map,
      grantTraverse().role("custom").relationship("*").map,
      grantTraverse().role("custom").relationship("*").database("foo").map,
      grantTraverse().role("custom").relationship("*").database("bar").map
    ))

    // WHEN
    execute("REVOKE TRAVERSE ON GRAPH foo RELATIONSHIPS * (*) FROM custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantTraverse().role("custom").node("*").map,
      grantTraverse().role("custom").relationship("*").map,
      grantTraverse().role("custom").relationship("*").database("bar").map
    ))

    // WHEN
    execute("REVOKE TRAVERSE ON GRAPH * RELATIONSHIPS * (*) FROM custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantTraverse().role("custom").node("*").map,
      grantTraverse().role("custom").relationship("*").database("bar").map
    ))
  }

  test("should revoke correct MATCH privilege different label qualifier") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    execute("CREATE DATABASE foo")
    execute("GRANT MATCH (bar) ON GRAPH foo TO custom")
    execute("GRANT MATCH (bar) ON GRAPH foo NODES A (*) TO custom")
    execute("GRANT MATCH (bar) ON GRAPH foo NODES B (*) TO custom")

    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantTraverse().database("foo").role("custom").node("*").map,
      grantRead().database("foo").role("custom").property("bar").node("*").map,
      grantTraverse().database("foo").role("custom").relationship("*").map,
      grantRead().database("foo").role("custom").relationship("*").property("bar").map,
      grantTraverse().database("foo").role("custom").node("A").map,
      grantRead().database("foo").role("custom").property("bar").node("A").map,
      grantTraverse().database("foo").role("custom").node("B").map,
      grantRead().database("foo").role("custom").property("bar").node("B").map
    ))

    // WHEN
    execute("REVOKE MATCH (bar) ON GRAPH foo NODES A (*) FROM custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantTraverse().database("foo").role("custom").node("*").map,
      grantRead().database("foo").role("custom").property("bar").node("*").map,
      grantTraverse().database("foo").role("custom").relationship("*").map,
      grantRead().database("foo").role("custom").relationship("*").property("bar").map,
      grantTraverse().database("foo").role("custom").node("A").map, // TODO: should be removed when revoking MATCH also revokes traverse
      grantTraverse().database("foo").role("custom").node("B").map,
      grantRead().database("foo").role("custom").property("bar").node("B").map
    ))

    // WHEN
    execute("REVOKE MATCH (bar) ON GRAPH foo NODES * (*) FROM custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantTraverse().database("foo").role("custom").relationship("*").map,
      grantRead().database("foo").role("custom").relationship("*").property("bar").map,
      grantTraverse().database("foo").role("custom").node("*").map, // TODO: should be removed when revoking MATCH also revokes traverse
      grantTraverse().database("foo").role("custom").node("A").map, // TODO: should be removed when revoking MATCH also revokes traverse
      grantTraverse().database("foo").role("custom").node("B").map,
      grantRead().database("foo").role("custom").property("bar").node("B").map
    ))
  }

  test("should revoke correct MATCH privilege different relationship type qualifier") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    execute("CREATE DATABASE foo")
    execute("GRANT MATCH (bar) ON GRAPH foo TO custom")
    execute("GRANT MATCH (bar) ON GRAPH foo RELATIONSHIPS A (*) TO custom")
    execute("GRANT MATCH (bar) ON GRAPH foo RELATIONSHIPS B (*) TO custom")

    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantTraverse().database("foo").role("custom").node("*").map,
      grantTraverse().database("foo").role("custom").relationship("*").map,
      grantTraverse().database("foo").role("custom").relationship("A").map,
      grantTraverse().database("foo").role("custom").relationship("B").map,
      grantRead().database("foo").role("custom").node("*").property("bar").map,
      grantRead().database("foo").role("custom").relationship("*").property("bar").map,
      grantRead().database("foo").role("custom").property("bar").relationship("A").map,
      grantRead().database("foo").role("custom").property("bar").relationship("B").map
    ))

    // WHEN
    execute("REVOKE MATCH (bar) ON GRAPH foo RELATIONSHIPS A (*) FROM custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantTraverse().database("foo").role("custom").node("*").map,
      grantTraverse().database("foo").role("custom").relationship("*").map,
      grantTraverse().database("foo").role("custom").relationship("A").map,
      grantTraverse().database("foo").role("custom").relationship("B").map,
      grantRead().database("foo").role("custom").node("*").property("bar").map,
      grantRead().database("foo").role("custom").relationship("*").property("bar").map,
      grantRead().database("foo").role("custom").property("bar").relationship("B").map
    ))

    // WHEN
    execute("REVOKE MATCH (bar) ON GRAPH foo RELATIONSHIPS * (*) FROM custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantTraverse().database("foo").role("custom").node("*").map,
      grantTraverse().database("foo").role("custom").relationship("*").map,
      grantTraverse().database("foo").role("custom").relationship("A").map,
      grantTraverse().database("foo").role("custom").relationship("B").map,
      grantRead().database("foo").role("custom").node("*").property("bar").map,
      grantRead().database("foo").role("custom").property("bar").relationship("B").map
    ))
  }

  test("should revoke correct MATCH privilege different property") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    execute("CREATE DATABASE foo")
    execute("GRANT MATCH (*) ON GRAPH foo TO custom")
    execute("GRANT MATCH (a) ON GRAPH foo TO custom")
    execute("GRANT MATCH (b) ON GRAPH foo TO custom")

    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantTraverse().database("foo").role("custom").node("*").map,
      grantTraverse().database("foo").role("custom").relationship("*").map,
      grantRead().database("foo").role("custom").node("*").map,
      grantRead().database("foo").role("custom").node("*").property("a").map,
      grantRead().database("foo").role("custom").node("*").property("b").map,
      grantRead().database("foo").role("custom").relationship("*").map,
      grantRead().database("foo").role("custom").relationship("*").property("a").map,
      grantRead().database("foo").role("custom").relationship("*").property("b").map
    ))

    // WHEN
    execute("REVOKE MATCH (a) ON GRAPH foo FROM custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantTraverse().database("foo").role("custom").node("*").map,
      grantTraverse().database("foo").role("custom").relationship("*").map,
      grantRead().database("foo").role("custom").node("*").map,
      grantRead().database("foo").role("custom").node("*").property("b").map,
      grantRead().database("foo").role("custom").relationship("*").map,
      grantRead().database("foo").role("custom").relationship("*").property("b").map
    ))

    // WHEN
    execute("REVOKE MATCH (*) ON GRAPH foo FROM custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantTraverse().database("foo").role("custom").node("*").map,
      grantTraverse().database("foo").role("custom").relationship("*").map,
      grantRead().database("foo").role("custom").node("*").property("b").map,
      grantRead().database("foo").role("custom").relationship("*").property("b").map
    ))

    // WHEN
    execute("REVOKE MATCH (b) ON GRAPH foo FROM custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      // TODO: this should be an empty set
      grantTraverse().database("foo").role("custom").node("*").map,
      grantTraverse().database("foo").role("custom").relationship("*").map
    ))
  }

  test("should revoke correct MATCH privilege different databases") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    execute("CREATE DATABASE foo")
    execute("CREATE DATABASE bar")
    execute("GRANT MATCH (*) ON GRAPH * TO custom")
    execute("GRANT MATCH (*) ON GRAPH foo TO custom")
    execute("GRANT MATCH (*) ON GRAPH bar TO custom")

    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantTraverse().role("custom").node("*").map,
      grantTraverse().role("custom").node("*").database("foo").map,
      grantTraverse().role("custom").node("*").database("bar").map,
      grantTraverse().role("custom").relationship("*").map,
      grantTraverse().role("custom").relationship("*").database("foo").map,
      grantTraverse().role("custom").relationship("*").database("bar").map,
      grantRead().role("custom").node("*").map,
      grantRead().role("custom").node("*").database("foo").map,
      grantRead().role("custom").node("*").database("bar").map,
      grantRead().role("custom").relationship("*").map,
      grantRead().role("custom").relationship("*").database("foo").map,
      grantRead().role("custom").relationship("*").database("bar").map
    ))

    // WHEN
    execute("REVOKE MATCH (*) ON GRAPH foo FROM custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantTraverse().role("custom").node("*").map,
      grantTraverse().role("custom").node("*").database("foo").map, // TODO: should be removed when revoking MATCH also revokes traverse
      grantTraverse().role("custom").node("*").database("bar").map,
      grantTraverse().role("custom").relationship("*").map,
      grantTraverse().role("custom").relationship("*").database("foo").map, // TODO: should be removed when revoking MATCH also revokes traverse
      grantTraverse().role("custom").relationship("*").database("bar").map,
      grantRead().role("custom").node("*").map,
      grantRead().role("custom").node("*").database("bar").map,
      grantRead().role("custom").relationship("*").map,
      grantRead().role("custom").relationship("*").database("bar").map
    ))

    // WHEN
    execute("REVOKE MATCH (*) ON GRAPH * FROM custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantTraverse().role("custom").node("*").map, // TODO: should be removed when revoking MATCH also revokes traverse
      grantTraverse().role("custom").node("*").database("foo").map,
      grantTraverse().role("custom").node("*").database("bar").map,
      grantTraverse().role("custom").relationship("*").map, // TODO: should be removed when revoking MATCH also revokes traverse
      grantTraverse().role("custom").relationship("*").database("foo").map,
      grantTraverse().role("custom").relationship("*").database("bar").map,
      grantRead().role("custom").node("*").database("bar").map,
      grantRead().role("custom").relationship("*").database("bar").map
    ))
  }

  test("should revoke correct traverse and read privileges from different MATCH privileges") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    execute("CREATE DATABASE foo")
    execute("GRANT MATCH (foo,bar) ON GRAPH foo NODES A,B (*) TO custom")
    execute("GRANT MATCH (foo,bar) ON GRAPH foo RELATIONSHIPS A,B (*) TO custom")

    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantTraverse().database("foo").role("custom").node("A").map,
      grantTraverse().database("foo").role("custom").node("B").map,
      grantTraverse().database("foo").role("custom").relationship("A").map,
      grantTraverse().database("foo").role("custom").relationship("B").map,
      grantRead().database("foo").role("custom").node("A").property("foo").map,
      grantRead().database("foo").role("custom").node("B").property("foo").map,
      grantRead().database("foo").role("custom").relationship("A").property("foo").map,
      grantRead().database("foo").role("custom").relationship("B").property("foo").map,
      grantRead().database("foo").role("custom").node("A").property("bar").map,
      grantRead().database("foo").role("custom").node("B").property("bar").map,
      grantRead().database("foo").role("custom").relationship("A").property("bar").map,
      grantRead().database("foo").role("custom").relationship("B").property("bar").map
    ))

    // WHEN
    execute("REVOKE TRAVERSE ON GRAPH foo NODES A (*) FROM custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantTraverse().database("foo").role("custom").node("B").map,
      grantTraverse().database("foo").role("custom").relationship("A").map,
      grantTraverse().database("foo").role("custom").relationship("B").map,
      grantRead().database("foo").role("custom").node("A").property("foo").map,
      grantRead().database("foo").role("custom").node("B").property("foo").map,
      grantRead().database("foo").role("custom").relationship("A").property("foo").map,
      grantRead().database("foo").role("custom").relationship("B").property("foo").map,
      grantRead().database("foo").role("custom").node("A").property("bar").map,
      grantRead().database("foo").role("custom").node("B").property("bar").map,
      grantRead().database("foo").role("custom").relationship("A").property("bar").map,
      grantRead().database("foo").role("custom").relationship("B").property("bar").map
    ))

    // WHEN
    execute("REVOKE TRAVERSE ON GRAPH foo RELATIONSHIPS B (*) FROM custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantTraverse().database("foo").role("custom").node("B").map,
      grantTraverse().database("foo").role("custom").relationship("A").map,
      grantRead().database("foo").role("custom").node("A").property("foo").map,
      grantRead().database("foo").role("custom").node("B").property("foo").map,
      grantRead().database("foo").role("custom").relationship("A").property("foo").map,
      grantRead().database("foo").role("custom").relationship("B").property("foo").map,
      grantRead().database("foo").role("custom").node("A").property("bar").map,
      grantRead().database("foo").role("custom").node("B").property("bar").map,
      grantRead().database("foo").role("custom").relationship("A").property("bar").map,
      grantRead().database("foo").role("custom").relationship("B").property("bar").map
    ))

    // WHEN
    execute("REVOKE READ (foo,bar) ON GRAPH foo NODES B (*) FROM custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantTraverse().database("foo").role("custom").node("B").map,
      grantTraverse().database("foo").role("custom").relationship("A").map,
      grantRead().database("foo").role("custom").node("A").property("foo").map,
      grantRead().database("foo").role("custom").relationship("A").property("foo").map,
      grantRead().database("foo").role("custom").relationship("B").property("foo").map,
      grantRead().database("foo").role("custom").node("A").property("bar").map,
      grantRead().database("foo").role("custom").relationship("A").property("bar").map,
      grantRead().database("foo").role("custom").relationship("B").property("bar").map
    ))

    // WHEN
    execute("REVOKE READ (foo,bar) ON GRAPH foo RELATIONSHIPS A (*) FROM custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantTraverse().database("foo").role("custom").node("B").map,
      grantTraverse().database("foo").role("custom").relationship("A").map,
      grantRead().database("foo").role("custom").node("A").property("foo").map,
      grantRead().database("foo").role("custom").relationship("B").property("foo").map,
      grantRead().database("foo").role("custom").node("A").property("bar").map,
      grantRead().database("foo").role("custom").relationship("B").property("bar").map
    ))

    // WHEN
    execute("REVOKE READ (foo) ON GRAPH foo NODES A (*) FROM custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantTraverse().database("foo").role("custom").node("B").map,
      grantTraverse().database("foo").role("custom").relationship("A").map,
      grantRead().database("foo").role("custom").relationship("B").property("foo").map,
      grantRead().database("foo").role("custom").node("A").property("bar").map,
      grantRead().database("foo").role("custom").relationship("B").property("bar").map
    ))

    // WHEN
    execute("REVOKE READ (foo) ON GRAPH foo RELATIONSHIPS B (*) FROM custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantTraverse().database("foo").role("custom").node("B").map,
      grantTraverse().database("foo").role("custom").relationship("A").map,
      grantRead().database("foo").role("custom").node("A").property("bar").map,
      grantRead().database("foo").role("custom").relationship("B").property("bar").map
    ))
  }

  test("should revoke correct MATCH privilege from different traverse, read and MATCH privileges") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    execute("CREATE DATABASE foo")
    execute("GRANT MATCH (*) ON GRAPH foo TO custom")
    execute("GRANT MATCH (a) ON GRAPH foo TO custom")
    execute("GRANT READ  (b) ON GRAPH foo TO custom")

    execute("GRANT TRAVERSE  ON GRAPH foo NODES A (*) TO custom")
    execute("GRANT MATCH (a) ON GRAPH foo NODES A (*) TO custom")

    execute("GRANT TRAVERSE  ON GRAPH foo RELATIONSHIPS A (*) TO custom")
    execute("GRANT MATCH (a) ON GRAPH foo RELATIONSHIPS A (*) TO custom")

    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantTraverse().role("custom").database("foo").node("*").map, // From both MATCH *
      grantTraverse().role("custom").database("foo").relationship("*").map, // From both MATCH *
      grantRead().database("foo").role("custom").node("*").map,
      grantRead().database("foo").role("custom").node("*").property("a").map,
      grantRead().database("foo").role("custom").node("*").property("b").map,
      grantRead().database("foo").role("custom").relationship("*").map,
      grantRead().database("foo").role("custom").relationship("*").property("a").map,
      grantRead().database("foo").role("custom").relationship("*").property("b").map,

      grantTraverse().role("custom").database("foo").node("A").map, // From both MATCH and TRAVERSE
      grantTraverse().role("custom").database("foo").relationship("A").map, // From both MATCH and TRAVERSE
      grantRead().database("foo").role("custom").property("a").node("A").map,
      grantRead().database("foo").role("custom").property("a").relationship("A").map
    ))

    // WHEN
    execute("REVOKE MATCH (b) ON GRAPH foo NODES * (*) FROM custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantTraverse().role("custom").database("foo").node("*").map,
      grantTraverse().role("custom").database("foo").relationship("*").map,
      grantRead().database("foo").role("custom").node("*").map,
      grantRead().database("foo").role("custom").node("*").property("a").map,
      grantRead().database("foo").role("custom").relationship("*").map,
      grantRead().database("foo").role("custom").relationship("*").property("a").map,
      grantRead().database("foo").role("custom").relationship("*").property("b").map,

      grantTraverse().role("custom").database("foo").node("A").map,
      grantTraverse().role("custom").database("foo").relationship("A").map,
      grantRead().database("foo").role("custom").property("a").node("A").map,
      grantRead().database("foo").role("custom").property("a").relationship("A").map
    ))

    // WHEN
    execute("REVOKE MATCH (a) ON GRAPH foo RELATIONSHIP * (*) FROM custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantTraverse().role("custom").database("foo").node("*").map,
      grantTraverse().role("custom").database("foo").relationship("*").map,
      grantRead().database("foo").role("custom").node("*").map,
      grantRead().database("foo").role("custom").node("*").property("a").map,
      grantRead().database("foo").role("custom").relationship("*").map,
      grantRead().database("foo").role("custom").relationship("*").property("b").map,

      grantTraverse().role("custom").database("foo").node("A").map,
      grantTraverse().role("custom").database("foo").relationship("A").map,
      grantRead().database("foo").role("custom").property("a").node("A").map,
      grantRead().database("foo").role("custom").property("a").relationship("A").map
    ))

    // WHEN
    execute("REVOKE MATCH (a) ON GRAPH foo NODES A (*) FROM custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantTraverse().role("custom").database("foo").node("*").map,
      grantTraverse().role("custom").database("foo").relationship("*").map,
      grantRead().database("foo").role("custom").node("*").map,
      grantRead().database("foo").role("custom").node("*").property("a").map,
      grantRead().database("foo").role("custom").relationship("*").map,
      grantRead().database("foo").role("custom").relationship("*").property("b").map,

      grantTraverse().role("custom").database("foo").node("A").map,
      grantTraverse().role("custom").database("foo").relationship("A").map,
      grantRead().database("foo").role("custom").property("a").relationship("A").map
    ))

    // WHEN
    execute("REVOKE MATCH (a) ON GRAPH foo RELATIONSHIPS A (*) FROM custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantTraverse().role("custom").database("foo").node("*").map,
      grantTraverse().role("custom").database("foo").relationship("*").map,
      grantRead().database("foo").role("custom").node("*").map,
      grantRead().database("foo").role("custom").node("*").property("a").map,
      grantRead().database("foo").role("custom").relationship("*").map,
      grantRead().database("foo").role("custom").relationship("*").property("b").map,

      grantTraverse().role("custom").database("foo").node("A").map,
      grantTraverse().role("custom").database("foo").relationship("A").map
    ))
  }

  test("should revoke correct elements privilege when granted as nodes + relationships") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    execute("GRANT MATCH (*) ON GRAPH * NODES A TO custom")
    execute("GRANT MATCH (*) ON GRAPH * RELATIONSHIPS A TO custom")

    execute("GRANT MATCH (*) ON GRAPH * NODES * TO custom")
    execute("GRANT MATCH (*) ON GRAPH * RELATIONSHIPS * TO custom")

    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantTraverse().role("custom").node("A").map,
      grantTraverse().role("custom").node("*").map,
      grantTraverse().role("custom").relationship("A").map,
      grantTraverse().role("custom").relationship("*").map,
      grantRead().role("custom").node("A").map,
      grantRead().role("custom").node("*").map,
      grantRead().role("custom").relationship("A").map,
      grantRead().role("custom").relationship("*").map
    ))

    // WHEN
    execute("REVOKE MATCH (*) ON GRAPH * ELEMENTS * FROM custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantTraverse().role("custom").node("A").map,
      grantTraverse().role("custom").node("*").map, // TODO: should be removed when revoking MATCH also revokes traverse
      grantTraverse().role("custom").relationship("A").map,
      grantTraverse().role("custom").relationship("*").map, // TODO: should be removed when revoking MATCH also revokes traverse
      grantRead().role("custom").node("A").map,
      grantRead().role("custom").relationship("A").map
    ))
  }

  test("should fail revoke elements privilege when granted only nodes or relationships") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    execute("ALTER USER neo4j SET PASSWORD 'abc' CHANGE NOT REQUIRED")

    execute("GRANT MATCH (foo) ON GRAPH * NODES * TO custom")
    execute("GRANT MATCH (bar) ON GRAPH * RELATIONSHIPS * TO custom")

    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantTraverse().role("custom").node("*").map,
      grantTraverse().role("custom").relationship("*").map,
      grantRead().role("custom").property("foo").node("*").map,
      grantRead().role("custom").property("bar").relationship("*").map
    ))

    // WHEN
    val error1 = the[QueryExecutionException] thrownBy {
      executeOnSystem("neo4j", "abc", "REVOKE MATCH (foo) ON GRAPH * ELEMENTS * FROM custom")
    }
    // THEN
    error1.getMessage should include("The privilege 'read foo ON GRAPH * RELATIONSHIPS *' does not exist.")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantTraverse().role("custom").node("*").map,
      grantTraverse().role("custom").relationship("*").map,
      grantRead().role("custom").property("foo").node("*").map,
      grantRead().role("custom").property("bar").relationship("*").map
    ))

    // WHEN
    val error2 = the[QueryExecutionException] thrownBy {
      executeOnSystem("neo4j", "abc", "REVOKE MATCH (bar) ON GRAPH * FROM custom")
    }
    // THEN
    error2.getMessage should include("The privilege 'read bar ON GRAPH * NODES *' does not exist.")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      grantTraverse().role("custom").node("*").map,
      grantTraverse().role("custom").relationship("*").map,
      grantRead().role("custom").property("foo").node("*").map,
      grantRead().role("custom").property("bar").relationship("*").map
    ))
  }

  test("should fail revoke privilege from non-existent role") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    execute("GRANT TRAVERSE ON GRAPH * NODES * (*) TO custom")
    execute("GRANT READ (*) ON GRAPH * NODES * (*) TO custom")
    execute("GRANT MATCH (*) ON GRAPH * NODES A (*) TO custom")

    // WHEN
    val error1 = the[InvalidArgumentsException] thrownBy {
      execute("REVOKE TRAVERSE ON GRAPH * NODES * (*) FROM wrongRole")
    }

    // THEN
    error1.getMessage should (be("The role 'wrongRole' does not have the specified privilege: traverse ON GRAPH * NODES *.") or
      be("The role 'wrongRole' does not exist."))

    // WHEN
    val error2 = the[InvalidArgumentsException] thrownBy {
      execute("REVOKE READ (*) ON GRAPH * NODES * (*) FROM wrongRole")
    }

    // THEN
    error2.getMessage should (be("The role 'wrongRole' does not have the specified privilege: read * ON GRAPH * NODES *.") or
      be("The role 'wrongRole' does not exist."))

    // WHEN
    val error3 = the[InvalidArgumentsException] thrownBy {
      execute("REVOKE MATCH (*) ON GRAPH * NODES A (*) FROM wrongRole")
    }
    // THEN
    error3.getMessage should (include("The role 'wrongRole' does not have the specified privilege") or
      be("The role 'wrongRole' does not exist."))
  }

  test("should fail revoke privilege not granted to role") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    execute("CREATE ROLE role")
    execute("GRANT TRAVERSE ON GRAPH * NODES * (*) TO custom")
    execute("GRANT READ (*) ON GRAPH * NODES * (*) TO custom")
    execute("GRANT MATCH (*) ON GRAPH * NODES A (*) TO custom")

    // WHEN
    val errorTraverse = the[InvalidArgumentsException] thrownBy {
      execute("REVOKE TRAVERSE ON GRAPH * NODES * (*) FROM role")
    }
    // THEN
    errorTraverse.getMessage should include("The role 'role' does not have the specified privilege")

    // WHEN
    val errorRead = the[InvalidArgumentsException] thrownBy {
      execute("REVOKE READ (*) ON GRAPH * NODES * (*) FROM role")
    }
    // THEN
    errorRead.getMessage should include("The role 'role' does not have the specified privilege")

    // WHEN
    val errorMatch = the[InvalidArgumentsException] thrownBy {
      execute("REVOKE MATCH (*) ON GRAPH * NODES A (*) FROM role")
    }
    // THEN
    errorMatch.getMessage should include("The role 'role' does not have the specified privilege")
  }

  test("should fail when revoking traversal privilege with missing database") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    execute("GRANT TRAVERSE ON GRAPH * NODES * (*) TO custom")
    the[InvalidArgumentsException] thrownBy {
      execute("REVOKE TRAVERSE ON GRAPH foo NODES * (*) FROM custom")
    } should have message "The privilege 'find  ON GRAPH foo NODES *' does not exist."
  }

  test("should fail when revoking read privilege with missing database") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    execute("GRANT READ (*) ON GRAPH * NODES * (*) TO custom")
    the[InvalidArgumentsException] thrownBy {
      execute("REVOKE READ (*) ON GRAPH foo NODES * (*) FROM custom")
    } should have message "The privilege 'read * ON GRAPH foo NODES *' does not exist."
  }

  test("should fail when revoking MATCH privilege with missing database") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    execute("GRANT MATCH (*) ON GRAPH * NODES * (*) TO custom")

    // WHEN
    val e = the[InvalidArgumentsException] thrownBy {
      execute("REVOKE MATCH (*) ON GRAPH foo NODES * (*) FROM custom")
    }
    // THEN
    e.getMessage should (be("The privilege 'find  ON GRAPH foo NODES *' does not exist.") or
      be("The privilege 'read * ON GRAPH foo NODES *' does not exist."))
  }

  test("should fail when revoking traversal privilege to custom role when not on system database") {
    the[DatabaseManagementException] thrownBy {
      // WHEN
      execute("REVOKE TRAVERSE ON GRAPH * NODES * (*) FROM custom")
      // THEN
    } should have message
      "This is a DDL command and it should be executed against the system database: REVOKE TRAVERSE"
  }

  test("should fail when revoking read privilege to custom role when not on system database") {
    the[DatabaseManagementException] thrownBy {
      // WHEN
      execute("REVOKE READ (*) ON GRAPH * NODES * (*) FROM custom")
      // THEN
    } should have message
      "This is a DDL command and it should be executed against the system database: REVOKE READ"
  }

  test("should fail when revoking MATCH privilege to custom role when not on system database") {
    the[DatabaseManagementException] thrownBy {
      // WHEN
      execute("REVOKE MATCH (*) ON GRAPH * NODES * (*) FROM custom")
      // THEN
    } should have message
      "This is a DDL command and it should be executed against the system database: REVOKE MATCH"
  }

  // Tests for actual behaviour of authorization rules for restricted users based on privileges

  test("should match nodes when granted traversal privilege to custom role for all databases and all labels") {
    // GIVEN
    setupUserJoeWithCustomRole()

    selectDatabase(DEFAULT_DATABASE_NAME)
    graph.execute("CREATE (n:A {name:'a'})")
    an[AuthorizationViolationException] shouldBe thrownBy {
      executeOnDefault("joe", "soap", "MATCH (n) RETURN labels(n)")
    }

    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT TRAVERSE ON GRAPH * NODES A (*) TO custom")

    executeOnDefault("joe", "soap", "MATCH (n) RETURN labels(n)", resultHandler = (row, _) => {
      row.get("labels(n)").asInstanceOf[util.Collection[String]] should contain("A")
    }) should be(1)
  }

  test("should read properties when granted read privilege to custom role for all databases and all labels") {
    // GIVEN
    setupUserJoeWithCustomRole()
    execute("GRANT TRAVERSE ON GRAPH * NODES A (*) TO custom")

    selectDatabase(DEFAULT_DATABASE_NAME)
    graph.execute("CREATE (n:A {name:'a'})")
    executeOnDefault("joe", "soap", "MATCH (n) RETURN n.name", resultHandler = (row, _) => {
      row.get("n.name") should be(null)
    }) should be(1)

    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT READ (name) ON GRAPH * NODES A (*) TO custom")

    // WHEN
    executeOnDefault("joe", "soap", "MATCH (n) RETURN n.name", resultHandler = (row, _) => {
      row.get("n.name") should be("a")
    }) should be(1)
  }

  test("should read properties when granted MATCH privilege to custom role for all databases and all labels") {
    // GIVEN
    setupUserJoeWithCustomRole()

    // WHEN
    selectDatabase(DEFAULT_DATABASE_NAME)
    graph.execute("CREATE (n:A {name:'a'})")

    // THEN
    an[AuthorizationViolationException] shouldBe thrownBy {
      executeOnDefault("joe", "soap", "MATCH (n) RETURN labels(n)")
    }

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT MATCH (name) ON GRAPH * NODES A (*) TO custom")

    // THEN
    executeOnDefault("joe", "soap", "MATCH (n) RETURN n.name", resultHandler = (row, _) => {
      row.get("n.name") should be("a")
    }) should be(1)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("REVOKE READ (name) ON GRAPH * NODES A (*) FROM custom")

    // THEN
    executeOnDefault("joe", "soap", "MATCH (n) RETURN n.name", resultHandler = (row, _) => {
      row.get("n.name") should be(null)
    }) should be(1)
  }

  test("read privilege for node should not imply traverse privilege") {
    // GIVEN
    setupUserJoeWithCustomRole()

    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE (n:A {name:'a'})")

    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT READ (name) ON GRAPH * NODES A (*) TO custom")

    // WHEN
    executeOnDefault("joe", "soap", "MATCH (n) RETURN n.name") should be(0)
  }

  test("read privilege for relationship should not imply traverse privilege") {
    // GIVEN
    setupUserJoeWithCustomRole()

    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE ()-[:REL {name:'a'}]->()")

    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT READ (name) ON GRAPH * RELATIONSHIPS REL (*) TO custom")

    // WHEN
    executeOnDefault("joe", "soap", "MATCH ()-[r]-() RETURN r.name") should be(0)
  }

  test("should see properties and nodes depending on granted traverse and read privileges for role") {
    // GIVEN
    setupMultilabelData
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE USER joe SET PASSWORD 'soap' CHANGE NOT REQUIRED")
    execute("CREATE ROLE role1")
    execute("CREATE ROLE role2")
    execute("CREATE ROLE role3")

    execute("GRANT TRAVERSE ON GRAPH * NODES * (*) TO role1")
    execute("GRANT READ (*) ON GRAPH * NODES * (*) TO role1")

    execute("GRANT TRAVERSE ON GRAPH * NODES * (*) TO role2")
    execute("GRANT READ (foo) ON GRAPH * NODES A (*) TO role2")
    execute("GRANT READ (bar) ON GRAPH * NODES B (*) TO role2")

    execute("GRANT TRAVERSE ON GRAPH * NODES A (*) TO role3")
    execute("GRANT READ (foo) ON GRAPH * NODES A (*) TO role3")
    execute("GRANT READ (bar) ON GRAPH * NODES B (*) TO role3")

    // WHEN
    an[AuthorizationViolationException] shouldBe thrownBy {
      executeOnDefault("joe", "soap", "MATCH (n) RETURN labels(n), n.foo, n.bar") should be(0)
    }

    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT ROLE role1 TO joe")

    val expected1 = List(
      (":A", 1, 2),
      (":B", 3, 4),
      (":A:B", 5, 6),
      ("", 7, 8)
    )

    executeOnDefault("joe", "soap", "MATCH (n) RETURN reduce(s = '', x IN labels(n) | s + ':' + x) AS labels, n.foo, n.bar ORDER BY n.foo, n.bar",
      resultHandler = (row, index) => {
        (row.getString("labels"), row.getNumber("n.foo"), row.getNumber("n.bar")) should be(expected1(index))
      }) should be(4)

    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("REVOKE ROLE role1 FROM joe")
    execute("GRANT ROLE role2 TO joe")

    val expected2 = List(
      (":A", 1, null),
      (":A:B", 5, 6),
      (":B", null, 4),
      ("", null, null)
    )

    executeOnDefault("joe", "soap", "MATCH (n) RETURN reduce(s = '', x IN labels(n) | s + ':' + x) AS labels, n.foo, n.bar ORDER BY n.foo, n.bar",
      resultHandler = (row, index) => {
        (row.getString("labels"), row.getNumber("n.foo"), row.getNumber("n.bar")) should be(expected2(index))
      }) should be(4)

    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("REVOKE ROLE role2 FROM joe")
    execute("GRANT ROLE role3 TO joe")

    val expected3 = List(
      (":A", 1, null),
      (":A:B", 5, 6)
    )

    executeOnDefault("joe", "soap", "MATCH (n) RETURN reduce(s = '', x IN labels(n) | s + ':' + x) AS labels, n.foo, n.bar ORDER BY n.foo, n.bar",
      resultHandler = (row, index) => {
        (row.getString("labels"), row.getNumber("n.foo"), row.getNumber("n.bar")) should be(expected3(index))
      }) should be(2)
  }

  test("should see properties and nodes depending on granted MATCH privileges for role") {
    // GIVEN
    setupMultilabelData
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE USER joe SET PASSWORD 'soap' CHANGE NOT REQUIRED")
    execute("CREATE ROLE role1")
    execute("CREATE ROLE role2")
    execute("CREATE ROLE role3")

    execute("GRANT MATCH (*) ON GRAPH * NODES * (*) TO role1")

    execute("GRANT TRAVERSE ON GRAPH * NODES * (*) TO role2")
    execute("GRANT MATCH (foo) ON GRAPH * NODES A (*) TO role2")
    execute("GRANT MATCH (bar) ON GRAPH * NODES B (*) TO role2")

    execute("GRANT TRAVERSE ON GRAPH * NODES A (*) TO role3")
    execute("GRANT READ (foo) ON GRAPH * NODES A (*) TO role3")
    execute("GRANT READ (bar) ON GRAPH * NODES B (*) TO role3")

    // WHEN
    an[AuthorizationViolationException] shouldBe thrownBy {
      executeOnDefault("joe", "soap", "MATCH (n) RETURN labels(n), n.foo, n.bar") should be(0)
    }

    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT ROLE role1 TO joe")

    val expected1 = List(
      (":A", 1, 2),
      (":B", 3, 4),
      (":A:B", 5, 6),
      ("", 7, 8)
    )

    executeOnDefault("joe", "soap", "MATCH (n) RETURN reduce(s = '', x IN labels(n) | s + ':' + x) AS labels, n.foo, n.bar ORDER BY n.foo, n.bar",
      resultHandler = (row, index) => {
        (row.getString("labels"), row.getNumber("n.foo"), row.getNumber("n.bar")) should be(expected1(index))
      }) should be(4)

    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("REVOKE ROLE role1 FROM joe")
    execute("GRANT ROLE role2 TO joe")

    val expected2 = List(
      (":A", 1, null),
      (":A:B", 5, 6),
      (":B", null, 4),
      ("", null, null)
    )

    executeOnDefault("joe", "soap", "MATCH (n) RETURN reduce(s = '', x IN labels(n) | s + ':' + x) AS labels, n.foo, n.bar ORDER BY n.foo, n.bar",
      resultHandler = (row, index) => {
        (row.getString("labels"), row.getNumber("n.foo"), row.getNumber("n.bar")) should be(expected2(index))
      }) should be(4)

    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("REVOKE ROLE role2 FROM joe")
    execute("GRANT ROLE role3 TO joe")

    val expected3 = List(
      (":A", 1, null),
      (":A:B", 5, 6)
    )

    executeOnDefault("joe", "soap", "MATCH (n) RETURN reduce(s = '', x IN labels(n) | s + ':' + x) AS labels, n.foo, n.bar ORDER BY n.foo, n.bar",
      resultHandler = (row, index) => {
        (row.getString("labels"), row.getNumber("n.foo"), row.getNumber("n.bar")) should be(expected3(index))
      }) should be(2)
  }

  test("should see properties and nodes when revoking privileges for role") {
    // GIVEN
    setupMultilabelData
    selectDatabase(SYSTEM_DATABASE_NAME)
    setupUserJoeWithCustomRole()

    // WHEN
    an[AuthorizationViolationException] shouldBe thrownBy {
      executeOnDefault("joe", "soap", "MATCH (n) RETURN labels(n), n.foo, n.bar") should be(0)
    }

    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT TRAVERSE ON GRAPH * NODES * (*) TO custom")
    execute("GRANT TRAVERSE ON GRAPH * NODES A (*) TO custom")

    execute("GRANT READ (*) ON GRAPH * NODES * (*) TO custom")
    execute("GRANT READ (foo) ON GRAPH * NODES A (*) TO custom")
    execute("GRANT READ (bar) ON GRAPH * NODES B (*) TO custom")

    val expected1 = List(
      (":A", 1, 2),
      (":B", 3, 4),
      (":A:B", 5, 6),
      ("", 7, 8)
    )

    executeOnDefault("joe", "soap", "MATCH (n) RETURN reduce(s = '', x IN labels(n) | s + ':' + x) AS labels, n.foo, n.bar ORDER BY n.foo, n.bar",
      resultHandler = (row, index) => {
        (row.getString("labels"), row.getNumber("n.foo"), row.getNumber("n.bar")) should be(expected1(index))
      }) should be(4)

    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("REVOKE READ (*) ON GRAPH * NODES * (*) FROM custom")

    val expected2 = List(
      (":A", 1, null),
      (":A:B", 5, 6),
      (":B", null, 4),
      ("", null, null)
    )

    executeOnDefault("joe", "soap", "MATCH (n) RETURN reduce(s = '', x IN labels(n) | s + ':' + x) AS labels, n.foo, n.bar ORDER BY n.foo, n.bar",
      resultHandler = (row, index) => {
        (row.getString("labels"), row.getNumber("n.foo"), row.getNumber("n.bar")) should be(expected2(index))
      }) should be(4)

    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("REVOKE TRAVERSE ON GRAPH * NODES * (*) FROM custom")

    val expected3 = List(
      (":A", 1, null),
      (":A:B", 5, 6)
    )

    executeOnDefault("joe", "soap", "MATCH (n) RETURN reduce(s = '', x IN labels(n) | s + ':' + x) AS labels, n.foo, n.bar ORDER BY n.foo, n.bar",
      resultHandler = (row, index) => {
        (row.getString("labels"), row.getNumber("n.foo"), row.getNumber("n.bar")) should be(expected3(index))
      }) should be(2)
  }

  test("should find relationship when granted traversal privilege") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    setupUserJoeWithCustomRole()
    execute("GRANT MATCH (*) ON GRAPH * NODES * TO custom")

    selectDatabase(DEFAULT_DATABASE_NAME)
    graph.execute("CREATE (:A {name:'a'})-[:LOVES]->(:A {name:'b'})")

    val expected = List(
      "a", "b"
    )
    executeOnDefault("joe", "soap", "MATCH (n) RETURN n.name", resultHandler = (row, index) => {
      row.get("n.name") should be(expected(index))
    }) should be(2)

    executeOnDefault("joe", "soap", "MATCH (n)-->(m) RETURN n.name", resultHandler = (_, _) => {
      fail("should not get a match")
    }) should be(0)

    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT TRAVERSE ON GRAPH * RELATIONSHIPS LOVES TO custom")

    executeOnDefault("joe", "soap", "MATCH (n)-->(m) RETURN n.name", resultHandler = (row, _) => {
      row.get("n.name") should be("a")
    }) should be(1)
  }

  test("should get correct count for all relationships with traversal privilege") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    setupUserJoeWithCustomRole()
    execute("GRANT MATCH (*) ON GRAPH * NODES A TO custom")

    selectDatabase(DEFAULT_DATABASE_NAME)
    graph.execute("CREATE (a:A {name:'a'}), (a)-[:LOVES]->(:B {name:'b'}), (a)-[:LOVES]->(:A {name:'c'})")
    executeOnDefault("joe", "soap", "MATCH ()-[r]->() RETURN count(r)", resultHandler = (row, _) => {
      row.get("count(r)") should be(0)
    }) should be(1)

    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT TRAVERSE ON GRAPH * RELATIONSHIPS * TO custom")

    executeOnDefault("joe", "soap", "MATCH ()-[r]->() RETURN count(r)", resultHandler = (row, _) => {
      row.get("count(r)") should be(1)
    }) should be(1)

    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT MATCH (*) ON GRAPH * NODES * TO custom")

    executeOnDefault("joe", "soap", "MATCH ()-[r]->() RETURN count(r)", resultHandler = (row, _) => {
      row.get("count(r)") should be(2)
    }) should be(1)
  }

  test("should get correct count for specific relationship with traversal privilege") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    setupUserJoeWithCustomRole()
    execute("GRANT MATCH (*) ON GRAPH * NODES A TO custom")

    selectDatabase(DEFAULT_DATABASE_NAME)
    graph.execute("CREATE (a:A {name:'a'}), (a)-[:LOVES]->(:B {name:'b'}), (a)-[:LOVES]->(:A {name:'c'})")
    executeOnDefault("joe", "soap", "MATCH ()-[r:LOVES]->() RETURN count(r)", resultHandler = (row, _) => {
      row.get("count(r)") should be(0)
    }) should be(1)

    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT TRAVERSE ON GRAPH * RELATIONSHIPS LOVES TO custom")

    executeOnDefault("joe", "soap", "MATCH ()-[r:LOVES]->() RETURN count(r)", resultHandler = (row, _) => {
      row.get("count(r)") should be(1)
    }) should be(1)

    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT MATCH (*) ON GRAPH * NODES B TO custom")

    executeOnDefault("joe", "soap", "MATCH ()-[r:LOVES]->() RETURN count(r)", resultHandler = (row, _) => {
      row.get("count(r)") should be(2)
    }) should be(1)
  }

  test("should get relationships for a matched node") {

    import scala.collection.JavaConverters._
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    setupUserJoeWithCustomRole()
    execute("GRANT MATCH (*) ON GRAPH * NODES * TO custom")
    execute("GRANT TRAVERSE ON GRAPH * RELATIONSHIPS A TO custom")

    selectDatabase(DEFAULT_DATABASE_NAME)
    graph.execute("CREATE (a:Start), (a)-[:A]->(), (a)-[:B]->()")

    executeOnDefault("joe", "soap", "MATCH (a:Start) RETURN a", resultHandler = (row, _) => {
      val node = row.get("a").asInstanceOf[Node]
      node.getDegree() should be(1)
      node.getRelationships().asScala.map(_.getType.name()).toSet should be(Set("A"))
    }) should be(1)

    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT TRAVERSE ON GRAPH * RELATIONSHIPS * TO custom")

    executeOnDefault("joe", "soap", "MATCH (a:Start) RETURN a", resultHandler = (row, _) => {
      val node = row.get("a").asInstanceOf[Node]
      node.getDegree() should be(2)
      node.getRelationships().asScala.map(_.getType.name()).toSet should be(Set("A", "B"))
    }) should be(1)
  }

  test("should get relationship types and count from procedure") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    setupUserJoeWithCustomRole()
    execute("GRANT TRAVERSE ON GRAPH * NODES A TO custom")
    val query = "CALL db.relationshipTypes YIELD relationshipType as reltype, relationshipCount as count"

    selectDatabase(DEFAULT_DATABASE_NAME)
    graph.execute("CREATE (a:A), (a)-[:A]->(:A), (a)-[:B]->(:A), (a)-[:B]->(:B)")

    // WHEN ... THEN
    executeOnDefault("joe", "soap", query, resultHandler = (_, _) => {
      fail("should get no result")
    }) should be(0)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT TRAVERSE ON GRAPH * RELATIONSHIPS A TO custom")

    // THEN
    executeOnDefault("joe", "soap", query, resultHandler = (row, _) => {
      row.get("reltype") should be("A")
      row.get("count") should be(1)
    }) should be(1)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT TRAVERSE ON GRAPH * RELATIONSHIPS B TO custom")

    val expected1 = List(
      ("A", 1),
      ("B", 1)
    )

    // THEN
    executeOnDefault("joe", "soap", query, resultHandler = (row, index) => {
      (row.get("reltype"), row.get("count")) should be(expected1(index))
    }) should be(2)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT TRAVERSE ON GRAPH * NODES B TO custom")

    val expected2 = List(
      ("A", 1),
      ("B", 2)
    )

    // THEN
    executeOnDefault("joe", "soap", query, resultHandler = (row, index) => {
      (row.get("reltype"), row.get("count")) should be(expected2(index))
    }) should be(2)
  }

  test("should only see properties on relationship with read privilege") {
    setupUserJoeWithCustomRole()
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT TRAVERSE ON GRAPH * NODES * TO custom")

    selectDatabase(DEFAULT_DATABASE_NAME)
    execute(
      """
        |CREATE ()-[:A {id: 1, foo: 2}]->(),
        |()-[:B {id: 3, foo: 4}]->()
      """.stripMargin)

    val query = "MATCH ()-[r]->() RETURN properties(r) as props ORDER BY r.id"

    executeOnDefault("joe", "soap", query, resultHandler = (_, _) => {
      fail("should get no result")
    }) should be(0)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT TRAVERSE ON GRAPH * RELATIONSHIPS * TO custom")

    // THEN
    executeOnDefault("joe", "soap", query, resultHandler = (row, _) => {
      row.get("props") should equal(util.Collections.emptyMap())
    }) should be(2)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT READ (id) ON GRAPH * RELATIONSHIPS A TO custom")

    // THEN
    val expected1 = List(
      util.Map.of("id", 1L),
      util.Collections.emptyMap()
    )

    executeOnDefault("joe", "soap", query, resultHandler = (row, index) => {
      row.get("props") should equal(expected1(index))
    }) should be(2)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT READ (foo) ON GRAPH * RELATIONSHIPS * TO custom")

    // THEN
    val expected2 = List(
      util.Map.of("id", 1L, "foo", 2L),
      util.Map.of("foo", 4L)
    )

    executeOnDefault("joe", "soap", query, resultHandler = (row, index) => {
      row.get("props") should equal(expected2(index))
    }) should be(2)
  }

  test("should see properties and relationships depending on granted MATCH privileges for role") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    setupUserJoeWithCustomRole()
    execute("GRANT TRAVERSE ON GRAPH * NODES * TO custom")
    execute("GRANT READ (id) ON GRAPH * TO custom")

    selectDatabase(DEFAULT_DATABASE_NAME)
    execute(
      """
        |CREATE ()-[:A {id: 1, foo: 1}]->(),
        |()-[:A {id: 2, bar: 1}]->(),
        |()-[:A {id: 3, foo: 0, bar: 0}]->(),
        |()-[:A {id: 4, foo: 0, bar: 1}]->(),
        |()-[:A {id: 5, foo: 1, bar: 1}]->(),
        |()-[:A {id: 6, foo: 1, bar: 0}]->()
      """.stripMargin)

    val query = "MATCH ()-[r]->() WHERE r.foo = 1 OR r.bar = 1 RETURN r.id, r.foo, r.bar"

    executeOnDefault("joe", "soap", query, resultHandler = (_, _) => {
      fail("should get no result")
    }) should be(0)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT MATCH (foo) ON GRAPH * RELATIONSHIPS A TO custom")

    // THEN
    val expected1 = List(
      (1, 1, null),
      (5, 1, null),
      (6, 1, null)
    )

    executeOnDefault("joe", "soap", query, resultHandler = (row, index) => {
      (row.get("r.id"), row.get("r.foo"), row.get("r.bar")) should be(expected1(index))
    }) should be(3)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT MATCH (bar) ON GRAPH * RELATIONSHIPS A TO custom")

    // THEN
    val expected2 = List(
      (1, 1, null),
      (2, null, 1),
      (4, 0, 1),
      (5, 1, 1),
      (6, 1, 0)
    )

    executeOnDefault("joe", "soap", query, resultHandler = (row, index) => {
      (row.get("r.id"), row.get("r.foo"), row.get("r.bar")) should be(expected2(index))
    }) should be(5)
  }

  test("should see properties and relationships depending on granted MATCH privileges for role fulltext index") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    setupUserJoeWithCustomRole()
    execute("GRANT TRAVERSE ON GRAPH * NODES * TO custom")
    execute("GRANT READ (id) ON GRAPH * TO custom")

    selectDatabase(DEFAULT_DATABASE_NAME)
    execute(
      """
        |CREATE
        |()-[:A {id: 1, foo: 'true'}]->(),
        |()-[:A {id: 2, bar: 'true'}]->(),
        |()-[:A {id: 3, foo: 'false', bar: 'false'}]->(),
        |()-[:A {id: 4, foo: 'false', bar: 'true'}]->(),
        |()-[:A {id: 5, foo: 'true', bar: 'true'}]->(),
        |()-[:A {id: 6, foo: 'true', bar: 'false'}]->()
      """.stripMargin)

    execute("CALL db.index.fulltext.createRelationshipIndex('relIndex', ['A'], ['foo', 'bar'])")
    execute("CALL db.awaitIndexes()")

    val query = "CALL db.index.fulltext.queryRelationships('relIndex', 'true') YIELD relationship AS r RETURN r.id, r.foo, r.bar ORDER BY r.id"

    executeOnDefault("joe", "soap", query, resultHandler = (_, _) => {
      fail("should get no result")
    }) should be(0)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT MATCH (foo) ON GRAPH * RELATIONSHIPS A TO custom")

    // THEN
    executeOnDefault("joe", "soap", query, resultHandler = (_, _) => {
      fail("should get no result")
    }) should be(0)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT MATCH (bar) ON GRAPH * RELATIONSHIPS A TO custom")

    // THEN
    val expected = List(
      (1, "true", null),
      (2, null, "true"),
      (4, "false", "true"),
      (5, "true", "true"),
      (6, "true", "false")
    )

    executeOnDefault("joe", "soap", query, resultHandler = (row, index) => {
      (row.get("r.id"), row.get("r.foo"), row.get("r.bar")) should be(expected(index))
    }) should be(5)
  }

  test("should give correct results with relationship") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    setupUserJoeWithCustomRole()
    execute("GRANT TRAVERSE ON GRAPH * NODES A TO custom")

    selectDatabase(DEFAULT_DATABASE_NAME)
    execute(
      """
        |CREATE (:A)-[:A {prop: 'string with words in it'}]->(:A),
        |(:A)-[:B {prop: 'another string with words in it'}]->(:A),
        |(:C)-[:B {prop: 'this string contains words'}]->(:A),
        |(:A)-[:B {prop: 'this is just a string'}]->(:A),
        |()-[:A {prop: 'words words words'}]->(:A)
      """.stripMargin)

    val query = "MATCH ()-[r]->() WHERE r.prop contains 'words' RETURN r.prop AS prop"

    executeOnDefault("joe", "soap", query, resultHandler = (_, _) => {
      fail("should get no result")
    }) should be(0)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT TRAVERSE ON GRAPH * RELATIONSHIP A TO custom")

    // THEN
    executeOnDefault("joe", "soap", query, resultHandler = (_, _) => {
      fail("should get no result")
    }) should be(0)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT READ (prop) ON GRAPH * RELATIONSHIP A TO custom")

    // THEN
    executeOnDefault("joe", "soap", query, resultHandler = (row, _) => {
      row.get("prop") should be("string with words in it")
    }) should be(1)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT READ (prop) ON GRAPH * RELATIONSHIP B TO custom")

    // THEN
    executeOnDefault("joe", "soap", query, resultHandler = (row, _) => {
      row.get("prop") should be("string with words in it")
    }) should be(1)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT TRAVERSE ON GRAPH * RELATIONSHIP B TO custom")

    // THEN
    val expected1 = List(
      "string with words in it",
      "another string with words in it"
    )

    executeOnDefault("joe", "soap", query, resultHandler = (row, index) => {
      row.get("prop") should be(expected1(index))
    }) should be(2)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT TRAVERSE ON GRAPH * NODES C TO custom")

    // THEN
    val expected2 = List(
      "string with words in it",
      "another string with words in it",
      "this string contains words"
    )

    executeOnDefault("joe", "soap", query, resultHandler = (row, index) => {
      row.get("prop") should be(expected2(index))
    }) should be(3)
  }

  test("should give correct results with relationship fulltext index") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    setupUserJoeWithCustomRole()
    execute("GRANT TRAVERSE ON GRAPH * NODES A TO custom")

    selectDatabase(DEFAULT_DATABASE_NAME)
    execute(
      """
        |CREATE (:A)-[:A {prop: 'string with words in it'}]->(:A),
        |(:A)-[:B {prop: 'another string with words in it'}]->(:A),
        |(:C)-[:B {prop: 'this string contains words'}]->(:A),
        |(:A)-[:B {prop: 'this is just a string'}]->(:A),
        |()-[:A {prop: 'words words words'}]->(:A)
      """.stripMargin)

    execute("CALL db.index.fulltext.createRelationshipIndex('relIndex', ['A', 'B'], ['prop'])")
    execute("CALL db.awaitIndexes()")

    val query = "CALL db.index.fulltext.queryRelationships('relIndex', 'words') YIELD relationship RETURN relationship.prop AS prop"

    executeOnDefault("joe", "soap", query, resultHandler = (_, _) => {
      fail("should get no result")
    }) should be(0)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT TRAVERSE ON GRAPH * RELATIONSHIP A TO custom")

    // THEN
    executeOnDefault("joe", "soap", query, resultHandler = (_, _) => {
      fail("should get no result")
    }) should be(0)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT READ (prop) ON GRAPH * RELATIONSHIP A TO custom")

    // THEN
    executeOnDefault("joe", "soap", query, resultHandler = (row, _) => {
      row.get("prop") should be("string with words in it")
    }) should be(1)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT READ (prop) ON GRAPH * RELATIONSHIP B TO custom")

    // THEN
    executeOnDefault("joe", "soap", query, resultHandler = (row, _) => {
      row.get("prop") should be("string with words in it")
    }) should be(1)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT TRAVERSE ON GRAPH * RELATIONSHIP B TO custom")

    // THEN
    val expected1 = List(
      "string with words in it",
      "another string with words in it"
    )

    executeOnDefault("joe", "soap", query, resultHandler = (row, index) => {
      row.get("prop") should be(expected1(index))
    }) should be(2)

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("GRANT TRAVERSE ON GRAPH * NODES C TO custom")

    // THEN
    val expected2 = List(
      "string with words in it",
      "another string with words in it",
      "this string contains words"
    )

    executeOnDefault("joe", "soap", query, resultHandler = (row, index) => {
      row.get("prop") should be(expected2(index))
    }) should be(3)
  }

  test("should read properties when granted MATCH privilege to custom role for a specific database") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE DATABASE foo")
    selectDatabase("foo")
    execute("CREATE (:B {name:'b'})")
    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE (:A {name:'a'})")
    selectDatabase(SYSTEM_DATABASE_NAME)

    setupUserJoeWithCustomRole()

    // WHEN
    execute(s"GRANT MATCH (*) ON GRAPH $DEFAULT_DATABASE_NAME NODES * (*) TO custom")

    // THEN
    executeOnDefault("joe", "soap", "MATCH (n) RETURN n.name", resultHandler = (row, _) => {
      row.get("n.name") should be("a")
    }) should be(1)

    the[AuthorizationViolationException] thrownBy {
      executeOn("foo", "joe", "soap", "MATCH (n) RETURN n.name")
    } should have message "Read operations are not allowed for user 'joe' with roles [custom]."
  }

  test("should rollback transaction") {
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE ROLE custom")
    val tx = graph.beginTransaction(Transaction.Type.explicit, LoginContext.AUTH_DISABLED)
    try {
      val result: Result = new RichGraphDatabaseQueryService(graph).execute("GRANT TRAVERSE ON GRAPH * NODES A,B TO custom")
      result.accept(_ => true)
      tx.failure()
    } finally {
      tx.close()
    }
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set.empty)
  }

  // helper variable, methods and class

  private def setupMultilabelData = {
    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE (n:A {foo:1, bar:2})")
    execute("CREATE (n:B {foo:3, bar:4})")
    execute("CREATE (n:A:B {foo:5, bar:6})")
    execute("CREATE (n {foo:7, bar:8})")
  }

  private type builderType = (PrivilegeMapBuilder, String) => PrivilegeMapBuilder

  private def addNode(source: PrivilegeMapBuilder, name: String): PrivilegeMapBuilder = source.node(name)

  private def addRel(source: PrivilegeMapBuilder, name: String): PrivilegeMapBuilder = source.relationship(name)
}
