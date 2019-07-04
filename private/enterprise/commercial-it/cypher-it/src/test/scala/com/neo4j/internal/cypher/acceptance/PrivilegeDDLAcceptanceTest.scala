/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.internal.cypher.acceptance

import org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME
import org.neo4j.cypher.DatabaseManagementException
import org.neo4j.dbms.api.DatabaseNotFoundException
import org.neo4j.graphdb.QueryExecutionException
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException

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

  // helper variable, methods and class

  private type builderType = (PrivilegeMapBuilder, String) => PrivilegeMapBuilder

  private def addNode(source: PrivilegeMapBuilder, name: String): PrivilegeMapBuilder = source.node(name)

  private def addRel(source: PrivilegeMapBuilder, name: String): PrivilegeMapBuilder = source.relationship(name)
}
