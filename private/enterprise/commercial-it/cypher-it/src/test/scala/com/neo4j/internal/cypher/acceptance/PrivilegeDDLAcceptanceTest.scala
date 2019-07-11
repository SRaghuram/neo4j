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
      traverse().role("editor").node("*").map,
      traverse().role("editor").relationship("*").map,
      read().role("editor").node("*").map,
      read().role("editor").relationship("*").map,
      write().role("editor").node("*").map,
      write().role("editor").relationship("*").map,
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
      traverse().role("admin").user("neo4j").node("*").map,
      traverse().role("admin").user("neo4j").relationship("*").map,
      read().role("admin").user("neo4j").node("*").map,
      read().role("admin").user("neo4j").relationship("*").map,
      write().role("admin").user("neo4j").node("*").map,
      write().role("admin").user("neo4j").relationship("*").map,
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

  // Tests for granting and denying privileges

  Seq(
    ("grant", "GRANT", "GRANTED"),
    ("deny", "DENY", "DENIED"),
  ).foreach {
    case (grantOrDeny, grantOrDenyCommand, grantOrDenyRelType) =>

      Seq(
        ("traversal", "TRAVERSE", Set(traverse(grantOrDenyRelType))),
        ("read", "READ (*)", Set(read(grantOrDenyRelType))),
        ("match", "MATCH (*)", Set(traverse(grantOrDenyRelType), read(grantOrDenyRelType)))
      ).foreach {
        case (actionName, actionCommand, startExpected) =>

          test(s"should $grantOrDeny $actionName privilege to custom role for all databases and all element types") {
            // GIVEN
            selectDatabase(SYSTEM_DATABASE_NAME)
            execute("CREATE ROLE custom")

            // WHEN
            execute(s"$grantOrDenyCommand $actionCommand ON GRAPH * TO custom")

            // THEN
            execute("SHOW ROLE custom PRIVILEGES").toSet should be(
              startExpected.map(_.role("custom").node("*").map) ++
                startExpected.map(_.role("custom").relationship("*").map)
            )
          }

          test(s"should fail ${grantOrDeny}ing $actionName privilege for all databases and all labels to non-existing role") {
            // GIVEN
            selectDatabase(SYSTEM_DATABASE_NAME)

            // WHEN
            the[InvalidArgumentsException] thrownBy {
              // WHEN
              execute(s"$grantOrDenyCommand $actionCommand ON GRAPH * NODES * (*) TO custom")
              // THEN
            } should have message "Role 'custom' does not exist."

            // WHEN
            the[InvalidArgumentsException] thrownBy {
              // WHEN
              execute(s"$grantOrDenyCommand $actionCommand ON GRAPH * RELATIONSHIPS * (*) TO custom")
              // THEN
            } should have message "Role 'custom' does not exist."

            // WHEN
            the[InvalidArgumentsException] thrownBy {
              // WHEN
              execute(s"$grantOrDenyCommand $actionCommand ON GRAPH * TO custom")
              // THEN
            } should have message "Role 'custom' does not exist."

            // THEN
            execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set())
          }

          test(s"should fail when ${grantOrDeny}ing $actionName privilege with missing database") {
            // GIVEN
            selectDatabase(SYSTEM_DATABASE_NAME)
            execute("CREATE ROLE custom")
            the[DatabaseNotFoundException] thrownBy {
              execute(s"$grantOrDenyCommand $actionCommand ON GRAPH foo TO custom")
            } should have message "Database 'foo' does not exist."
          }

          test(s"should fail when ${grantOrDeny}ing $actionName privilege to custom role when not on system database") {
            val commandName = actionCommand.split(" ").head
            the[DatabaseManagementException] thrownBy {
              // WHEN
              execute(s"$grantOrDenyCommand $actionCommand ON GRAPH * TO custom")
              // THEN
            } should have message
              s"This is a DDL command and it should be executed against the system database: $grantOrDenyCommand $commandName"
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
                execute("SHOW ROLE custom PRIVILEGES").toSet should be(startExpected.map(expected => segmentFunction(expected.role("custom"), "*").map))
              }

              test(s"should $grantOrDeny $actionName privilege to custom role for all databases but only a specific $segmentName (that does not need to exist)") {
                // GIVEN
                selectDatabase(SYSTEM_DATABASE_NAME)
                execute("CREATE ROLE custom")

                // WHEN
                execute(s"$grantOrDenyCommand $actionCommand ON GRAPH * $segmentCommand A (*) TO custom")

                // THEN
                execute("SHOW ROLE custom PRIVILEGES").toSet should be(startExpected.map(expected => segmentFunction(expected.role("custom"), "A").map))
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
                  be(startExpected.map(expected => segmentFunction(expected.role("custom").database("foo"), "A").map))
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
                  be(startExpected.map(expected => segmentFunction(expected.role("custom").database("foo"), "*").map))
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
                execute("SHOW ROLE custom PRIVILEGES").toSet should be(
                  startExpected.map(expected => segmentFunction(expected.role("custom").database("foo"), "A").map) ++
                    startExpected.map(expected => segmentFunction(expected.role("custom").database("foo"), "B").map)
                )
              }

              test(s"should $grantOrDeny $actionName privilege to custom role for a specific database and multiple ${segmentName}s in one grant") {
                // GIVEN
                selectDatabase(SYSTEM_DATABASE_NAME)
                execute("CREATE ROLE custom")
                execute("CREATE DATABASE foo")

                // WHEN
                execute(s"$grantOrDenyCommand $actionCommand ON GRAPH foo $segmentCommand A, B (*) TO custom")

                // THEN
                execute("SHOW ROLE custom PRIVILEGES").toSet should be(
                  startExpected.map(expected => segmentFunction(expected.role("custom").database("foo"), "A").map) ++
                    startExpected.map(expected => segmentFunction(expected.role("custom").database("foo"), "B").map)
                )
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

          test(s"should $grantOrDeny $actionName privilege to custom role for all databases and all elements") {
            // GIVEN
            selectDatabase(SYSTEM_DATABASE_NAME)
            execute("CREATE ROLE custom")

            // WHEN
            execute(s"$grantOrDenyCommand $actionCommand ON GRAPH * ELEMENTS * (*) TO custom")

            // THEN
            execute("SHOW ROLE custom PRIVILEGES").toSet should be(
              startExpected.map(_.role("custom").node("*").map) ++
                startExpected.map(_.role("custom").relationship("*").map)
            )
          }

          test(s"should $grantOrDeny $actionName privilege to custom role for all databases but only a specific element (that does not need to exist)") {
            // GIVEN
            selectDatabase(SYSTEM_DATABASE_NAME)
            execute("CREATE ROLE custom")

            // WHEN
            execute(s"$grantOrDenyCommand $actionCommand ON GRAPH * ELEMENTS A (*) TO custom")

            // THEN
            execute("SHOW ROLE custom PRIVILEGES").toSet should be(
              startExpected.map(_.role("custom").node("A").map) ++
                startExpected.map(_.role("custom").relationship("A").map)
            )
          }

          test(s"should $grantOrDeny $actionName privilege to custom role for a specific database and a specific element") {
            // GIVEN
            selectDatabase(SYSTEM_DATABASE_NAME)
            execute("CREATE ROLE custom")
            execute("CREATE DATABASE foo")

            // WHEN
            execute(s"$grantOrDenyCommand $actionCommand ON GRAPH foo ELEMENTS A (*) TO custom")

            // THEN
            execute("SHOW ROLE custom PRIVILEGES").toSet should be(
              startExpected.map(_.role("custom").database("foo").node("A").map) ++
                startExpected.map(_.role("custom").database("foo").relationship("A").map)
            )
          }

          test(s"should $grantOrDeny $actionName privilege to custom role for a specific database and all elements") {
            // GIVEN
            selectDatabase(SYSTEM_DATABASE_NAME)
            execute("CREATE ROLE custom")
            execute("CREATE DATABASE foo")

            // WHEN
            execute(s"$grantOrDenyCommand $actionCommand ON GRAPH foo ELEMENTS * (*) TO custom")

            // THEN
            execute("SHOW ROLE custom PRIVILEGES").toSet should be(
              startExpected.map(_.role("custom").database("foo").node("*").map) ++
                startExpected.map(_.role("custom").database("foo").relationship("*").map)
            )
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
            execute("SHOW ROLE custom PRIVILEGES").toSet should be(
              startExpected.map(_.role("custom").database("foo").node("A").map) ++
                startExpected.map(_.role("custom").database("foo").relationship("A").map) ++
                startExpected.map(_.role("custom").database("foo").node("B").map) ++
                startExpected.map(_.role("custom").database("foo").relationship("B").map)
            )
          }

          test(s"should $grantOrDeny $actionName privilege to custom role for a specific database and multiple elements in one grant") {
            // GIVEN
            selectDatabase(SYSTEM_DATABASE_NAME)
            execute("CREATE ROLE custom")
            execute("CREATE DATABASE foo")

            // WHEN
            execute(s"$grantOrDenyCommand $actionCommand ON GRAPH foo ELEMENTS A, B (*) TO custom")

            // THEN
            execute("SHOW ROLE custom PRIVILEGES").toSet should be(
              startExpected.map(_.role("custom").database("foo").node("A").map) ++
                startExpected.map(_.role("custom").database("foo").relationship("A").map) ++
                startExpected.map(_.role("custom").database("foo").node("B").map) ++
                startExpected.map(_.role("custom").database("foo").relationship("B").map)
            )
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
            execute("SHOW ROLE role1 PRIVILEGES").toSet should be(
              startExpected.map(_.role("role1").database("foo").node("A").map) ++
                startExpected.map(_.role("role1").database("foo").relationship("A").map) ++
                startExpected.map(_.role("role1").database("foo").node("B").map) ++
                startExpected.map(_.role("role1").database("foo").relationship("B").map)
            )
            execute("SHOW ROLE role2 PRIVILEGES").toSet should be(
              startExpected.map(_.role("role2").database("foo").node("A").map) ++
                startExpected.map(_.role("role2").database("foo").relationship("A").map) ++
                startExpected.map(_.role("role2").database("foo").node("B").map) ++
                startExpected.map(_.role("role2").database("foo").relationship("B").map)
            )
          }
      }
  }

  // Tests for granting and denying privileges on properties
  Seq(
    ("grant", "GRANT", "GRANTED"),
    ("deny", "DENY", "DENIED"),
  ).foreach {
    case (grantOrDeny, grantOrDenyCommand, grantOrDenyRelType) =>

      Seq(
        ("read", "READ", Set.empty),
        ("match", "MATCH", Set(traverse(grantOrDenyRelType)))
      ).foreach {
        case (actionName, actionCommand, expectedTraverse) =>

          test(s"should $grantOrDeny $actionName privilege for specific property to custom role for all databases and all element types") {
            // GIVEN
            selectDatabase(SYSTEM_DATABASE_NAME)
            execute("CREATE ROLE custom")

            // WHEN
            execute(s"$grantOrDenyCommand $actionCommand (bar) ON GRAPH * TO custom")

            // THEN
            execute("SHOW ROLE custom PRIVILEGES").toSet should be(
              Set(read(grantOrDenyRelType).role("custom").property("bar").node("*").map,
                read(grantOrDenyRelType).role("custom").property("bar").relationship("*").map) ++
                expectedTraverse.map(_.role("custom").node("*").map) ++
                expectedTraverse.map(_.role("custom").relationship("*").map)
            )
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
                execute(s"$grantOrDenyCommand $actionCommand (bar) ON GRAPH * $segmentCommand * (*) TO custom")

                // THEN
                execute("SHOW ROLE custom PRIVILEGES").toSet should be(
                  Set(segmentFunction(read(grantOrDenyRelType).role("custom").property("bar"), "*").map) ++
                    expectedTraverse.map(expected => segmentFunction(expected.role("custom"), "*").map)
                )
              }

              test(s"should $grantOrDeny $actionName privilege for specific property to custom role for all databases but only a specific $segmentName") {
                // GIVEN
                selectDatabase(SYSTEM_DATABASE_NAME)
                execute("CREATE ROLE custom")

                // WHEN
                execute(s"$grantOrDenyCommand $actionCommand (bar) ON GRAPH * $segmentCommand A (*) TO custom")

                // THEN
                execute("SHOW ROLE custom PRIVILEGES").toSet should be(
                  Set(segmentFunction(read(grantOrDenyRelType).role("custom").property("bar"), "A").map) ++
                    expectedTraverse.map(expected => segmentFunction(expected.role("custom"), "A").map)
                )
              }

              test(s"should $grantOrDeny $actionName privilege for specific property to custom role for a specific database and a specific $segmentName") {
                // GIVEN
                selectDatabase(SYSTEM_DATABASE_NAME)
                execute("CREATE ROLE custom")
                execute("CREATE DATABASE foo")

                // WHEN
                execute(s"$grantOrDenyCommand $actionCommand (bar) ON GRAPH foo $segmentCommand A (*) TO custom")

                // THEN
                execute("SHOW ROLE custom PRIVILEGES").toSet should be(
                  Set(segmentFunction(read(grantOrDenyRelType).database("foo").role("custom").property("bar"), "A").map) ++
                    expectedTraverse.map(expected => segmentFunction(expected.database("foo").role("custom"), "A").map)
                )
              }

              test(s"should $grantOrDeny $actionName privilege for specific property to custom role for a specific database and all ${segmentName}s") {
                // GIVEN
                selectDatabase(SYSTEM_DATABASE_NAME)
                execute("CREATE ROLE custom")
                execute("CREATE DATABASE foo")

                // WHEN
                execute(s"$grantOrDenyCommand $actionCommand (bar) ON GRAPH foo $segmentCommand * (*) TO custom")

                // THEN
                execute("SHOW ROLE custom PRIVILEGES").toSet should be(
                  Set(segmentFunction(read(grantOrDenyRelType).database("foo").role("custom").property("bar"), "*").map) ++
                    expectedTraverse.map(expected => segmentFunction(expected.database("foo").role("custom"), "*").map)
                )
              }

              test(s"should $grantOrDeny $actionName privilege for specific property to custom role for a specific database and multiple ${segmentName}s") {
                // GIVEN
                selectDatabase(SYSTEM_DATABASE_NAME)
                execute("CREATE ROLE custom")
                execute("CREATE DATABASE foo")

                // WHEN
                execute(s"$grantOrDenyCommand $actionCommand (bar) ON GRAPH foo $segmentCommand A (*) TO custom")
                execute(s"$grantOrDenyCommand $actionCommand (bar) ON GRAPH foo $segmentCommand B (*) TO custom")

                // THEN
                execute("SHOW ROLE custom PRIVILEGES").toSet should be(
                  Set(segmentFunction(read(grantOrDenyRelType).database("foo").role("custom").property("bar"), "A").map,
                    segmentFunction(read(grantOrDenyRelType).database("foo").role("custom").property("bar"), "B").map) ++
                    expectedTraverse.map(expected => segmentFunction(expected.database("foo").role("custom"), "A").map) ++
                    expectedTraverse.map(expected => segmentFunction(expected.database("foo").role("custom"), "B").map)
                )
              }

              test(s"should $grantOrDeny $actionName privilege for multiple properties to custom role for a specific database and specific $segmentName") {
                // GIVEN
                selectDatabase(SYSTEM_DATABASE_NAME)
                execute("CREATE ROLE custom")
                execute("CREATE DATABASE foo")

                // WHEN
                execute(s"$grantOrDenyCommand $actionCommand (bar) ON GRAPH foo $segmentCommand A (*) TO custom")
                execute(s"$grantOrDenyCommand $actionCommand (baz) ON GRAPH foo $segmentCommand A (*) TO custom")

                // THEN
                execute("SHOW ROLE custom PRIVILEGES").toSet should be(
                  Set(segmentFunction(read(grantOrDenyRelType).database("foo").role("custom").property("bar"), "A").map,
                    segmentFunction(read(grantOrDenyRelType).database("foo").role("custom").property("baz"), "A").map) ++
                    expectedTraverse.map(expected => segmentFunction(expected.database("foo").role("custom"), "A").map)
                )
              }

              test(s"should $grantOrDeny $actionName privilege for multiple properties to custom role for a specific database and multiple ${segmentName}s") {
                // GIVEN
                selectDatabase(SYSTEM_DATABASE_NAME)
                execute("CREATE ROLE custom")
                execute("CREATE DATABASE foo")

                // WHEN
                execute(s"$grantOrDenyCommand $actionCommand (bar) ON GRAPH foo $segmentCommand A (*) TO custom")
                execute(s"$grantOrDenyCommand $actionCommand (baz) ON GRAPH foo $segmentCommand B (*) TO custom")

                // THEN
                execute("SHOW ROLE custom PRIVILEGES").toSet should be(
                  Set(segmentFunction(read(grantOrDenyRelType).database("foo").role("custom").property("bar"), "A").map,
                    segmentFunction(read(grantOrDenyRelType).database("foo").role("custom").property("baz"), "B").map) ++
                    expectedTraverse.map(expected => segmentFunction(expected.database("foo").role("custom"), "A").map) ++
                    expectedTraverse.map(expected => segmentFunction(expected.database("foo").role("custom"), "B").map)
                )
              }

              test(s"should $grantOrDeny $actionName privilege for multiple properties to multiple roles for a specific database and multiple ${segmentName}s in a single grant") {
                // GIVEN
                selectDatabase(SYSTEM_DATABASE_NAME)
                execute("CREATE ROLE role1")
                execute("CREATE ROLE role2")
                execute("CREATE ROLE role3")
                execute("CREATE DATABASE foo")

                // WHEN
                execute(s"$grantOrDenyCommand $actionCommand (a, b, c) ON GRAPH foo $segmentCommand A, B, C (*) TO role1, role2, role3")

                // THEN
                val expected = (for (l <- Seq("A", "B", "C")) yield {
                  (for (p <- Seq("a", "b", "c")) yield {
                    segmentFunction(read(grantOrDenyRelType).database("foo").property(p), l)
                  }) ++ expectedTraverse.map(expected => segmentFunction(expected.database("foo"), l))
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
            execute(s"$grantOrDenyCommand $actionCommand (bar) ON GRAPH * ELEMENTS * (*) TO custom")

            // THEN
            execute("SHOW ROLE custom PRIVILEGES").toSet should be(
              Set(read(grantOrDenyRelType).role("custom").property("bar").node("*").map,
                read(grantOrDenyRelType).role("custom").property("bar").relationship("*").map) ++
                expectedTraverse.map(_.role("custom").node("*").map) ++
                expectedTraverse.map(_.role("custom").relationship("*").map)
            )
          }

          test(s"should $grantOrDeny $actionName privilege for specific property to custom role for all databases but only a specific element") {
            // GIVEN
            selectDatabase(SYSTEM_DATABASE_NAME)
            execute("CREATE ROLE custom")

            // WHEN
            execute(s"$grantOrDenyCommand $actionCommand (bar) ON GRAPH * ELEMENTS A (*) TO custom")

            // THEN
            execute("SHOW ROLE custom PRIVILEGES").toSet should be(
              Set(read(grantOrDenyRelType).role("custom").property("bar").node("A").map,
                read(grantOrDenyRelType).role("custom").property("bar").relationship("A").map) ++
                expectedTraverse.map(_.role("custom").node("A").map) ++
                expectedTraverse.map(_.role("custom").relationship("A").map)
            )
          }

          test(s"should $grantOrDeny $actionName privilege for specific property to custom role for a specific database and a specific element") {
            // GIVEN
            selectDatabase(SYSTEM_DATABASE_NAME)
            execute("CREATE ROLE custom")
            execute("CREATE DATABASE foo")

            // WHEN
            execute(s"$grantOrDenyCommand $actionCommand (bar) ON GRAPH foo ELEMENTS A (*) TO custom")

            // THEN
            execute("SHOW ROLE custom PRIVILEGES").toSet should be(
              Set(read(grantOrDenyRelType).role("custom").database("foo").property("bar").node("A").map,
                read(grantOrDenyRelType).role("custom").database("foo").property("bar").relationship("A").map) ++
                expectedTraverse.map(_.role("custom").database("foo").node("A").map) ++
                expectedTraverse.map(_.role("custom").database("foo").relationship("A").map)
            )
          }

          test(s"should $grantOrDeny $actionName privilege for specific property to custom role for a specific database and all elements") {
            // GIVEN
            selectDatabase(SYSTEM_DATABASE_NAME)
            execute("CREATE ROLE custom")
            execute("CREATE DATABASE foo")

            // WHEN
            execute(s"$grantOrDenyCommand $actionCommand (bar) ON GRAPH foo ELEMENTS * (*) TO custom")

            // THEN
            execute("SHOW ROLE custom PRIVILEGES").toSet should be(
              Set(read(grantOrDenyRelType).role("custom").database("foo").property("bar").node("*").map,
                read(grantOrDenyRelType).role("custom").database("foo").property("bar").relationship("*").map) ++
                expectedTraverse.map(_.role("custom").database("foo").node("*").map) ++
                expectedTraverse.map(_.role("custom").database("foo").relationship("*").map)
            )
          }

          test(s"should $grantOrDeny $actionName privilege for specific property to custom role for a specific database and multiple elements") {
            // GIVEN
            selectDatabase(SYSTEM_DATABASE_NAME)
            execute("CREATE ROLE custom")
            execute("CREATE DATABASE foo")

            // WHEN
            execute(s"$grantOrDenyCommand $actionCommand (bar) ON GRAPH foo ELEMENTS A (*) TO custom")
            execute(s"$grantOrDenyCommand $actionCommand (bar) ON GRAPH foo ELEMENTS B (*) TO custom")

            // THEN
            execute("SHOW ROLE custom PRIVILEGES").toSet should be(
              Set(read(grantOrDenyRelType).role("custom").database("foo").property("bar").node("A").map,
                read(grantOrDenyRelType).role("custom").database("foo").property("bar").relationship("A").map,
                read(grantOrDenyRelType).role("custom").database("foo").property("bar").node("B").map,
                read(grantOrDenyRelType).role("custom").database("foo").property("bar").relationship("B").map) ++
                expectedTraverse.map(_.role("custom").database("foo").node("A").map) ++
                expectedTraverse.map(_.role("custom").database("foo").relationship("A").map) ++
                expectedTraverse.map(_.role("custom").database("foo").node("B").map) ++
                expectedTraverse.map(_.role("custom").database("foo").relationship("B").map)
            )
          }

          test(s"should $grantOrDeny $actionName privilege for multiple properties to custom role for a specific database and specific element") {
            // GIVEN
            selectDatabase(SYSTEM_DATABASE_NAME)
            execute("CREATE ROLE custom")
            execute("CREATE DATABASE foo")

            // WHEN
            execute(s"$grantOrDenyCommand $actionCommand (bar) ON GRAPH foo ELEMENTS A (*) TO custom")
            execute(s"$grantOrDenyCommand $actionCommand (baz) ON GRAPH foo ELEMENTS A (*) TO custom")

            // THEN
            execute("SHOW ROLE custom PRIVILEGES").toSet should be(
              Set(read(grantOrDenyRelType).role("custom").database("foo").property("bar").node("A").map,
                read(grantOrDenyRelType).role("custom").database("foo").property("bar").relationship("A").map,
                read(grantOrDenyRelType).role("custom").database("foo").property("baz").node("A").map,
                read(grantOrDenyRelType).role("custom").database("foo").property("baz").relationship("A").map) ++
                expectedTraverse.map(_.role("custom").database("foo").node("A").map) ++
                expectedTraverse.map(_.role("custom").database("foo").relationship("A").map)
            )
          }

          test(s"should $grantOrDeny $actionName privilege for multiple properties to custom role for a specific database and multiple elements") {
            // GIVEN
            selectDatabase(SYSTEM_DATABASE_NAME)
            execute("CREATE ROLE custom")
            execute("CREATE DATABASE foo")

            // WHEN
            execute(s"$grantOrDenyCommand $actionCommand (bar) ON GRAPH foo ELEMENTS A (*) TO custom")
            execute(s"$grantOrDenyCommand $actionCommand (baz) ON GRAPH foo ELEMENTS B (*) TO custom")

            // THEN
            execute("SHOW ROLE custom PRIVILEGES").toSet should be(
              Set(read(grantOrDenyRelType).role("custom").database("foo").property("bar").node("A").map,
                read(grantOrDenyRelType).role("custom").database("foo").property("bar").relationship("A").map,
                read(grantOrDenyRelType).role("custom").database("foo").property("baz").node("B").map,
                read(grantOrDenyRelType).role("custom").database("foo").property("baz").relationship("B").map) ++
                expectedTraverse.map(_.role("custom").database("foo").node("A").map) ++
                expectedTraverse.map(_.role("custom").database("foo").relationship("A").map) ++
                expectedTraverse.map(_.role("custom").database("foo").node("B").map) ++
                expectedTraverse.map(_.role("custom").database("foo").relationship("B").map)
            )
          }

          test(s"should $grantOrDeny $actionName privilege for multiple properties to multiple roles for a specific database and multiple elements in a single grant") {
            // GIVEN
            selectDatabase(SYSTEM_DATABASE_NAME)
            execute("CREATE ROLE role1")
            execute("CREATE ROLE role2")
            execute("CREATE ROLE role3")
            execute("CREATE DATABASE foo")

            // WHEN
            execute(s"$grantOrDenyCommand $actionCommand (a, b, c) ON GRAPH foo ELEMENTS A, B, C (*) TO role1, role2, role3")

            // THEN
            val expected = (for (l <- Seq("A", "B", "C")) yield {
              (for (p <- Seq("a", "b", "c")) yield {
                Seq(read(grantOrDenyRelType).database("foo").property(p).node(l),
                  read(grantOrDenyRelType).database("foo").property(p).relationship(l))
              }).flatten ++ expectedTraverse.map(expected => expected.database("foo").node(l)) ++
                expectedTraverse.map(expected => expected.database("foo").relationship(l))
            }).flatten

            execute("SHOW ROLE role1 PRIVILEGES").toSet should be(expected.map(_.role("role1").map).toSet)
            execute("SHOW ROLE role2 PRIVILEGES").toSet should be(expected.map(_.role("role2").map).toSet)
            execute("SHOW ROLE role3 PRIVILEGES").toSet should be(expected.map(_.role("role3").map).toSet)
          }
      }
  }

  // Tests for REVOKE READ, TRAVERSE and MATCH

  Seq(
    ("grant", "GRANT", "GRANTED", "GRANT "),
    ("deny", "DENY", "DENIED", "DENY "),
    ("grant", "GRANT", "GRANTED", ""),
    ("deny", "DENY", "DENIED", ""),
  ).foreach {
    case (grantOrDeny, grantOrDenyCommand, grantOrDenyRelType, revokeType) =>

      test(s"should revoke correct $grantOrDeny read privilege different label qualifier with REVOKE $revokeType") {
        // GIVEN
        selectDatabase(SYSTEM_DATABASE_NAME)
        execute("CREATE ROLE custom")
        execute("CREATE DATABASE foo")
        execute(s"$grantOrDenyCommand READ (bar) ON GRAPH foo NODES * (*) TO custom")
        execute(s"$grantOrDenyCommand READ (bar) ON GRAPH foo NODES A (*) TO custom")
        execute(s"$grantOrDenyCommand READ (bar) ON GRAPH foo NODES B (*) TO custom")
        execute(s"$grantOrDenyCommand READ (bar) ON GRAPH foo RELATIONSHIPS * (*) TO custom")
        execute(s"$grantOrDenyCommand READ (bar) ON GRAPH foo RELATIONSHIPS A (*) TO custom")
        execute(s"$grantOrDenyCommand READ (bar) ON GRAPH foo RELATIONSHIPS B (*) TO custom")

        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          read(grantOrDenyRelType).database("foo").role("custom").property("bar").relationship("*").map,
          read(grantOrDenyRelType).database("foo").role("custom").property("bar").relationship("A").map,
          read(grantOrDenyRelType).database("foo").role("custom").property("bar").relationship("B").map,
          read(grantOrDenyRelType).database("foo").role("custom").property("bar").node("*").map,
          read(grantOrDenyRelType).database("foo").role("custom").property("bar").node("A").map,
          read(grantOrDenyRelType).database("foo").role("custom").property("bar").node("B").map
        ))

        // WHEN
        execute(s"REVOKE $revokeType READ (bar) ON GRAPH foo NODES A (*) FROM custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          read(grantOrDenyRelType).database("foo").role("custom").property("bar").relationship("*").map,
          read(grantOrDenyRelType).database("foo").role("custom").property("bar").relationship("A").map,
          read(grantOrDenyRelType).database("foo").role("custom").property("bar").relationship("B").map,
          read(grantOrDenyRelType).database("foo").role("custom").property("bar").node("*").map,
          read(grantOrDenyRelType).database("foo").role("custom").property("bar").node("B").map
        ))

        // WHEN
        execute(s"REVOKE $revokeType READ (bar) ON GRAPH foo NODES * (*) FROM custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          read(grantOrDenyRelType).database("foo").role("custom").property("bar").relationship("*").map,
          read(grantOrDenyRelType).database("foo").role("custom").property("bar").relationship("A").map,
          read(grantOrDenyRelType).database("foo").role("custom").property("bar").relationship("B").map,
          read(grantOrDenyRelType).database("foo").role("custom").property("bar").node("B").map
        ))
      }

      test(s"should revoke correct $grantOrDeny read privilege different relationship type qualifier with REVOKE $revokeType") {
        // GIVEN
        selectDatabase(SYSTEM_DATABASE_NAME)
        execute("CREATE ROLE custom")
        execute("CREATE DATABASE foo")
        execute(s"$grantOrDenyCommand READ (bar) ON GRAPH foo NODES * (*) TO custom")
        execute(s"$grantOrDenyCommand READ (bar) ON GRAPH foo NODES A (*) TO custom")
        execute(s"$grantOrDenyCommand READ (bar) ON GRAPH foo NODES B (*) TO custom")
        execute(s"$grantOrDenyCommand READ (bar) ON GRAPH foo RELATIONSHIPS * (*) TO custom")
        execute(s"$grantOrDenyCommand READ (bar) ON GRAPH foo RELATIONSHIPS A (*) TO custom")
        execute(s"$grantOrDenyCommand READ (bar) ON GRAPH foo RELATIONSHIPS B (*) TO custom")

        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          read(grantOrDenyRelType).database("foo").role("custom").property("bar").node("*").map,
          read(grantOrDenyRelType).database("foo").role("custom").property("bar").node("A").map,
          read(grantOrDenyRelType).database("foo").role("custom").property("bar").node("B").map,
          read(grantOrDenyRelType).database("foo").role("custom").property("bar").relationship("*").map,
          read(grantOrDenyRelType).database("foo").role("custom").property("bar").relationship("A").map,
          read(grantOrDenyRelType).database("foo").role("custom").property("bar").relationship("B").map
        ))

        // WHEN
        execute(s"REVOKE $revokeType READ (bar) ON GRAPH foo RELATIONSHIPS A (*) FROM custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          read(grantOrDenyRelType).database("foo").role("custom").property("bar").node("*").map,
          read(grantOrDenyRelType).database("foo").role("custom").property("bar").node("A").map,
          read(grantOrDenyRelType).database("foo").role("custom").property("bar").node("B").map,
          read(grantOrDenyRelType).database("foo").role("custom").property("bar").relationship("*").map,
          read(grantOrDenyRelType).database("foo").role("custom").property("bar").relationship("B").map
        ))

        // WHEN
        execute(s"REVOKE $revokeType READ (bar) ON GRAPH foo RELATIONSHIPS * (*) FROM custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          read(grantOrDenyRelType).database("foo").role("custom").property("bar").node("*").map,
          read(grantOrDenyRelType).database("foo").role("custom").property("bar").node("A").map,
          read(grantOrDenyRelType).database("foo").role("custom").property("bar").node("B").map,
          read(grantOrDenyRelType).database("foo").role("custom").property("bar").relationship("B").map
        ))
      }

      test(s"should revoke correct $grantOrDeny read privilege different element type qualifier with REVOKE $revokeType") {
        // GIVEN
        selectDatabase(SYSTEM_DATABASE_NAME)
        execute("CREATE ROLE custom")
        execute("CREATE DATABASE foo")
        execute(s"$grantOrDenyCommand READ (bar) ON GRAPH foo ELEMENTS * (*) TO custom")
        execute(s"$grantOrDenyCommand READ (bar) ON GRAPH foo ELEMENTS A (*) TO custom")
        execute(s"$grantOrDenyCommand READ (bar) ON GRAPH foo ELEMENTS B, C (*) TO custom")

        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          read(grantOrDenyRelType).database("foo").role("custom").property("bar").node("*").map,
          read(grantOrDenyRelType).database("foo").role("custom").property("bar").node("A").map,
          read(grantOrDenyRelType).database("foo").role("custom").property("bar").node("B").map,
          read(grantOrDenyRelType).database("foo").role("custom").property("bar").node("C").map,
          read(grantOrDenyRelType).database("foo").role("custom").property("bar").relationship("*").map,
          read(grantOrDenyRelType).database("foo").role("custom").property("bar").relationship("A").map,
          read(grantOrDenyRelType).database("foo").role("custom").property("bar").relationship("B").map,
          read(grantOrDenyRelType).database("foo").role("custom").property("bar").relationship("C").map
        ))

        // WHEN
        execute(s"REVOKE $revokeType READ (bar) ON GRAPH foo ELEMENTS A (*) FROM custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          read(grantOrDenyRelType).database("foo").role("custom").property("bar").node("*").map,
          read(grantOrDenyRelType).database("foo").role("custom").property("bar").node("B").map,
          read(grantOrDenyRelType).database("foo").role("custom").property("bar").node("C").map,
          read(grantOrDenyRelType).database("foo").role("custom").property("bar").relationship("*").map,
          read(grantOrDenyRelType).database("foo").role("custom").property("bar").relationship("B").map,
          read(grantOrDenyRelType).database("foo").role("custom").property("bar").relationship("C").map
        ))

        // WHEN
        execute(s"REVOKE $revokeType READ (bar) ON GRAPH foo ELEMENTS * (*) FROM custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          read(grantOrDenyRelType).database("foo").role("custom").property("bar").node("B").map,
          read(grantOrDenyRelType).database("foo").role("custom").property("bar").node("C").map,
          read(grantOrDenyRelType).database("foo").role("custom").property("bar").relationship("B").map,
          read(grantOrDenyRelType).database("foo").role("custom").property("bar").relationship("C").map
        ))

        // WHEN
        execute(s"REVOKE $revokeType READ (bar) ON GRAPH foo ELEMENTS B, C (*) FROM custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set.empty)
      }

      test(s"should revoke correct $grantOrDeny read privilege different property with REVOKE $revokeType") {
        // GIVEN
        selectDatabase(SYSTEM_DATABASE_NAME)
        execute("CREATE ROLE custom")
        execute("CREATE DATABASE foo")
        execute(s"$grantOrDenyCommand READ (*) ON GRAPH foo TO custom")
        execute(s"$grantOrDenyCommand READ (a) ON GRAPH foo TO custom")
        execute(s"$grantOrDenyCommand READ (b) ON GRAPH foo TO custom")

        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          read(grantOrDenyRelType).database("foo").role("custom").node("*").property("a").map,
          read(grantOrDenyRelType).database("foo").role("custom").node("*").property("b").map,
          read(grantOrDenyRelType).database("foo").role("custom").node("*").map,
          read(grantOrDenyRelType).database("foo").role("custom").relationship("*").property("a").map,
          read(grantOrDenyRelType).database("foo").role("custom").relationship("*").property("b").map,
          read(grantOrDenyRelType).database("foo").role("custom").relationship("*").map
        ))

        // WHEN
        execute(s"REVOKE $revokeType READ (a) ON GRAPH foo NODES * (*) FROM custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          read(grantOrDenyRelType).database("foo").role("custom").node("*").property("b").map,
          read(grantOrDenyRelType).database("foo").role("custom").node("*").map,
          read(grantOrDenyRelType).database("foo").role("custom").relationship("*").property("a").map,
          read(grantOrDenyRelType).database("foo").role("custom").relationship("*").property("b").map,
          read(grantOrDenyRelType).database("foo").role("custom").relationship("*").map
        ))

        // WHEN
        execute(s"REVOKE $revokeType READ (*) ON GRAPH foo RELATIONSHIPS * (*) FROM custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          read(grantOrDenyRelType).database("foo").role("custom").node("*").property("b").map,
          read(grantOrDenyRelType).database("foo").role("custom").node("*").map,
          read(grantOrDenyRelType).database("foo").role("custom").relationship("*").property("a").map,
          read(grantOrDenyRelType).database("foo").role("custom").relationship("*").property("b").map
        ))

        // WHEN
        execute(s"REVOKE $revokeType READ (b) ON GRAPH foo FROM custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          read(grantOrDenyRelType).database("foo").role("custom").node("*").map,
          read(grantOrDenyRelType).database("foo").role("custom").relationship("*").property("a").map
        ))
      }

      test(s"should revoke correct $grantOrDeny read privilege different databases with REVOKE $revokeType") {
        // GIVEN
        selectDatabase(SYSTEM_DATABASE_NAME)
        execute("CREATE ROLE custom")
        execute("CREATE DATABASE foo")
        execute("CREATE DATABASE bar")
        execute(s"$grantOrDenyCommand READ (*) ON GRAPH * TO custom")
        execute(s"$grantOrDenyCommand READ (*) ON GRAPH foo TO custom")
        execute(s"$grantOrDenyCommand READ (*) ON GRAPH bar TO custom")

        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          read(grantOrDenyRelType).role("custom").node("*").map,
          read(grantOrDenyRelType).role("custom").node("*").database("foo").map,
          read(grantOrDenyRelType).role("custom").node("*").database("bar").map,
          read(grantOrDenyRelType).role("custom").relationship("*").map,
          read(grantOrDenyRelType).role("custom").relationship("*").database("foo").map,
          read(grantOrDenyRelType).role("custom").relationship("*").database("bar").map
        ))

        // WHEN
        execute(s"REVOKE $revokeType READ (*) ON GRAPH foo FROM custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          read(grantOrDenyRelType).role("custom").node("*").map,
          read(grantOrDenyRelType).role("custom").node("*").database("bar").map,
          read(grantOrDenyRelType).role("custom").relationship("*").map,
          read(grantOrDenyRelType).role("custom").relationship("*").database("bar").map
        ))

        // WHEN
        execute(s"REVOKE $revokeType READ (*) ON GRAPH * FROM custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          read(grantOrDenyRelType).role("custom").node("*").database("bar").map,
          read(grantOrDenyRelType).role("custom").relationship("*").database("bar").map
        ))
      }

      test(s"should revoke correct $grantOrDeny traverse node privilege different label qualifier with REVOKE $revokeType") {
        // GIVEN
        selectDatabase(SYSTEM_DATABASE_NAME)
        execute("CREATE ROLE custom")
        execute("CREATE DATABASE foo")
        execute(s"$grantOrDenyCommand TRAVERSE ON GRAPH foo TO custom")
        execute(s"$grantOrDenyCommand TRAVERSE ON GRAPH foo NODES A (*) TO custom")
        execute(s"$grantOrDenyCommand TRAVERSE ON GRAPH foo NODES B (*) TO custom")

        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          traverse(grantOrDenyRelType).database("foo").role("custom").node("*").map,
          traverse(grantOrDenyRelType).database("foo").role("custom").relationship("*").map,
          traverse(grantOrDenyRelType).database("foo").role("custom").node("A").map,
          traverse(grantOrDenyRelType).database("foo").role("custom").node("B").map
        ))

        // WHEN
        execute(s"REVOKE $revokeType TRAVERSE ON GRAPH foo NODES A (*) FROM custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          traverse(grantOrDenyRelType).database("foo").role("custom").node("*").map,
          traverse(grantOrDenyRelType).database("foo").role("custom").relationship("*").map,
          traverse(grantOrDenyRelType).database("foo").role("custom").node("B").map
        ))

        // WHEN
        execute(s"REVOKE $revokeType TRAVERSE ON GRAPH foo NODES * (*) FROM custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          traverse(grantOrDenyRelType).database("foo").role("custom").relationship("*").map,
          traverse(grantOrDenyRelType).database("foo").role("custom").node("B").map
        ))
      }

      test(s"should revoke correct $grantOrDeny traverse node privilege different databases with REVOKE $revokeType") {
        // GIVEN
        selectDatabase(SYSTEM_DATABASE_NAME)
        execute("CREATE ROLE custom")
        execute("CREATE DATABASE foo")
        execute("CREATE DATABASE bar")
        execute(s"$grantOrDenyCommand TRAVERSE ON GRAPH * TO custom")
        execute(s"$grantOrDenyCommand TRAVERSE ON GRAPH foo NODES * (*) TO custom")
        execute(s"$grantOrDenyCommand TRAVERSE ON GRAPH bar NODES * (*) TO custom")

        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          traverse(grantOrDenyRelType).role("custom").node("*").map,
          traverse(grantOrDenyRelType).role("custom").relationship("*").map,
          traverse(grantOrDenyRelType).role("custom").node("*").database("foo").map,
          traverse(grantOrDenyRelType).role("custom").node("*").database("bar").map
        ))

        // WHEN
        execute(s"REVOKE $revokeType TRAVERSE ON GRAPH foo NODES * (*) FROM custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          traverse(grantOrDenyRelType).role("custom").node("*").map,
          traverse(grantOrDenyRelType).role("custom").relationship("*").map,
          traverse(grantOrDenyRelType).role("custom").node("*").database("bar").map
        ))

        // WHEN
        execute(s"REVOKE $revokeType TRAVERSE ON GRAPH * NODES * (*) FROM custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          traverse(grantOrDenyRelType).role("custom").relationship("*").map,
          traverse(grantOrDenyRelType).role("custom").node("*").database("bar").map
        ))
      }

      test(s"should revoke correct $grantOrDeny traverse relationships privilege different type qualifier with REVOKE $revokeType") {
        // GIVEN
        selectDatabase(SYSTEM_DATABASE_NAME)
        execute("CREATE ROLE custom")
        execute("CREATE DATABASE foo")
        execute(s"$grantOrDenyCommand TRAVERSE ON GRAPH foo TO custom")
        execute(s"$grantOrDenyCommand TRAVERSE ON GRAPH foo RELATIONSHIPS A (*) TO custom")
        execute(s"$grantOrDenyCommand TRAVERSE ON GRAPH foo RELATIONSHIPS B (*) TO custom")

        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          traverse(grantOrDenyRelType).database("foo").role("custom").node("*").map,
          traverse(grantOrDenyRelType).database("foo").role("custom").relationship("*").map,
          traverse(grantOrDenyRelType).database("foo").role("custom").relationship("A").map,
          traverse(grantOrDenyRelType).database("foo").role("custom").relationship("B").map
        ))

        // WHEN
        execute(s"REVOKE $revokeType TRAVERSE ON GRAPH foo RELATIONSHIPS A (*) FROM custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          traverse(grantOrDenyRelType).database("foo").role("custom").node("*").map,
          traverse(grantOrDenyRelType).database("foo").role("custom").relationship("*").map,
          traverse(grantOrDenyRelType).database("foo").role("custom").relationship("B").map
        ))

        // WHEN
        execute(s"REVOKE $revokeType TRAVERSE ON GRAPH foo RELATIONSHIPS * (*) FROM custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          traverse(grantOrDenyRelType).database("foo").role("custom").node("*").map,
          traverse(grantOrDenyRelType).database("foo").role("custom").relationship("B").map
        ))
      }

      test(s"should revoke correct $grantOrDeny traverse relationship privilege different databases with REVOKE $revokeType") {
        // GIVEN
        selectDatabase(SYSTEM_DATABASE_NAME)
        execute("CREATE ROLE custom")
        execute("CREATE DATABASE foo")
        execute("CREATE DATABASE bar")
        execute(s"$grantOrDenyCommand TRAVERSE ON GRAPH * TO custom")
        execute(s"$grantOrDenyCommand TRAVERSE ON GRAPH foo RELATIONSHIPS * (*) TO custom")
        execute(s"$grantOrDenyCommand TRAVERSE ON GRAPH bar RELATIONSHIPS * (*) TO custom")

        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          traverse(grantOrDenyRelType).role("custom").node("*").map,
          traverse(grantOrDenyRelType).role("custom").relationship("*").map,
          traverse(grantOrDenyRelType).role("custom").relationship("*").database("foo").map,
          traverse(grantOrDenyRelType).role("custom").relationship("*").database("bar").map
        ))

        // WHEN
        execute(s"REVOKE $revokeType TRAVERSE ON GRAPH foo RELATIONSHIPS * (*) FROM custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          traverse(grantOrDenyRelType).role("custom").node("*").map,
          traverse(grantOrDenyRelType).role("custom").relationship("*").map,
          traverse(grantOrDenyRelType).role("custom").relationship("*").database("bar").map
        ))

        // WHEN
        execute(s"REVOKE $revokeType TRAVERSE ON GRAPH * RELATIONSHIPS * (*) FROM custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          traverse(grantOrDenyRelType).role("custom").node("*").map,
          traverse(grantOrDenyRelType).role("custom").relationship("*").database("bar").map
        ))
      }

      test(s"should revoke correct $grantOrDeny traverse elements privilege different type qualifier with REVOKE $revokeType") {
        // GIVEN
        selectDatabase(SYSTEM_DATABASE_NAME)
        execute("CREATE ROLE custom")
        execute("CREATE DATABASE foo")
        execute(s"$grantOrDenyCommand TRAVERSE ON GRAPH foo TO custom")
        execute(s"$grantOrDenyCommand TRAVERSE ON GRAPH foo ELEMENTS A, B, C (*) TO custom")

        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          traverse(grantOrDenyRelType).database("foo").role("custom").node("*").map,
          traverse(grantOrDenyRelType).database("foo").role("custom").node("A").map,
          traverse(grantOrDenyRelType).database("foo").role("custom").node("B").map,
          traverse(grantOrDenyRelType).database("foo").role("custom").node("C").map,
          traverse(grantOrDenyRelType).database("foo").role("custom").relationship("*").map,
          traverse(grantOrDenyRelType).database("foo").role("custom").relationship("A").map,
          traverse(grantOrDenyRelType).database("foo").role("custom").relationship("B").map,
          traverse(grantOrDenyRelType).database("foo").role("custom").relationship("C").map
        ))

        // WHEN
        execute(s"REVOKE $revokeType TRAVERSE ON GRAPH foo ELEMENTS A (*) FROM custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          traverse(grantOrDenyRelType).database("foo").role("custom").node("*").map,
          traverse(grantOrDenyRelType).database("foo").role("custom").node("B").map,
          traverse(grantOrDenyRelType).database("foo").role("custom").node("C").map,
          traverse(grantOrDenyRelType).database("foo").role("custom").relationship("*").map,
          traverse(grantOrDenyRelType).database("foo").role("custom").relationship("B").map,
          traverse(grantOrDenyRelType).database("foo").role("custom").relationship("C").map
        ))

        // WHEN
        execute(s"REVOKE $revokeType TRAVERSE ON GRAPH foo ELEMENTS * (*) FROM custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          traverse(grantOrDenyRelType).database("foo").role("custom").node("B").map,
          traverse(grantOrDenyRelType).database("foo").role("custom").node("C").map,
          traverse(grantOrDenyRelType).database("foo").role("custom").relationship("B").map,
          traverse(grantOrDenyRelType).database("foo").role("custom").relationship("C").map
        ))

        // WHEN
        execute(s"REVOKE $revokeType TRAVERSE ON GRAPH foo ELEMENTS B, C (*) FROM custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set.empty)
      }

      test(s"should revoke correct $grantOrDeny traverse element privilege different databases with REVOKE $revokeType") {
        // GIVEN
        selectDatabase(SYSTEM_DATABASE_NAME)
        execute("CREATE ROLE custom")
        execute("CREATE DATABASE foo")
        execute("CREATE DATABASE bar")
        execute(s"$grantOrDenyCommand TRAVERSE ON GRAPH * TO custom")
        execute(s"$grantOrDenyCommand TRAVERSE ON GRAPH foo ELEMENTS * (*) TO custom")
        execute(s"$grantOrDenyCommand TRAVERSE ON GRAPH bar ELEMENTS * (*) TO custom")

        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          traverse(grantOrDenyRelType).role("custom").node("*").map,
          traverse(grantOrDenyRelType).role("custom").node("*").database("foo").map,
          traverse(grantOrDenyRelType).role("custom").node("*").database("bar").map,
          traverse(grantOrDenyRelType).role("custom").relationship("*").map,
          traverse(grantOrDenyRelType).role("custom").relationship("*").database("foo").map,
          traverse(grantOrDenyRelType).role("custom").relationship("*").database("bar").map
        ))

        // WHEN
        execute(s"REVOKE $revokeType TRAVERSE ON GRAPH foo ELEMENTS * (*) FROM custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          traverse(grantOrDenyRelType).role("custom").node("*").map,
          traverse(grantOrDenyRelType).role("custom").node("*").database("bar").map,
          traverse(grantOrDenyRelType).role("custom").relationship("*").map,
          traverse(grantOrDenyRelType).role("custom").relationship("*").database("bar").map
        ))

        // WHEN
        execute(s"REVOKE $revokeType TRAVERSE ON GRAPH * ELEMENTS * (*) FROM custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          traverse(grantOrDenyRelType).role("custom").node("*").database("bar").map,
          traverse(grantOrDenyRelType).role("custom").relationship("*").database("bar").map
        ))
      }

      test(s"should revoke correct $grantOrDeny MATCH privilege different label qualifier with REVOKE $revokeType") {
        // GIVEN
        selectDatabase(SYSTEM_DATABASE_NAME)
        execute("CREATE ROLE custom")
        execute("CREATE DATABASE foo")
        execute(s"$grantOrDenyCommand MATCH (bar) ON GRAPH foo TO custom")
        execute(s"$grantOrDenyCommand MATCH (bar) ON GRAPH foo NODES A (*) TO custom")
        execute(s"$grantOrDenyCommand MATCH (bar) ON GRAPH foo NODES B (*) TO custom")

        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          traverse(grantOrDenyRelType).database("foo").role("custom").node("*").map,
          read(grantOrDenyRelType).database("foo").role("custom").property("bar").node("*").map,
          traverse(grantOrDenyRelType).database("foo").role("custom").relationship("*").map,
          read(grantOrDenyRelType).database("foo").role("custom").relationship("*").property("bar").map,
          traverse(grantOrDenyRelType).database("foo").role("custom").node("A").map,
          read(grantOrDenyRelType).database("foo").role("custom").property("bar").node("A").map,
          traverse(grantOrDenyRelType).database("foo").role("custom").node("B").map,
          read(grantOrDenyRelType).database("foo").role("custom").property("bar").node("B").map
        ))

        // WHEN
        execute(s"REVOKE $revokeType MATCH (bar) ON GRAPH foo NODES A (*) FROM custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          traverse(grantOrDenyRelType).database("foo").role("custom").node("*").map,
          read(grantOrDenyRelType).database("foo").role("custom").property("bar").node("*").map,
          traverse(grantOrDenyRelType).database("foo").role("custom").relationship("*").map,
          read(grantOrDenyRelType).database("foo").role("custom").relationship("*").property("bar").map,
          traverse(grantOrDenyRelType).database("foo").role("custom").node("A").map, // TODO: should be removed when revoking MATCH also revokes traverse
          traverse(grantOrDenyRelType).database("foo").role("custom").node("B").map,
          read(grantOrDenyRelType).database("foo").role("custom").property("bar").node("B").map
        ))

        // WHEN
        execute(s"REVOKE $revokeType MATCH (bar) ON GRAPH foo NODES * (*) FROM custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          traverse(grantOrDenyRelType).database("foo").role("custom").relationship("*").map,
          read(grantOrDenyRelType).database("foo").role("custom").relationship("*").property("bar").map,
          traverse(grantOrDenyRelType).database("foo").role("custom").node("*").map, // TODO: should be removed when revoking MATCH also revokes traverse
          traverse(grantOrDenyRelType).database("foo").role("custom").node("A").map, // TODO: should be removed when revoking MATCH also revokes traverse
          traverse(grantOrDenyRelType).database("foo").role("custom").node("B").map,
          read(grantOrDenyRelType).database("foo").role("custom").property("bar").node("B").map
        ))
      }

      test(s"should revoke correct $grantOrDeny MATCH privilege different relationship type qualifier with REVOKE $revokeType") {
        // GIVEN
        selectDatabase(SYSTEM_DATABASE_NAME)
        execute("CREATE ROLE custom")
        execute("CREATE DATABASE foo")
        execute(s"$grantOrDenyCommand MATCH (bar) ON GRAPH foo TO custom")
        execute(s"$grantOrDenyCommand MATCH (bar) ON GRAPH foo RELATIONSHIPS A (*) TO custom")
        execute(s"$grantOrDenyCommand MATCH (bar) ON GRAPH foo RELATIONSHIPS B (*) TO custom")

        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          traverse(grantOrDenyRelType).database("foo").role("custom").node("*").map,
          traverse(grantOrDenyRelType).database("foo").role("custom").relationship("*").map,
          traverse(grantOrDenyRelType).database("foo").role("custom").relationship("A").map,
          traverse(grantOrDenyRelType).database("foo").role("custom").relationship("B").map,
          read(grantOrDenyRelType).database("foo").role("custom").node("*").property("bar").map,
          read(grantOrDenyRelType).database("foo").role("custom").relationship("*").property("bar").map,
          read(grantOrDenyRelType).database("foo").role("custom").property("bar").relationship("A").map,
          read(grantOrDenyRelType).database("foo").role("custom").property("bar").relationship("B").map
        ))

        // WHEN
        execute(s"REVOKE $revokeType MATCH (bar) ON GRAPH foo RELATIONSHIPS A (*) FROM custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          traverse(grantOrDenyRelType).database("foo").role("custom").node("*").map,
          traverse(grantOrDenyRelType).database("foo").role("custom").relationship("*").map,
          traverse(grantOrDenyRelType).database("foo").role("custom").relationship("A").map,
          traverse(grantOrDenyRelType).database("foo").role("custom").relationship("B").map,
          read(grantOrDenyRelType).database("foo").role("custom").node("*").property("bar").map,
          read(grantOrDenyRelType).database("foo").role("custom").relationship("*").property("bar").map,
          read(grantOrDenyRelType).database("foo").role("custom").property("bar").relationship("B").map
        ))

        // WHEN
        execute(s"REVOKE $revokeType MATCH (bar) ON GRAPH foo RELATIONSHIPS * (*) FROM custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          traverse(grantOrDenyRelType).database("foo").role("custom").node("*").map,
          traverse(grantOrDenyRelType).database("foo").role("custom").relationship("*").map,
          traverse(grantOrDenyRelType).database("foo").role("custom").relationship("A").map,
          traverse(grantOrDenyRelType).database("foo").role("custom").relationship("B").map,
          read(grantOrDenyRelType).database("foo").role("custom").node("*").property("bar").map,
          read(grantOrDenyRelType).database("foo").role("custom").property("bar").relationship("B").map
        ))
      }

      test(s"should revoke correct $grantOrDeny MATCH privilege different element type qualifier with REVOKE $revokeType") {
        // GIVEN
        selectDatabase(SYSTEM_DATABASE_NAME)
        execute("CREATE ROLE custom")
        execute("CREATE DATABASE foo")
        execute(s"$grantOrDenyCommand MATCH (bar) ON GRAPH foo TO custom")
        execute(s"$grantOrDenyCommand MATCH (bar) ON GRAPH foo ELEMENTS A, B (*) TO custom")

        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          traverse(grantOrDenyRelType).database("foo").role("custom").node("*").map,
          traverse(grantOrDenyRelType).database("foo").role("custom").node("A").map,
          traverse(grantOrDenyRelType).database("foo").role("custom").node("B").map,
          traverse(grantOrDenyRelType).database("foo").role("custom").relationship("*").map,
          traverse(grantOrDenyRelType).database("foo").role("custom").relationship("A").map,
          traverse(grantOrDenyRelType).database("foo").role("custom").relationship("B").map,
          read(grantOrDenyRelType).database("foo").role("custom").property("bar").node("*").map,
          read(grantOrDenyRelType).database("foo").role("custom").property("bar").node("A").map,
          read(grantOrDenyRelType).database("foo").role("custom").property("bar").node("B").map,
          read(grantOrDenyRelType).database("foo").role("custom").property("bar").relationship("*").map,
          read(grantOrDenyRelType).database("foo").role("custom").property("bar").relationship("A").map,
          read(grantOrDenyRelType).database("foo").role("custom").property("bar").relationship("B").map
        ))

        // WHEN
        execute(s"REVOKE $revokeType MATCH (bar) ON GRAPH foo ELEMENTS A (*) FROM custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          traverse(grantOrDenyRelType).database("foo").role("custom").node("*").map,
          traverse(grantOrDenyRelType).database("foo").role("custom").node("A").map, // TODO: should be removed when revoking MATCH also revokes traverse
          traverse(grantOrDenyRelType).database("foo").role("custom").node("B").map,
          traverse(grantOrDenyRelType).database("foo").role("custom").relationship("*").map,
          traverse(grantOrDenyRelType).database("foo").role("custom").relationship("A").map, // TODO: should be removed when revoking MATCH also revokes traverse
          traverse(grantOrDenyRelType).database("foo").role("custom").relationship("B").map,
          read(grantOrDenyRelType).database("foo").role("custom").property("bar").node("*").map,
          read(grantOrDenyRelType).database("foo").role("custom").property("bar").node("B").map,
          read(grantOrDenyRelType).database("foo").role("custom").property("bar").relationship("*").map,
          read(grantOrDenyRelType).database("foo").role("custom").property("bar").relationship("B").map
        ))

        // WHEN
        execute(s"REVOKE $revokeType MATCH (bar) ON GRAPH foo ELEMENTS * (*) FROM custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          traverse(grantOrDenyRelType).database("foo").role("custom").node("*").map, // TODO: should be removed when revoking MATCH also revokes traverse
          traverse(grantOrDenyRelType).database("foo").role("custom").node("A").map, // TODO: should be removed when revoking MATCH also revokes traverse
          traverse(grantOrDenyRelType).database("foo").role("custom").node("B").map,
          traverse(grantOrDenyRelType).database("foo").role("custom").relationship("*").map, // TODO: should be removed when revoking MATCH also revokes traverse
          traverse(grantOrDenyRelType).database("foo").role("custom").relationship("A").map, // TODO: should be removed when revoking MATCH also revokes traverse
          traverse(grantOrDenyRelType).database("foo").role("custom").relationship("B").map,
          read(grantOrDenyRelType).database("foo").role("custom").property("bar").node("B").map,
          read(grantOrDenyRelType).database("foo").role("custom").property("bar").relationship("B").map
        ))

        // WHEN
        execute(s"$grantOrDenyCommand MATCH (bar) ON GRAPH foo ELEMENTS C (*) TO custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          traverse(grantOrDenyRelType).database("foo").role("custom").node("*").map, // TODO: should be removed when revoking MATCH also revokes traverse
          traverse(grantOrDenyRelType).database("foo").role("custom").node("A").map, // TODO: should be removed when revoking MATCH also revokes traverse
          traverse(grantOrDenyRelType).database("foo").role("custom").node("B").map,
          traverse(grantOrDenyRelType).database("foo").role("custom").node("C").map,
          traverse(grantOrDenyRelType).database("foo").role("custom").relationship("*").map, // TODO: should be removed when revoking MATCH also revokes traverse
          traverse(grantOrDenyRelType).database("foo").role("custom").relationship("A").map, // TODO: should be removed when revoking MATCH also revokes traverse
          traverse(grantOrDenyRelType).database("foo").role("custom").relationship("B").map,
          traverse(grantOrDenyRelType).database("foo").role("custom").relationship("C").map,
          read(grantOrDenyRelType).database("foo").role("custom").property("bar").node("B").map,
          read(grantOrDenyRelType).database("foo").role("custom").property("bar").node("C").map,
          read(grantOrDenyRelType).database("foo").role("custom").property("bar").relationship("B").map,
          read(grantOrDenyRelType).database("foo").role("custom").property("bar").relationship("C").map
        ))

        // WHEN
        execute(s"REVOKE $revokeType MATCH (bar) ON GRAPH foo ELEMENTS B, C (*) FROM custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          // TODO: this should be an empty set when revoking MATCH also revokes traverse
          traverse(grantOrDenyRelType).database("foo").role("custom").node("*").map,
          traverse(grantOrDenyRelType).database("foo").role("custom").node("A").map,
          traverse(grantOrDenyRelType).database("foo").role("custom").node("B").map,
          traverse(grantOrDenyRelType).database("foo").role("custom").node("C").map,
          traverse(grantOrDenyRelType).database("foo").role("custom").relationship("*").map,
          traverse(grantOrDenyRelType).database("foo").role("custom").relationship("A").map,
          traverse(grantOrDenyRelType).database("foo").role("custom").relationship("B").map,
          traverse(grantOrDenyRelType).database("foo").role("custom").relationship("C").map,
        ))
      }

      test(s"should revoke correct $grantOrDeny MATCH privilege different property with REVOKE $revokeType") {
        // GIVEN
        selectDatabase(SYSTEM_DATABASE_NAME)
        execute("CREATE ROLE custom")
        execute("CREATE DATABASE foo")
        execute(s"$grantOrDenyCommand MATCH (*) ON GRAPH foo TO custom")
        execute(s"$grantOrDenyCommand MATCH (a) ON GRAPH foo TO custom")
        execute(s"$grantOrDenyCommand MATCH (b) ON GRAPH foo TO custom")

        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          traverse(grantOrDenyRelType).database("foo").role("custom").node("*").map,
          traverse(grantOrDenyRelType).database("foo").role("custom").relationship("*").map,
          read(grantOrDenyRelType).database("foo").role("custom").node("*").map,
          read(grantOrDenyRelType).database("foo").role("custom").node("*").property("a").map,
          read(grantOrDenyRelType).database("foo").role("custom").node("*").property("b").map,
          read(grantOrDenyRelType).database("foo").role("custom").relationship("*").map,
          read(grantOrDenyRelType).database("foo").role("custom").relationship("*").property("a").map,
          read(grantOrDenyRelType).database("foo").role("custom").relationship("*").property("b").map
        ))

        // WHEN
        execute(s"REVOKE $revokeType MATCH (a) ON GRAPH foo FROM custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          traverse(grantOrDenyRelType).database("foo").role("custom").node("*").map,
          traverse(grantOrDenyRelType).database("foo").role("custom").relationship("*").map,
          read(grantOrDenyRelType).database("foo").role("custom").node("*").map,
          read(grantOrDenyRelType).database("foo").role("custom").node("*").property("b").map,
          read(grantOrDenyRelType).database("foo").role("custom").relationship("*").map,
          read(grantOrDenyRelType).database("foo").role("custom").relationship("*").property("b").map
        ))

        // WHEN
        execute(s"REVOKE $revokeType MATCH (*) ON GRAPH foo FROM custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          traverse(grantOrDenyRelType).database("foo").role("custom").node("*").map,
          traverse(grantOrDenyRelType).database("foo").role("custom").relationship("*").map,
          read(grantOrDenyRelType).database("foo").role("custom").node("*").property("b").map,
          read(grantOrDenyRelType).database("foo").role("custom").relationship("*").property("b").map
        ))

        // WHEN
        execute(s"REVOKE $revokeType MATCH (b) ON GRAPH foo FROM custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          // TODO: this should be an empty set when revoking MATCH also revokes traverse
          traverse(grantOrDenyRelType).database("foo").role("custom").node("*").map,
          traverse(grantOrDenyRelType).database("foo").role("custom").relationship("*").map
        ))
      }

      test(s"should revoke correct $grantOrDeny MATCH privilege different databases with REVOKE $revokeType") {
        // GIVEN
        selectDatabase(SYSTEM_DATABASE_NAME)
        execute("CREATE ROLE custom")
        execute("CREATE DATABASE foo")
        execute("CREATE DATABASE bar")
        execute(s"$grantOrDenyCommand MATCH (*) ON GRAPH * TO custom")
        execute(s"$grantOrDenyCommand MATCH (*) ON GRAPH foo TO custom")
        execute(s"$grantOrDenyCommand MATCH (*) ON GRAPH bar TO custom")

        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          traverse(grantOrDenyRelType).role("custom").node("*").map,
          traverse(grantOrDenyRelType).role("custom").node("*").database("foo").map,
          traverse(grantOrDenyRelType).role("custom").node("*").database("bar").map,
          traverse(grantOrDenyRelType).role("custom").relationship("*").map,
          traverse(grantOrDenyRelType).role("custom").relationship("*").database("foo").map,
          traverse(grantOrDenyRelType).role("custom").relationship("*").database("bar").map,
          read(grantOrDenyRelType).role("custom").node("*").map,
          read(grantOrDenyRelType).role("custom").node("*").database("foo").map,
          read(grantOrDenyRelType).role("custom").node("*").database("bar").map,
          read(grantOrDenyRelType).role("custom").relationship("*").map,
          read(grantOrDenyRelType).role("custom").relationship("*").database("foo").map,
          read(grantOrDenyRelType).role("custom").relationship("*").database("bar").map
        ))

        // WHEN
        execute(s"REVOKE $revokeType MATCH (*) ON GRAPH foo FROM custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          traverse(grantOrDenyRelType).role("custom").node("*").map,
          traverse(grantOrDenyRelType).role("custom").node("*").database("foo").map, // TODO: should be removed when revoking MATCH also revokes traverse
          traverse(grantOrDenyRelType).role("custom").node("*").database("bar").map,
          traverse(grantOrDenyRelType).role("custom").relationship("*").map,
          traverse(grantOrDenyRelType).role("custom").relationship("*").database("foo").map, // TODO: should be removed when revoking MATCH also revokes traverse
          traverse(grantOrDenyRelType).role("custom").relationship("*").database("bar").map,
          read(grantOrDenyRelType).role("custom").node("*").map,
          read(grantOrDenyRelType).role("custom").node("*").database("bar").map,
          read(grantOrDenyRelType).role("custom").relationship("*").map,
          read(grantOrDenyRelType).role("custom").relationship("*").database("bar").map
        ))

        // WHEN
        execute(s"REVOKE $revokeType MATCH (*) ON GRAPH * FROM custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          traverse(grantOrDenyRelType).role("custom").node("*").map, // TODO: should be removed when revoking MATCH also revokes traverse
          traverse(grantOrDenyRelType).role("custom").node("*").database("foo").map,
          traverse(grantOrDenyRelType).role("custom").node("*").database("bar").map,
          traverse(grantOrDenyRelType).role("custom").relationship("*").map, // TODO: should be removed when revoking MATCH also revokes traverse
          traverse(grantOrDenyRelType).role("custom").relationship("*").database("foo").map,
          traverse(grantOrDenyRelType).role("custom").relationship("*").database("bar").map,
          read(grantOrDenyRelType).role("custom").node("*").database("bar").map,
          read(grantOrDenyRelType).role("custom").relationship("*").database("bar").map
        ))
      }

      test(s"should revoke correct $grantOrDeny traverse and read privileges from different MATCH privileges with REVOKE $revokeType") {
        // GIVEN
        selectDatabase(SYSTEM_DATABASE_NAME)
        execute("CREATE ROLE custom")
        execute(s"CREATE DATABASE foo")
        execute(s"$grantOrDenyCommand MATCH (foo,bar) ON GRAPH foo NODES A,B (*) TO custom")
        execute(s"$grantOrDenyCommand MATCH (foo,bar) ON GRAPH foo RELATIONSHIPS A,B (*) TO custom")

        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          traverse(grantOrDenyRelType).database("foo").role("custom").node("A").map,
          traverse(grantOrDenyRelType).database("foo").role("custom").node("B").map,
          traverse(grantOrDenyRelType).database("foo").role("custom").relationship("A").map,
          traverse(grantOrDenyRelType).database("foo").role("custom").relationship("B").map,
          read(grantOrDenyRelType).database("foo").role("custom").node("A").property("foo").map,
          read(grantOrDenyRelType).database("foo").role("custom").node("B").property("foo").map,
          read(grantOrDenyRelType).database("foo").role("custom").relationship("A").property("foo").map,
          read(grantOrDenyRelType).database("foo").role("custom").relationship("B").property("foo").map,
          read(grantOrDenyRelType).database("foo").role("custom").node("A").property("bar").map,
          read(grantOrDenyRelType).database("foo").role("custom").node("B").property("bar").map,
          read(grantOrDenyRelType).database("foo").role("custom").relationship("A").property("bar").map,
          read(grantOrDenyRelType).database("foo").role("custom").relationship("B").property("bar").map
        ))

        // WHEN
        execute(s"REVOKE $revokeType TRAVERSE ON GRAPH foo NODES A (*) FROM custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          traverse(grantOrDenyRelType).database("foo").role("custom").node("B").map,
          traverse(grantOrDenyRelType).database("foo").role("custom").relationship("A").map,
          traverse(grantOrDenyRelType).database("foo").role("custom").relationship("B").map,
          read(grantOrDenyRelType).database("foo").role("custom").node("A").property("foo").map,
          read(grantOrDenyRelType).database("foo").role("custom").node("B").property("foo").map,
          read(grantOrDenyRelType).database("foo").role("custom").relationship("A").property("foo").map,
          read(grantOrDenyRelType).database("foo").role("custom").relationship("B").property("foo").map,
          read(grantOrDenyRelType).database("foo").role("custom").node("A").property("bar").map,
          read(grantOrDenyRelType).database("foo").role("custom").node("B").property("bar").map,
          read(grantOrDenyRelType).database("foo").role("custom").relationship("A").property("bar").map,
          read(grantOrDenyRelType).database("foo").role("custom").relationship("B").property("bar").map
        ))

        // WHEN
        execute(s"REVOKE $revokeType TRAVERSE ON GRAPH foo RELATIONSHIPS B (*) FROM custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          traverse(grantOrDenyRelType).database("foo").role("custom").node("B").map,
          traverse(grantOrDenyRelType).database("foo").role("custom").relationship("A").map,
          read(grantOrDenyRelType).database("foo").role("custom").node("A").property("foo").map,
          read(grantOrDenyRelType).database("foo").role("custom").node("B").property("foo").map,
          read(grantOrDenyRelType).database("foo").role("custom").relationship("A").property("foo").map,
          read(grantOrDenyRelType).database("foo").role("custom").relationship("B").property("foo").map,
          read(grantOrDenyRelType).database("foo").role("custom").node("A").property("bar").map,
          read(grantOrDenyRelType).database("foo").role("custom").node("B").property("bar").map,
          read(grantOrDenyRelType).database("foo").role("custom").relationship("A").property("bar").map,
          read(grantOrDenyRelType).database("foo").role("custom").relationship("B").property("bar").map
        ))

        // WHEN
        execute(s"REVOKE $revokeType READ (foo,bar) ON GRAPH foo NODES B (*) FROM custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          traverse(grantOrDenyRelType).database("foo").role("custom").node("B").map,
          traverse(grantOrDenyRelType).database("foo").role("custom").relationship("A").map,
          read(grantOrDenyRelType).database("foo").role("custom").node("A").property("foo").map,
          read(grantOrDenyRelType).database("foo").role("custom").relationship("A").property("foo").map,
          read(grantOrDenyRelType).database("foo").role("custom").relationship("B").property("foo").map,
          read(grantOrDenyRelType).database("foo").role("custom").node("A").property("bar").map,
          read(grantOrDenyRelType).database("foo").role("custom").relationship("A").property("bar").map,
          read(grantOrDenyRelType).database("foo").role("custom").relationship("B").property("bar").map
        ))

        // WHEN
        execute(s"REVOKE $revokeType READ (foo,bar) ON GRAPH foo RELATIONSHIPS A (*) FROM custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          traverse(grantOrDenyRelType).database("foo").role("custom").node("B").map,
          traverse(grantOrDenyRelType).database("foo").role("custom").relationship("A").map,
          read(grantOrDenyRelType).database("foo").role("custom").node("A").property("foo").map,
          read(grantOrDenyRelType).database("foo").role("custom").relationship("B").property("foo").map,
          read(grantOrDenyRelType).database("foo").role("custom").node("A").property("bar").map,
          read(grantOrDenyRelType).database("foo").role("custom").relationship("B").property("bar").map
        ))

        // WHEN
        execute(s"REVOKE $revokeType READ (foo) ON GRAPH foo NODES A (*) FROM custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          traverse(grantOrDenyRelType).database("foo").role("custom").node("B").map,
          traverse(grantOrDenyRelType).database("foo").role("custom").relationship("A").map,
          read(grantOrDenyRelType).database("foo").role("custom").relationship("B").property("foo").map,
          read(grantOrDenyRelType).database("foo").role("custom").node("A").property("bar").map,
          read(grantOrDenyRelType).database("foo").role("custom").relationship("B").property("bar").map
        ))

        // WHEN
        execute(s"REVOKE $revokeType READ (foo) ON GRAPH foo RELATIONSHIPS B (*) FROM custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          traverse(grantOrDenyRelType).database("foo").role("custom").node("B").map,
          traverse(grantOrDenyRelType).database("foo").role("custom").relationship("A").map,
          read(grantOrDenyRelType).database("foo").role("custom").node("A").property("bar").map,
          read(grantOrDenyRelType).database("foo").role("custom").relationship("B").property("bar").map
        ))
      }

      test(s"should revoke correct $grantOrDeny traverse and read privileges from different MATCH privileges on elements with REVOKE $revokeType") {
        // GIVEN
        selectDatabase(SYSTEM_DATABASE_NAME)
        execute("CREATE ROLE custom")
        execute(s"$grantOrDenyCommand MATCH (foo,bar) ON GRAPH * ELEMENTS A,B (*) TO custom")

        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          traverse(grantOrDenyRelType).role("custom").node("A").map,
          traverse(grantOrDenyRelType).role("custom").node("B").map,
          traverse(grantOrDenyRelType).role("custom").relationship("A").map,
          traverse(grantOrDenyRelType).role("custom").relationship("B").map,
          read(grantOrDenyRelType).role("custom").node("A").property("foo").map,
          read(grantOrDenyRelType).role("custom").node("B").property("foo").map,
          read(grantOrDenyRelType).role("custom").node("A").property("bar").map,
          read(grantOrDenyRelType).role("custom").node("B").property("bar").map,
          read(grantOrDenyRelType).role("custom").relationship("A").property("foo").map,
          read(grantOrDenyRelType).role("custom").relationship("B").property("foo").map,
          read(grantOrDenyRelType).role("custom").relationship("A").property("bar").map,
          read(grantOrDenyRelType).role("custom").relationship("B").property("bar").map
        ))

        // WHEN
        execute(s"REVOKE $revokeType TRAVERSE ON GRAPH * ELEMENTS A (*) FROM custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          traverse(grantOrDenyRelType).role("custom").node("B").map,
          traverse(grantOrDenyRelType).role("custom").relationship("B").map,
          read(grantOrDenyRelType).role("custom").node("A").property("foo").map,
          read(grantOrDenyRelType).role("custom").node("B").property("foo").map,
          read(grantOrDenyRelType).role("custom").node("A").property("bar").map,
          read(grantOrDenyRelType).role("custom").node("B").property("bar").map,
          read(grantOrDenyRelType).role("custom").relationship("A").property("foo").map,
          read(grantOrDenyRelType).role("custom").relationship("B").property("foo").map,
          read(grantOrDenyRelType).role("custom").relationship("A").property("bar").map,
          read(grantOrDenyRelType).role("custom").relationship("B").property("bar").map
        ))

        // WHEN
        execute(s"REVOKE $revokeType TRAVERSE ON GRAPH * ELEMENTS B (*) FROM custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          read(grantOrDenyRelType).role("custom").node("A").property("foo").map,
          read(grantOrDenyRelType).role("custom").node("B").property("foo").map,
          read(grantOrDenyRelType).role("custom").node("A").property("bar").map,
          read(grantOrDenyRelType).role("custom").node("B").property("bar").map,
          read(grantOrDenyRelType).role("custom").relationship("A").property("foo").map,
          read(grantOrDenyRelType).role("custom").relationship("B").property("foo").map,
          read(grantOrDenyRelType).role("custom").relationship("A").property("bar").map,
          read(grantOrDenyRelType).role("custom").relationship("B").property("bar").map
        ))

        // WHEN
        execute(s"REVOKE $revokeType READ (foo,bar) ON GRAPH * ELEMENTS B (*) FROM custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          read(grantOrDenyRelType).role("custom").node("A").property("foo").map,
          read(grantOrDenyRelType).role("custom").node("A").property("bar").map,
          read(grantOrDenyRelType).role("custom").relationship("A").property("foo").map,
          read(grantOrDenyRelType).role("custom").relationship("A").property("bar").map,
        ))

        // WHEN
        execute(s"REVOKE $revokeType READ (foo) ON GRAPH * ELEMENTS A (*) FROM custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          read(grantOrDenyRelType).role("custom").node("A").property("bar").map,
          read(grantOrDenyRelType).role("custom").relationship("A").property("bar").map,
        ))
      }

      test(s"should revoke correct $grantOrDeny MATCH privilege from different traverse, read and MATCH privileges with REVOKE $revokeType") {
        // GIVEN
        selectDatabase(SYSTEM_DATABASE_NAME)
        execute("CREATE ROLE custom")
        execute("CREATE DATABASE foo")
        execute(s"$grantOrDenyCommand MATCH (*) ON GRAPH foo TO custom")
        execute(s"$grantOrDenyCommand MATCH (a) ON GRAPH foo TO custom")
        execute(s"$grantOrDenyCommand READ  (b) ON GRAPH foo TO custom")

        execute(s"$grantOrDenyCommand TRAVERSE  ON GRAPH foo NODES A (*) TO custom")
        execute(s"$grantOrDenyCommand MATCH (a) ON GRAPH foo NODES A (*) TO custom")

        execute(s"$grantOrDenyCommand TRAVERSE  ON GRAPH foo RELATIONSHIPS A (*) TO custom")
        execute(s"$grantOrDenyCommand MATCH (a) ON GRAPH foo RELATIONSHIPS A (*) TO custom")

        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          traverse(grantOrDenyRelType).role("custom").database("foo").node("*").map, // From both MATCH *
          traverse(grantOrDenyRelType).role("custom").database("foo").relationship("*").map, // From both MATCH *
          read(grantOrDenyRelType).database("foo").role("custom").node("*").map,
          read(grantOrDenyRelType).database("foo").role("custom").node("*").property("a").map,
          read(grantOrDenyRelType).database("foo").role("custom").node("*").property("b").map,
          read(grantOrDenyRelType).database("foo").role("custom").relationship("*").map,
          read(grantOrDenyRelType).database("foo").role("custom").relationship("*").property("a").map,
          read(grantOrDenyRelType).database("foo").role("custom").relationship("*").property("b").map,

          traverse(grantOrDenyRelType).role("custom").database("foo").node("A").map, // From both MATCH and TRAVERSE
          traverse(grantOrDenyRelType).role("custom").database("foo").relationship("A").map, // From both MATCH and TRAVERSE
          read(grantOrDenyRelType).database("foo").role("custom").property("a").node("A").map,
          read(grantOrDenyRelType).database("foo").role("custom").property("a").relationship("A").map
        ))

        // WHEN
        execute(s"REVOKE $revokeType MATCH (b) ON GRAPH foo NODES * (*) FROM custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          traverse(grantOrDenyRelType).role("custom").database("foo").node("*").map,
          traverse(grantOrDenyRelType).role("custom").database("foo").relationship("*").map,
          read(grantOrDenyRelType).database("foo").role("custom").node("*").map,
          read(grantOrDenyRelType).database("foo").role("custom").node("*").property("a").map,
          read(grantOrDenyRelType).database("foo").role("custom").relationship("*").map,
          read(grantOrDenyRelType).database("foo").role("custom").relationship("*").property("a").map,
          read(grantOrDenyRelType).database("foo").role("custom").relationship("*").property("b").map,

          traverse(grantOrDenyRelType).role("custom").database("foo").node("A").map,
          traverse(grantOrDenyRelType).role("custom").database("foo").relationship("A").map,
          read(grantOrDenyRelType).database("foo").role("custom").property("a").node("A").map,
          read(grantOrDenyRelType).database("foo").role("custom").property("a").relationship("A").map
        ))

        // WHEN
        execute(s"REVOKE $revokeType MATCH (a) ON GRAPH foo RELATIONSHIP * (*) FROM custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          traverse(grantOrDenyRelType).role("custom").database("foo").node("*").map,
          traverse(grantOrDenyRelType).role("custom").database("foo").relationship("*").map,
          read(grantOrDenyRelType).database("foo").role("custom").node("*").map,
          read(grantOrDenyRelType).database("foo").role("custom").node("*").property("a").map,
          read(grantOrDenyRelType).database("foo").role("custom").relationship("*").map,
          read(grantOrDenyRelType).database("foo").role("custom").relationship("*").property("b").map,

          traverse(grantOrDenyRelType).role("custom").database("foo").node("A").map,
          traverse(grantOrDenyRelType).role("custom").database("foo").relationship("A").map,
          read(grantOrDenyRelType).database("foo").role("custom").property("a").node("A").map,
          read(grantOrDenyRelType).database("foo").role("custom").property("a").relationship("A").map
        ))

        // WHEN
        execute(s"REVOKE $revokeType MATCH (a) ON GRAPH foo NODES A (*) FROM custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          traverse(grantOrDenyRelType).role("custom").database("foo").node("*").map,
          traverse(grantOrDenyRelType).role("custom").database("foo").relationship("*").map,
          read(grantOrDenyRelType).database("foo").role("custom").node("*").map,
          read(grantOrDenyRelType).database("foo").role("custom").node("*").property("a").map,
          read(grantOrDenyRelType).database("foo").role("custom").relationship("*").map,
          read(grantOrDenyRelType).database("foo").role("custom").relationship("*").property("b").map,

          traverse(grantOrDenyRelType).role("custom").database("foo").node("A").map,
          traverse(grantOrDenyRelType).role("custom").database("foo").relationship("A").map,
          read(grantOrDenyRelType).database("foo").role("custom").property("a").relationship("A").map
        ))

        // WHEN
        execute(s"REVOKE $revokeType MATCH (a) ON GRAPH foo RELATIONSHIPS A (*) FROM custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          traverse(grantOrDenyRelType).role("custom").database("foo").node("*").map,
          traverse(grantOrDenyRelType).role("custom").database("foo").relationship("*").map,
          read(grantOrDenyRelType).database("foo").role("custom").node("*").map,
          read(grantOrDenyRelType).database("foo").role("custom").node("*").property("a").map,
          read(grantOrDenyRelType).database("foo").role("custom").relationship("*").map,
          read(grantOrDenyRelType).database("foo").role("custom").relationship("*").property("b").map,

          traverse(grantOrDenyRelType).role("custom").database("foo").node("A").map,
          traverse(grantOrDenyRelType).role("custom").database("foo").relationship("A").map
        ))
      }

      test(s"should revoke correct $grantOrDeny MATCH privilege from different traverse, read and MATCH privileges on elements with REVOKE $revokeType") {
        // GIVEN
        selectDatabase(SYSTEM_DATABASE_NAME)
        execute("CREATE ROLE custom")
        execute(s"$grantOrDenyCommand MATCH (*) ON GRAPH * ELEMENTS * (*) TO custom")
        execute(s"$grantOrDenyCommand MATCH (a) ON GRAPH * ELEMENTS * (*) TO custom")
        execute(s"$grantOrDenyCommand READ  (b) ON GRAPH * ELEMENTS * (*) TO custom")

        execute(s"$grantOrDenyCommand TRAVERSE  ON GRAPH * ELEMENTS A (*) TO custom")
        execute(s"$grantOrDenyCommand MATCH (a) ON GRAPH * ELEMENTS A (*) TO custom")

        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          traverse(grantOrDenyRelType).role("custom").node("*").map, // From both MATCH ELEMENTS *
          traverse(grantOrDenyRelType).role("custom").relationship("*").map, // From both MATCH ELEMENTS *
          read(grantOrDenyRelType).role("custom").node("*").map,
          read(grantOrDenyRelType).role("custom").node("*").property("a").map,
          read(grantOrDenyRelType).role("custom").node("*").property("b").map,
          read(grantOrDenyRelType).role("custom").relationship("*").map,
          read(grantOrDenyRelType).role("custom").relationship("*").property("a").map,
          read(grantOrDenyRelType).role("custom").relationship("*").property("b").map,

          traverse(grantOrDenyRelType).role("custom").node("A").map, // From both MATCH and TRAVERSE
          traverse(grantOrDenyRelType).role("custom").relationship("A").map, // From both MATCH and TRAVERSE
          read(grantOrDenyRelType).role("custom").node("A").property("a").map,
          read(grantOrDenyRelType).role("custom").relationship("A").property("a").map
        ))

        // WHEN
        execute(s"REVOKE $revokeType MATCH (b) ON GRAPH * ELEMENTS * (*) FROM custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          traverse(grantOrDenyRelType).role("custom").node("*").map,
          traverse(grantOrDenyRelType).role("custom").relationship("*").map,
          read(grantOrDenyRelType).role("custom").node("*").map,
          read(grantOrDenyRelType).role("custom").node("*").property("a").map,
          read(grantOrDenyRelType).role("custom").relationship("*").map,
          read(grantOrDenyRelType).role("custom").relationship("*").property("a").map,

          traverse(grantOrDenyRelType).role("custom").node("A").map,
          traverse(grantOrDenyRelType).role("custom").relationship("A").map,
          read(grantOrDenyRelType).role("custom").node("A").property("a").map,
          read(grantOrDenyRelType).role("custom").relationship("A").property("a").map
        ))

        // WHEN
        execute(s"REVOKE $revokeType MATCH (a) ON GRAPH * ELEMENTS * (*) FROM custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          traverse(grantOrDenyRelType).role("custom").node("*").map,
          traverse(grantOrDenyRelType).role("custom").relationship("*").map,
          read(grantOrDenyRelType).role("custom").node("*").map,
          read(grantOrDenyRelType).role("custom").relationship("*").map,

          traverse(grantOrDenyRelType).role("custom").node("A").map,
          traverse(grantOrDenyRelType).role("custom").relationship("A").map,
          read(grantOrDenyRelType).role("custom").node("A").property("a").map,
          read(grantOrDenyRelType).role("custom").relationship("A").property("a").map
        ))

        // WHEN
        execute(s"REVOKE $revokeType MATCH (a) ON GRAPH * ELEMENTS A (*) FROM custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          traverse(grantOrDenyRelType).role("custom").node("*").map,
          traverse(grantOrDenyRelType).role("custom").relationship("*").map,
          read(grantOrDenyRelType).role("custom").node("*").map,
          read(grantOrDenyRelType).role("custom").relationship("*").map,

          traverse(grantOrDenyRelType).role("custom").node("A").map,
          traverse(grantOrDenyRelType).role("custom").relationship("A").map,
        ))
      }

       test(s"should revoke correct $grantOrDeny elements privilege when granted as nodes + relationships with REVOKE $revokeType") {
        // GIVEN
        selectDatabase(SYSTEM_DATABASE_NAME)
        execute("CREATE ROLE custom")
        execute(s"$grantOrDenyCommand MATCH (*) ON GRAPH * NODES A TO custom")
        execute(s"$grantOrDenyCommand MATCH (*) ON GRAPH * RELATIONSHIPS A TO custom")

        execute(s"$grantOrDenyCommand MATCH (*) ON GRAPH * NODES * TO custom")
        execute(s"$grantOrDenyCommand MATCH (*) ON GRAPH * RELATIONSHIPS * TO custom")

        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          traverse(grantOrDenyRelType).role("custom").node("A").map,
          traverse(grantOrDenyRelType).role("custom").node("*").map,
          traverse(grantOrDenyRelType).role("custom").relationship("A").map,
          traverse(grantOrDenyRelType).role("custom").relationship("*").map,
          read(grantOrDenyRelType).role("custom").node("A").map,
          read(grantOrDenyRelType).role("custom").node("*").map,
          read(grantOrDenyRelType).role("custom").relationship("A").map,
          read(grantOrDenyRelType).role("custom").relationship("*").map
        ))

        // WHEN
        execute(s"REVOKE $revokeType MATCH (*) ON GRAPH * ELEMENTS * FROM custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          traverse(grantOrDenyRelType).role("custom").node("A").map,
          traverse(grantOrDenyRelType).role("custom").node("*").map, // TODO: should be removed when revoking MATCH also revokes traverse
          traverse(grantOrDenyRelType).role("custom").relationship("A").map,
          traverse(grantOrDenyRelType).role("custom").relationship("*").map, // TODO: should be removed when revoking MATCH also revokes traverse
          read(grantOrDenyRelType).role("custom").node("A").map,
          read(grantOrDenyRelType).role("custom").relationship("A").map
        ))
      }

      test(s"should revoke correct $grantOrDeny elements privilege when granted as specific nodes + relationships with REVOKE $revokeType") {
        // GIVEN
        selectDatabase(SYSTEM_DATABASE_NAME)
        execute("CREATE ROLE custom")
        execute(s"$grantOrDenyCommand MATCH (*) ON GRAPH * NODES A TO custom")
        execute(s"$grantOrDenyCommand MATCH (*) ON GRAPH * RELATIONSHIPS A TO custom")

        execute(s"$grantOrDenyCommand MATCH (*) ON GRAPH * NODES * TO custom")
        execute(s"$grantOrDenyCommand MATCH (*) ON GRAPH * RELATIONSHIPS * TO custom")

        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          traverse(grantOrDenyRelType).role("custom").node("A").map,
          traverse(grantOrDenyRelType).role("custom").node("*").map,
          traverse(grantOrDenyRelType).role("custom").relationship("A").map,
          traverse(grantOrDenyRelType).role("custom").relationship("*").map,
          read(grantOrDenyRelType).role("custom").node("A").map,
          read(grantOrDenyRelType).role("custom").node("*").map,
          read(grantOrDenyRelType).role("custom").relationship("A").map,
          read(grantOrDenyRelType).role("custom").relationship("*").map
        ))

        // WHEN
        execute(s"REVOKE $revokeType MATCH (*) ON GRAPH * ELEMENTS A FROM custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          traverse(grantOrDenyRelType).role("custom").node("A").map, // TODO: should be removed when revoking MATCH also revokes traverse
          traverse(grantOrDenyRelType).role("custom").node("*").map,
          traverse(grantOrDenyRelType).role("custom").relationship("A").map, // TODO: should be removed when revoking MATCH also revokes traverse
          traverse(grantOrDenyRelType).role("custom").relationship("*").map,
          read(grantOrDenyRelType).role("custom").node("*").map,
          read(grantOrDenyRelType).role("custom").relationship("*").map
        ))
      }

      test(s"should fail revoke $grantOrDeny elements privilege when granted only nodes or relationships with REVOKE $revokeType") {
        // GIVEN
        selectDatabase(SYSTEM_DATABASE_NAME)
        execute("CREATE ROLE custom")
        execute("ALTER USER neo4j SET PASSWORD 'abc' CHANGE NOT REQUIRED")

        execute(s"$grantOrDenyCommand MATCH (foo) ON GRAPH * NODES * TO custom")
        execute(s"$grantOrDenyCommand MATCH (bar) ON GRAPH * RELATIONSHIPS * TO custom")

        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          traverse(grantOrDenyRelType).role("custom").node("*").map,
          traverse(grantOrDenyRelType).role("custom").relationship("*").map,
          read(grantOrDenyRelType).role("custom").property("foo").node("*").map,
          read(grantOrDenyRelType).role("custom").property("bar").relationship("*").map
        ))

        // WHEN
        val error1 = the[QueryExecutionException] thrownBy {
          executeOnSystem("neo4j", "abc", s"REVOKE $revokeType MATCH (foo) ON GRAPH * ELEMENTS * FROM custom")
        }
        // THEN
        error1.getMessage should include(s"The privilege '${revokeType}read foo ON GRAPH * RELATIONSHIPS *' does not exist.")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          traverse(grantOrDenyRelType).role("custom").node("*").map,
          traverse(grantOrDenyRelType).role("custom").relationship("*").map,
          read(grantOrDenyRelType).role("custom").property("foo").node("*").map,
          read(grantOrDenyRelType).role("custom").property("bar").relationship("*").map
        ))

        // WHEN
        val error2 = the[QueryExecutionException] thrownBy {
          executeOnSystem("neo4j", "abc", s"REVOKE $revokeType MATCH (bar) ON GRAPH * FROM custom")
        }
        // THEN
        error2.getMessage should include(s"The privilege '${revokeType}read bar ON GRAPH * NODES *' does not exist.")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          traverse(grantOrDenyRelType).role("custom").node("*").map,
          traverse(grantOrDenyRelType).role("custom").relationship("*").map,
          read(grantOrDenyRelType).role("custom").property("foo").node("*").map,
          read(grantOrDenyRelType).role("custom").property("bar").relationship("*").map
        ))
      }

      test(s"should fail revoke $grantOrDeny privilege from non-existent role with REVOKE $revokeType") {
        // GIVEN
        selectDatabase(SYSTEM_DATABASE_NAME)
        execute("CREATE ROLE custom")
        execute(s"$grantOrDenyCommand TRAVERSE ON GRAPH * NODES * (*) TO custom")
        execute(s"$grantOrDenyCommand READ (*) ON GRAPH * NODES * (*) TO custom")
        execute(s"$grantOrDenyCommand MATCH (*) ON GRAPH * NODES A (*) TO custom")

        // WHEN
        val error1 = the[InvalidArgumentsException] thrownBy {
          execute(s"REVOKE $revokeType TRAVERSE ON GRAPH * NODES * (*) FROM wrongRole")
        }

        // THEN
        error1.getMessage should (be("The role 'wrongRole' does not have the specified privilege: traverse ON GRAPH * NODES *.") or
          be("The role 'wrongRole' does not exist."))

        // WHEN
        val error2 = the[InvalidArgumentsException] thrownBy {
          execute(s"REVOKE $revokeType READ (*) ON GRAPH * NODES * (*) FROM wrongRole")
        }

        // THEN
        error2.getMessage should (be("The role 'wrongRole' does not have the specified privilege: read * ON GRAPH * NODES *.") or
          be("The role 'wrongRole' does not exist."))

        // WHEN
        val error3 = the[InvalidArgumentsException] thrownBy {
          execute(s"REVOKE $revokeType MATCH (*) ON GRAPH * NODES A (*) FROM wrongRole")
        }
        // THEN
        error3.getMessage should (include("The role 'wrongRole' does not have the specified privilege") or
          be("The role 'wrongRole' does not exist."))
      }

      test(s"should fail revoke $grantOrDeny privilege not granted to role with REVOKE $revokeType") {
        // GIVEN
        selectDatabase(SYSTEM_DATABASE_NAME)
        execute("CREATE ROLE custom")
        execute("CREATE ROLE role")
        execute(s"$grantOrDenyCommand TRAVERSE ON GRAPH * NODES * (*) TO custom")
        execute(s"$grantOrDenyCommand READ (*) ON GRAPH * NODES * (*) TO custom")
        execute(s"$grantOrDenyCommand MATCH (*) ON GRAPH * NODES A (*) TO custom")

        // WHEN
        val errorTraverse = the[InvalidArgumentsException] thrownBy {
          execute(s"REVOKE $revokeType TRAVERSE ON GRAPH * NODES * (*) FROM role")
        }
        // THEN
        errorTraverse.getMessage should include("The role 'role' does not have the specified privilege")

        // WHEN
        val errorRead = the[InvalidArgumentsException] thrownBy {
          execute(s"REVOKE $revokeType READ (*) ON GRAPH * NODES * (*) FROM role")
        }
        // THEN
        errorRead.getMessage should include("The role 'role' does not have the specified privilege")

        // WHEN
        val errorMatch = the[InvalidArgumentsException] thrownBy {
          execute(s"REVOKE $revokeType MATCH (*) ON GRAPH * NODES A (*) FROM role")
        }
        // THEN
        errorMatch.getMessage should include("The role 'role' does not have the specified privilege")
      }

      test(s"should fail when revoking $grantOrDeny traversal privilege with missing database with REVOKE $revokeType") {
        // GIVEN
        selectDatabase(SYSTEM_DATABASE_NAME)
        execute("CREATE ROLE custom")
        execute(s"$grantOrDenyCommand TRAVERSE ON GRAPH * NODES * (*) TO custom")
        the[InvalidArgumentsException] thrownBy {
          execute(s"REVOKE $revokeType TRAVERSE ON GRAPH foo NODES * (*) FROM custom")
        } should have message s"The privilege '${revokeType}find  ON GRAPH foo NODES *' does not exist."
      }

      test(s"should fail when revoking $grantOrDeny read privilege with missing database with REVOKE $revokeType") {
        // GIVEN
        selectDatabase(SYSTEM_DATABASE_NAME)
        execute("CREATE ROLE custom")
        execute(s"$grantOrDenyCommand READ (*) ON GRAPH * NODES * (*) TO custom")
        the[InvalidArgumentsException] thrownBy {
          execute(s"REVOKE $revokeType READ (*) ON GRAPH foo NODES * (*) FROM custom")
        } should have message s"The privilege '${revokeType}read * ON GRAPH foo NODES *' does not exist."
      }

      test(s"should fail when revoking $grantOrDeny MATCH privilege with missing database with REVOKE $revokeType") {
        // GIVEN
        selectDatabase(SYSTEM_DATABASE_NAME)
        execute("CREATE ROLE custom")
        execute(s"$grantOrDenyCommand MATCH (*) ON GRAPH * NODES * (*) TO custom")

        // WHEN
        val e = the[InvalidArgumentsException] thrownBy {
          execute(s"REVOKE $revokeType MATCH (*) ON GRAPH foo NODES * (*) FROM custom")
        }
        // THEN
        e.getMessage should (be(s"The privilege '${revokeType}find  ON GRAPH foo NODES *' does not exist.") or
          be(s"The privilege '${revokeType}read * ON GRAPH foo NODES *' does not exist."))
      }

      test(s"should fail when revoking $grantOrDeny traversal privilege to custom role when not on system database with REVOKE$revokeType") {
        the[DatabaseManagementException] thrownBy {
          // WHEN
          execute(s"REVOKE $revokeType TRAVERSE ON GRAPH * NODES * (*) FROM custom")
          // THEN
        } should have message
          s"This is a DDL command and it should be executed against the system database: REVOKE ${revokeType}TRAVERSE"
      }

      test(s"should fail when revoking $grantOrDeny read privilege to custom role when not on system database with REVOKE $revokeType") {
        the[DatabaseManagementException] thrownBy {
          // WHEN
          execute(s"REVOKE $revokeType READ (*) ON GRAPH * NODES * (*) FROM custom")
          // THEN
        } should have message
          s"This is a DDL command and it should be executed against the system database: REVOKE ${revokeType}READ"
      }

      test(s"should fail when revoking $grantOrDeny MATCH privilege to custom role when not on system database with REVOKE $revokeType") {
        the[DatabaseManagementException] thrownBy {
          // WHEN
          execute(s"REVOKE $revokeType MATCH (*) ON GRAPH * NODES * (*) FROM custom")
          // THEN
        } should have message
          s"This is a DDL command and it should be executed against the system database: REVOKE ${revokeType}MATCH"
      }
  }

  Seq(
    ("label", "NODES", addNode: builderType),
    ("relationship type", "RELATIONSHIPS", addRel: builderType)
  ).foreach {
    case (segmentName, segmentCommand, segmentFunction: builderType) =>

      test(s"should revoke both grant and deny when revoking traverse $segmentName privilege") {
        // GIVEN
        selectDatabase(SYSTEM_DATABASE_NAME)
        execute("CREATE ROLE custom")

        // WHEN
        execute(s"GRANT TRAVERSE ON GRAPH * $segmentCommand A (*) TO custom")
        execute(s"DENY TRAVERSE ON GRAPH * $segmentCommand A (*) TO custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          segmentFunction(traverse("GRANTED").role("custom"), "A").map,
          segmentFunction(traverse("DENIED").role("custom"), "A").map
        ))

        // WHEN
        execute(s"REVOKE TRAVERSE ON GRAPH * $segmentCommand A (*) FROM custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set.empty)
      }

      test(s"should revoke both grant and deny when revoking read $segmentName privilege") {
        // GIVEN
        selectDatabase(SYSTEM_DATABASE_NAME)
        execute("CREATE ROLE custom")

        // WHEN
        execute(s"GRANT READ (prop) ON GRAPH * $segmentCommand A (*) TO custom")
        execute(s"DENY READ (prop) ON GRAPH * $segmentCommand A (*) TO custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          segmentFunction(read("GRANTED").role("custom").property("prop"), "A").map,
          segmentFunction(read("DENIED").role("custom").property("prop"), "A").map
        ))

        // WHEN
        execute(s"REVOKE READ (prop) ON GRAPH * $segmentCommand A (*) FROM custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set.empty)
      }

      test(s"should revoke both grant and deny when revoking match $segmentName privilege") {
        // GIVEN
        selectDatabase(SYSTEM_DATABASE_NAME)
        execute("CREATE ROLE custom")

        // WHEN
        execute(s"GRANT MATCH (prop) ON GRAPH * $segmentCommand A (*) TO custom")
        execute(s"DENY MATCH (prop) ON GRAPH * $segmentCommand A (*) TO custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          segmentFunction(traverse("GRANTED").role("custom"), "A").map,
          segmentFunction(traverse("DENIED").role("custom"), "A").map,
          segmentFunction(read("GRANTED").role("custom").property("prop"), "A").map,
          segmentFunction(read("DENIED").role("custom").property("prop"), "A").map
        ))

        // WHEN
        execute(s"REVOKE MATCH (prop) ON GRAPH * $segmentCommand A (*) FROM custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          segmentFunction(traverse("GRANTED").role("custom"), "A").map,
          segmentFunction(traverse("DENIED").role("custom"), "A").map
        ))
      }
  }

  // helper variable, methods and class

  private type builderType = (PrivilegeMapBuilder, String) => PrivilegeMapBuilder

  private def addNode(source: PrivilegeMapBuilder, name: String): PrivilegeMapBuilder = source.node(name)

  private def addRel(source: PrivilegeMapBuilder, name: String): PrivilegeMapBuilder = source.relationship(name)
}
