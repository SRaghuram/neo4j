/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.internal.cypher.acceptance

import org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME

class SetPropertyAdministrationCommandAcceptanceTest extends AdministrationCommandAcceptanceTestBase {

  test("should return empty counts to the outside for commands that update the system graph internally") {
    // GIVEN
    execute("CREATE ROLE custom")

    // Notice: They are executed in succession so they have to make sense in that order
    assertQueriesAndSubQueryCounts(List(
      "GRANT SET PROPERTY { prop1 } ON GRAPH * TO custom" -> 2,
      "REVOKE SET PROPERTY { prop1 } ON GRAPH * RELATIONSHIPS * FROM custom" -> 1,
      "DENY SET PROPERTY { prop1, prop2 } ON GRAPH *  TO custom" -> 4,
      "REVOKE DENY SET PROPERTY { prop1, prop2 } ON GRAPH * NODES * FROM custom" -> 2,
    ))
  }

    Seq(
      ("grant", "GRANT", granted: privilegeFunction),
      ("deny", "DENY", denied: privilegeFunction),
    ).foreach {
      case (grantOrDeny, grantOrDenyCommand, grantedOrDenied) =>

        // Tests for granting and denying write privileges

        test(s"should $grantOrDeny set property privilege to custom role for all databases and a specific property") {
          // GIVEN
          execute("CREATE ROLE custom")

          // WHEN
          execute(s"$grantOrDenyCommand SET PROPERTY {a} ON GRAPH * TO custom")

          // THEN
          execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
            grantedOrDenied(setProperty).role("custom").property("a").node("*").map,
            grantedOrDenied(setProperty).role("custom").property("a").relationship("*").map,
          ))
        }

        test(s"should $grantOrDeny set property privilege to custom role for a specific database and all properties") {
          // GIVEN
          execute("CREATE ROLE custom")
          execute("CREATE DATABASE foo")

          // WHEN
          execute(s"$grantOrDenyCommand SET PROPERTY {*} ON GRAPH foo TO custom")

          // THEN
          execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
            grantedOrDenied(setProperty).role("custom").database("foo").node("*").map,
            grantedOrDenied(setProperty).role("custom").database("foo").relationship("*").map,
          ))
        }

        test(s"should $grantOrDeny set property privilege to custom role for specific nodes and relationships") {
          // GIVEN
          execute("CREATE ROLE custom")
          execute("CREATE DATABASE foo")

          // WHEN
          execute(s"$grantOrDenyCommand SET PROPERTY {*} ON GRAPH * NODES a, b TO custom")
          execute(s"$grantOrDenyCommand SET PROPERTY {prop} ON GRAPH foo RELATIONSHIP c TO custom")

          // THEN
          execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
            grantedOrDenied(setProperty).role("custom").database("*").node("a").map,
            grantedOrDenied(setProperty).role("custom").database("*").node("b").map,
            grantedOrDenied(setProperty).role("custom").database("foo").property("prop").relationship("c").map,
          ))
        }

        test(s"should $grantOrDeny set property to custom role for multiple databases and multiple properties") {
          // GIVEN
          execute("CREATE ROLE custom")
          execute("CREATE DATABASE foo")
          execute("CREATE DATABASE bar")

          // WHEN
          execute(s"$grantOrDenyCommand SET PROPERTY {prop1, prop2} ON GRAPH foo, bar NODES * TO custom")

          // THEN
          execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
            grantedOrDenied(setProperty).role("custom").database("foo").property("prop1").node("*").map,
            grantedOrDenied(setProperty).role("custom").database("bar").property("prop1").node("*").map,
            grantedOrDenied(setProperty).role("custom").database("foo").property("prop2").node("*").map,
            grantedOrDenied(setProperty).role("custom").database("bar").property("prop2").node("*").map
          ))
        }

        test(s"should $grantOrDeny set property privilege to custom role for specific database and all labels using parameter") {
          // GIVEN
          execute("CREATE ROLE custom")

          // WHEN
          execute(s"$grantOrDenyCommand SET PROPERTY {*} ON GRAPH $$db NODES * TO custom", Map("db" -> DEFAULT_DATABASE_NAME))

          // THEN
          execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
            grantedOrDenied(setProperty).database(DEFAULT_DATABASE_NAME).role("custom").node("*").map,
          ))
        }

        test(s"should $grantOrDeny set property privilege to multiple roles in a single grant") {
          // GIVEN
          execute("CREATE ROLE role1")
          execute("CREATE ROLE role2")
          execute("CREATE ROLE role3")
          execute("CREATE DATABASE foo")

          // WHEN
          execute(s"$grantOrDenyCommand SET PROPERTY { prop } ON GRAPH foo RELATIONSHIPS * TO role1, role2, role3")

          // THEN
          val expected: Seq[PrivilegeMapBuilder] = Seq(
            grantedOrDenied(setProperty).database("foo").property("prop").relationship("*"),
          )

          execute("SHOW ROLE role1 PRIVILEGES").toSet should be(expected.map(_.role("role1").map).toSet)
          execute("SHOW ROLE role2 PRIVILEGES").toSet should be(expected.map(_.role("role2").map).toSet)
          execute("SHOW ROLE role3 PRIVILEGES").toSet should be(expected.map(_.role("role3").map).toSet)
        }

        // Tests for revoke grant and revoke deny write privileges

        test(s"should revoke correct $grantOrDeny set property privilege different databases") {
          // GIVEN
          execute("CREATE ROLE custom")
          execute("CREATE DATABASE foo")
          execute("CREATE DATABASE bar")
          execute(s"$grantOrDenyCommand SET PROPERTY {*} ON GRAPH * NODES * TO custom")
          execute(s"$grantOrDenyCommand SET PROPERTY {*} ON GRAPH foo NODES * TO custom")
          execute(s"$grantOrDenyCommand SET PROPERTY {*} ON GRAPH bar NODES * TO custom")

          // WHEN
          execute(s"REVOKE $grantOrDenyCommand SET PROPERTY {*} ON GRAPH foo FROM custom")

          // THEN
          execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
            grantedOrDenied(setProperty).role("custom").node("*").map,
            grantedOrDenied(setProperty).role("custom").database("bar").node("*").map,
          ))

          // WHEN
          execute(s"REVOKE $grantOrDenyCommand SET PROPERTY {*} ON GRAPH * FROM custom")

          // THEN
          execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
            grantedOrDenied(setProperty).role("custom").database("bar").node("*").map,
          ))
        }

        test(s"should be able to revoke set property if only having $grantOrDeny") {
          // GIVEN
          execute("CREATE ROLE custom")

          // WHEN
          execute(s"$grantOrDenyCommand SET PROPERTY { prop } ON GRAPH * NODES * TO custom")

          // THEN
          execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
            grantedOrDenied(setProperty).role("custom").property("prop").node("*").map,
          ))

          // WHEN
          execute(s"REVOKE SET PROPERTY { prop } ON GRAPH * FROM custom")

          // THEN
          execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set.empty)

        }

        test(s"should do nothing when revoking $grantOrDeny set property privilege from non-existent role") {
          // GIVEN
          execute("CREATE ROLE custom")
          execute("CREATE DATABASE foo")
          execute(s"$grantOrDenyCommand SET PROPERTY {*} ON GRAPH * TO custom")

          // WHEN
          execute(s"REVOKE $grantOrDenyCommand SET PROPERTY {*} ON GRAPH * FROM wrongRole")

          // THEN
          execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
            grantedOrDenied(setProperty).role("custom").node("*").map,
            grantedOrDenied(setProperty).role("custom").relationship("*").map,
          ))
        }

        test(s"should do nothing when revoking $grantOrDeny set property privilege not granted to role") {
          // GIVEN
          execute("CREATE ROLE custom")
          execute("CREATE ROLE role")
          execute(s"$grantOrDenyCommand SET PROPERTY { prop } ON GRAPH * TO custom")

          // WHEN
          execute(s"REVOKE $grantOrDenyCommand SET PROPERTY { prop } ON GRAPH * FROM role")
          // THEN
          execute("SHOW ROLE role PRIVILEGES").toSet should be(Set.empty)
        }
    }

}
