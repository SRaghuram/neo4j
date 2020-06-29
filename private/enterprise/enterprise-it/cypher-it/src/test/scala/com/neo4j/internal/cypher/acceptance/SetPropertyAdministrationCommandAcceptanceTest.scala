/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.internal.cypher.acceptance

import org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME
import org.neo4j.graphdb.security.AuthorizationViolationException

class SetPropertyAdministrationCommandAcceptanceTest extends AdministrationCommandAcceptanceTestBase with EnterpriseComponentVersionTestSupport {

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

      // Tests for granting and denying set property privileges

      test(s"should $grantOrDeny set property privilege to custom role for all graphs and a specific property") {
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

      test(s"should $grantOrDeny set property privilege to custom role for a specific graph and all properties") {
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

      test(s"should $grantOrDeny set property to custom role for multiple graphs and multiple properties") {
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

      test(s"should $grantOrDeny set property privilege to custom role for specific graph and all labels using parameter") {
        // GIVEN
        execute("CREATE ROLE custom")

        // WHEN
        execute(s"$grantOrDenyCommand SET PROPERTY {*} ON GRAPH $$graph NODES * TO custom", Map("graph" -> DEFAULT_DATABASE_NAME))

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

      // Tests for revoke grant and revoke deny set property privilege

      test(s"should revoke correct $grantOrDeny set property privilege different graphs") {
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

  test(s"revoke should revoke both grant and deny") {
    // GIVEN
    execute("CREATE ROLE custom")
    execute("CREATE ROLE role")
    execute("GRANT SET PROPERTY { prop } ON GRAPH * TO custom")
    execute("DENY SET PROPERTY { prop } ON GRAPH * TO custom")

    // WHEN
    execute(s"REVOKE SET PROPERTY { prop } ON GRAPH * FROM custom")
    // THEN
    execute("SHOW ROLE role PRIVILEGES").toSet should be(Set.empty)
  }

  test(s"revoke grant should revoke only grant") {
    // GIVEN
    execute("CREATE ROLE custom")
    execute("GRANT SET PROPERTY { prop } ON GRAPH * TO custom")
    execute("DENY SET PROPERTY { prop } ON GRAPH * TO custom")

    // WHEN
    execute(s"REVOKE GRANT SET PROPERTY { prop } ON GRAPH * FROM custom")
    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      denied(setProperty).role("custom").property("prop").node("*").map,
      denied(setProperty).role("custom").property("prop").relationship("*").map,
    ))
  }

  test(s"revoke deny should revoke only deny") {
    // GIVEN
    execute("CREATE ROLE custom")
    execute("GRANT SET PROPERTY { prop } ON GRAPH * TO custom")
    execute("DENY SET PROPERTY { prop } ON GRAPH * TO custom")

    // WHEN
    execute(s"REVOKE DENY SET PROPERTY { prop } ON GRAPH * FROM custom")
    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      granted(setProperty).role("custom").property("prop").node("*").map,
      granted(setProperty).role("custom").property("prop").relationship("*").map,
    ))
  }

  test(s"revoke something not granted should do nothing") {
    // GIVEN
    execute("CREATE ROLE custom")
    execute("GRANT SET PROPERTY { prop } ON GRAPH * TO custom")

    // WHEN
    execute(s"REVOKE DENY SET PROPERTY { prop } ON GRAPH * FROM custom")
    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      granted(setProperty).role("custom").property("prop").node("*").map,
      granted(setProperty).role("custom").property("prop").relationship("*").map,
    ))
  }

  // Implementation tests

  withAllSystemGraphVersions(unsupportedWhenNotLatest) {

    test("set property should allow setting a property on a node") {
      // GIVEN
      setupUserWithCustomRole()
      execute("GRANT MATCH {*} ON GRAPH * TO custom")
      execute(s"GRANT SET PROPERTY { prop } ON GRAPH * TO custom")

      selectDatabase(DEFAULT_DATABASE_NAME)
      execute("CALL db.createProperty('prop')")
      execute("CREATE ()")

      // WHEN
      executeOnDefault("joe", "soap", "MATCH (n) SET n.prop = 'value'")

      // THEN
      execute("MATCH (n{prop:'value'}) RETURN n").toSet should have size 1
    }

    test("set property should allow remove a property from a node") {
      // GIVEN
      setupUserWithCustomRole()
      execute("GRANT MATCH {*} ON GRAPH * TO custom")
      execute(s"GRANT SET PROPERTY { prop } ON GRAPH * TO custom")

      selectDatabase(DEFAULT_DATABASE_NAME)
      execute("CALL db.createProperty('prop')")
      execute("CREATE ({prop:'value'})")

      // WHEN
      executeOnDefault("joe", "soap", "MATCH (n) SET n.prop = null")

      // THEN
      execute("MATCH (n{prop:'value'}) RETURN n").toSet should have size 0
    }

    test("set property on a node should allow specific property only") {
      // GIVEN
      setupUserWithCustomRole()
      execute("GRANT MATCH {*} ON GRAPH * TO custom")
      execute(s"GRANT SET PROPERTY { prop } ON GRAPH * TO custom")

      selectDatabase(DEFAULT_DATABASE_NAME)
      execute("CALL db.createProperty('prop')")
      execute("CALL db.createProperty('prop2')")
      execute("CREATE ()")

      // WHEN
      executeOnDefault("joe", "soap", "MATCH (n) SET n.prop = 'value'")
      the[AuthorizationViolationException] thrownBy {
        // WHEN
        executeOnDefault("joe", "soap", "MATCH (n) SET n.prop2 = 'value'")
        // THEN
      } should have message "Set property for property 'prop2' is not allowed for user 'joe' with roles [PUBLIC, custom]."

      // THEN
      execute("MATCH (n{prop:'value'}) RETURN n").toSet should have size 1
    }

    test("set property on a relationship should allow specific property only") {
      // GIVEN
      setupUserWithCustomRole()
      execute("GRANT MATCH {*} ON GRAPH * TO custom")
      execute(s"GRANT SET PROPERTY { prop } ON GRAPH * TO custom")

      selectDatabase(DEFAULT_DATABASE_NAME)
      execute("CALL db.createProperty('prop')")
      execute("CALL db.createProperty('prop2')")
      execute("CREATE (:A)-[:R]->(:B)")

      // WHEN
      executeOnDefault("joe", "soap", "MATCH (:A)-[r:R]->(:B) SET r.prop = 'value'")
      the[AuthorizationViolationException] thrownBy {
        // WHEN
        executeOnDefault("joe", "soap", "MATCH (:A)-[r:R]->(:B) SET r.prop2 = 'value'")
        // THEN
      } should have message "Set property for property 'prop2' is not allowed for user 'joe' with roles [PUBLIC, custom]."

      // THEN
      execute("MATCH (:A)-[r:R]->(:B) RETURN r.prop").toSet should be(Set(Map("r.prop" -> "value")))
    }

    test("set property should allow setting a property on a relationship") {
      // GIVEN
      setupUserWithCustomRole()
      execute("GRANT MATCH {*} ON GRAPH * TO custom")
      execute(s"GRANT SET PROPERTY { prop } ON GRAPH * TO custom")

      selectDatabase(DEFAULT_DATABASE_NAME)
      execute("CALL db.createProperty('prop')")
      execute("CREATE (:A)-[:R]->(:B)")

      // WHEN
      executeOnDefault("joe", "soap", "MATCH (:A)-[r:R]->(:B) SET r.prop = 'value'")

      // THEN
      execute("MATCH (:A)-[r:R]->(:B) RETURN r.prop").toSet should be(Set(Map("r.prop" -> "value")))
    }

    test("set property should allow deleting a property from a relationship") {
      // GIVEN
      setupUserWithCustomRole()
      execute("GRANT MATCH {*} ON GRAPH * TO custom")
      execute(s"GRANT SET PROPERTY { prop } ON GRAPH * TO custom")

      selectDatabase(DEFAULT_DATABASE_NAME)
      execute("CALL db.createProperty('prop')")
      execute("CREATE (:A)-[:R{prop:'value'}]->(:B)")

      // WHEN
      executeOnDefault("joe", "soap", "MATCH (:A)-[r:R]->(:B) SET r.prop = null")

      // THEN
      val c = execute("MATCH (:A)-[r:R{prop:'value'}]->(:B) RETURN r.prop").toSet should be(empty)
    }

    test("deny set property should not allow setting a specific property on a node") {
      // GIVEN
      setupUserWithCustomRole()
      execute("GRANT MATCH {*} ON GRAPH * TO custom")
      execute(s"GRANT SET PROPERTY { * } ON GRAPH * TO custom")
      execute(s"DENY SET PROPERTY { prop } ON GRAPH * NODES * TO custom")

      selectDatabase(DEFAULT_DATABASE_NAME)
      execute("CALL db.createProperty('prop')")
      execute("CREATE ()")

      the[AuthorizationViolationException] thrownBy {
        // WHEN
        executeOnDefault("joe", "soap", "MATCH (n) SET n.prop = 'value'")
        // THEN
      } should have message "Set property for property 'prop' is not allowed for user 'joe' with roles [PUBLIC, custom]."

      // THEN
      execute("MATCH (n{prop:'value'}) RETURN n").toSet should be(empty)
    }

    test("deny set property should not allow setting a specific property on a relationship") {
      // GIVEN
      setupUserWithCustomRole()
      execute("GRANT MATCH {*} ON GRAPH * TO custom")
      execute(s"GRANT SET PROPERTY { * } ON GRAPH * TO custom")
      execute(s"DENY SET PROPERTY { prop } ON GRAPH * RELATIONSHIPS * TO custom")

      selectDatabase(DEFAULT_DATABASE_NAME)
      execute("CALL db.createProperty('prop')")
      execute("CREATE (:A)-[:R]->(:B)")

      the[AuthorizationViolationException] thrownBy {
        // WHEN
        executeOnDefault("joe", "soap", "MATCH (n)-[r:R]->(o) SET r.prop = 'value'")
        // THEN
      } should have message "Set property for property 'prop' is not allowed for user 'joe' with roles [PUBLIC, custom]."

      // THEN
      execute("MATCH (n{prop:'value'}) RETURN n").toSet should be(empty)
    }

    test("denying all writes prevents setting property") {
      // GIVEN
      setupUserWithCustomRole()
      execute("GRANT MATCH {*} ON GRAPH * TO custom")
      execute("GRANT SET PROPERTY {prop} ON GRAPH * TO custom")
      execute("DENY WRITE ON GRAPH * TO custom")

      selectDatabase(DEFAULT_DATABASE_NAME)
      execute("CALL db.createProperty('prop')")
      execute("CREATE (:A)-[:R]->(:B)")

      the[AuthorizationViolationException] thrownBy {
        // WHEN
        executeOnDefault("joe", "soap", "MATCH (n)-[r:R]->(o) SET r.prop = 'value'")
        // THEN
      } should have message "Set property for property 'prop' is not allowed for user 'joe' with roles [PUBLIC, custom]."

      execute("MATCH (n{prop:'value'}) RETURN n").toSet should be(empty)
    }

    test("deny set property should override general write permission") {
      // GIVEN
      setupUserWithCustomRole()
      execute("GRANT MATCH {*} ON GRAPH * TO custom")
      execute(s"GRANT WRITE ON GRAPH * TO custom")
      execute(s"DENY SET PROPERTY { prop } ON GRAPH * RELATIONSHIPS * TO custom")

      selectDatabase(DEFAULT_DATABASE_NAME)
      execute("CALL db.createProperty('prop')")
      execute("CREATE (:A)-[:R]->(:B)")

      the[AuthorizationViolationException] thrownBy {
        // WHEN
        executeOnDefault("joe", "soap", "MATCH (n)-[r:R]->(o) SET r.prop = 'value'")
        // THEN
      } should have message "Set property for property 'prop' is not allowed for user 'joe' with roles [PUBLIC, custom]."

      // THEN
      execute("MATCH (n{prop:'value'}) RETURN n").toSet should be(empty)
    }
  }
}
