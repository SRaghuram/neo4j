/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.internal.cypher.acceptance

// Tests for REVOKE TRAVERSE, REVOKE READ and REVOKE MATCH
class RevokePrivilegeAdministrationCommandAcceptanceTest extends AdministrationCommandAcceptanceTestBase {

  Seq(
    ("grant", "GRANT", granted: privilegeFunction, "GRANT "),
    ("deny", "DENY", denied: privilegeFunction, "DENY "),
    ("grant", "GRANT", granted: privilegeFunction, ""),
    ("deny", "DENY", denied: privilegeFunction, ""),
  ).foreach {
    case (grantOrDeny, grantOrDenyCommand, grantedOrDenied, revokeType) =>

      test(s"should revoke correct $grantOrDeny read privilege different label qualifier with REVOKE $revokeType") {
        // GIVEN
        execute("CREATE ROLE custom")
        execute("CREATE DATABASE foo")
        execute(s"$grantOrDenyCommand READ {bar} ON GRAPH foo NODES * (*) TO custom")
        execute(s"$grantOrDenyCommand READ {bar} ON GRAPH foo NODES A (*) TO custom")
        execute(s"$grantOrDenyCommand READ {bar} ON GRAPH foo NODES B (*) TO custom")
        execute(s"$grantOrDenyCommand READ {bar} ON GRAPH foo RELATIONSHIPS * (*) TO custom")
        execute(s"$grantOrDenyCommand READ {bar} ON GRAPH foo RELATIONSHIPS A (*) TO custom")
        execute(s"$grantOrDenyCommand READ {bar} ON GRAPH foo RELATIONSHIPS B (*) TO custom")

        // WHEN
        execute(s"REVOKE $revokeType READ {bar} ON GRAPH $$db NODES A (*) FROM $$role", Map("db" -> "foo", "role" -> "custom"))

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          grantedOrDenied(read).database("foo").role("custom").property("bar").relationship("*").map,
          grantedOrDenied(read).database("foo").role("custom").property("bar").relationship("A").map,
          grantedOrDenied(read).database("foo").role("custom").property("bar").relationship("B").map,
          grantedOrDenied(read).database("foo").role("custom").property("bar").node("*").map,
          grantedOrDenied(read).database("foo").role("custom").property("bar").node("B").map
        ))

        // WHEN
        execute(s"REVOKE $revokeType READ {bar} ON GRAPH $$db NODES * (*) FROM $$role", Map("db" -> "foo", "role" -> "custom"))

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          grantedOrDenied(read).database("foo").role("custom").property("bar").relationship("*").map,
          grantedOrDenied(read).database("foo").role("custom").property("bar").relationship("A").map,
          grantedOrDenied(read).database("foo").role("custom").property("bar").relationship("B").map,
          grantedOrDenied(read).database("foo").role("custom").property("bar").node("B").map
        ))
      }

      test(s"should revoke correct $grantOrDeny read privilege different relationship type qualifier with REVOKE $revokeType") {
        // GIVEN
        execute("CREATE ROLE custom")
        execute("CREATE DATABASE foo")
        execute(s"$grantOrDenyCommand READ {bar} ON GRAPH foo NODES * (*) TO custom")
        execute(s"$grantOrDenyCommand READ {bar} ON GRAPH foo NODES A (*) TO custom")
        execute(s"$grantOrDenyCommand READ {bar} ON GRAPH foo NODES B (*) TO custom")
        execute(s"$grantOrDenyCommand READ {bar} ON GRAPH foo RELATIONSHIPS * (*) TO custom")
        execute(s"$grantOrDenyCommand READ {bar} ON GRAPH foo RELATIONSHIPS A (*) TO custom")
        execute(s"$grantOrDenyCommand READ {bar} ON GRAPH foo RELATIONSHIPS B (*) TO custom")

        // WHEN
        execute(s"REVOKE $revokeType READ {bar} ON GRAPH foo RELATIONSHIPS A (*) FROM custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          grantedOrDenied(read).database("foo").role("custom").property("bar").node("*").map,
          grantedOrDenied(read).database("foo").role("custom").property("bar").node("A").map,
          grantedOrDenied(read).database("foo").role("custom").property("bar").node("B").map,
          grantedOrDenied(read).database("foo").role("custom").property("bar").relationship("*").map,
          grantedOrDenied(read).database("foo").role("custom").property("bar").relationship("B").map
        ))

        // WHEN
        execute(s"REVOKE $revokeType READ {bar} ON GRAPH foo RELATIONSHIPS * (*) FROM custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          grantedOrDenied(read).database("foo").role("custom").property("bar").node("*").map,
          grantedOrDenied(read).database("foo").role("custom").property("bar").node("A").map,
          grantedOrDenied(read).database("foo").role("custom").property("bar").node("B").map,
          grantedOrDenied(read).database("foo").role("custom").property("bar").relationship("B").map
        ))
      }

      test(s"should revoke correct $grantOrDeny read privilege different element type qualifier with REVOKE $revokeType") {
        // GIVEN
        execute("CREATE ROLE custom")
        execute("CREATE DATABASE foo")
        execute(s"$grantOrDenyCommand READ {bar} ON GRAPH foo ELEMENTS * (*) TO custom")
        execute(s"$grantOrDenyCommand READ {bar} ON GRAPH foo ELEMENTS A (*) TO custom")
        execute(s"$grantOrDenyCommand READ {bar} ON GRAPH foo ELEMENTS B, C (*) TO custom")

        // WHEN
        execute(s"REVOKE $revokeType READ {bar} ON GRAPH foo ELEMENTS A (*) FROM custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          grantedOrDenied(read).database("foo").role("custom").property("bar").node("*").map,
          grantedOrDenied(read).database("foo").role("custom").property("bar").node("B").map,
          grantedOrDenied(read).database("foo").role("custom").property("bar").node("C").map,
          grantedOrDenied(read).database("foo").role("custom").property("bar").relationship("*").map,
          grantedOrDenied(read).database("foo").role("custom").property("bar").relationship("B").map,
          grantedOrDenied(read).database("foo").role("custom").property("bar").relationship("C").map
        ))

        // WHEN
        execute(s"REVOKE $revokeType READ {bar} ON GRAPH foo ELEMENTS * (*) FROM custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          grantedOrDenied(read).database("foo").role("custom").property("bar").node("B").map,
          grantedOrDenied(read).database("foo").role("custom").property("bar").node("C").map,
          grantedOrDenied(read).database("foo").role("custom").property("bar").relationship("B").map,
          grantedOrDenied(read).database("foo").role("custom").property("bar").relationship("C").map
        ))

        // WHEN
        execute(s"REVOKE $revokeType READ {bar} ON GRAPH foo ELEMENTS B, C (*) FROM custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set.empty)
      }

      test(s"should revoke correct $grantOrDeny read privilege different property with REVOKE $revokeType") {
        // GIVEN
        execute("CREATE ROLE custom")
        execute("CREATE DATABASE foo")
        execute(s"$grantOrDenyCommand READ {*} ON GRAPH foo TO custom")
        execute(s"$grantOrDenyCommand READ {a} ON GRAPH foo TO custom")
        execute(s"$grantOrDenyCommand READ {b} ON GRAPH foo TO custom")

        // WHEN
        execute(s"REVOKE $revokeType READ {a} ON GRAPH foo NODES * (*) FROM custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          grantedOrDenied(read).database("foo").role("custom").node("*").property("b").map,
          grantedOrDenied(read).database("foo").role("custom").node("*").map,
          grantedOrDenied(read).database("foo").role("custom").relationship("*").property("a").map,
          grantedOrDenied(read).database("foo").role("custom").relationship("*").property("b").map,
          grantedOrDenied(read).database("foo").role("custom").relationship("*").map
        ))

        // WHEN
        execute(s"REVOKE $revokeType READ {*} ON GRAPH foo RELATIONSHIPS * (*) FROM custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          grantedOrDenied(read).database("foo").role("custom").node("*").property("b").map,
          grantedOrDenied(read).database("foo").role("custom").node("*").map,
          grantedOrDenied(read).database("foo").role("custom").relationship("*").property("a").map,
          grantedOrDenied(read).database("foo").role("custom").relationship("*").property("b").map
        ))

        // WHEN
        execute(s"REVOKE $revokeType READ {b} ON GRAPH foo FROM custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          grantedOrDenied(read).database("foo").role("custom").node("*").map,
          grantedOrDenied(read).database("foo").role("custom").relationship("*").property("a").map
        ))
      }

      test(s"should revoke correct $grantOrDeny read privilege different databases with REVOKE $revokeType") {
        // GIVEN
        execute("CREATE ROLE custom")
        execute("CREATE DATABASE foo")
        execute("CREATE DATABASE bar")
        execute(s"$grantOrDenyCommand READ {*} ON GRAPH * TO custom")
        execute(s"$grantOrDenyCommand READ {*} ON GRAPH foo TO custom")
        execute(s"$grantOrDenyCommand READ {*} ON GRAPH bar TO custom")

        // WHEN
        execute(s"REVOKE $revokeType READ {*} ON GRAPH foo FROM custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          grantedOrDenied(read).role("custom").node("*").map,
          grantedOrDenied(read).role("custom").node("*").database("bar").map,
          grantedOrDenied(read).role("custom").relationship("*").map,
          grantedOrDenied(read).role("custom").relationship("*").database("bar").map
        ))

        // WHEN
        execute(s"REVOKE $revokeType READ {*} ON GRAPH * FROM custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          grantedOrDenied(read).role("custom").node("*").database("bar").map,
          grantedOrDenied(read).role("custom").relationship("*").database("bar").map
        ))
      }

      test(s"should revoke correct $grantOrDeny traverse node privilege different label qualifier with REVOKE $revokeType") {
        // GIVEN
        execute("CREATE ROLE custom")
        execute("CREATE DATABASE foo")
        execute(s"$grantOrDenyCommand TRAVERSE ON GRAPH foo TO custom")
        execute(s"$grantOrDenyCommand TRAVERSE ON GRAPH foo NODES A (*) TO custom")
        execute(s"$grantOrDenyCommand TRAVERSE ON GRAPH foo NODES B (*) TO custom")

        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          grantedOrDenied(traverse).database("foo").role("custom").node("*").map,
          grantedOrDenied(traverse).database("foo").role("custom").relationship("*").map,
          grantedOrDenied(traverse).database("foo").role("custom").node("A").map,
          grantedOrDenied(traverse).database("foo").role("custom").node("B").map
        ))

        // WHEN
        execute(s"REVOKE $revokeType TRAVERSE ON GRAPH foo NODES A (*) FROM custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          grantedOrDenied(traverse).database("foo").role("custom").node("*").map,
          grantedOrDenied(traverse).database("foo").role("custom").relationship("*").map,
          grantedOrDenied(traverse).database("foo").role("custom").node("B").map
        ))

        // WHEN
        execute(s"REVOKE $revokeType TRAVERSE ON GRAPH foo NODES * (*) FROM custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          grantedOrDenied(traverse).database("foo").role("custom").relationship("*").map,
          grantedOrDenied(traverse).database("foo").role("custom").node("B").map
        ))
      }

      test(s"should revoke correct $grantOrDeny traverse node privilege different databases with REVOKE $revokeType") {
        // GIVEN
        execute("CREATE ROLE custom")
        execute("CREATE DATABASE foo")
        execute("CREATE DATABASE bar")
        execute(s"$grantOrDenyCommand TRAVERSE ON GRAPH * TO custom")
        execute(s"$grantOrDenyCommand TRAVERSE ON GRAPH foo NODES * (*) TO custom")
        execute(s"$grantOrDenyCommand TRAVERSE ON GRAPH bar NODES * (*) TO custom")

        // WHEN
        execute(s"REVOKE $revokeType TRAVERSE ON GRAPH foo NODES * (*) FROM custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          grantedOrDenied(traverse).role("custom").node("*").map,
          grantedOrDenied(traverse).role("custom").relationship("*").map,
          grantedOrDenied(traverse).role("custom").node("*").database("bar").map
        ))

        // WHEN
        execute(s"REVOKE $revokeType TRAVERSE ON GRAPH * NODES * (*) FROM custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          grantedOrDenied(traverse).role("custom").relationship("*").map,
          grantedOrDenied(traverse).role("custom").node("*").database("bar").map
        ))
      }

      test(s"should revoke correct $grantOrDeny traverse relationships privilege different type qualifier with REVOKE $revokeType") {
        // GIVEN
        execute("CREATE ROLE custom")
        execute("CREATE DATABASE foo")
        execute(s"$grantOrDenyCommand TRAVERSE ON GRAPH foo TO custom")
        execute(s"$grantOrDenyCommand TRAVERSE ON GRAPH foo RELATIONSHIPS A (*) TO custom")
        execute(s"$grantOrDenyCommand TRAVERSE ON GRAPH foo RELATIONSHIPS B (*) TO custom")

        // WHEN
        execute(s"REVOKE $revokeType TRAVERSE ON GRAPH $$db RELATIONSHIPS A (*) FROM $$role", Map("db" -> "foo", "role" -> "custom"))

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          grantedOrDenied(traverse).database("foo").role("custom").node("*").map,
          grantedOrDenied(traverse).database("foo").role("custom").relationship("*").map,
          grantedOrDenied(traverse).database("foo").role("custom").relationship("B").map
        ))

        // WHEN
        execute(s"REVOKE $revokeType TRAVERSE ON GRAPH $$db RELATIONSHIPS * (*) FROM $$role", Map("db" -> "foo", "role" -> "custom"))

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          grantedOrDenied(traverse).database("foo").role("custom").node("*").map,
          grantedOrDenied(traverse).database("foo").role("custom").relationship("B").map
        ))
      }

      test(s"should revoke correct $grantOrDeny traverse relationship privilege different databases with REVOKE $revokeType") {
        // GIVEN
        execute("CREATE ROLE custom")
        execute("CREATE DATABASE foo")
        execute("CREATE DATABASE bar")
        execute(s"$grantOrDenyCommand TRAVERSE ON GRAPH * TO custom")
        execute(s"$grantOrDenyCommand TRAVERSE ON GRAPH foo RELATIONSHIPS * (*) TO custom")
        execute(s"$grantOrDenyCommand TRAVERSE ON GRAPH bar RELATIONSHIPS * (*) TO custom")

        // WHEN
        execute(s"REVOKE $revokeType TRAVERSE ON GRAPH foo RELATIONSHIPS * (*) FROM custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          grantedOrDenied(traverse).role("custom").node("*").map,
          grantedOrDenied(traverse).role("custom").relationship("*").map,
          grantedOrDenied(traverse).role("custom").relationship("*").database("bar").map
        ))

        // WHEN
        execute(s"REVOKE $revokeType TRAVERSE ON GRAPH * RELATIONSHIPS * (*) FROM custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          grantedOrDenied(traverse).role("custom").node("*").map,
          grantedOrDenied(traverse).role("custom").relationship("*").database("bar").map
        ))
      }

      test(s"should revoke correct $grantOrDeny traverse elements privilege different type qualifier with REVOKE $revokeType") {
        // GIVEN
        execute("CREATE ROLE custom")
        execute("CREATE DATABASE foo")
        execute(s"$grantOrDenyCommand TRAVERSE ON GRAPH foo TO custom")
        execute(s"$grantOrDenyCommand TRAVERSE ON GRAPH foo ELEMENTS A, B, C (*) TO custom")

        // WHEN
        execute(s"REVOKE $revokeType TRAVERSE ON GRAPH foo ELEMENTS A (*) FROM custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          grantedOrDenied(traverse).database("foo").role("custom").node("*").map,
          grantedOrDenied(traverse).database("foo").role("custom").node("B").map,
          grantedOrDenied(traverse).database("foo").role("custom").node("C").map,
          grantedOrDenied(traverse).database("foo").role("custom").relationship("*").map,
          grantedOrDenied(traverse).database("foo").role("custom").relationship("B").map,
          grantedOrDenied(traverse).database("foo").role("custom").relationship("C").map
        ))

        // WHEN
        execute(s"REVOKE $revokeType TRAVERSE ON GRAPH foo ELEMENTS * (*) FROM custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          grantedOrDenied(traverse).database("foo").role("custom").node("B").map,
          grantedOrDenied(traverse).database("foo").role("custom").node("C").map,
          grantedOrDenied(traverse).database("foo").role("custom").relationship("B").map,
          grantedOrDenied(traverse).database("foo").role("custom").relationship("C").map
        ))

        // WHEN
        execute(s"REVOKE $revokeType TRAVERSE ON GRAPH foo ELEMENTS B, C (*) FROM custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set.empty)
      }

      test(s"should revoke correct $grantOrDeny traverse element privilege different databases with REVOKE $revokeType") {
        // GIVEN
        execute("CREATE ROLE custom")
        execute("CREATE DATABASE foo")
        execute("CREATE DATABASE bar")
        execute(s"$grantOrDenyCommand TRAVERSE ON GRAPH * TO custom")
        execute(s"$grantOrDenyCommand TRAVERSE ON GRAPH foo ELEMENTS * (*) TO custom")
        execute(s"$grantOrDenyCommand TRAVERSE ON GRAPH bar ELEMENTS * (*) TO custom")

        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          grantedOrDenied(traverse).role("custom").node("*").map,
          grantedOrDenied(traverse).role("custom").node("*").database("foo").map,
          grantedOrDenied(traverse).role("custom").node("*").database("bar").map,
          grantedOrDenied(traverse).role("custom").relationship("*").map,
          grantedOrDenied(traverse).role("custom").relationship("*").database("foo").map,
          grantedOrDenied(traverse).role("custom").relationship("*").database("bar").map
        ))

        // WHEN
        execute(s"REVOKE $revokeType TRAVERSE ON GRAPH foo ELEMENTS * (*) FROM custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          grantedOrDenied(traverse).role("custom").node("*").map,
          grantedOrDenied(traverse).role("custom").node("*").database("bar").map,
          grantedOrDenied(traverse).role("custom").relationship("*").map,
          grantedOrDenied(traverse).role("custom").relationship("*").database("bar").map
        ))

        // WHEN
        execute(s"REVOKE $revokeType TRAVERSE ON GRAPH * ELEMENTS * (*) FROM custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          grantedOrDenied(traverse).role("custom").node("*").database("bar").map,
          grantedOrDenied(traverse).role("custom").relationship("*").database("bar").map
        ))
      }

      test(s"should revoke correct $grantOrDeny MATCH privilege different label qualifier with REVOKE $revokeType") {
        // GIVEN
        execute("CREATE ROLE custom")
        execute("CREATE DATABASE foo")
        execute(s"$grantOrDenyCommand MATCH {bar} ON GRAPH foo TO custom")
        execute(s"$grantOrDenyCommand MATCH {bar} ON GRAPH foo NODES A (*) TO custom")
        execute(s"$grantOrDenyCommand MATCH {bar} ON GRAPH foo NODES B (*) TO custom")

        // WHEN
        execute(s"REVOKE $revokeType MATCH {bar} ON GRAPH foo NODES A (*) FROM custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          grantedOrDenied(matchPrivilege).database("foo").role("custom").property("bar").node("*").map,
          grantedOrDenied(matchPrivilege).database("foo").role("custom").relationship("*").property("bar").map,
          grantedOrDenied(matchPrivilege).database("foo").role("custom").property("bar").node("B").map
        ))

        // WHEN
        execute(s"REVOKE $revokeType MATCH {bar} ON GRAPH foo NODES * (*) FROM custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          grantedOrDenied(matchPrivilege).database("foo").role("custom").relationship("*").property("bar").map,
          grantedOrDenied(matchPrivilege).database("foo").role("custom").property("bar").node("B").map
        ))
      }

      test(s"should revoke correct $grantOrDeny MATCH privilege different relationship type qualifier with REVOKE $revokeType") {
        // GIVEN
        execute("CREATE ROLE custom")
        execute("CREATE DATABASE foo")
        execute(s"$grantOrDenyCommand MATCH {bar} ON GRAPH foo TO custom")
        execute(s"$grantOrDenyCommand MATCH {bar} ON GRAPH foo RELATIONSHIPS A (*) TO custom")
        execute(s"$grantOrDenyCommand MATCH {bar} ON GRAPH foo RELATIONSHIPS B (*) TO custom")

        // WHEN
        execute(s"REVOKE $revokeType MATCH {bar} ON GRAPH foo RELATIONSHIPS A (*) FROM custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          grantedOrDenied(matchPrivilege).database("foo").role("custom").node("*").property("bar").map,
          grantedOrDenied(matchPrivilege).database("foo").role("custom").relationship("*").property("bar").map,
          grantedOrDenied(matchPrivilege).database("foo").role("custom").property("bar").relationship("B").map
        ))

        // WHEN
        execute(s"REVOKE $revokeType MATCH {bar} ON GRAPH foo RELATIONSHIPS * (*) FROM custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          grantedOrDenied(matchPrivilege).database("foo").role("custom").node("*").property("bar").map,
          grantedOrDenied(matchPrivilege).database("foo").role("custom").property("bar").relationship("B").map
        ))
      }

      test(s"should revoke correct $grantOrDeny MATCH privilege different element type qualifier with REVOKE $revokeType") {
        // GIVEN
        execute("CREATE ROLE custom")
        execute("CREATE DATABASE foo")
        execute(s"$grantOrDenyCommand MATCH {bar} ON GRAPH foo TO custom")
        execute(s"$grantOrDenyCommand MATCH {bar} ON GRAPH foo ELEMENTS A, B (*) TO custom")

        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          grantedOrDenied(matchPrivilege).database("foo").role("custom").property("bar").node("*").map,
          grantedOrDenied(matchPrivilege).database("foo").role("custom").property("bar").node("A").map,
          grantedOrDenied(matchPrivilege).database("foo").role("custom").property("bar").node("B").map,
          grantedOrDenied(matchPrivilege).database("foo").role("custom").property("bar").relationship("*").map,
          grantedOrDenied(matchPrivilege).database("foo").role("custom").property("bar").relationship("A").map,
          grantedOrDenied(matchPrivilege).database("foo").role("custom").property("bar").relationship("B").map
        ))

        // WHEN
        execute(s"REVOKE $revokeType MATCH {bar} ON GRAPH $$db ELEMENTS A (*) FROM $$role", Map("db" -> "foo", "role" -> "custom"))

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          grantedOrDenied(matchPrivilege).database("foo").role("custom").property("bar").node("*").map,
          grantedOrDenied(matchPrivilege).database("foo").role("custom").property("bar").node("B").map,
          grantedOrDenied(matchPrivilege).database("foo").role("custom").property("bar").relationship("*").map,
          grantedOrDenied(matchPrivilege).database("foo").role("custom").property("bar").relationship("B").map
        ))

        // WHEN
        execute(s"REVOKE $revokeType MATCH {bar} ON GRAPH $$db ELEMENTS * (*) FROM $$role", Map("db" -> "foo", "role" -> "custom"))

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          grantedOrDenied(matchPrivilege).database("foo").role("custom").property("bar").node("B").map,
          grantedOrDenied(matchPrivilege).database("foo").role("custom").property("bar").relationship("B").map
        ))

        // WHEN
        execute(s"$grantOrDenyCommand MATCH {bar} ON GRAPH foo ELEMENTS C (*) TO custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          grantedOrDenied(matchPrivilege).database("foo").role("custom").property("bar").node("B").map,
          grantedOrDenied(matchPrivilege).database("foo").role("custom").property("bar").node("C").map,
          grantedOrDenied(matchPrivilege).database("foo").role("custom").property("bar").relationship("B").map,
          grantedOrDenied(matchPrivilege).database("foo").role("custom").property("bar").relationship("C").map
        ))

        // WHEN
        execute(s"REVOKE $revokeType MATCH {bar} ON GRAPH $$db ELEMENTS B, C (*) FROM $$role", Map("db" -> "foo", "role" -> "custom"))

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set.empty)
      }

      test(s"should revoke correct $grantOrDeny MATCH privilege different property with REVOKE $revokeType") {
        // GIVEN
        execute("CREATE ROLE custom")
        execute("CREATE DATABASE foo")
        execute(s"$grantOrDenyCommand MATCH {*} ON GRAPH foo TO custom")
        execute(s"$grantOrDenyCommand MATCH {a} ON GRAPH foo TO custom")
        execute(s"$grantOrDenyCommand MATCH {b} ON GRAPH foo TO custom")

        // WHEN
        execute(s"REVOKE $revokeType MATCH {a} ON GRAPH foo FROM custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          grantedOrDenied(matchPrivilege).database("foo").role("custom").node("*").map,
          grantedOrDenied(matchPrivilege).database("foo").role("custom").node("*").property("b").map,
          grantedOrDenied(matchPrivilege).database("foo").role("custom").relationship("*").map,
          grantedOrDenied(matchPrivilege).database("foo").role("custom").relationship("*").property("b").map
        ))

        // WHEN
        execute(s"REVOKE $revokeType MATCH {*} ON GRAPH foo FROM custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          grantedOrDenied(matchPrivilege).database("foo").role("custom").node("*").property("b").map,
          grantedOrDenied(matchPrivilege).database("foo").role("custom").relationship("*").property("b").map
        ))

        // WHEN
        execute(s"REVOKE $revokeType MATCH {b} ON GRAPH foo FROM custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set.empty)
      }

      test(s"should revoke correct $grantOrDeny MATCH privilege different databases with REVOKE $revokeType") {
        // GIVEN
        execute("CREATE ROLE custom")
        execute("CREATE DATABASE foo")
        execute("CREATE DATABASE bar")
        execute(s"$grantOrDenyCommand MATCH {*} ON GRAPH * TO custom")
        execute(s"$grantOrDenyCommand MATCH {*} ON GRAPH foo TO custom")
        execute(s"$grantOrDenyCommand MATCH {*} ON GRAPH bar TO custom")

        // WHEN
        execute(s"REVOKE $revokeType MATCH {*} ON GRAPH foo FROM custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          grantedOrDenied(matchPrivilege).role("custom").node("*").map,
          grantedOrDenied(matchPrivilege).role("custom").node("*").database("bar").map,
          grantedOrDenied(matchPrivilege).role("custom").relationship("*").map,
          grantedOrDenied(matchPrivilege).role("custom").relationship("*").database("bar").map
        ))

        // WHEN
        execute(s"REVOKE $revokeType MATCH {*} ON GRAPH * FROM custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          grantedOrDenied(matchPrivilege).role("custom").node("*").database("bar").map,
          grantedOrDenied(matchPrivilege).role("custom").relationship("*").database("bar").map
        ))
      }

      test(s"should not revoke $grantOrDeny traverse and read privileges from when having MATCH privileges with REVOKE $revokeType") {
        // GIVEN
        execute("CREATE ROLE custom")
        execute(s"CREATE DATABASE foo")
        execute(s"$grantOrDenyCommand MATCH {foo,bar} ON GRAPH foo NODES A,B (*) TO custom")
        execute(s"$grantOrDenyCommand MATCH {foo,bar} ON GRAPH foo RELATIONSHIPS A,B (*) TO custom")

        val expected = Set(
          grantedOrDenied(matchPrivilege).database("foo").role("custom").node("A").property("foo").map,
          grantedOrDenied(matchPrivilege).database("foo").role("custom").node("B").property("foo").map,
          grantedOrDenied(matchPrivilege).database("foo").role("custom").relationship("A").property("foo").map,
          grantedOrDenied(matchPrivilege).database("foo").role("custom").relationship("B").property("foo").map,
          grantedOrDenied(matchPrivilege).database("foo").role("custom").node("A").property("bar").map,
          grantedOrDenied(matchPrivilege).database("foo").role("custom").node("B").property("bar").map,
          grantedOrDenied(matchPrivilege).database("foo").role("custom").relationship("A").property("bar").map,
          grantedOrDenied(matchPrivilege).database("foo").role("custom").relationship("B").property("bar").map
        )

        // WHEN
        execute(s"REVOKE $revokeType TRAVERSE ON GRAPH foo NODES A (*) FROM custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(expected)

        // WHEN
        execute(s"REVOKE $revokeType READ {foo,bar} ON GRAPH foo RELATIONSHIPS B (*) FROM custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(expected)
      }

      test(s"should not revoke $grantOrDeny traverse and read privileges from different MATCH privileges on elements with REVOKE $revokeType") {
        // GIVEN
        execute("CREATE ROLE custom")
        execute(s"$grantOrDenyCommand MATCH {foo,bar} ON GRAPH * ELEMENTS A,B (*) TO custom")

        val expected = Set(
          grantedOrDenied(matchPrivilege).role("custom").node("A").property("foo").map,
          grantedOrDenied(matchPrivilege).role("custom").node("B").property("foo").map,
          grantedOrDenied(matchPrivilege).role("custom").node("A").property("bar").map,
          grantedOrDenied(matchPrivilege).role("custom").node("B").property("bar").map,
          grantedOrDenied(matchPrivilege).role("custom").relationship("A").property("foo").map,
          grantedOrDenied(matchPrivilege).role("custom").relationship("B").property("foo").map,
          grantedOrDenied(matchPrivilege).role("custom").relationship("A").property("bar").map,
          grantedOrDenied(matchPrivilege).role("custom").relationship("B").property("bar").map
        )

        // WHEN
        execute(s"REVOKE $revokeType TRAVERSE ON GRAPH * ELEMENTS A (*) FROM custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(expected)

        // WHEN
        execute(s"REVOKE $revokeType READ {foo,bar} ON GRAPH * ELEMENTS B (*) FROM custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(expected)
      }

      test(s"should revoke correct $grantOrDeny MATCH privilege from different traverse, read and MATCH privileges with REVOKE $revokeType") {
        // GIVEN
        execute("CREATE ROLE custom")
        execute("CREATE DATABASE foo")
        execute(s"$grantOrDenyCommand MATCH {*} ON GRAPH foo TO custom")
        execute(s"$grantOrDenyCommand MATCH {a} ON GRAPH foo TO custom")
        execute(s"$grantOrDenyCommand READ  {b} ON GRAPH foo TO custom")

        execute(s"$grantOrDenyCommand TRAVERSE  ON GRAPH foo NODES A (*) TO custom")
        execute(s"$grantOrDenyCommand MATCH {a} ON GRAPH foo NODES A (*) TO custom")

        execute(s"$grantOrDenyCommand TRAVERSE  ON GRAPH foo RELATIONSHIPS A (*) TO custom")
        execute(s"$grantOrDenyCommand MATCH {a} ON GRAPH foo RELATIONSHIPS A (*) TO custom")

        // WHEN
        execute(s"REVOKE $revokeType MATCH {b} ON GRAPH foo NODES * (*) FROM custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          grantedOrDenied(matchPrivilege).database("foo").role("custom").node("*").map,
          grantedOrDenied(matchPrivilege).database("foo").role("custom").node("*").property("a").map,
          grantedOrDenied(read).database("foo").role("custom").node("*").property("b").map,
          grantedOrDenied(matchPrivilege).database("foo").role("custom").relationship("*").map,
          grantedOrDenied(matchPrivilege).database("foo").role("custom").relationship("*").property("a").map,
          grantedOrDenied(read).database("foo").role("custom").relationship("*").property("b").map,

          grantedOrDenied(traverse).role("custom").database("foo").node("A").map,
          grantedOrDenied(traverse).role("custom").database("foo").relationship("A").map,
          grantedOrDenied(matchPrivilege).database("foo").role("custom").property("a").node("A").map,
          grantedOrDenied(matchPrivilege).database("foo").role("custom").property("a").relationship("A").map
        ))

        // WHEN
        execute(s"REVOKE $revokeType MATCH {a} ON GRAPH foo RELATIONSHIP * (*) FROM custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          grantedOrDenied(matchPrivilege).database("foo").role("custom").node("*").map,
          grantedOrDenied(matchPrivilege).database("foo").role("custom").node("*").property("a").map,
          grantedOrDenied(read).database("foo").role("custom").node("*").property("b").map,
          grantedOrDenied(matchPrivilege).database("foo").role("custom").relationship("*").map,
          grantedOrDenied(read).database("foo").role("custom").relationship("*").property("b").map,

          grantedOrDenied(traverse).role("custom").database("foo").node("A").map,
          grantedOrDenied(traverse).role("custom").database("foo").relationship("A").map,
          grantedOrDenied(matchPrivilege).database("foo").role("custom").property("a").node("A").map,
          grantedOrDenied(matchPrivilege).database("foo").role("custom").property("a").relationship("A").map
        ))

        // WHEN
        execute(s"REVOKE $revokeType MATCH {a} ON GRAPH foo NODES A (*) FROM custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          grantedOrDenied(matchPrivilege).database("foo").role("custom").node("*").map,
          grantedOrDenied(matchPrivilege).database("foo").role("custom").node("*").property("a").map,
          grantedOrDenied(read).database("foo").role("custom").node("*").property("b").map,
          grantedOrDenied(matchPrivilege).database("foo").role("custom").relationship("*").map,
          grantedOrDenied(read).database("foo").role("custom").relationship("*").property("b").map,

          grantedOrDenied(traverse).role("custom").database("foo").node("A").map,
          grantedOrDenied(traverse).role("custom").database("foo").relationship("A").map,
          grantedOrDenied(matchPrivilege).database("foo").role("custom").property("a").relationship("A").map
        ))

        // WHEN
        execute(s"REVOKE $revokeType MATCH {a} ON GRAPH foo RELATIONSHIPS A (*) FROM custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          grantedOrDenied(matchPrivilege).database("foo").role("custom").node("*").map,
          grantedOrDenied(matchPrivilege).database("foo").role("custom").node("*").property("a").map,
          grantedOrDenied(read).database("foo").role("custom").node("*").property("b").map,
          grantedOrDenied(matchPrivilege).database("foo").role("custom").relationship("*").map,
          grantedOrDenied(read).database("foo").role("custom").relationship("*").property("b").map,

          grantedOrDenied(traverse).role("custom").database("foo").node("A").map,
          grantedOrDenied(traverse).role("custom").database("foo").relationship("A").map
        ))
      }

      test(s"should revoke correct $grantOrDeny MATCH privilege from different traverse, read and MATCH privileges on elements with REVOKE $revokeType") {
        // GIVEN
        execute("CREATE ROLE custom")
        execute(s"$grantOrDenyCommand MATCH {*} ON GRAPH * ELEMENTS * (*) TO custom")
        execute(s"$grantOrDenyCommand MATCH {a} ON GRAPH * ELEMENTS * (*) TO custom")
        execute(s"$grantOrDenyCommand READ  {b} ON GRAPH * ELEMENTS * (*) TO custom")

        execute(s"$grantOrDenyCommand TRAVERSE  ON GRAPH * ELEMENTS A (*) TO custom")
        execute(s"$grantOrDenyCommand MATCH {a} ON GRAPH * ELEMENTS A (*) TO custom")

        // WHEN
        execute(s"REVOKE $revokeType MATCH {b} ON GRAPH * ELEMENTS * (*) FROM custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          grantedOrDenied(matchPrivilege).role("custom").node("*").map,
          grantedOrDenied(matchPrivilege).role("custom").node("*").property("a").map,
          grantedOrDenied(read).role("custom").node("*").property("b").map,
          grantedOrDenied(matchPrivilege).role("custom").relationship("*").map,
          grantedOrDenied(matchPrivilege).role("custom").relationship("*").property("a").map,
          grantedOrDenied(read).role("custom").relationship("*").property("b").map,

          grantedOrDenied(traverse).role("custom").node("A").map,
          grantedOrDenied(traverse).role("custom").relationship("A").map,
          grantedOrDenied(matchPrivilege).role("custom").node("A").property("a").map,
          grantedOrDenied(matchPrivilege).role("custom").relationship("A").property("a").map
        ))

        // WHEN
        execute(s"REVOKE $revokeType MATCH {a} ON GRAPH * ELEMENTS * (*) FROM custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          grantedOrDenied(matchPrivilege).role("custom").node("*").map,
          grantedOrDenied(read).role("custom").node("*").property("b").map,
          grantedOrDenied(matchPrivilege).role("custom").relationship("*").map,
          grantedOrDenied(read).role("custom").relationship("*").property("b").map,

          grantedOrDenied(traverse).role("custom").node("A").map,
          grantedOrDenied(traverse).role("custom").relationship("A").map,
          grantedOrDenied(matchPrivilege).role("custom").node("A").property("a").map,
          grantedOrDenied(matchPrivilege).role("custom").relationship("A").property("a").map
        ))

        // WHEN
        execute(s"REVOKE $revokeType MATCH {a} ON GRAPH * ELEMENTS A (*) FROM custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          grantedOrDenied(matchPrivilege).role("custom").node("*").map,
          grantedOrDenied(read).role("custom").node("*").property("b").map,
          grantedOrDenied(matchPrivilege).role("custom").relationship("*").map,
          grantedOrDenied(read).role("custom").relationship("*").property("b").map,

          grantedOrDenied(traverse).role("custom").node("A").map,
          grantedOrDenied(traverse).role("custom").relationship("A").map
        ))
      }

      test(s"should revoke correct $grantOrDeny elements privilege when granted as nodes + relationships with REVOKE $revokeType") {
        // GIVEN
        execute("CREATE ROLE custom")
        execute(s"$grantOrDenyCommand MATCH {*} ON GRAPH * NODES A TO custom")
        execute(s"$grantOrDenyCommand MATCH {*} ON GRAPH * RELATIONSHIPS A TO custom")

        execute(s"$grantOrDenyCommand MATCH {*} ON GRAPH * NODES * TO custom")
        execute(s"$grantOrDenyCommand MATCH {*} ON GRAPH * RELATIONSHIPS * TO custom")

        // WHEN
        execute(s"REVOKE $revokeType MATCH {*} ON GRAPH * ELEMENTS * FROM custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          grantedOrDenied(matchPrivilege).role("custom").node("A").map,
          grantedOrDenied(matchPrivilege).role("custom").relationship("A").map
        ))
      }

      test(s"should revoke correct $grantOrDeny elements privilege when granted as specific nodes + relationships with REVOKE $revokeType") {
        // GIVEN
        execute("CREATE ROLE custom")
        execute(s"$grantOrDenyCommand MATCH {*} ON GRAPH * NODES A TO custom")
        execute(s"$grantOrDenyCommand MATCH {*} ON GRAPH * RELATIONSHIPS A TO custom")

        execute(s"$grantOrDenyCommand MATCH {*} ON GRAPH * NODES * TO custom")
        execute(s"$grantOrDenyCommand MATCH {*} ON GRAPH * RELATIONSHIPS * TO custom")

        // WHEN
        execute(s"REVOKE $revokeType MATCH {*} ON GRAPH * ELEMENTS A FROM custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          grantedOrDenied(matchPrivilege).role("custom").node("*").map,
          grantedOrDenied(matchPrivilege).role("custom").relationship("*").map
        ))
      }

      test(s"should revoke existing part when revoking $grantOrDeny elements privilege when granted only nodes or relationships with REVOKE $revokeType") {
        // GIVEN
        execute("CREATE ROLE custom")
        execute("ALTER USER neo4j SET PASSWORD 'abc' CHANGE NOT REQUIRED")

        execute(s"$grantOrDenyCommand MATCH {foo} ON GRAPH * NODES * TO custom")
        execute(s"$grantOrDenyCommand MATCH {bar} ON GRAPH * RELATIONSHIPS * TO custom")

        // WHEN
        executeOnSystem("neo4j", "abc", s"REVOKE $revokeType MATCH {foo} ON GRAPH * ELEMENTS * FROM custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          grantedOrDenied(matchPrivilege).role("custom").property("bar").relationship("*").map
        ))

        // WHEN
        executeOnSystem("neo4j", "abc", s"REVOKE $revokeType MATCH {bar} ON GRAPH * FROM custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set.empty)
      }

      test(s"should do nothing when revoking $grantOrDeny privilege from non-existent role with REVOKE $revokeType") {
        // GIVEN
        execute("CREATE ROLE custom")
        execute(s"$grantOrDenyCommand TRAVERSE ON GRAPH * NODES * (*) TO custom")
        execute(s"$grantOrDenyCommand READ {*} ON GRAPH * NODES * (*) TO custom")
        execute(s"$grantOrDenyCommand MATCH {*} ON GRAPH * NODES A (*) TO custom")

        // WHEN
        execute(s"REVOKE $revokeType TRAVERSE ON GRAPH * NODES * (*) FROM wrongRole")

        // THEN
        execute("SHOW ROLE wrongRole PRIVILEGES").toSet should be(Set.empty)

        // WHEN
        execute(s"REVOKE $revokeType READ {*} ON GRAPH * NODES * (*) FROM wrongRole")

        // THEN
        execute("SHOW ROLE wrongRole PRIVILEGES").toSet should be(Set.empty)

        // WHEN
        execute(s"REVOKE $revokeType MATCH {*} ON GRAPH * NODES A (*) FROM wrongRole")

        // THEN
        execute("SHOW ROLE wrongRole PRIVILEGES").toSet should be(Set.empty)
      }

      test(s"should do nothing when revoking $grantOrDeny privilege not granted to role with REVOKE $revokeType") {
        // GIVEN
        execute("CREATE ROLE custom")
        execute("CREATE ROLE role")
        execute(s"$grantOrDenyCommand TRAVERSE ON GRAPH * NODES * (*) TO custom")
        execute(s"$grantOrDenyCommand READ {*} ON GRAPH * NODES * (*) TO custom")
        execute(s"$grantOrDenyCommand MATCH {*} ON GRAPH * NODES A (*) TO custom")

        // WHEN
        execute(s"REVOKE $revokeType TRAVERSE ON GRAPH * NODES * (*) FROM role")

        // THEN
        execute("SHOW ROLE role PRIVILEGES").toSet should be(Set.empty)

        // WHEN
        execute(s"REVOKE $revokeType READ {*} ON GRAPH * NODES * (*) FROM role")

        // THEN
        execute("SHOW ROLE role PRIVILEGES").toSet should be(Set.empty)

        // WHEN
        execute(s"REVOKE $revokeType MATCH {*} ON GRAPH * NODES A (*) FROM role")

        // THEN
        execute("SHOW ROLE role PRIVILEGES").toSet should be(Set.empty)
      }
  }

  Seq(
    ("label", "NODES", addNode: builderType),
    ("relationship type", "RELATIONSHIPS", addRel: builderType)
  ).foreach {
    case (segmentName, segmentCommand, segmentFunction: builderType) =>

      test(s"should revoke both grant and deny when revoking traverse $segmentName privilege") {
        // GIVEN
        execute("CREATE ROLE custom")
        execute(s"GRANT TRAVERSE ON GRAPH * $segmentCommand A (*) TO custom")
        execute(s"DENY TRAVERSE ON GRAPH * $segmentCommand A (*) TO custom")

        // WHEN
        execute(s"REVOKE TRAVERSE ON GRAPH * $segmentCommand A (*) FROM custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set.empty)
      }

      test(s"should revoke both grant and deny when revoking read $segmentName privilege") {
        // GIVEN
        execute("CREATE ROLE custom")
        execute(s"GRANT READ {prop} ON GRAPH * $segmentCommand A (*) TO custom")
        execute(s"DENY READ {prop} ON GRAPH * $segmentCommand A (*) TO custom")

        // WHEN
        execute(s"REVOKE READ {prop} ON GRAPH * $segmentCommand A (*) FROM custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set.empty)
      }

      test(s"should revoke both grant and deny when revoking match $segmentName privilege") {
        // GIVEN
        execute("CREATE ROLE custom")
        execute(s"GRANT MATCH {prop} ON GRAPH * $segmentCommand A (*) TO custom")
        execute(s"DENY MATCH {prop} ON GRAPH * $segmentCommand A (*) TO custom")

        // WHEN
        execute(s"REVOKE MATCH {prop} ON GRAPH * $segmentCommand A (*) FROM custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set.empty)
      }
  }
}
