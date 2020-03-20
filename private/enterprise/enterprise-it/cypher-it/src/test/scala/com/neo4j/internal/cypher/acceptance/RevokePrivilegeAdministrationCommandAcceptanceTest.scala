/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.internal.cypher.acceptance

import org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME
import org.neo4j.exceptions.DatabaseAdministrationException
import org.scalatest.enablers.Messaging.messagingNatureOfThrowable

// Tests for REVOKE TRAVERSE, REVOKE READ and REVOKE MATCH
class RevokePrivilegeAdministrationCommandAcceptanceTest extends AdministrationCommandAcceptanceTestBase {

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
        execute(s"$grantOrDenyCommand READ {bar} ON GRAPH foo NODES * (*) TO custom")
        execute(s"$grantOrDenyCommand READ {bar} ON GRAPH foo NODES A (*) TO custom")
        execute(s"$grantOrDenyCommand READ {bar} ON GRAPH foo NODES B (*) TO custom")
        execute(s"$grantOrDenyCommand READ {bar} ON GRAPH foo RELATIONSHIPS * (*) TO custom")
        execute(s"$grantOrDenyCommand READ {bar} ON GRAPH foo RELATIONSHIPS A (*) TO custom")
        execute(s"$grantOrDenyCommand READ {bar} ON GRAPH foo RELATIONSHIPS B (*) TO custom")

        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          read(grantOrDenyRelType).database("foo").role("custom").property("bar").relationship("*").map,
          read(grantOrDenyRelType).database("foo").role("custom").property("bar").relationship("A").map,
          read(grantOrDenyRelType).database("foo").role("custom").property("bar").relationship("B").map,
          read(grantOrDenyRelType).database("foo").role("custom").property("bar").node("*").map,
          read(grantOrDenyRelType).database("foo").role("custom").property("bar").node("A").map,
          read(grantOrDenyRelType).database("foo").role("custom").property("bar").node("B").map
        ))

        // WHEN
        execute(s"REVOKE $revokeType READ {bar} ON GRAPH $$db NODES A (*) FROM $$role", Map("db" -> "foo", "role" -> "custom"))

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          read(grantOrDenyRelType).database("foo").role("custom").property("bar").relationship("*").map,
          read(grantOrDenyRelType).database("foo").role("custom").property("bar").relationship("A").map,
          read(grantOrDenyRelType).database("foo").role("custom").property("bar").relationship("B").map,
          read(grantOrDenyRelType).database("foo").role("custom").property("bar").node("*").map,
          read(grantOrDenyRelType).database("foo").role("custom").property("bar").node("B").map
        ))

        // WHEN
        execute(s"REVOKE $revokeType READ {bar} ON GRAPH $$db NODES * (*) FROM $$role", Map("db" -> "foo", "role" -> "custom"))

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
        execute(s"$grantOrDenyCommand READ {bar} ON GRAPH foo NODES * (*) TO custom")
        execute(s"$grantOrDenyCommand READ {bar} ON GRAPH foo NODES A (*) TO custom")
        execute(s"$grantOrDenyCommand READ {bar} ON GRAPH foo NODES B (*) TO custom")
        execute(s"$grantOrDenyCommand READ {bar} ON GRAPH foo RELATIONSHIPS * (*) TO custom")
        execute(s"$grantOrDenyCommand READ {bar} ON GRAPH foo RELATIONSHIPS A (*) TO custom")
        execute(s"$grantOrDenyCommand READ {bar} ON GRAPH foo RELATIONSHIPS B (*) TO custom")

        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          read(grantOrDenyRelType).database("foo").role("custom").property("bar").node("*").map,
          read(grantOrDenyRelType).database("foo").role("custom").property("bar").node("A").map,
          read(grantOrDenyRelType).database("foo").role("custom").property("bar").node("B").map,
          read(grantOrDenyRelType).database("foo").role("custom").property("bar").relationship("*").map,
          read(grantOrDenyRelType).database("foo").role("custom").property("bar").relationship("A").map,
          read(grantOrDenyRelType).database("foo").role("custom").property("bar").relationship("B").map
        ))

        // WHEN
        execute(s"REVOKE $revokeType READ {bar} ON GRAPH foo RELATIONSHIPS A (*) FROM custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          read(grantOrDenyRelType).database("foo").role("custom").property("bar").node("*").map,
          read(grantOrDenyRelType).database("foo").role("custom").property("bar").node("A").map,
          read(grantOrDenyRelType).database("foo").role("custom").property("bar").node("B").map,
          read(grantOrDenyRelType).database("foo").role("custom").property("bar").relationship("*").map,
          read(grantOrDenyRelType).database("foo").role("custom").property("bar").relationship("B").map
        ))

        // WHEN
        execute(s"REVOKE $revokeType READ {bar} ON GRAPH foo RELATIONSHIPS * (*) FROM custom")

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
        execute(s"$grantOrDenyCommand READ {bar} ON GRAPH foo ELEMENTS * (*) TO custom")
        execute(s"$grantOrDenyCommand READ {bar} ON GRAPH foo ELEMENTS A (*) TO custom")
        execute(s"$grantOrDenyCommand READ {bar} ON GRAPH foo ELEMENTS B, C (*) TO custom")

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
        execute(s"REVOKE $revokeType READ {bar} ON GRAPH foo ELEMENTS A (*) FROM custom")

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
        execute(s"REVOKE $revokeType READ {bar} ON GRAPH foo ELEMENTS * (*) FROM custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          read(grantOrDenyRelType).database("foo").role("custom").property("bar").node("B").map,
          read(grantOrDenyRelType).database("foo").role("custom").property("bar").node("C").map,
          read(grantOrDenyRelType).database("foo").role("custom").property("bar").relationship("B").map,
          read(grantOrDenyRelType).database("foo").role("custom").property("bar").relationship("C").map
        ))

        // WHEN
        execute(s"REVOKE $revokeType READ {bar} ON GRAPH foo ELEMENTS B, C (*) FROM custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set.empty)
      }

      test(s"should revoke correct $grantOrDeny read privilege different property with REVOKE $revokeType") {
        // GIVEN
        selectDatabase(SYSTEM_DATABASE_NAME)
        execute("CREATE ROLE custom")
        execute("CREATE DATABASE foo")
        execute(s"$grantOrDenyCommand READ {*} ON GRAPH foo TO custom")
        execute(s"$grantOrDenyCommand READ {a} ON GRAPH foo TO custom")
        execute(s"$grantOrDenyCommand READ {b} ON GRAPH foo TO custom")

        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          read(grantOrDenyRelType).database("foo").role("custom").node("*").property("a").map,
          read(grantOrDenyRelType).database("foo").role("custom").node("*").property("b").map,
          read(grantOrDenyRelType).database("foo").role("custom").node("*").map,
          read(grantOrDenyRelType).database("foo").role("custom").relationship("*").property("a").map,
          read(grantOrDenyRelType).database("foo").role("custom").relationship("*").property("b").map,
          read(grantOrDenyRelType).database("foo").role("custom").relationship("*").map
        ))

        // WHEN
        execute(s"REVOKE $revokeType READ {a} ON GRAPH foo NODES * (*) FROM custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          read(grantOrDenyRelType).database("foo").role("custom").node("*").property("b").map,
          read(grantOrDenyRelType).database("foo").role("custom").node("*").map,
          read(grantOrDenyRelType).database("foo").role("custom").relationship("*").property("a").map,
          read(grantOrDenyRelType).database("foo").role("custom").relationship("*").property("b").map,
          read(grantOrDenyRelType).database("foo").role("custom").relationship("*").map
        ))

        // WHEN
        execute(s"REVOKE $revokeType READ {*} ON GRAPH foo RELATIONSHIPS * (*) FROM custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          read(grantOrDenyRelType).database("foo").role("custom").node("*").property("b").map,
          read(grantOrDenyRelType).database("foo").role("custom").node("*").map,
          read(grantOrDenyRelType).database("foo").role("custom").relationship("*").property("a").map,
          read(grantOrDenyRelType).database("foo").role("custom").relationship("*").property("b").map
        ))

        // WHEN
        execute(s"REVOKE $revokeType READ {b} ON GRAPH foo FROM custom")

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
        execute(s"$grantOrDenyCommand READ {*} ON GRAPH * TO custom")
        execute(s"$grantOrDenyCommand READ {*} ON GRAPH foo TO custom")
        execute(s"$grantOrDenyCommand READ {*} ON GRAPH bar TO custom")

        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          read(grantOrDenyRelType).role("custom").node("*").map,
          read(grantOrDenyRelType).role("custom").node("*").database("foo").map,
          read(grantOrDenyRelType).role("custom").node("*").database("bar").map,
          read(grantOrDenyRelType).role("custom").relationship("*").map,
          read(grantOrDenyRelType).role("custom").relationship("*").database("foo").map,
          read(grantOrDenyRelType).role("custom").relationship("*").database("bar").map
        ))

        // WHEN
        execute(s"REVOKE $revokeType READ {*} ON GRAPH foo FROM custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          read(grantOrDenyRelType).role("custom").node("*").map,
          read(grantOrDenyRelType).role("custom").node("*").database("bar").map,
          read(grantOrDenyRelType).role("custom").relationship("*").map,
          read(grantOrDenyRelType).role("custom").relationship("*").database("bar").map
        ))

        // WHEN
        execute(s"REVOKE $revokeType READ {*} ON GRAPH * FROM custom")

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
        execute(s"REVOKE $revokeType TRAVERSE ON GRAPH $$db RELATIONSHIPS A (*) FROM $$role", Map("db" -> "foo", "role" -> "custom"))

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          traverse(grantOrDenyRelType).database("foo").role("custom").node("*").map,
          traverse(grantOrDenyRelType).database("foo").role("custom").relationship("*").map,
          traverse(grantOrDenyRelType).database("foo").role("custom").relationship("B").map
        ))

        // WHEN
        execute(s"REVOKE $revokeType TRAVERSE ON GRAPH $$db RELATIONSHIPS * (*) FROM $$role", Map("db" -> "foo", "role" -> "custom"))

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
        execute(s"$grantOrDenyCommand MATCH {bar} ON GRAPH foo TO custom")
        execute(s"$grantOrDenyCommand MATCH {bar} ON GRAPH foo NODES A (*) TO custom")
        execute(s"$grantOrDenyCommand MATCH {bar} ON GRAPH foo NODES B (*) TO custom")

        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          matchPrivilege(grantOrDenyRelType).database("foo").role("custom").property("bar").node("*").map,
          matchPrivilege(grantOrDenyRelType).database("foo").role("custom").relationship("*").property("bar").map,
          matchPrivilege(grantOrDenyRelType).database("foo").role("custom").property("bar").node("A").map,
          matchPrivilege(grantOrDenyRelType).database("foo").role("custom").property("bar").node("B").map
        ))

        // WHEN
        execute(s"REVOKE $revokeType MATCH {bar} ON GRAPH foo NODES A (*) FROM custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          matchPrivilege(grantOrDenyRelType).database("foo").role("custom").property("bar").node("*").map,
          matchPrivilege(grantOrDenyRelType).database("foo").role("custom").relationship("*").property("bar").map,
          matchPrivilege(grantOrDenyRelType).database("foo").role("custom").property("bar").node("B").map
        ))

        // WHEN
        execute(s"REVOKE $revokeType MATCH {bar} ON GRAPH foo NODES * (*) FROM custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          matchPrivilege(grantOrDenyRelType).database("foo").role("custom").relationship("*").property("bar").map,
          matchPrivilege(grantOrDenyRelType).database("foo").role("custom").property("bar").node("B").map
        ))
      }

      test(s"should revoke correct $grantOrDeny MATCH privilege different relationship type qualifier with REVOKE $revokeType") {
        // GIVEN
        selectDatabase(SYSTEM_DATABASE_NAME)
        execute("CREATE ROLE custom")
        execute("CREATE DATABASE foo")
        execute(s"$grantOrDenyCommand MATCH {bar} ON GRAPH foo TO custom")
        execute(s"$grantOrDenyCommand MATCH {bar} ON GRAPH foo RELATIONSHIPS A (*) TO custom")
        execute(s"$grantOrDenyCommand MATCH {bar} ON GRAPH foo RELATIONSHIPS B (*) TO custom")

        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          matchPrivilege(grantOrDenyRelType).database("foo").role("custom").node("*").property("bar").map,
          matchPrivilege(grantOrDenyRelType).database("foo").role("custom").relationship("*").property("bar").map,
          matchPrivilege(grantOrDenyRelType).database("foo").role("custom").property("bar").relationship("A").map,
          matchPrivilege(grantOrDenyRelType).database("foo").role("custom").property("bar").relationship("B").map
        ))

        // WHEN
        execute(s"REVOKE $revokeType MATCH {bar} ON GRAPH foo RELATIONSHIPS A (*) FROM custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          matchPrivilege(grantOrDenyRelType).database("foo").role("custom").node("*").property("bar").map,
          matchPrivilege(grantOrDenyRelType).database("foo").role("custom").relationship("*").property("bar").map,
          matchPrivilege(grantOrDenyRelType).database("foo").role("custom").property("bar").relationship("B").map
        ))

        // WHEN
        execute(s"REVOKE $revokeType MATCH {bar} ON GRAPH foo RELATIONSHIPS * (*) FROM custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          matchPrivilege(grantOrDenyRelType).database("foo").role("custom").node("*").property("bar").map,
          matchPrivilege(grantOrDenyRelType).database("foo").role("custom").property("bar").relationship("B").map
        ))
      }

      test(s"should revoke correct $grantOrDeny MATCH privilege different element type qualifier with REVOKE $revokeType") {
        // GIVEN
        selectDatabase(SYSTEM_DATABASE_NAME)
        execute("CREATE ROLE custom")
        execute("CREATE DATABASE foo")
        execute(s"$grantOrDenyCommand MATCH {bar} ON GRAPH foo TO custom")
        execute(s"$grantOrDenyCommand MATCH {bar} ON GRAPH foo ELEMENTS A, B (*) TO custom")

        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          matchPrivilege(grantOrDenyRelType).database("foo").role("custom").property("bar").node("*").map,
          matchPrivilege(grantOrDenyRelType).database("foo").role("custom").property("bar").node("A").map,
          matchPrivilege(grantOrDenyRelType).database("foo").role("custom").property("bar").node("B").map,
          matchPrivilege(grantOrDenyRelType).database("foo").role("custom").property("bar").relationship("*").map,
          matchPrivilege(grantOrDenyRelType).database("foo").role("custom").property("bar").relationship("A").map,
          matchPrivilege(grantOrDenyRelType).database("foo").role("custom").property("bar").relationship("B").map
        ))

        // WHEN
        execute(s"REVOKE $revokeType MATCH {bar} ON GRAPH $$db ELEMENTS A (*) FROM $$role", Map("db" -> "foo", "role" -> "custom"))

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          matchPrivilege(grantOrDenyRelType).database("foo").role("custom").property("bar").node("*").map,
          matchPrivilege(grantOrDenyRelType).database("foo").role("custom").property("bar").node("B").map,
          matchPrivilege(grantOrDenyRelType).database("foo").role("custom").property("bar").relationship("*").map,
          matchPrivilege(grantOrDenyRelType).database("foo").role("custom").property("bar").relationship("B").map
        ))

        // WHEN
        execute(s"REVOKE $revokeType MATCH {bar} ON GRAPH $$db ELEMENTS * (*) FROM $$role", Map("db" -> "foo", "role" -> "custom"))

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          matchPrivilege(grantOrDenyRelType).database("foo").role("custom").property("bar").node("B").map,
          matchPrivilege(grantOrDenyRelType).database("foo").role("custom").property("bar").relationship("B").map
        ))

        // WHEN
        execute(s"$grantOrDenyCommand MATCH {bar} ON GRAPH foo ELEMENTS C (*) TO custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          matchPrivilege(grantOrDenyRelType).database("foo").role("custom").property("bar").node("B").map,
          matchPrivilege(grantOrDenyRelType).database("foo").role("custom").property("bar").node("C").map,
          matchPrivilege(grantOrDenyRelType).database("foo").role("custom").property("bar").relationship("B").map,
          matchPrivilege(grantOrDenyRelType).database("foo").role("custom").property("bar").relationship("C").map
        ))

        // WHEN
        execute(s"REVOKE $revokeType MATCH {bar} ON GRAPH $$db ELEMENTS B, C (*) FROM $$role", Map("db" -> "foo", "role" -> "custom"))

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set.empty)
      }

      test(s"should revoke correct $grantOrDeny MATCH privilege different property with REVOKE $revokeType") {
        // GIVEN
        selectDatabase(SYSTEM_DATABASE_NAME)
        execute("CREATE ROLE custom")
        execute("CREATE DATABASE foo")
        execute(s"$grantOrDenyCommand MATCH {*} ON GRAPH foo TO custom")
        execute(s"$grantOrDenyCommand MATCH {a} ON GRAPH foo TO custom")
        execute(s"$grantOrDenyCommand MATCH {b} ON GRAPH foo TO custom")

        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          matchPrivilege(grantOrDenyRelType).database("foo").role("custom").node("*").map,
          matchPrivilege(grantOrDenyRelType).database("foo").role("custom").node("*").property("a").map,
          matchPrivilege(grantOrDenyRelType).database("foo").role("custom").node("*").property("b").map,
          matchPrivilege(grantOrDenyRelType).database("foo").role("custom").relationship("*").map,
          matchPrivilege(grantOrDenyRelType).database("foo").role("custom").relationship("*").property("a").map,
          matchPrivilege(grantOrDenyRelType).database("foo").role("custom").relationship("*").property("b").map
        ))

        // WHEN
        execute(s"REVOKE $revokeType MATCH {a} ON GRAPH foo FROM custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          matchPrivilege(grantOrDenyRelType).database("foo").role("custom").node("*").map,
          matchPrivilege(grantOrDenyRelType).database("foo").role("custom").node("*").property("b").map,
          matchPrivilege(grantOrDenyRelType).database("foo").role("custom").relationship("*").map,
          matchPrivilege(grantOrDenyRelType).database("foo").role("custom").relationship("*").property("b").map
        ))

        // WHEN
        execute(s"REVOKE $revokeType MATCH {*} ON GRAPH foo FROM custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          matchPrivilege(grantOrDenyRelType).database("foo").role("custom").node("*").property("b").map,
          matchPrivilege(grantOrDenyRelType).database("foo").role("custom").relationship("*").property("b").map
        ))

        // WHEN
        execute(s"REVOKE $revokeType MATCH {b} ON GRAPH foo FROM custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set.empty)
      }

      test(s"should revoke correct $grantOrDeny MATCH privilege different databases with REVOKE $revokeType") {
        // GIVEN
        selectDatabase(SYSTEM_DATABASE_NAME)
        execute("CREATE ROLE custom")
        execute("CREATE DATABASE foo")
        execute("CREATE DATABASE bar")
        execute(s"$grantOrDenyCommand MATCH {*} ON GRAPH * TO custom")
        execute(s"$grantOrDenyCommand MATCH {*} ON GRAPH foo TO custom")
        execute(s"$grantOrDenyCommand MATCH {*} ON GRAPH bar TO custom")

        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          matchPrivilege(grantOrDenyRelType).role("custom").node("*").map,
          matchPrivilege(grantOrDenyRelType).role("custom").node("*").database("foo").map,
          matchPrivilege(grantOrDenyRelType).role("custom").node("*").database("bar").map,
          matchPrivilege(grantOrDenyRelType).role("custom").relationship("*").map,
          matchPrivilege(grantOrDenyRelType).role("custom").relationship("*").database("foo").map,
          matchPrivilege(grantOrDenyRelType).role("custom").relationship("*").database("bar").map
        ))

        // WHEN
        execute(s"REVOKE $revokeType MATCH {*} ON GRAPH foo FROM custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          matchPrivilege(grantOrDenyRelType).role("custom").node("*").map,
          matchPrivilege(grantOrDenyRelType).role("custom").node("*").database("bar").map,
          matchPrivilege(grantOrDenyRelType).role("custom").relationship("*").map,
          matchPrivilege(grantOrDenyRelType).role("custom").relationship("*").database("bar").map
        ))

        // WHEN
        execute(s"REVOKE $revokeType MATCH {*} ON GRAPH * FROM custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          matchPrivilege(grantOrDenyRelType).role("custom").node("*").database("bar").map,
          matchPrivilege(grantOrDenyRelType).role("custom").relationship("*").database("bar").map
        ))
      }

      test(s"should not revoke $grantOrDeny traverse and read privileges from when having MATCH privileges with REVOKE $revokeType") {
        // GIVEN
        selectDatabase(SYSTEM_DATABASE_NAME)
        execute("CREATE ROLE custom")
        execute(s"CREATE DATABASE foo")
        execute(s"$grantOrDenyCommand MATCH {foo,bar} ON GRAPH foo NODES A,B (*) TO custom")
        execute(s"$grantOrDenyCommand MATCH {foo,bar} ON GRAPH foo RELATIONSHIPS A,B (*) TO custom")

        val expected = Set(
          matchPrivilege(grantOrDenyRelType).database("foo").role("custom").node("A").property("foo").map,
          matchPrivilege(grantOrDenyRelType).database("foo").role("custom").node("B").property("foo").map,
          matchPrivilege(grantOrDenyRelType).database("foo").role("custom").relationship("A").property("foo").map,
          matchPrivilege(grantOrDenyRelType).database("foo").role("custom").relationship("B").property("foo").map,
          matchPrivilege(grantOrDenyRelType).database("foo").role("custom").node("A").property("bar").map,
          matchPrivilege(grantOrDenyRelType).database("foo").role("custom").node("B").property("bar").map,
          matchPrivilege(grantOrDenyRelType).database("foo").role("custom").relationship("A").property("bar").map,
          matchPrivilege(grantOrDenyRelType).database("foo").role("custom").relationship("B").property("bar").map
        )

        execute("SHOW ROLE custom PRIVILEGES").toSet should be(expected)

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
        selectDatabase(SYSTEM_DATABASE_NAME)
        execute("CREATE ROLE custom")
        execute(s"$grantOrDenyCommand MATCH {foo,bar} ON GRAPH * ELEMENTS A,B (*) TO custom")

        val expected = Set(
          matchPrivilege(grantOrDenyRelType).role("custom").node("A").property("foo").map,
          matchPrivilege(grantOrDenyRelType).role("custom").node("B").property("foo").map,
          matchPrivilege(grantOrDenyRelType).role("custom").node("A").property("bar").map,
          matchPrivilege(grantOrDenyRelType).role("custom").node("B").property("bar").map,
          matchPrivilege(grantOrDenyRelType).role("custom").relationship("A").property("foo").map,
          matchPrivilege(grantOrDenyRelType).role("custom").relationship("B").property("foo").map,
          matchPrivilege(grantOrDenyRelType).role("custom").relationship("A").property("bar").map,
          matchPrivilege(grantOrDenyRelType).role("custom").relationship("B").property("bar").map
        )

        execute("SHOW ROLE custom PRIVILEGES").toSet should be(expected)

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
        selectDatabase(SYSTEM_DATABASE_NAME)
        execute("CREATE ROLE custom")
        execute("CREATE DATABASE foo")
        execute(s"$grantOrDenyCommand MATCH {*} ON GRAPH foo TO custom")
        execute(s"$grantOrDenyCommand MATCH {a} ON GRAPH foo TO custom")
        execute(s"$grantOrDenyCommand READ  {b} ON GRAPH foo TO custom")

        execute(s"$grantOrDenyCommand TRAVERSE  ON GRAPH foo NODES A (*) TO custom")
        execute(s"$grantOrDenyCommand MATCH {a} ON GRAPH foo NODES A (*) TO custom")

        execute(s"$grantOrDenyCommand TRAVERSE  ON GRAPH foo RELATIONSHIPS A (*) TO custom")
        execute(s"$grantOrDenyCommand MATCH {a} ON GRAPH foo RELATIONSHIPS A (*) TO custom")

        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          matchPrivilege(grantOrDenyRelType).database("foo").role("custom").node("*").map,
          matchPrivilege(grantOrDenyRelType).database("foo").role("custom").node("*").property("a").map,
          read(grantOrDenyRelType).database("foo").role("custom").node("*").property("b").map,
          matchPrivilege(grantOrDenyRelType).database("foo").role("custom").relationship("*").map,
          matchPrivilege(grantOrDenyRelType).database("foo").role("custom").relationship("*").property("a").map,
          read(grantOrDenyRelType).database("foo").role("custom").relationship("*").property("b").map,

          traverse(grantOrDenyRelType).role("custom").database("foo").node("A").map,
          traverse(grantOrDenyRelType).role("custom").database("foo").relationship("A").map,
          matchPrivilege(grantOrDenyRelType).database("foo").role("custom").property("a").node("A").map,
          matchPrivilege(grantOrDenyRelType).database("foo").role("custom").property("a").relationship("A").map
        ))

        // WHEN
        execute(s"REVOKE $revokeType MATCH {b} ON GRAPH foo NODES * (*) FROM custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          matchPrivilege(grantOrDenyRelType).database("foo").role("custom").node("*").map,
          matchPrivilege(grantOrDenyRelType).database("foo").role("custom").node("*").property("a").map,
          read(grantOrDenyRelType).database("foo").role("custom").node("*").property("b").map,
          matchPrivilege(grantOrDenyRelType).database("foo").role("custom").relationship("*").map,
          matchPrivilege(grantOrDenyRelType).database("foo").role("custom").relationship("*").property("a").map,
          read(grantOrDenyRelType).database("foo").role("custom").relationship("*").property("b").map,

          traverse(grantOrDenyRelType).role("custom").database("foo").node("A").map,
          traverse(grantOrDenyRelType).role("custom").database("foo").relationship("A").map,
          matchPrivilege(grantOrDenyRelType).database("foo").role("custom").property("a").node("A").map,
          matchPrivilege(grantOrDenyRelType).database("foo").role("custom").property("a").relationship("A").map
        ))

        // WHEN
        execute(s"REVOKE $revokeType MATCH {a} ON GRAPH foo RELATIONSHIP * (*) FROM custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          matchPrivilege(grantOrDenyRelType).database("foo").role("custom").node("*").map,
          matchPrivilege(grantOrDenyRelType).database("foo").role("custom").node("*").property("a").map,
          read(grantOrDenyRelType).database("foo").role("custom").node("*").property("b").map,
          matchPrivilege(grantOrDenyRelType).database("foo").role("custom").relationship("*").map,
          read(grantOrDenyRelType).database("foo").role("custom").relationship("*").property("b").map,

          traverse(grantOrDenyRelType).role("custom").database("foo").node("A").map,
          traverse(grantOrDenyRelType).role("custom").database("foo").relationship("A").map,
          matchPrivilege(grantOrDenyRelType).database("foo").role("custom").property("a").node("A").map,
          matchPrivilege(grantOrDenyRelType).database("foo").role("custom").property("a").relationship("A").map
        ))

        // WHEN
        execute(s"REVOKE $revokeType MATCH {a} ON GRAPH foo NODES A (*) FROM custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          matchPrivilege(grantOrDenyRelType).database("foo").role("custom").node("*").map,
          matchPrivilege(grantOrDenyRelType).database("foo").role("custom").node("*").property("a").map,
          read(grantOrDenyRelType).database("foo").role("custom").node("*").property("b").map,
          matchPrivilege(grantOrDenyRelType).database("foo").role("custom").relationship("*").map,
          read(grantOrDenyRelType).database("foo").role("custom").relationship("*").property("b").map,

          traverse(grantOrDenyRelType).role("custom").database("foo").node("A").map,
          traverse(grantOrDenyRelType).role("custom").database("foo").relationship("A").map,
          matchPrivilege(grantOrDenyRelType).database("foo").role("custom").property("a").relationship("A").map
        ))

        // WHEN
        execute(s"REVOKE $revokeType MATCH {a} ON GRAPH foo RELATIONSHIPS A (*) FROM custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          matchPrivilege(grantOrDenyRelType).database("foo").role("custom").node("*").map,
          matchPrivilege(grantOrDenyRelType).database("foo").role("custom").node("*").property("a").map,
          read(grantOrDenyRelType).database("foo").role("custom").node("*").property("b").map,
          matchPrivilege(grantOrDenyRelType).database("foo").role("custom").relationship("*").map,
          read(grantOrDenyRelType).database("foo").role("custom").relationship("*").property("b").map,

          traverse(grantOrDenyRelType).role("custom").database("foo").node("A").map,
          traverse(grantOrDenyRelType).role("custom").database("foo").relationship("A").map
        ))
      }

      test(s"should revoke correct $grantOrDeny MATCH privilege from different traverse, read and MATCH privileges on elements with REVOKE $revokeType") {
        // GIVEN
        selectDatabase(SYSTEM_DATABASE_NAME)
        execute("CREATE ROLE custom")
        execute(s"$grantOrDenyCommand MATCH {*} ON GRAPH * ELEMENTS * (*) TO custom")
        execute(s"$grantOrDenyCommand MATCH {a} ON GRAPH * ELEMENTS * (*) TO custom")
        execute(s"$grantOrDenyCommand READ  {b} ON GRAPH * ELEMENTS * (*) TO custom")

        execute(s"$grantOrDenyCommand TRAVERSE  ON GRAPH * ELEMENTS A (*) TO custom")
        execute(s"$grantOrDenyCommand MATCH {a} ON GRAPH * ELEMENTS A (*) TO custom")

        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          matchPrivilege(grantOrDenyRelType).role("custom").node("*").map,
          matchPrivilege(grantOrDenyRelType).role("custom").node("*").property("a").map,
          read(grantOrDenyRelType).role("custom").node("*").property("b").map,
          matchPrivilege(grantOrDenyRelType).role("custom").relationship("*").map,
          matchPrivilege(grantOrDenyRelType).role("custom").relationship("*").property("a").map,
          read(grantOrDenyRelType).role("custom").relationship("*").property("b").map,

          traverse(grantOrDenyRelType).role("custom").node("A").map,
          traverse(grantOrDenyRelType).role("custom").relationship("A").map,
          matchPrivilege(grantOrDenyRelType).role("custom").node("A").property("a").map,
          matchPrivilege(grantOrDenyRelType).role("custom").relationship("A").property("a").map
        ))

        // WHEN
        execute(s"REVOKE $revokeType MATCH {b} ON GRAPH * ELEMENTS * (*) FROM custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          matchPrivilege(grantOrDenyRelType).role("custom").node("*").map,
          matchPrivilege(grantOrDenyRelType).role("custom").node("*").property("a").map,
          read(grantOrDenyRelType).role("custom").node("*").property("b").map,
          matchPrivilege(grantOrDenyRelType).role("custom").relationship("*").map,
          matchPrivilege(grantOrDenyRelType).role("custom").relationship("*").property("a").map,
          read(grantOrDenyRelType).role("custom").relationship("*").property("b").map,

          traverse(grantOrDenyRelType).role("custom").node("A").map,
          traverse(grantOrDenyRelType).role("custom").relationship("A").map,
          matchPrivilege(grantOrDenyRelType).role("custom").node("A").property("a").map,
          matchPrivilege(grantOrDenyRelType).role("custom").relationship("A").property("a").map
        ))

        // WHEN
        execute(s"REVOKE $revokeType MATCH {a} ON GRAPH * ELEMENTS * (*) FROM custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          matchPrivilege(grantOrDenyRelType).role("custom").node("*").map,
          read(grantOrDenyRelType).role("custom").node("*").property("b").map,
          matchPrivilege(grantOrDenyRelType).role("custom").relationship("*").map,
          read(grantOrDenyRelType).role("custom").relationship("*").property("b").map,

          traverse(grantOrDenyRelType).role("custom").node("A").map,
          traverse(grantOrDenyRelType).role("custom").relationship("A").map,
          matchPrivilege(grantOrDenyRelType).role("custom").node("A").property("a").map,
          matchPrivilege(grantOrDenyRelType).role("custom").relationship("A").property("a").map
        ))

        // WHEN
        execute(s"REVOKE $revokeType MATCH {a} ON GRAPH * ELEMENTS A (*) FROM custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          matchPrivilege(grantOrDenyRelType).role("custom").node("*").map,
          read(grantOrDenyRelType).role("custom").node("*").property("b").map,
          matchPrivilege(grantOrDenyRelType).role("custom").relationship("*").map,
          read(grantOrDenyRelType).role("custom").relationship("*").property("b").map,

          traverse(grantOrDenyRelType).role("custom").node("A").map,
          traverse(grantOrDenyRelType).role("custom").relationship("A").map
        ))
      }

      test(s"should revoke correct $grantOrDeny elements privilege when granted as nodes + relationships with REVOKE $revokeType") {
        // GIVEN
        selectDatabase(SYSTEM_DATABASE_NAME)
        execute("CREATE ROLE custom")
        execute(s"$grantOrDenyCommand MATCH {*} ON GRAPH * NODES A TO custom")
        execute(s"$grantOrDenyCommand MATCH {*} ON GRAPH * RELATIONSHIPS A TO custom")

        execute(s"$grantOrDenyCommand MATCH {*} ON GRAPH * NODES * TO custom")
        execute(s"$grantOrDenyCommand MATCH {*} ON GRAPH * RELATIONSHIPS * TO custom")

        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          matchPrivilege(grantOrDenyRelType).role("custom").node("A").map,
          matchPrivilege(grantOrDenyRelType).role("custom").node("*").map,
          matchPrivilege(grantOrDenyRelType).role("custom").relationship("A").map,
          matchPrivilege(grantOrDenyRelType).role("custom").relationship("*").map
        ))

        // WHEN
        execute(s"REVOKE $revokeType MATCH {*} ON GRAPH * ELEMENTS * FROM custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          matchPrivilege(grantOrDenyRelType).role("custom").node("A").map,
          matchPrivilege(grantOrDenyRelType).role("custom").relationship("A").map
        ))
      }

      test(s"should revoke correct $grantOrDeny elements privilege when granted as specific nodes + relationships with REVOKE $revokeType") {
        // GIVEN
        selectDatabase(SYSTEM_DATABASE_NAME)
        execute("CREATE ROLE custom")
        execute(s"$grantOrDenyCommand MATCH {*} ON GRAPH * NODES A TO custom")
        execute(s"$grantOrDenyCommand MATCH {*} ON GRAPH * RELATIONSHIPS A TO custom")

        execute(s"$grantOrDenyCommand MATCH {*} ON GRAPH * NODES * TO custom")
        execute(s"$grantOrDenyCommand MATCH {*} ON GRAPH * RELATIONSHIPS * TO custom")

        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          matchPrivilege(grantOrDenyRelType).role("custom").node("A").map,
          matchPrivilege(grantOrDenyRelType).role("custom").node("*").map,
          matchPrivilege(grantOrDenyRelType).role("custom").relationship("A").map,
          matchPrivilege(grantOrDenyRelType).role("custom").relationship("*").map
        ))

        // WHEN
        execute(s"REVOKE $revokeType MATCH {*} ON GRAPH * ELEMENTS A FROM custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          matchPrivilege(grantOrDenyRelType).role("custom").node("*").map,
          matchPrivilege(grantOrDenyRelType).role("custom").relationship("*").map
        ))
      }

      test(s"should revoke existing part when revoking $grantOrDeny elements privilege when granted only nodes or relationships with REVOKE $revokeType") {
        // GIVEN
        selectDatabase(SYSTEM_DATABASE_NAME)
        execute("CREATE ROLE custom")
        execute("ALTER USER neo4j SET PASSWORD 'abc' CHANGE NOT REQUIRED")

        execute(s"$grantOrDenyCommand MATCH {foo} ON GRAPH * NODES * TO custom")
        execute(s"$grantOrDenyCommand MATCH {bar} ON GRAPH * RELATIONSHIPS * TO custom")

        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          matchPrivilege(grantOrDenyRelType).role("custom").property("foo").node("*").map,
          matchPrivilege(grantOrDenyRelType).role("custom").property("bar").relationship("*").map
        ))

        // WHEN
        executeOnSystem("neo4j", "abc", s"REVOKE $revokeType MATCH {foo} ON GRAPH * ELEMENTS * FROM custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          matchPrivilege(grantOrDenyRelType).role("custom").property("bar").relationship("*").map
        ))

        // WHEN
        executeOnSystem("neo4j", "abc", s"REVOKE $revokeType MATCH {bar} ON GRAPH * FROM custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set.empty)
      }

      test(s"should do nothing when revoking $grantOrDeny privilege from non-existent role with REVOKE $revokeType") {
        // GIVEN
        selectDatabase(SYSTEM_DATABASE_NAME)
        execute("CREATE ROLE custom")
        execute(s"$grantOrDenyCommand TRAVERSE ON GRAPH * NODES * (*) TO custom")
        execute(s"$grantOrDenyCommand READ {*} ON GRAPH * NODES * (*) TO custom")
        execute(s"$grantOrDenyCommand MATCH {*} ON GRAPH * NODES A (*) TO custom")
        execute("SHOW ROLE wrongRole PRIVILEGES").toSet should be(Set.empty)

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
        selectDatabase(SYSTEM_DATABASE_NAME)
        execute("CREATE ROLE custom")
        execute("CREATE ROLE role")
        execute(s"$grantOrDenyCommand TRAVERSE ON GRAPH * NODES * (*) TO custom")
        execute(s"$grantOrDenyCommand READ {*} ON GRAPH * NODES * (*) TO custom")
        execute(s"$grantOrDenyCommand MATCH {*} ON GRAPH * NODES A (*) TO custom")
        execute("SHOW ROLE role PRIVILEGES").toSet should be(Set.empty)

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

      test(s"should do nothing when revoking $grantOrDeny traversal privilege with missing database with REVOKE $revokeType") {
        // GIVEN
        selectDatabase(SYSTEM_DATABASE_NAME)
        execute("CREATE ROLE custom")
        execute(s"$grantOrDenyCommand TRAVERSE ON GRAPH * NODES * (*) TO custom")

        val customPrivileges = Set(traverse(grantOrDenyRelType).role("custom").node("*").map)
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(customPrivileges)

        // WHEN
        execute(s"REVOKE $revokeType TRAVERSE ON GRAPH foo NODES * (*) FROM custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(customPrivileges)
      }

      test(s"should do nothing when revoking $grantOrDeny read privilege with missing database with REVOKE $revokeType") {
        // GIVEN
        selectDatabase(SYSTEM_DATABASE_NAME)
        execute("CREATE ROLE custom")
        execute(s"$grantOrDenyCommand READ {*} ON GRAPH * NODES * (*) TO custom")

        val customPrivileges = Set(read(grantOrDenyRelType).role("custom").node("*").map)
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(customPrivileges)

        // WHEN
        execute(s"REVOKE $revokeType READ {*} ON GRAPH foo NODES * (*) FROM custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(customPrivileges)
      }

      test(s"should do nothing when revoking $grantOrDeny MATCH privilege with missing database with REVOKE $revokeType") {
        // GIVEN
        selectDatabase(SYSTEM_DATABASE_NAME)
        execute("CREATE ROLE custom")
        execute(s"$grantOrDenyCommand MATCH {*} ON GRAPH * NODES * (*) TO custom")

        val customPrivileges = Set(matchPrivilege(grantOrDenyRelType).role("custom").node("*").map)
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(customPrivileges)

        // WHEN
        execute(s"REVOKE $revokeType MATCH {*} ON GRAPH foo NODES * (*) FROM custom")
        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(customPrivileges)
      }

      test(s"should fail when revoking $grantOrDeny traversal privilege to custom role when not on system database with REVOKE $revokeType") {
        the[DatabaseAdministrationException] thrownBy {
          // WHEN
          execute(s"REVOKE $revokeType TRAVERSE ON GRAPH * NODES * (*) FROM custom")
          // THEN
        } should have message
          s"This is an administration command and it should be executed against the system database: REVOKE ${revokeType}TRAVERSE"
      }

      test(s"should fail when revoking $grantOrDeny read privilege to custom role when not on system database with REVOKE $revokeType") {
        the[DatabaseAdministrationException] thrownBy {
          // WHEN
          execute(s"REVOKE $revokeType READ {*} ON GRAPH * NODES * (*) FROM custom")
          // THEN
        } should have message
          s"This is an administration command and it should be executed against the system database: REVOKE ${revokeType}READ"
      }

      test(s"should fail when revoking $grantOrDeny MATCH privilege to custom role when not on system database with REVOKE $revokeType") {
        the[DatabaseAdministrationException] thrownBy {
          // WHEN
          execute(s"REVOKE $revokeType MATCH {*} ON GRAPH * NODES * (*) FROM custom")
          // THEN
        } should have message
          s"This is an administration command and it should be executed against the system database: REVOKE ${revokeType}MATCH"
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
          segmentFunction(traverse(GRANTED).role("custom"), "A").map,
          segmentFunction(traverse(DENIED).role("custom"), "A").map
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
        execute(s"GRANT READ {prop} ON GRAPH * $segmentCommand A (*) TO custom")
        execute(s"DENY READ {prop} ON GRAPH * $segmentCommand A (*) TO custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          segmentFunction(read(GRANTED).role("custom").property("prop"), "A").map,
          segmentFunction(read(DENIED).role("custom").property("prop"), "A").map
        ))

        // WHEN
        execute(s"REVOKE READ {prop} ON GRAPH * $segmentCommand A (*) FROM custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set.empty)
      }

      test(s"should revoke both grant and deny when revoking match $segmentName privilege") {
        // GIVEN
        selectDatabase(SYSTEM_DATABASE_NAME)
        execute("CREATE ROLE custom")

        // WHEN
        execute(s"GRANT MATCH {prop} ON GRAPH * $segmentCommand A (*) TO custom")
        execute(s"DENY MATCH {prop} ON GRAPH * $segmentCommand A (*) TO custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
          segmentFunction(matchPrivilege(GRANTED).role("custom").property("prop"), "A").map,
          segmentFunction(matchPrivilege(DENIED).role("custom").property("prop"), "A").map
        ))

        // WHEN
        execute(s"REVOKE MATCH {prop} ON GRAPH * $segmentCommand A (*) FROM custom")

        // THEN
        execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set.empty)
      }
  }
}
