/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.internal.cypher.acceptance

import org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME
import org.neo4j.exceptions.DatabaseAdministrationException
import org.neo4j.exceptions.SyntaxException
import org.neo4j.graphdb.QueryExecutionException
import org.neo4j.graphdb.security.AuthorizationViolationException
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException

import scala.collection.JavaConverters.mapAsJavaMapConverter

class PrivilegeAdministrationCommandAcceptanceTest extends AdministrationCommandAcceptanceTestBase with EnterpriseComponentVersionTestSupport {

  test("should return empty counts to the outside for commands that update the system graph internally") {
    //TODO: ADD ANY NEW UPDATING COMMANDS HERE

    // GIVEN
    execute("CREATE ROLE custom")
    execute("CREATE DATABASE foo")
    execute("CREATE DATABASE bar")
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

      "GRANT READ {a,b,c} ON GRAPH foo, bar ELEMENTS p, q TO a, b, c" -> 72,  // 2 graphs * 3 props * 3 roles * 2 labels/types * 2 elements(nodes,rels)

      "GRANT ALL GRAPH PRIVILEGES ON GRAPH foo TO custom" -> 1
    ))
  }

  // Tests for showing privileges

  test("should not show privileges as non admin") {
    // GIVEN
    execute("CREATE USER foo SET PASSWORD 'bar' CHANGE NOT REQUIRED")

    // WHEN & THEN
    the[AuthorizationViolationException] thrownBy {
      executeOnSystem("foo", "bar", "SHOW PRIVILEGES")
    } should have message PERMISSION_DENIED_SHOW_PRIVILEGE
  }

  test("should show privileges for users") {
    execute("SHOW PRIVILEGES").toSet should be(defaultRolePrivileges)
  }

  test("should show privileges WHERE access = 'GRANTED' AND action = 'match'") {

    // WHEN
    val expected = Set("reader", "editor", "publisher", "architect", "admin").flatMap { role =>
      Set(
        granted(matchPrivilege).role(role).node("*").map,
        granted(matchPrivilege).role(role).relationship("*").map
      )
    }
    execute("SHOW PRIVILEGES WHERE access = 'GRANTED' AND action = 'match'").toSet should be(expected)
  }

  test("should not show privileges when an invalid column is specified in the where clause") {

    val theException = the[SyntaxException] thrownBy {
      // WHEN
      execute("SHOW PRIVILEGES WHERE missing = 'notthere'")
    }
    // THEN
    theException.getMessage should startWith ("Variable `missing` not defined (line 1, column 23 (offset: 22))")
  }

  test("should show privileges with YIELD action, access") {

    val expected = defaultRolePrivileges.map(_.filterKeys(Set("action","access").contains))

    // WHEN
    execute("SHOW PRIVILEGES YIELD action, access").toSet should be (expected)
  }

  test("should not show privileges with YIELD missing") {

    val theException = the[SyntaxException] thrownBy {
      // WHEN
      execute("SHOW PRIVILEGES YIELD missing")
    }

    theException.getMessage should startWith("Variable `missing` not defined")
    theException.getMessage should include("(line 1, column 23 (offset: 22))")

  }

  test("should show privileges with YIELD role, action, access ORDER BY action, role, access WHERE access = 'GRANTED'") {

    val expected = defaultRolePrivileges.toList
      .sortBy(row => Seq("action", "access", "role").map(row).mkString(","))
      .map(_.filterKeys(Set("action","access","role").contains))

    // WHEN
    execute("SHOW PRIVILEGES YIELD role, action, access ORDER BY action, role, access WHERE access = 'GRANTED'").toList should be(expected)
  }

  test("should show privileges with YIELD role, action, access WHERE access = 'GRANTED' using default ordering") {

    val expected = defaultRolePrivileges.toList
      .sortBy(row => Seq("role", "action", "access").map(row).mkString(","))
      .map(_.filterKeys(Set("role", "action", "access").contains))

    // WHEN
    execute("SHOW PRIVILEGES YIELD role, action, access WHERE access = 'GRANTED' ").toList should be(expected)
  }

  test("should show privileges with YIELD role, action, access WHERE access = 'GRANTED' RETURN * using default ordering") {

    val expected = defaultRolePrivileges.toList
      .sortBy(row => Seq("role", "action", "access").map(row).mkString(","))
      .map(_.filterKeys(Set("role", "action", "access").contains))

    // WHEN
    execute("SHOW PRIVILEGES YIELD role, action, access WHERE access = 'GRANTED' RETURN *").toList should be(expected)
  }

  test("should show privileges with YIELD role, action, access SKIP 5 LIMIT 5 ORDER BY action, role, access") {

    val expected = defaultRolePrivileges.toList
      .sortBy(row => Seq("action", "role", "access").map(row).mkString(","))
      .map(_.filterKeys(Set("role", "action", "access").contains))
      .slice(5, 10)

    // WHEN
    execute("SHOW PRIVILEGES YIELD role, action, access ORDER BY action, role, access SKIP 5 LIMIT 5 ").toList should be(expected)
  }

  test("should show privileges with YIELD role RETURN DISTINCT role") {

    val expected = Set(Map("role" -> "PUBLIC"), Map("role" -> "admin"), Map("role" -> "architect"), Map("role" -> "editor"), Map("role" -> "publisher"),
      Map("role" -> "reader"))

    // WHEN
    execute("SHOW PRIVILEGES YIELD role RETURN DISTINCT role").toSet should be(expected)
  }

    test("should show privileges with RETURN role, count(*) AS count ORDER BY role") {

      val expected = defaultRolePrivileges.toList
        .groupBy(_.apply("role"))
        .map{ case (role, privs) => Map("role" -> role, "count" -> privs.length)}.toList
        .sortBy(_.apply("role").asInstanceOf[String])

      // WHEN
      execute("SHOW PRIVILEGES YIELD role RETURN role, count(*) AS count ORDER BY role").toList should be(expected)
    }

    test("should show privileges with RETURN role, count(*) ORDER BY role with no aliasing") {

      val expected = defaultRolePrivileges.toList
        .groupBy(_.apply("role"))
        .map{ case (role, privs) => Map("role" -> role, "count(*)" -> privs.length)}.toList
        .sortBy(_.apply("role").asInstanceOf[String])

      // WHEN
      execute("SHOW PRIVILEGES YIELD role RETURN role, count(*) ORDER BY role").toList should be(expected)
    }

  test("should show user privileges with RETURN user, collect(role) as roles") {

     // WHEN
    execute("SHOW USER neo4j PRIVILEGES YIELD * RETURN user, collect(role) as roles").toSet should be(
      Set(Map("user" -> "neo4j", "roles" -> List("PUBLIC", "PUBLIC", "PUBLIC", "admin", "admin", "admin", "admin", "admin", "admin", "admin", "admin", "admin")))
    )
  }

  test("should not show privileges with RETURN role, count(missing) AS count") {

    val theException = the[SyntaxException] thrownBy {
      // WHEN
      execute("SHOW PRIVILEGES YIELD role, missing RETURN role, count(missing) AS count")
    }

    theException.getMessage should startWith("Variable `missing` not defined")
    theException.getMessage should include("(line 1, column 29 (offset: 28))")
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

  test("should show privileges for multiple roles") {
    // WHEN
    val result = execute("SHOW ROLES reader, $role PRIVILEGES", Map("role" -> "editor"))

    // THEN
    val expected = Set(
      granted(access).role("reader").map,
      granted(matchPrivilege).role("reader").node("*").map,
      granted(matchPrivilege).role("reader").relationship("*").map,
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
    } should have message PERMISSION_DENIED_SHOW_PRIVILEGE
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

  test("should show role privileges with YIELD, WHERE and RETURN") {
    // GIVEN
    execute("CREATE ROLE custom")
    execute("GRANT TRAVERSE ON GRAPH * NODES * TO custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES YIELD * WHERE access = 'GRANTED' RETURN *").toSet should
      be(Set(Map("role" -> "custom", "segment" -> "NODE(*)", "resource" -> "graph", "graph" -> "*", "action" -> "traverse", "access" -> "GRANTED")))
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

  test("should show privileges for multiple users") {
    // GIVEN
    setupUserWithCustomRole()

    // WHEN
    val result = execute("SHOW USERS $userParam, joe PRIVILEGES", Map("userParam" -> "neo4j"))

    // THEN
    result.toSet should be(defaultUserPrivileges ++ publicPrivileges("joe") ++ Set(
      granted(access).role("custom").user("joe").map
    ))
  }

  test("should show privileges for specific user WHERE role = 'PUBLIC'") {
    // WHEN
    execute("SHOW USER neo4j PRIVILEGES WHERE role = 'PUBLIC'").toSet should be(publicPrivileges("neo4j"))
  }

  test("should show privileges for specific user YIELD role, action, access") {

    val expected = defaultUserPrivileges.map(_.filterKeys(Set("role", "action", "access").contains))

    // WHEN
    execute("SHOW USER neo4j PRIVILEGES YIELD role, action, access").toSet should be (expected)
  }

  test("should show privileges for specific user YIELD role, action, access ORDER BY action, role, access") {

    val expected = defaultUserPrivileges.toList
      .sortBy(row => Seq("action", "role", "access").map(row).mkString(","))
      .map(_.filterKeys(Set("role", "action", "access").contains))

    // WHEN
    execute("SHOW USER neo4j PRIVILEGES YIELD role, action, access ORDER BY action, role, access")
      .toList should be (expected)

  }

  test("should show privileges for specific user with aliased items in YIELD") {

    val expected = List(Map("foo" -> "GRANTED", "bar" -> "PUBLIC"))
    execute("SHOW USER neo4j PRIVILEGES YIELD segment, access as foo, resource as blah, role as bar where bar = 'PUBLIC' AND segment = 'database' RETURN foo, bar")
      .toList should be (expected)
  }

  test("should show privileges for specific user with aliased items in YIELD and ORDER BY in return") {

    val expected = List(Map("res" -> "all_properties", "bar" -> "admin"), Map("res" -> "all_properties", "bar" -> "admin"), Map("res" -> "database", "bar" -> "admin"),
      Map("res" -> "database", "bar" -> "admin"), Map("res" -> "database", "bar" -> "admin"))
    execute("SHOW USER neo4j PRIVILEGES YIELD access as foo, resource as res, role as bar WHERE bar = 'admin' RETURN res, bar ORDER BY res LIMIT 5")
      .toList should be(expected)
  }

  test("should show privileges for specific user with aliased items in RETURN") {

    val expected = List(Map("foo" -> "GRANTED", "bar" -> "PUBLIC"))
    execute("SHOW USER neo4j PRIVILEGES YIELD access, resource, role, segment where role = 'PUBLIC' AND segment = 'database' RETURN access as foo, role as bar")
      .toList should be (expected)
  }

  test("should not show privileges for specific user with clashing aliases in YIELD") {

    val exception = the[SyntaxException] thrownBy {
      execute("SHOW USER neo4j PRIVILEGES YIELD access as foo, resource as foo where bar = 'PUBLIC' RETURN foo, bar")
    }
    exception.getMessage should startWith("Multiple result columns with the same name are not supported (line 1, column 34 (offset: 33))")
  }

  test("should show privileges for specific user YIELD access, resource, role, segment WHERE role = 'PUBLIC'") {

    val expected = defaultUserPrivileges
      .map(_.filterKeys(Set("access", "resource", "role", "segment").contains))
      .filter(map => map.get("role").contains("PUBLIC"))

    // WHEN
    execute("SHOW USER neo4j PRIVILEGES YIELD access, resource, role, segment WHERE role = 'PUBLIC'").toSet should be(expected)
  }

  test("should not show privileges when WHERE references column omitted from YIELD") {

    // WHEN
    val exception = the[SyntaxException] thrownBy {
      execute("SHOW USER neo4j PRIVILEGES YIELD access, resource WHERE role = 'PUBLIC'").toList
    }
    exception.getMessage should startWith("Variable `role` not defined (line 1, column 57 (offset: 56))")
  }

  test("should not show privileges when ORDER BY references column omitted from YIELD'") {

    // WHEN
    val exception = the[SyntaxException] thrownBy {
      execute("SHOW USER neo4j PRIVILEGES YIELD access, resource ORDER BY role").toList
    }
    exception.getMessage should startWith("Variable `role` not defined (line 1, column 60 (offset: 59))")
  }

  test("should not show privileges for specific user YIELD foo, bar") {

    // WHEN
    val exception = the[SyntaxException] thrownBy {
      execute("SHOW USER neo4j PRIVILEGES YIELD foo, bar").toList
    }
    exception.getMessage should startWith("Variable `foo` not defined (line 1, column 34 (offset: 33))")
  }

  test("should show privileges for specific user RETURN user, count(access) AS count") {
    val expected = Set(Map("user" -> "neo4j", "count" -> 12))

    // WHEN
    execute("SHOW USER neo4j PRIVILEGES YIELD user, access RETURN user, count(access) AS count")
      .toSet should be(expected)
  }

  test("should show privileges for specific user RETURN count(access) AS count") {
    val expected = Set(Map("count" -> 12))

    // WHEN
    execute("SHOW USER neo4j PRIVILEGES YIELD access RETURN count(access) AS count")
      .toSet should be(expected)
  }

  test("should show privileges for specific user RETURN user ORDER BY graph") {
    // WHEN
    execute("SHOW USER neo4j PRIVILEGES YIELD user, access, graph RETURN user ORDER BY graph")
      .toList should be (List.fill(12)(Map("user" -> "neo4j")))
  }

  test("should show privileges for current user RETURN user ORDER BY graph") {
    //GIVEN
    setupUserWithCustomRole()

    // WHEN
    executeOnSystem("joe", "soap", "SHOW USER PRIVILEGES YIELD user, access, graph RETURN user ", resultHandler = (row, _) => {
      // THEN
      row.get("user") should be("joe")
    })
  }

  test("should not show privileges for specific user RETURN user, count(access) AS count ORDER BY graph") {
    // WHEN
    val theException = the[SyntaxException] thrownBy execute("SHOW USER neo4j PRIVILEGES YIELD user, access, graph RETURN user, count(access) AS count ORDER BY graph")

    // THEN
    theException.getMessage should startWith(
      "In a WITH/RETURN with DISTINCT or an aggregation, it is not possible to access variables declared before the WITH/RETURN: graph")
    theException.getMessage should include("(line 1, column 99 (offset: 98))")
  }

  test("should show privileges for current user with aliasing") {
    //GIVEN
    setupUserWithCustomRole()

    // WHEN
    executeOnSystem("joe", "soap", "SHOW USER PRIVILEGES YIELD user as person RETURN person as user", resultHandler = (row, _) => {
      // THEN
      row.get("user") should be("joe")
    })
  }

  test("should show privileges for current user with aliasing in return") {
    //GIVEN
    setupUserWithCustomRole()

    // WHEN
    executeOnSystem("joe", "soap", "SHOW USER PRIVILEGES YIELD user RETURN user as person", resultHandler = (row, _) => {
      // THEN
      row.get("person") should be("joe")
    })
  }

  test("should show user privileges for current user as non admin") {
    // GIVEN
    execute("CREATE USER joe SET PASSWORD 'soap' CHANGE NOT REQUIRED")

    val expected = publicPrivileges("joe").toSeq.sortBy( p => p.get("segment") )

    // WHEN
    executeOnSystem("joe", "soap", "SHOW USER joe PRIVILEGES YIELD * ORDER BY segment", resultHandler = (row, idx) => {
      // THEN
      asPrivilegesResult(row) should be(expected(idx))
    }) should be(3)
  }

  test("should show user privileges for current user as non admin without specifying the user name") {
    // GIVEN
    execute("CREATE USER joe SET PASSWORD 'soap' CHANGE NOT REQUIRED")

    val expected = publicPrivileges("joe").toSeq.sortBy( p => p.get("segment") )

    // WHEN
    executeOnSystem("joe", "soap", "SHOW USER PRIVILEGES YIELD * ORDER BY segment", resultHandler = (row, idx) => {
      // THEN
      asPrivilegesResult(row) should be(expected(idx))
    }) should be(3)
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
    execute("SHOW USER bar PRIVILEGES").toSet should be(publicPrivileges("bar"))
  }

  test("should not show user privileges on a dropped role") {
    // GIVEN
    setupUserWithCustomRole("bar", "secret")
    execute("GRANT TRAVERSE ON GRAPH * NODES * TO custom")

    // WHEN
    execute("DROP ROLE custom")

    // THEN
    execute("SHOW USER bar PRIVILEGES").toSet should be(publicPrivileges("bar"))
  }

  test("should not show show user privilege when not using the system database") {
    //GIVEN
    selectDatabase(DEFAULT_DATABASE_NAME)

    // WHEN
    val exception = the[DatabaseAdministrationException] thrownBy {
      execute("SHOW USER neo4j PRIVILEGES YIELD * WHERE role = $role", "role"-> "PUBLIC")
    }

    // THEN
    exception.getMessage shouldBe "This is an administration command and it should be executed against the system database: SHOW PRIVILEGE"
  }

  // Tests for show grant/deny privileges as (revoke) commands

  for(asRevoke <- Seq(true, false)) {
    val preposition = if (asRevoke) "FROM" else "TO"
    val optionalRevoke = if (asRevoke) "REVOKE " else ""

    test(s"should show privileges as ${optionalRevoke.toLowerCase}commands") {
      // WHEN
      execute(s"SHOW PRIVILEGES AS ${optionalRevoke}COMMANDS").columnAs[String]("command").toSet should be(Set(
        s"${optionalRevoke}GRANT ACCESS ON DEFAULT DATABASE $preposition `PUBLIC`",
        s"${optionalRevoke}GRANT EXECUTE FUNCTION * ON DBMS $preposition `PUBLIC`",
        s"${optionalRevoke}GRANT EXECUTE PROCEDURE * ON DBMS $preposition `PUBLIC`",

        s"${optionalRevoke}GRANT ACCESS ON DATABASE * $preposition `reader`",
        s"${optionalRevoke}GRANT MATCH {*} ON GRAPH * NODE * $preposition `reader`",
        s"${optionalRevoke}GRANT MATCH {*} ON GRAPH * RELATIONSHIP * $preposition `reader`",

        s"${optionalRevoke}GRANT ACCESS ON DATABASE * $preposition `editor`",
        s"${optionalRevoke}GRANT MATCH {*} ON GRAPH * NODE * $preposition `editor`",
        s"${optionalRevoke}GRANT MATCH {*} ON GRAPH * RELATIONSHIP * $preposition `editor`",
        s"${optionalRevoke}GRANT WRITE ON GRAPH * $preposition `editor`",

        s"${optionalRevoke}GRANT ACCESS ON DATABASE * $preposition `publisher`",
        s"${optionalRevoke}GRANT MATCH {*} ON GRAPH * NODE * $preposition `publisher`",
        s"${optionalRevoke}GRANT MATCH {*} ON GRAPH * RELATIONSHIP * $preposition `publisher`",
        s"${optionalRevoke}GRANT WRITE ON GRAPH * $preposition `publisher`",
        s"${optionalRevoke}GRANT NAME MANAGEMENT ON DATABASE * $preposition `publisher`",

        s"${optionalRevoke}GRANT ACCESS ON DATABASE * $preposition `architect`",
        s"${optionalRevoke}GRANT MATCH {*} ON GRAPH * NODE * $preposition `architect`",
        s"${optionalRevoke}GRANT MATCH {*} ON GRAPH * RELATIONSHIP * $preposition `architect`",
        s"${optionalRevoke}GRANT WRITE ON GRAPH * $preposition `architect`",
        s"${optionalRevoke}GRANT CONSTRAINT MANAGEMENT ON DATABASE * $preposition `architect`",
        s"${optionalRevoke}GRANT INDEX MANAGEMENT ON DATABASE * $preposition `architect`",
        s"${optionalRevoke}GRANT NAME MANAGEMENT ON DATABASE * $preposition `architect`",

        s"${optionalRevoke}GRANT ACCESS ON DATABASE * $preposition `admin`",
        s"${optionalRevoke}GRANT MATCH {*} ON GRAPH * NODE * $preposition `admin`",
        s"${optionalRevoke}GRANT MATCH {*} ON GRAPH * RELATIONSHIP * $preposition `admin`",
        s"${optionalRevoke}GRANT WRITE ON GRAPH * $preposition `admin`",
        s"${optionalRevoke}GRANT CONSTRAINT MANAGEMENT ON DATABASE * $preposition `admin`",
        s"${optionalRevoke}GRANT INDEX MANAGEMENT ON DATABASE * $preposition `admin`",
        s"${optionalRevoke}GRANT NAME MANAGEMENT ON DATABASE * $preposition `admin`",
        s"${optionalRevoke}GRANT TRANSACTION MANAGEMENT (*) ON DATABASE * $preposition `admin`",
        s"${optionalRevoke}GRANT START ON DATABASE * $preposition `admin`",
        s"${optionalRevoke}GRANT STOP ON DATABASE * $preposition `admin`",
        s"${optionalRevoke}GRANT ALL DBMS PRIVILEGES ON DBMS $preposition `admin`"
      ))
    }

    test(s"should show role privileges as ${optionalRevoke.toLowerCase}commands") {
      // WHEN
      execute(s"SHOW ROLE admin PRIVILEGES AS ${optionalRevoke}COMMANDS").columnAs[String]("command").toSet should be(Set(
        s"${optionalRevoke}GRANT MATCH {*} ON GRAPH * NODE * $preposition `admin`",
        s"${optionalRevoke}GRANT MATCH {*} ON GRAPH * RELATIONSHIP * $preposition `admin`",
        s"${optionalRevoke}GRANT WRITE ON GRAPH * $preposition `admin`",
        s"${optionalRevoke}GRANT ACCESS ON DATABASE * $preposition `admin`",
        s"${optionalRevoke}GRANT CONSTRAINT MANAGEMENT ON DATABASE * $preposition `admin`",
        s"${optionalRevoke}GRANT INDEX MANAGEMENT ON DATABASE * $preposition `admin`",
        s"${optionalRevoke}GRANT NAME MANAGEMENT ON DATABASE * $preposition `admin`",
        s"${optionalRevoke}GRANT TRANSACTION MANAGEMENT (*) ON DATABASE * $preposition `admin`",
        s"${optionalRevoke}GRANT ALL DBMS PRIVILEGES ON DBMS $preposition `admin`",
        s"${optionalRevoke}GRANT STOP ON DATABASE * $preposition `admin`",
        s"${optionalRevoke}GRANT START ON DATABASE * $preposition `admin`",
      ))
    }

    test(s"should show parameterized role privileges as ${optionalRevoke.toLowerCase}commands") {
      // WHEN
      execute(s"SHOW ROLE $$role PRIVILEGES AS ${optionalRevoke}COMMAND", Map("role" -> "admin")).columnAs[String]("command").toSet should be(Set(
        s"${optionalRevoke}GRANT MATCH {*} ON GRAPH * NODE * $preposition `admin`",
        s"${optionalRevoke}GRANT MATCH {*} ON GRAPH * RELATIONSHIP * $preposition `admin`",
        s"${optionalRevoke}GRANT WRITE ON GRAPH * $preposition `admin`",
        s"${optionalRevoke}GRANT ACCESS ON DATABASE * $preposition `admin`",
        s"${optionalRevoke}GRANT CONSTRAINT MANAGEMENT ON DATABASE * $preposition `admin`",
        s"${optionalRevoke}GRANT INDEX MANAGEMENT ON DATABASE * $preposition `admin`",
        s"${optionalRevoke}GRANT NAME MANAGEMENT ON DATABASE * $preposition `admin`",
        s"${optionalRevoke}GRANT TRANSACTION MANAGEMENT (*) ON DATABASE * $preposition `admin`",
        s"${optionalRevoke}GRANT ALL DBMS PRIVILEGES ON DBMS $preposition `admin`",
        s"${optionalRevoke}GRANT STOP ON DATABASE * $preposition `admin`",
        s"${optionalRevoke}GRANT START ON DATABASE * $preposition `admin`",
      ))
    }

    test(s"should show multiple parameterized role privileges as parameterized ${optionalRevoke.toLowerCase}commands") {
      // GIVEN
      execute("CREATE ROLE foo")
      execute("CREATE ROLE bar")
      execute("CREATE ROLE baz")
      execute("GRANT STOP ON DATABASE * TO foo")
      execute("GRANT MATCH {*} ON GRAPH * NODE * TO bar")
      execute("DENY WRITE ON GRAPH * TO baz")

      // WHEN
      execute(s"SHOW ROLE foo, $$bar, $$baz PRIVILEGES AS ${optionalRevoke}COMMAND", Map("bar" -> "bar", "baz" -> "baz")).columnAs[String]("command").toSet should be(Set(
        s"${optionalRevoke}GRANT STOP ON DATABASE * $preposition `foo`",
        s"${optionalRevoke}GRANT MATCH {*} ON GRAPH * NODE * $preposition `bar`",
        s"${optionalRevoke}DENY WRITE ON GRAPH * $preposition `baz`"
      ))
    }

    test(s"should show EXECUTE privileges as ${optionalRevoke.toLowerCase}commands (differs on segment and not action like others)") {
      // GIVEN
      val executeCommands = executePrivileges.keys.toSeq
      execute("CREATE ROLE custom")
      execute("GRANT EXECUTE ADMIN PROCEDURES ON DBMS TO custom") // not included in 'executePrivileges' but part of 'dbmsPrivileges'
      executeCommands.foreach(c => {
        execute(s"GRANT $c * ON DBMS TO custom")
        execute(s"DENY $c math.* ON DBMS TO custom")
      })

      // WHEN
      val result = execute(s"SHOW ROLE custom PRIVILEGES AS ${optionalRevoke}COMMANDS")

      // THEN
      val expected = executeCommands.map(c => s"${optionalRevoke}GRANT $c * ON DBMS $preposition `custom`") ++
                     executeCommands.map(c => s"${optionalRevoke}DENY $c math.* ON DBMS $preposition `custom`") :+
                     s"${optionalRevoke}GRANT EXECUTE ADMIN PROCEDURES ON DBMS $preposition `custom`"
      result.columnAs[String]("command").toSet should be(expected.toSet)
    }

    test(s"should show user privileges as parameterized ${optionalRevoke.toLowerCase}commands") {
      // GIVEN
      setupUserWithCustomRole("user")
      execute("DENY MATCH {*} ON GRAPH * NODE * TO custom")
      execute("DENY EXECUTE PROCEDURE * ON DBMS TO custom")

      // WHEN
      execute(s"SHOW USER user PRIVILEGES AS ${optionalRevoke}COMMANDS").columnAs[String]("command").toSet should be(Set(
        s"${optionalRevoke}GRANT ACCESS ON DATABASE * $preposition $$role",
        s"${optionalRevoke}GRANT ACCESS ON DEFAULT DATABASE $preposition $$role",
        s"${optionalRevoke}GRANT EXECUTE FUNCTION * ON DBMS $preposition $$role",
        s"${optionalRevoke}GRANT EXECUTE PROCEDURE * ON DBMS $preposition $$role",
        s"${optionalRevoke}DENY EXECUTE PROCEDURE * ON DBMS $preposition $$role",
        s"${optionalRevoke}DENY MATCH {*} ON GRAPH * NODE * $preposition $$role"
      ))
    }

    test(s"should show parameterized user privileges as parameterized ${optionalRevoke.toLowerCase}commands") {
      // GIVEN
      setupUserWithCustomRole("user", "secret")

      // WHEN
      execute(s"SHOW USER $$user PRIVILEGES AS ${optionalRevoke}COMMANDS", Map("user" -> "user")).columnAs[String]("command").toSet should be(Set(
        s"${optionalRevoke}GRANT ACCESS ON DATABASE * $preposition $$role",
        s"${optionalRevoke}GRANT ACCESS ON DEFAULT DATABASE $preposition $$role",
        s"${optionalRevoke}GRANT EXECUTE FUNCTION * ON DBMS $preposition $$role",
        s"${optionalRevoke}GRANT EXECUTE PROCEDURE * ON DBMS $preposition $$role"
      ))
    }

    test(s"should show current user privileges as ${optionalRevoke.toLowerCase}commands") {
      // GIVEN
      setupUserWithCustomRole()

      // WHEN
      executeOnSystem("joe", "soap", s"SHOW USER PRIVILEGES AS ${optionalRevoke}COMMANDS YIELD command ORDER BY command", resultHandler = (row, idx) => {
        idx match {
          case 0 => row.get("command") should be(s"${optionalRevoke}GRANT ACCESS ON DATABASE * $preposition $$role")
          case 1 => row.get("command") should be(s"${optionalRevoke}GRANT ACCESS ON DEFAULT DATABASE $preposition $$role")
          case 2 => row.get("command") should be(s"${optionalRevoke}GRANT EXECUTE FUNCTION * ON DBMS $preposition $$role")
          case 3 => row.get("command") should be(s"${optionalRevoke}GRANT EXECUTE PROCEDURE * ON DBMS $preposition $$role")
          case _ => fail()
        }
      }) should be(4)
    }

    test(s"should show multiple parameterized user privileges as parameterized ${optionalRevoke.toLowerCase}commands") {
      // GIVEN
      setupUserWithCustomRole("foo")
      setupUserWithCustomRole("bar")
      setupUserWithCustomRole("baz")

      // WHEN
      execute(s"SHOW USER foo, $$bar, $$baz PRIVILEGES AS ${optionalRevoke}COMMANDS", Map("bar" -> "bar", "baz" -> "baz")).columnAs[String]("command").toSet should be(Set(
        s"${optionalRevoke}GRANT ACCESS ON DATABASE * $preposition $$role",
        s"${optionalRevoke}GRANT ACCESS ON DEFAULT DATABASE $preposition $$role",
        s"${optionalRevoke}GRANT EXECUTE FUNCTION * ON DBMS $preposition $$role",
        s"${optionalRevoke}GRANT EXECUTE PROCEDURE * ON DBMS $preposition $$role"
      ))
    }

    test(s"should filter on show privileges as ${optionalRevoke.toLowerCase}commands with WHERE") {
      // WHEN
      execute(s"SHOW PRIVILEGES AS ${optionalRevoke}COMMAND WHERE command CONTAINS 'START'").columnAs[String]("command").toSet should be(Set(
        s"${optionalRevoke}GRANT START ON DATABASE * $preposition `admin`",
      ))
    }

    test(s"should show privileges as ${optionalRevoke.toLowerCase}commands with yield") {
      // GIVEN
      execute("CREATE ROLE role")
      execute("GRANT ACCESS ON DATABASE * to role")

      //WHEN
      execute(s"SHOW ROLE role PRIVILEGES AS ${optionalRevoke}COMMANDS YIELD command RETURN command AS cypherCommands").columnAs[String]("cypherCommands").toSet should be(Set(
        s"${optionalRevoke}GRANT ACCESS ON DATABASE * $preposition `role`"
      ))
    }

    test(s"should give nothing when showing privileges as ${optionalRevoke.toLowerCase}commands for non-existing user") {
      // WHEN
      val resultFoo = execute(s"SHOW USER foo PRIVILEGES AS ${optionalRevoke}COMMANDS")

      // THEN
      resultFoo.toSet should be(Set.empty)

      // and an invalid (non-existing) one
      // WHEN
      val resultEmpty = execute(s"SHOW USER `` PRIVILEGES AS ${optionalRevoke}COMMANDS")

      // THEN
      resultEmpty.toSet should be(Set.empty)
    }

    test(s"should give nothing when showing privileges as ${optionalRevoke.toLowerCase}commands for a dropped user") {
      // GIVEN
      setupUserWithCustomRole("bar", "secret")
      execute("GRANT TRAVERSE ON GRAPH * NODES * TO custom")

      // WHEN
      execute("DROP USER bar")

      // THEN
      execute(s"SHOW USER bar PRIVILEGES AS ${optionalRevoke}COMMANDS").toSet should be(Set.empty)
    }

    test(s"should not show user privileges as ${optionalRevoke.toLowerCase}commands on a dropped role") {
      // GIVEN
      setupUserWithCustomRole("bar", "secret")
      execute("GRANT TRAVERSE ON GRAPH * NODES * TO custom")

      // WHEN
      execute("DROP ROLE custom")

      // THEN
      execute(s"SHOW USER bar PRIVILEGES AS ${optionalRevoke}COMMANDS").columnAs[String]("command").toSet should be(Set(
        s"${optionalRevoke}GRANT ACCESS ON DEFAULT DATABASE $preposition $$role",
        s"${optionalRevoke}GRANT EXECUTE FUNCTION * ON DBMS $preposition $$role",
        s"${optionalRevoke}GRANT EXECUTE PROCEDURE * ON DBMS $preposition $$role"
      ))
    }

    test(s"should not show show user privileges as ${optionalRevoke.toLowerCase}commands when not using the system database") {
      // GIVEN
      selectDatabase(DEFAULT_DATABASE_NAME)

      // WHEN
      val exception = the[DatabaseAdministrationException] thrownBy {
        execute(s"SHOW USER neo4j PRIVILEGES AS ${optionalRevoke}COMMANDS")
      }

      // THEN
      exception.getMessage shouldBe "This is an administration command and it should be executed against the system database: SHOW PRIVILEGE COMMANDS"
    }
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

          test(s"should $grantOrDeny $actionName privilege to custom role for all graphs and all element types") {
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
              execute(s"$grantOrDenyCommand $actionCommand ON GRAPH $$graph TO custom", Map("graph" -> "*"))
            }
            error.getMessage should be(s"Failed to $grantOrDeny $actionName privilege to role 'custom': Parameterized database and graph names do not support wildcards.")
          }

          Seq(
            ("label", "NODES", addNode: builderType),
            ("relationship type", "RELATIONSHIPS", addRel: builderType)
          ).foreach {
            case (segmentName, segmentCommand, segmentFunction: builderType) =>

              test(s"should $grantOrDeny $actionName privilege to custom role for all graphs and all ${segmentName}s") {
                // GIVEN
                execute("CREATE ROLE custom")

                // WHEN
                execute(s"$grantOrDenyCommand $actionCommand ON GRAPH * $segmentCommand * (*) TO custom")

                // THEN
                execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(segmentFunction(startExpected.role("custom"), "*").map))
              }

              test(s"should $grantOrDeny $actionName privilege to custom role for multiple graphs and all ${segmentName}s") {
                // GIVEN
                execute("CREATE ROLE custom")
                execute("CREATE DATABASE foo")
                execute("CREATE DATABASE bar")

                // WHEN
                execute(s"$grantOrDenyCommand $actionCommand ON GRAPH foo, bar $segmentCommand * (*) TO custom")

                // THEN
                execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(segmentFunction(startExpected.role("custom").graph("foo"), "*").map,
                  segmentFunction(startExpected.role("custom").graph("bar"), "*").map))
              }

              test(s"should $grantOrDeny $actionName privilege to custom role for all graphs but only a specific $segmentName (that does not need to exist)") {
                // GIVEN
                execute("CREATE ROLE custom")

                // WHEN
                execute(s"$grantOrDenyCommand $actionCommand ON GRAPH * $segmentCommand A (*) TO custom")

                // THEN
                execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(segmentFunction(startExpected.role("custom"), "A").map))
              }

              test(s"should $grantOrDeny $actionName privilege to custom role for a specific graph and a specific $segmentName") {
                // GIVEN
                execute("CREATE ROLE custom")
                execute("CREATE DATABASE foo")

                // WHEN
                execute(s"$grantOrDenyCommand $actionCommand ON GRAPH foo $segmentCommand A (*) TO custom")

                // THEN
                execute("SHOW ROLE custom PRIVILEGES").toSet should
                  be(Set(segmentFunction(startExpected.role("custom").graph("foo"), "A").map))
              }

              test(s"should $grantOrDeny $actionName privilege to custom role for a specific graph and all ${segmentName}s") {
                // GIVEN
                execute("CREATE ROLE custom")
                execute("CREATE DATABASE foo")

                // WHEN
                execute(s"$grantOrDenyCommand $actionCommand ON GRAPH foo $segmentCommand * (*) TO custom")

                // THEN
                execute("SHOW ROLE custom PRIVILEGES").toSet should
                  be(Set(segmentFunction(startExpected.role("custom").graph("foo"), "*").map))
              }

              test(s"should $grantOrDeny $actionName privilege to custom role for a specific graph and multiple ${segmentName}s") {
                // GIVEN
                execute("CREATE ROLE custom")
                execute("CREATE DATABASE foo")

                // WHEN
                execute(s"$grantOrDenyCommand $actionCommand ON GRAPH foo $segmentCommand A (*) TO custom")
                execute(s"$grantOrDenyCommand $actionCommand ON GRAPH foo $segmentCommand B (*) TO custom")

                // THEN
                execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
                  segmentFunction(startExpected.role("custom").graph("foo"), "A").map,
                  segmentFunction(startExpected.role("custom").graph("foo"), "B").map
                ))
              }

              test(s"should $grantOrDeny $actionName privilege to custom role for a specific graph and multiple ${segmentName}s in one grant") {
                // GIVEN
                execute("CREATE ROLE custom")
                execute("CREATE DATABASE foo")

                // WHEN
                execute(s"$grantOrDenyCommand $actionCommand ON GRAPH foo $segmentCommand A, B (*) TO custom")

                // THEN
                execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
                  segmentFunction(startExpected.role("custom").graph("foo"), "A").map,
                  segmentFunction(startExpected.role("custom").graph("foo"), "B").map
                ))
              }

              test(s"should $grantOrDeny $actionName privilege to multiple roles for a specific graph and multiple ${segmentName}s in one grant") {
                // GIVEN
                execute("CREATE ROLE role1")
                execute("CREATE ROLE role2")
                execute("CREATE DATABASE foo")

                // WHEN
                execute(s"$grantOrDenyCommand $actionCommand ON GRAPH foo $segmentCommand A, B (*) TO role1, role2")

                // THEN
                execute("SHOW ROLE role1 PRIVILEGES").toSet should be(Set(
                  segmentFunction(startExpected.role("role1").graph("foo"), "A").map,
                  segmentFunction(startExpected.role("role1").graph("foo"), "B").map
                ))
                execute("SHOW ROLE role2 PRIVILEGES").toSet should be(Set(
                  segmentFunction(startExpected.role("role2").graph("foo"), "A").map,
                  segmentFunction(startExpected.role("role2").graph("foo"), "B").map
                ))
              }

          }

          test(s"should $grantOrDeny $actionName privilege to custom role for all graphs and all elements") {
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

          test(s"should $grantOrDeny $actionName privilege to custom role for all graphs but only a specific element (that does not need to exist)") {
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

          test(s"should $grantOrDeny $actionName privilege to custom role for a specific graph and a specific element") {
            // GIVEN
            execute("CREATE ROLE custom")
            execute("CREATE DATABASE foo")

            // WHEN
            execute(s"$grantOrDenyCommand $actionCommand ON GRAPH foo ELEMENTS A (*) TO custom")

            // THEN
            execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
              startExpected.role("custom").graph("foo").node("A").map,
              startExpected.role("custom").graph("foo").relationship("A").map
            ))
          }

          test(s"should $grantOrDeny $actionName privilege to custom role for a specific graph and all elements") {
            // GIVEN
            execute("CREATE ROLE custom")
            execute("CREATE DATABASE foo")

            // WHEN
            execute(s"$grantOrDenyCommand $actionCommand ON GRAPH foo ELEMENTS * (*) TO custom")

            // THEN
            execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
              startExpected.role("custom").graph("foo").node("*").map,
              startExpected.role("custom").graph("foo").relationship("*").map
            ))
          }

          test(s"should $grantOrDeny $actionName privilege to custom role for specific graph and all element types using parameters") {
            // GIVEN
            execute("CREATE ROLE custom")

            // WHEN
            execute(s"$grantOrDenyCommand $actionCommand ON GRAPH $$graph TO custom", Map("graph" -> DEFAULT_DATABASE_NAME))

            // THEN
            execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
              startExpected.graph(DEFAULT_DATABASE_NAME).role("custom").node("*").map,
              startExpected.graph(DEFAULT_DATABASE_NAME).role("custom").relationship("*").map
            ))
          }

          test(s"should $grantOrDeny $actionName privilege to custom role for a specific graph and multiple elements") {
            // GIVEN
            execute("CREATE ROLE custom")
            execute("CREATE DATABASE foo")

            // WHEN
            execute(s"$grantOrDenyCommand $actionCommand ON GRAPH foo ELEMENTS A (*) TO custom")
            execute(s"$grantOrDenyCommand $actionCommand ON GRAPH foo ELEMENTS B (*) TO custom")

            // THEN
            execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
              startExpected.role("custom").graph("foo").node("A").map,
              startExpected.role("custom").graph("foo").relationship("A").map,
              startExpected.role("custom").graph("foo").node("B").map,
              startExpected.role("custom").graph("foo").relationship("B").map
            ))
          }

          test(s"should $grantOrDeny $actionName privilege to custom role for a specific graph and multiple elements in one grant") {
            // GIVEN
            execute("CREATE ROLE custom")
            execute("CREATE DATABASE foo")

            // WHEN
            execute(s"$grantOrDenyCommand $actionCommand ON GRAPH foo ELEMENTS A, B (*) TO custom")

            // THEN
            execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
              startExpected.role("custom").graph("foo").node("A").map,
              startExpected.role("custom").graph("foo").relationship("A").map,
              startExpected.role("custom").graph("foo").node("B").map,
              startExpected.role("custom").graph("foo").relationship("B").map
            ))
          }

          test(s"should $grantOrDeny $actionName privilege to multiple roles for a specific graph and multiple elements in one grant") {
            // GIVEN
            execute("CREATE ROLE role1")
            execute("CREATE ROLE role2")
            execute("CREATE DATABASE foo")

            // WHEN
            execute(s"$grantOrDenyCommand $actionCommand ON GRAPH foo ELEMENTS A, B (*) TO role1, role2")

            // THEN
            execute("SHOW ROLE role1 PRIVILEGES").toSet should be(Set(
              startExpected.role("role1").graph("foo").node("A").map,
              startExpected.role("role1").graph("foo").relationship("A").map,
              startExpected.role("role1").graph("foo").node("B").map,
              startExpected.role("role1").graph("foo").relationship("B").map
            ))
            execute("SHOW ROLE role2 PRIVILEGES").toSet should be(Set(
              startExpected.role("role2").graph("foo").node("A").map,
              startExpected.role("role2").graph("foo").relationship("A").map,
              startExpected.role("role2").graph("foo").node("B").map,
              startExpected.role("role2").graph("foo").relationship("B").map
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
    } should have message PERMISSION_DENIED_ASSIGN_PRIVILEGE
  }

  test("should not deny anything as non admin") {
    // GIVEN
    execute("CREATE USER foo SET PASSWORD 'bar' CHANGE NOT REQUIRED")

    // WHEN & THEN
    the[AuthorizationViolationException] thrownBy {
      executeOnSystem("foo", "bar", "DENY MATCH {bar} ON GRAPH * TO custom")
    } should have message PERMISSION_DENIED_ASSIGN_PRIVILEGE
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
      granted(traverse).role("custom").graph("bar").node("A").map,
      denied(traverse).role("custom").graph("bar").relationship("B").map,
      granted(read).role("custom").graph("bar").property("prop").node("A").map,
      denied(read).role("custom").graph("bar").property("prop2").node("B").map,
      granted(matchPrivilege).role("custom").graph("bar").property("prop3").node("C").map,
      denied(matchPrivilege).role("custom").graph("bar").property("prop4").node("D").map
    ))

    // WHEN
    execute("REVOKE GRANT TRAVERSE ON GRAPH bar NODES A FROM custom")
    execute("REVOKE DENY READ {prop2} ON GRAPH BAr NODES B FROM custom")
    execute("REVOKE MATCH {prop3} ON GRAPH BAr NODES C FROM custom")

    //THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      denied(traverse).role("custom").graph("bar").relationship("B").map,
      granted(read).role("custom").graph("bar").property("prop").node("A").map,
      denied(matchPrivilege).role("custom").graph("bar").property("prop4").node("D").map
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

          test(s"should $grantOrDeny $actionName privilege for specific property to custom role for all graphs and all element types") {
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

              test(s"should $grantOrDeny $actionName privilege for specific property to custom role for all graphs and all ${segmentName}s") {
                // GIVEN
                execute("CREATE ROLE custom")

                // WHEN
                execute(s"$grantOrDenyCommand $actionCommand {bar} ON GRAPH * $segmentCommand * (*) TO custom")

                // THEN
                execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
                  segmentFunction(startExpected.role("custom").property("bar"), "*").map
                ))
              }

              test(s"should $grantOrDeny $actionName privilege for specific property to custom role for all graphs but only a specific $segmentName") {
                // GIVEN
                execute("CREATE ROLE custom")

                // WHEN
                execute(s"$grantOrDenyCommand $actionCommand {bar} ON GRAPH * $segmentCommand A (*) TO custom")

                // THEN
                execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
                  segmentFunction(startExpected.role("custom").property("bar"), "A").map
                ))
              }

              test(s"should $grantOrDeny $actionName privilege for specific property to custom role for a specific graphs and a specific $segmentName") {
                // GIVEN
                execute("CREATE ROLE custom")
                execute("CREATE DATABASE foo")

                // WHEN
                execute(s"$grantOrDenyCommand $actionCommand {bar} ON GRAPH foo $segmentCommand A (*) TO custom")

                // THEN
                execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
                  segmentFunction(startExpected.graph("foo").role("custom").property("bar"), "A").map
                ))
              }

              test(s"should $grantOrDeny $actionName privilege for specific property to custom role for a specific graph and all ${segmentName}s") {
                // GIVEN
                execute("CREATE ROLE custom")
                execute("CREATE DATABASE foo")

                // WHEN
                execute(s"$grantOrDenyCommand $actionCommand {bar} ON GRAPH foo $segmentCommand * (*) TO custom")

                // THEN
                execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
                  segmentFunction(startExpected.graph("foo").role("custom").property("bar"), "*").map
                ))
              }

              test(s"should $grantOrDeny $actionName privilege for specific property to custom role for a specific graph and multiple ${segmentName}s") {
                // GIVEN
                execute("CREATE ROLE custom")
                execute("CREATE DATABASE foo")

                // WHEN
                execute(s"$grantOrDenyCommand $actionCommand {bar} ON GRAPH foo $segmentCommand A (*) TO custom")
                execute(s"$grantOrDenyCommand $actionCommand {bar} ON GRAPH foo $segmentCommand B (*) TO custom")

                // THEN
                execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
                  segmentFunction(startExpected.graph("foo").role("custom").property("bar"), "A").map,
                  segmentFunction(startExpected.graph("foo").role("custom").property("bar"), "B").map
                ))
              }

              test(s"should $grantOrDeny $actionName privilege for multiple properties to custom role for a specific graph and specific $segmentName") {
                // GIVEN
                execute("CREATE ROLE custom")
                execute("CREATE DATABASE foo")

                // WHEN
                execute(s"$grantOrDenyCommand $actionCommand {bar} ON GRAPH foo $segmentCommand A (*) TO custom")
                execute(s"$grantOrDenyCommand $actionCommand {baz} ON GRAPH foo $segmentCommand A (*) TO custom")

                // THEN
                execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
                  segmentFunction(startExpected.graph("foo").role("custom").property("bar"), "A").map,
                  segmentFunction(startExpected.graph("foo").role("custom").property("baz"), "A").map
                ))
              }

              test(s"should $grantOrDeny $actionName privilege for multiple properties to custom role for a specific graph and multiple ${segmentName}s") {
                // GIVEN
                execute("CREATE ROLE custom")
                execute("CREATE DATABASE foo")

                // WHEN
                execute(s"$grantOrDenyCommand $actionCommand {bar} ON GRAPH foo $segmentCommand A (*) TO custom")
                execute(s"$grantOrDenyCommand $actionCommand {baz} ON GRAPH foo $segmentCommand B (*) TO custom")

                // THEN
                execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
                  segmentFunction(startExpected.graph("foo").role("custom").property("bar"), "A").map,
                  segmentFunction(startExpected.graph("foo").role("custom").property("baz"), "B").map
                ))
              }

              test(s"should $grantOrDeny $actionName privilege for multiple properties to multiple roles for a specific graph and multiple ${segmentName}s in a single grant") {
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
                    segmentFunction(startExpected.graph("foo").property(p), l)
                  }
                }).flatten

                execute("SHOW ROLE role1 PRIVILEGES").toSet should be(expected.map(_.role("role1").map).toSet)
                execute("SHOW ROLE role2 PRIVILEGES").toSet should be(expected.map(_.role("role2").map).toSet)
                execute("SHOW ROLE role3 PRIVILEGES").toSet should be(expected.map(_.role("role3").map).toSet)
              }

          }

          test(s"should $grantOrDeny $actionName privilege for specific property to custom role for all graphs and all elements") {
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

          test(s"should $grantOrDeny $actionName privilege for specific property to custom role for all graphs but only a specific element") {
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

          test(s"should $grantOrDeny $actionName privilege for specific property to custom role for a specific graph and a specific element") {
            // GIVEN
            execute("CREATE ROLE custom")
            execute("CREATE DATABASE foo")

            // WHEN
            execute(s"$grantOrDenyCommand $actionCommand {bar} ON GRAPH foo ELEMENTS A (*) TO custom")

            // THEN
            execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
              startExpected.role("custom").graph("foo").property("bar").node("A").map,
              startExpected.role("custom").graph("foo").property("bar").relationship("A").map
            ))
          }

          test(s"should $grantOrDeny $actionName privilege for specific property to custom role for a specific graph and all elements") {
            // GIVEN
            execute("CREATE ROLE custom")
            execute("CREATE DATABASE foo")

            // WHEN
            execute(s"$grantOrDenyCommand $actionCommand {bar} ON GRAPH foo ELEMENTS * (*) TO custom")

            // THEN
            execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
              startExpected.role("custom").graph("foo").property("bar").node("*").map,
              startExpected.role("custom").graph("foo").property("bar").relationship("*").map
            ))
          }

          test(s"should $grantOrDeny $actionName privilege for specific property to custom role for a specific graph and multiple elements") {
            // GIVEN
            execute("CREATE ROLE custom")
            execute("CREATE DATABASE foo")

            // WHEN
            execute(s"$grantOrDenyCommand $actionCommand {bar} ON GRAPH foo ELEMENTS A (*) TO custom")
            execute(s"$grantOrDenyCommand $actionCommand {bar} ON GRAPH foo ELEMENTS B (*) TO custom")

            // THEN
            execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
              startExpected.role("custom").graph("foo").property("bar").node("A").map,
              startExpected.role("custom").graph("foo").property("bar").relationship("A").map,
              startExpected.role("custom").graph("foo").property("bar").node("B").map,
              startExpected.role("custom").graph("foo").property("bar").relationship("B").map
            ))
          }

          test(s"should $grantOrDeny $actionName privilege for multiple properties to custom role for a specific graph and specific element") {
            // GIVEN
            execute("CREATE ROLE custom")
            execute("CREATE DATABASE foo")

            // WHEN
            execute(s"$grantOrDenyCommand $actionCommand {bar} ON GRAPH foo ELEMENTS A (*) TO custom")
            execute(s"$grantOrDenyCommand $actionCommand {baz} ON GRAPH foo ELEMENTS A (*) TO custom")

            // THEN
            execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
              startExpected.role("custom").graph("foo").property("bar").node("A").map,
              startExpected.role("custom").graph("foo").property("bar").relationship("A").map,
              startExpected.role("custom").graph("foo").property("baz").node("A").map,
              startExpected.role("custom").graph("foo").property("baz").relationship("A").map
            ))
          }

          test(s"should $grantOrDeny $actionName privilege for multiple properties to custom role for a specific graph and multiple elements") {
            // GIVEN
            execute("CREATE ROLE custom")
            execute("CREATE DATABASE foo")

            // WHEN
            execute(s"$grantOrDenyCommand $actionCommand {bar} ON GRAPH foo ELEMENTS A (*) TO custom")
            execute(s"$grantOrDenyCommand $actionCommand {baz} ON GRAPH foo ELEMENTS B (*) TO custom")

            // THEN
            execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
              startExpected.role("custom").graph("foo").property("bar").node("A").map,
              startExpected.role("custom").graph("foo").property("bar").relationship("A").map,
              startExpected.role("custom").graph("foo").property("baz").node("B").map,
              startExpected.role("custom").graph("foo").property("baz").relationship("B").map
            ))
          }

          test(s"should $grantOrDeny $actionName privilege for multiple properties to multiple roles for a specific graph and multiple elements in a single grant") {
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
                Seq(startExpected.graph("foo").property(p).node(l),
                  startExpected.graph("foo").property(p).relationship(l))
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

    val expected = publicPrivileges("joe").toSeq.sortBy( p => p.get("segment") )

    // WHEN using a parameter name that used to be internal, but is not any more, it should work
    executeOnSystem("joe", "soap", "SHOW USER PRIVILEGES YIELD * ORDER BY segment", resultHandler = (row, idx) => {
      // THEN
      asPrivilegesResult(row) should be(expected(idx))
    }, params = Map[String, Object]("currentUser" -> "neo4j").asJava) should be(3)

    // WHEN using a parameter name that is the new internal name, an error should occur
    the[QueryExecutionException] thrownBy {
      executeOnSystem("joe", "soap", "SHOW USER PRIVILEGES",
        params = Map[String, Object]("__internal_currentUser" -> "neo4j").asJava)
    } should have message "The query contains a parameter with an illegal name: '__internal_currentUser'"
  }

  withAllSystemGraphVersions(allSupported) {
    val expectedVersion = _version.get  // capture transient state into closure

    test("Should be able to run SHOW PRIVILEGES on multiple versions of the system graph with different but valid results") {
      // Given
      execute("CREATE ROLE custom AS COPY OF admin")
      val expectedCurrent = defaultRolePrivilegesFor("admin", "custom")
      val expected = translatePrivileges(expectedCurrent, expectedVersion)

      // When && Then
      execute("SHOW ROLE custom PRIVILEGES").toSet should be(expected)
    }
  }
}
