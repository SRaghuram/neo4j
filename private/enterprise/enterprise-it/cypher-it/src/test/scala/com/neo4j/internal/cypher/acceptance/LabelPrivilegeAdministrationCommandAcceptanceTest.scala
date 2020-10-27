/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.internal.cypher.acceptance

import org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME
import org.neo4j.graphdb.security.AuthorizationViolationException

class LabelPrivilegeAdministrationCommandAcceptanceTest extends AdministrationCommandAcceptanceTestBase with EnterpriseComponentVersionTestSupport {

  test("should return empty counts to the outside for commands that update the system graph internally") {
    // GIVEN
    execute("CREATE ROLE custom")

    // Notice: They are executed in succession so they have to make sense in that order
    assertQueriesAndSubQueryCounts(List(
      "GRANT SET LABEL label1 ON GRAPH *  TO custom" -> 1,
      "REVOKE SET LABEL label1 ON GRAPH * FROM custom" -> 1,
      "DENY SET LABEL label1, label2 ON GRAPH *  TO custom" -> 2,
      "REVOKE DENY SET LABEL label1, label2 ON GRAPH * FROM custom" -> 2,

      "GRANT REMOVE LABEL * ON GRAPH * TO custom" -> 1,
      "DENY REMOVE LABEL * ON GRAPH * TO custom" -> 1,
      "REVOKE REMOVE LABEL * ON GRAPH * FROM custom" -> 2,
    ))
  }

  Seq(
    ("SET", setLabel),
    ("REMOVE", removeLabel)
  ).foreach { case (verb, action) =>
    Seq(
      ("grant", "GRANT", granted: privilegeFunction),
      ("deny", "DENY", denied: privilegeFunction),
    ).foreach {
      case (grantOrDeny, grantOrDenyCommand, grantedOrDenied) =>

        // Tests for granting and denying label privileges

        test(s"should $grantOrDeny $verb label privilege to custom role for all graphs and specific label") {
          // GIVEN
          execute("CREATE ROLE custom")

          // WHEN
          execute(s"$grantOrDenyCommand $verb LABEL a ON GRAPH * TO custom")

          // THEN
          execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
            grantedOrDenied(action).role("custom").label("a").map,
          ))
        }

        test(s"should $grantOrDeny $verb privilege to custom role for a default graph and all labels") {
          // GIVEN
          execute("CREATE ROLE custom")

          // WHEN
          execute(s"$grantOrDenyCommand $verb LABEL * ON DEFAULT GRAPH TO custom")

          // THEN
          execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
            grantedOrDenied(action).role("custom").graph(DEFAULT).label("*").map,
          ))
        }

        test(s"should $grantOrDeny $verb privilege to custom role for a specific graph and all labels") {
          // GIVEN
          execute("CREATE ROLE custom")
          execute("CREATE DATABASE foo")

          // WHEN
          execute(s"$grantOrDenyCommand $verb LABEL * ON GRAPH foo TO custom")

          // THEN
          execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
            grantedOrDenied(action).role("custom").graph("foo").label("*").map,
          ))
        }

        test(s"should $grantOrDeny $verb label to custom role for multiple graphs and multiple labels") {
          // GIVEN
          execute("CREATE ROLE custom")
          execute("CREATE DATABASE foo")
          execute("CREATE DATABASE bar")

          // WHEN
          execute(s"$grantOrDenyCommand $verb LABEL label1, label2 ON GRAPH foo, bar TO custom")

          // THEN
          execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
            grantedOrDenied(action).role("custom").graph("foo").label("label1").map,
            grantedOrDenied(action).role("custom").graph("bar").label("label1").map,
            grantedOrDenied(action).role("custom").graph("foo").label("label2").map,
            grantedOrDenied(action).role("custom").graph("bar").label("label2").map
          ))
        }

        test(s"should $grantOrDeny $verb label privilege to custom role for specific graph and all labels using parameter") {
          // GIVEN
          execute("CREATE ROLE custom")

          // WHEN
          execute(s"$grantOrDenyCommand $verb LABEL * ON GRAPH $$graph TO custom", Map("graph" -> DEFAULT_DATABASE_NAME))

          // THEN
          execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
            grantedOrDenied(action).graph(DEFAULT_DATABASE_NAME).role("custom").label("*").map,
          ))
        }

        test(s"should $grantOrDeny $verb label privilege to multiple roles in a single grant") {
          // GIVEN
          execute("CREATE ROLE role1")
          execute("CREATE ROLE role2")
          execute("CREATE ROLE role3")
          execute("CREATE DATABASE foo")

          // WHEN
          execute(s"$grantOrDenyCommand $verb LABEL label ON GRAPH foo TO role1, role2, role3")

          // THEN
          val expected: Seq[PrivilegeMapBuilder] = Seq(
            grantedOrDenied(action).graph("foo").label("label"),
          )

          execute("SHOW ROLE role1 PRIVILEGES").toSet should be(expected.map(_.role("role1").map).toSet)
          execute("SHOW ROLE role2 PRIVILEGES").toSet should be(expected.map(_.role("role2").map).toSet)
          execute("SHOW ROLE role3 PRIVILEGES").toSet should be(expected.map(_.role("role3").map).toSet)
        }

        // Tests for revoke grant and revoke deny label privileges

        test(s"should revoke correct $grantOrDeny $verb label privilege different graphs") {
          // GIVEN
          execute("CREATE ROLE custom")
          execute("CREATE DATABASE foo")
          execute("CREATE DATABASE bar")
          execute(s"$grantOrDenyCommand $verb LABEL * ON GRAPH * TO custom")
          execute(s"$grantOrDenyCommand $verb LABEL * ON GRAPH foo TO custom")
          execute(s"$grantOrDenyCommand $verb LABEL * ON GRAPH bar TO custom")

          // WHEN
          execute(s"REVOKE $grantOrDenyCommand $verb LABEL * ON GRAPH foo FROM custom")

          // THEN
          execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
            grantedOrDenied(action).role("custom").label("*").map,
            grantedOrDenied(action).role("custom").graph("bar").label("*").map,
          ))

          // WHEN
          execute(s"REVOKE $grantOrDenyCommand $verb LABEL * ON GRAPH * FROM custom")

          // THEN
          execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
            grantedOrDenied(action).role("custom").graph("bar").label("*").map,
          ))
        }

        test(s"should be able to revoke $verb label if only having $grantOrDeny") {
          // GIVEN
          execute("CREATE ROLE custom")

          // WHEN
          execute(s"$grantOrDenyCommand $verb LABEL label ON GRAPH * TO custom")

          // THEN
          execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
            grantedOrDenied(action).role("custom").label("label").map,
          ))

          // WHEN
          execute(s"REVOKE $verb LABEL label ON GRAPH * FROM custom")

          // THEN
          execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set.empty)

        }

        test(s"should be able to revoke $grantOrDeny $verb label using parameter") {
          // GIVEN
          execute("CREATE ROLE custom")
          execute("CREATE DATABASE foo")
          execute(s"$grantOrDenyCommand $verb LABEL * ON GRAPH foo TO custom")

          // WHEN
          execute(s"REVOKE $grantOrDenyCommand $verb LABEL * ON GRAPH $$graph FROM custom", Map("graph" -> "foo"))

          // THEN
          execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set.empty)
        }

        test(s"should do nothing when revoking $grantOrDeny $verb label privilege from non-existent role") {
          // GIVEN
          execute("CREATE ROLE custom")
          execute("CREATE DATABASE foo")
          execute(s"$grantOrDenyCommand $verb LABEL * ON GRAPH * TO custom")

          // WHEN
          execute(s"REVOKE $grantOrDenyCommand $verb LABEL * ON GRAPH * FROM wrongRole")

          // THEN
          execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
            grantedOrDenied(action).role("custom").label("*").map,
          ))
        }

        test(s"should do nothing when revoking $grantOrDeny $verb label privilege not granted to role") {
          // GIVEN
          execute("CREATE ROLE custom")
          execute("CREATE ROLE role")
          execute(s"$grantOrDenyCommand $verb LABEL label ON GRAPH * TO custom")

          // WHEN
          execute(s"REVOKE $grantOrDenyCommand $verb LABEL label ON GRAPH * FROM role")
          // THEN
          execute("SHOW ROLE role PRIVILEGES").toSet should be(Set.empty)
        }
    }

    // Tests for revoke label privileges

    test(s"should revoke both grant and deny $verb label privilege") {
      // GIVEN
      execute("CREATE ROLE custom")

      // WHEN
      execute(s"GRANT $verb LABEL foo ON GRAPH * TO custom")
      execute(s"DENY $verb LABEL foo ON GRAPH * TO custom")

      // THEN
      execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
        granted(action).role("custom").label("foo").map,
        denied(action).role("custom").label("foo").map
      ))

      // WHEN
      execute(s"REVOKE $verb LABEL foo ON GRAPH * FROM custom")

      // THEN
      execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set.empty)
    }

    test(s"should do nothing when revoking not granted $verb label privilege") {
      // GIVEN
      execute("CREATE ROLE custom")

      // WHEN
      execute(s"REVOKE $verb LABEL label1 ON GRAPH * FROM custom")

      // THEN
      execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set.empty)
    }

    test(s"should revoke $verb label privilege with one granted and two revoked labels") {
      // GIVEN
      execute("CREATE ROLE custom")
      execute(s"GRANT $verb LABEL label1, label2 ON GRAPH * TO custom")

      // WHEN
      execute(s"REVOKE $verb LABEL label1 ON GRAPH * FROM custom")

      // THEN
      execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(granted(action).role("custom").label("label2").map))
    }

    test(s"should revoke $verb label privilege with two granted and one revoked label") {
      // GIVEN
      execute("CREATE ROLE custom")
      execute(s"GRANT $verb LABEL label1 ON GRAPH * TO custom")

      // WHEN
      execute(s"REVOKE $verb LABEL label1, label2 ON GRAPH * FROM custom")

      // THEN
      execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set.empty)
    }
  }

  withAllSystemGraphVersions(unsupportedBefore41) {

    test("set label allows user to create a label") {
      // GIVEN
      setupUserWithCustomRole()
      execute("GRANT MATCH {*} ON GRAPH * TO custom")
      execute("GRANT SET LABEL * ON GRAPH * TO custom")

      selectDatabase(DEFAULT_DATABASE_NAME)
      execute("CALL db.createLabel('Label')")
      execute("CREATE ()")

      // WHEN
      executeOnDBMSDefault("joe", "soap", "MATCH (n) SET n:Label")

      // THEN
      execute("MATCH (n:Label) RETURN n").toSet should have size 1
    }

    test("remove label should allow user to remove a label") {
      // GIVEN
      setupUserWithCustomRole()
      execute("GRANT MATCH {*} ON GRAPH * TO custom")
      execute("GRANT REMOVE LABEL Label ON GRAPH * TO custom")

      selectDatabase(DEFAULT_DATABASE_NAME)
      execute("CREATE (:Label)")
      execute("MATCH (n:Label) RETURN n").toSet should have size 1

      // WHEN
      executeOnDBMSDefault("joe", "soap", "MATCH (n) Remove n:Label")

      // THEN
      execute("MATCH (n:Label) RETURN n.name").toSet should be(Set.empty)
    }
  }

  test("set label should only allow the allowed label") {
    // GIVEN
    setupUserWithCustomRole()
    execute("GRANT MATCH {*} ON GRAPH * TO custom")
    execute("GRANT SET LABEL foo ON GRAPH * TO custom")

    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CALL db.createLabel('bar')")
    execute("CALL db.createLabel('foo')")
    execute("CREATE ()")

    executeOnDBMSDefault("joe", "soap", "MATCH (n) SET n:foo")
    execute("MATCH (n:foo) RETURN n").toSet should have size 1

    the[AuthorizationViolationException] thrownBy {
      // WHEN
      executeOnDBMSDefault("joe", "soap", "MATCH (n) SET n:bar")
      // THEN
    } should have message "Set label for label 'bar' is not allowed for user 'joe' with roles [PUBLIC, custom]."
  }

  test("remove label should only allow removing allowed label") {
    // GIVEN
    setupUserWithCustomRole()
    execute("GRANT MATCH {*} ON GRAPH * TO custom")
    execute("GRANT REMOVE LABEL Label ON GRAPH * TO custom")

    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE (:Label :AnotherLabel)")

    // WHEN
    executeOnDBMSDefault("joe", "soap", "MATCH (n) REMOVE n:Label")
    execute("MATCH (n:Label) RETURN n").toSet should have size 0

    the[AuthorizationViolationException] thrownBy {
      // WHEN
      executeOnDBMSDefault("joe", "soap", "MATCH (n) REMOVE n:AnotherLabel")
      // THEN
    } should have message "Remove label for label 'AnotherLabel' is not allowed for user 'joe' with roles [PUBLIC, custom]."

    // THEN
    execute("MATCH (n:AnotherLabel) RETURN n").toSet should have size 1
  }

  test("deny set label should override grant") {
    // GIVEN
    setupUserWithCustomRole()
    execute("GRANT MATCH {*} ON GRAPH * TO custom")
    execute("GRANT SET LABEL * ON GRAPH * TO custom")
    execute("DENY SET LABEL Label ON GRAPH * TO custom")

    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CALL db.createLabel('Label')")
    execute("CREATE ()")

    the[AuthorizationViolationException] thrownBy {
      // WHEN
      executeOnDBMSDefault("joe", "soap", "MATCH (n) SET n:Label")
      // THEN
    } should have message "Set label for label 'Label' is not allowed for user 'joe' with roles [PUBLIC, custom]."
  }

  test("deny remove label should override grant") {
    // GIVEN
    setupUserWithCustomRole()
    execute("GRANT MATCH {*} ON GRAPH * TO custom")
    execute("GRANT REMOVE LABEL * ON GRAPH * TO custom")
    execute("DENY REMOVE LABEL Label ON GRAPH * TO custom")

    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE (:Label)")

    the[AuthorizationViolationException] thrownBy {
      // WHEN
      executeOnDBMSDefault("joe", "soap", "MATCH (n) REMOVE n:Label")
      // THEN
    } should have message "Remove label for label 'Label' is not allowed for user 'joe' with roles [PUBLIC, custom]."
  }

  test("deny set all labels should override grant") {
    // GIVEN
    setupUserWithCustomRole()
    execute("GRANT MATCH {*} ON GRAPH * TO custom")
    execute("GRANT SET LABEL Label ON GRAPH * TO custom")
    execute("DENY SET LABEL * ON GRAPH * TO custom")

    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CALL db.createLabel('Label')")
    execute("CREATE ()")


    the[AuthorizationViolationException] thrownBy {
      // WHEN
      executeOnDBMSDefault("joe", "soap", "MATCH (n) SET n:Label")
      // THEN
    } should have message "Set label for label 'Label' is not allowed for user 'joe' with roles [PUBLIC, custom]."
  }

  test("deny remove all labels should override grant") {
    // GIVEN
    setupUserWithCustomRole()
    execute("GRANT MATCH {*} ON GRAPH * TO custom")
    execute("GRANT REMOVE LABEL Label ON GRAPH * TO custom")
    execute("DENY REMOVE LABEL * ON GRAPH * TO custom")

    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE (:Label)")

    the[AuthorizationViolationException] thrownBy {
      // WHEN
      executeOnDBMSDefault("joe", "soap", "MATCH (n) REMOVE n:Label")
      // THEN
    } should have message "Remove label for label 'Label' is not allowed for user 'joe' with roles [PUBLIC, custom]."
  }

  test("denying all writes prevents setting labels") {
    // GIVEN
    setupUserWithCustomRole()
    execute("GRANT MATCH {*} ON GRAPH * TO custom")
    execute("GRANT SET LABEL * ON GRAPH * TO custom")
    execute("DENY WRITE ON GRAPH * TO custom")

    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CALL db.createLabel('Label')")
    execute("CREATE ()")

    the[AuthorizationViolationException] thrownBy {
      // WHEN
      executeOnDBMSDefault("joe", "soap", "MATCH (n) SET n:Label")
      // THEN
    } should have message "Set label for label 'Label' is not allowed for user 'joe' with roles [PUBLIC, custom]."

    // THEN
    execute("MATCH (n) RETURN labels(n)").toSet should be(Set(Map("labels(n)" -> List())))
  }

    test("deny set label should override general write permission") {
    // GIVEN
    setupUserWithCustomRole()
    execute("GRANT MATCH {*} ON GRAPH * TO custom")
    execute(s"GRANT WRITE ON GRAPH * TO custom")
    execute(s"DENY SET LABEL Label ON GRAPH * TO custom")

    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE ()")
    execute("CALL db.createLabel('Label')")

    the[AuthorizationViolationException] thrownBy {
      // WHEN
      executeOnDBMSDefault("joe", "soap", "MATCH (n) SET n:Label")
      // THEN
    } should have message "Set label for label 'Label' is not allowed for user 'joe' with roles [PUBLIC, custom]."

    // THEN
    execute("MATCH (n) RETURN labels(n)").toSet should be(Set(Map("labels(n)" -> List())))
  }

  test("denying all writes prevents removing labels") {
    // GIVEN
    setupUserWithCustomRole()
    execute("GRANT MATCH {*} ON GRAPH * TO custom")
    execute("GRANT REMOVE LABEL * ON GRAPH * TO custom")
    execute("DENY WRITE ON GRAPH * TO custom")

    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE (:Label)")

    the[AuthorizationViolationException] thrownBy {
      // WHEN
      executeOnDBMSDefault("joe", "soap", "MATCH (n) REMOVE n:Label")
      // THEN
    } should have message "Remove label for label 'Label' is not allowed for user 'joe' with roles [PUBLIC, custom]."

    // THEN
    execute("MATCH (n) RETURN labels(n)").toSet should be(Set(Map("labels(n)" -> List("Label"))))
  }

  test("deny remove label should override general write permission") {
    // GIVEN
    setupUserWithCustomRole()
    execute("GRANT MATCH {*} ON GRAPH * TO custom")
    execute(s"GRANT WRITE ON GRAPH * TO custom")
    execute(s"DENY REMOVE LABEL Label ON GRAPH * TO custom")

    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE (:Label)")

    the[AuthorizationViolationException] thrownBy {
      // WHEN
      executeOnDBMSDefault("joe", "soap", "MATCH (n) REMOVE n:Label")
      // THEN
    } should have message "Remove label for label 'Label' is not allowed for user 'joe' with roles [PUBLIC, custom]."

    // THEN
    execute("MATCH (n) RETURN labels(n)").toSet should be(Set(Map("labels(n)" -> List("Label"))))
  }

  test("setting a label that already exists will succeed if SET LABEL permission was granted") {
    // GIVEN
    setupUserWithCustomRole()
    execute("GRANT MATCH {*} ON GRAPH * TO custom")
    execute("GRANT SET LABEL * ON GRAPH * TO custom")

    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE (:Label)")

    // WHEN
    executeOnDBMSDefault("joe", "soap", "MATCH (n) SET n:Label")

    // THEN
    execute("MATCH (n:Label) RETURN n").toSet should have size 1
  }

  test("setting a label that already exists will succeed if SET LABEL permission was denied as no write takes place") {
    // GIVEN
    setupUserWithCustomRole()
    execute("GRANT MATCH {*} ON GRAPH * TO custom")
    execute("DENY SET LABEL * ON GRAPH * TO custom")

    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE (:Label)")

    // WHEN
    executeOnDBMSDefault("joe", "soap", "MATCH (n) SET n:Label")

    // THEN
    execute("MATCH (n:Label) RETURN n").toSet should have size 1
  }

  test("removing a label not present there will succeed if not allowed as no write occurs") {
    // GIVEN
    setupUserWithCustomRole()
    execute("GRANT MATCH {*} ON GRAPH * TO custom")
    execute("DENY REMOVE LABEL * ON GRAPH * TO custom")

    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE (:Label)")

    // WHEN
    executeOnDBMSDefault("joe", "soap", "MATCH (n) REMOVE n:AnotherLabel RETURN n")

    // THEN
    execute("MATCH (n:Label) RETURN n").toSet should have size 1
  }

  test("removing a label not present will succeed but do nothing if allowed") {
    // GIVEN
    setupUserWithCustomRole()
    execute("GRANT MATCH {*} ON GRAPH * TO custom")
    execute("GRANT REMOVE LABEL * ON GRAPH * TO custom")

    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE (:Label)")

    // WHEN
    executeOnDBMSDefault("joe", "soap", "MATCH (n) REMOVE n:AnotherLabel")

    // THEN
    execute("MATCH (n:Label) RETURN n").toSet should have size 1
  }

  test("should be allowed to remove label that was set in the same transaction even without privilege") {
    // GIVEN
    setupUserWithCustomRole()
    execute("GRANT MATCH {*} ON GRAPH * TO custom")
    execute("GRANT SET LABEL * ON GRAPH * TO custom")

    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE ()")
    execute("CALL db.createLabel('Label')")

    executeOnDBMSDefault("joe", "soap", "MATCH (n:Label) REMOVE n:Label", executeBefore = tx => {
      tx.execute("MATCH (n) SET n:Label")
    })
  }

  test("should not be allowed to remove additional labels that were not set in the same transaction without privilege") {
    // GIVEN
    setupUserWithCustomRole()
    execute("GRANT MATCH {*} ON GRAPH * TO custom")
    execute("GRANT SET LABEL * ON GRAPH * TO custom")

    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE ()")
    execute("CALL db.createLabel('Label')")
    execute("CREATE (:Label)")

    the[AuthorizationViolationException] thrownBy {
      // WHEN
      executeOnDBMSDefault("joe", "soap", "MATCH (n:Label) REMOVE n:Label", executeBefore = tx => {
        tx.execute("MATCH (n) SET n:Label")
      })
      // THEN
    } should have message "Remove label for label 'Label' is not allowed for user 'joe' with roles [PUBLIC, custom]."
  }

  test("should be allowed to add label that was removed in the same transaction even without privilege") {
    // GIVEN
    setupUserWithCustomRole()
    execute("GRANT MATCH {*} ON GRAPH * TO custom")
    execute("GRANT REMOVE LABEL * ON GRAPH * TO custom")

    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE (:Label)")

    executeOnDBMSDefault("joe", "soap", "MATCH (n) SET n:Label", executeBefore = tx => {
      tx.execute("MATCH (n:Label) REMOVE n:Label")
    })
  }

  test("should not be allowed to add label that was not removed in the same transaction without privilege") {
    // GIVEN
    setupUserWithCustomRole()
    execute("GRANT MATCH {*} ON GRAPH * TO custom")
    execute("GRANT REMOVE LABEL * ON GRAPH * TO custom")

    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE (:Label)")
    execute("CREATE ()")

    the[AuthorizationViolationException] thrownBy {
      // WHEN
      executeOnDBMSDefault("joe", "soap", "MATCH (n) SET n:Label", executeBefore = tx => {
        tx.execute("MATCH (n:Label) REMOVE n:Label")
      })
    } should have message "Set label for label 'Label' is not allowed for user 'joe' with roles [PUBLIC, custom]."
  }

  test("grant set label on default graph") {
    // GIVEN
    setupUserWithCustomRole()
    execute("GRANT SET LABEL * ON DEFAULT GRAPH TO custom")
    execute("GRANT TRAVERSE ON DEFAULT GRAPH TO custom")
    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE (n)")
    execute("CALL db.createLabel('Label')")

    // WHEN
    executeOnDBMSDefault( "joe", "soap", "MATCH (n) SET n:Label")

    //THEN
    execute("MATCH (n:Label) RETURN n").toSet should have size 1
  }

  test("grant set named label on default graph") {
    // GIVEN
    setupUserWithCustomRole()
    execute("GRANT SET LABEL Label ON DEFAULT GRAPH TO custom")
    execute("GRANT TRAVERSE ON DEFAULT GRAPH TO custom")
    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE (n)")
    execute("CALL db.createLabel('Label')")

    // WHEN
    executeOnDBMSDefault( "joe", "soap", "MATCH (n) SET n:Label")

    //THEN
    execute("MATCH (n:Label) RETURN n").toSet should have size 1
  }

  test("grant set different named label on default graph") {
    // GIVEN
    setupUserWithCustomRole()
    execute("GRANT SET LABEL Label ON DEFAULT GRAPH TO custom")
    execute("GRANT TRAVERSE ON DEFAULT GRAPH TO custom")
    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE (n)")
    execute("CALL db.createLabel('OtherLabel')")

    // WHEN, THEN
    the[AuthorizationViolationException] thrownBy {
      executeOnDBMSDefault( "joe", "soap", "MATCH (n) SET n:OtherLabel")
    } should have message "Set label for label 'OtherLabel' is not allowed for user 'joe' with roles [PUBLIC, custom]."

  }

  test("grant set label on default graph, should not allow on other graph") {
    // GIVEN
    setupUserWithCustomRole()
    execute("GRANT SET LABEL * ON DEFAULT GRAPH TO custom")
    execute("GRANT TRAVERSE ON GRAPH * TO custom")
    execute("CREATE DATABASE foo")
    selectDatabase("foo")
    execute("CREATE (n)")
    execute("CALL db.createLabel('Label')")

    // WHEN, THEN
    the[AuthorizationViolationException] thrownBy {
      executeOn("foo", "joe", "soap", "MATCH (n) SET n:Label")
    } should have message "Set label for label 'Label' is not allowed for user 'joe' with roles [PUBLIC, custom]."
  }

  test("grant remove label on default graph") {
    // GIVEN
    setupUserWithCustomRole()
    execute("GRANT TRAVERSE ON GRAPH * TO custom")
    execute("GRANT REMOVE LABEL * ON DEFAULT GRAPH TO custom")
    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE (n:Label)")

    // WHEN
    executeOnDBMSDefault( "joe", "soap", "MATCH (n:Label) REMOVE n:Label")

    //THEN
    execute("MATCH(n:Label) RETURN count(n)").toSet should be(Set(Map("count(n)" -> 0)))
  }

  test("deny remove label should override grant on default graph") {
    // GIVEN
    setupUserWithCustomRole()
    execute("GRANT MATCH {*} ON DEFAULT GRAPH TO custom")
    execute("GRANT REMOVE LABEL * ON DEFAULT GRAPH TO custom")
    execute("DENY REMOVE LABEL Label ON DEFAULT GRAPH TO custom")

    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE (:Label)")

    the[AuthorizationViolationException] thrownBy {
      // WHEN
      executeOnDBMSDefault("joe", "soap", "MATCH (n) REMOVE n:Label")
      // THEN
    } should have message "Remove label for label 'Label' is not allowed for user 'joe' with roles [PUBLIC, custom]."
  }

  test("deny set label on default graph, should allow on other graph") {
    // GIVEN
    setupUserWithCustomRole()
    execute("GRANT TRAVERSE ON GRAPH * TO custom")
    execute("GRANT SET LABEL * ON GRAPH * TO custom")
    execute("DENY SET LABEL * ON DEFAULT GRAPH TO custom")
    execute("CREATE DATABASE foo")
    selectDatabase("foo")
    execute("CALL db.createLabel('Label')")
    execute("CREATE (n)")

    // WHEN
    executeOn("foo", "joe", "soap", "MATCH (n) SET n:Label")

    // THEN
    execute("MATCH(n:Label) RETURN count(n)").toSet should be(Set(Map("count(n)" -> 1)))
  }

  test("deny set label on default graph") {
    // GIVEN
    setupUserWithCustomRole()
    execute("GRANT TRAVERSE ON GRAPH * TO custom")
    execute("GRANT SET LABEL * ON GRAPH * TO custom")
    execute("DENY SET LABEL * ON DEFAULT GRAPH TO custom")
    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CALL db.createLabel('Label')")
    execute("CREATE (n)")

    // WHEN, THEN
    the[AuthorizationViolationException] thrownBy {
      executeOnDBMSDefault( "joe", "soap", "MATCH (n) SET n:Label")
    } should have message "Set label for label 'Label' is not allowed for user 'joe' with roles [PUBLIC, custom]."
  }

}
