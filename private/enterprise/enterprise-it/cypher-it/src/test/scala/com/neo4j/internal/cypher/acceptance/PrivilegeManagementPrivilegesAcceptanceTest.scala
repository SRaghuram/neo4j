/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.internal.cypher.acceptance

import org.neo4j.graphdb.security.AuthorizationViolationException

import scala.collection.mutable

class PrivilegeManagementPrivilegesAcceptanceTest extends AdministrationCommandAcceptanceTestBase {

  // Privilege tests

  test("should not revoke other privilege management privileges when revoking privilege management") {
    // GIVEN
    createRoleWithOnlyAdminPrivilege()
    execute("CREATE ROLE custom AS COPY OF adminOnly")
    execute("GRANT SHOW PRIVILEGE ON DBMS TO custom")
    execute("GRANT ASSIGN PRIVILEGE ON DBMS TO custom")
    execute("GRANT REMOVE PRIVILEGE ON DBMS TO custom")
    execute("GRANT PRIVILEGE MANAGEMENT ON DBMS TO custom")

    // WHEN
    execute("REVOKE PRIVILEGE MANAGEMENT ON DBMS FROM custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      granted(adminPrivilege).role("custom").map,
      granted(adminAction("show_privilege")).role("custom").map,
      granted(adminAction("assign_privilege")).role("custom").map,
      granted(adminAction("remove_privilege")).role("custom").map
    ))
  }

  test("Should revoke sub-privilege even if privilege management exists") {
    // Given
    execute("CREATE ROLE custom")
    execute("GRANT SHOW PRIVILEGE ON DBMS TO custom")
    execute("GRANT ASSIGN PRIVILEGE ON DBMS TO custom")
    execute("GRANT REMOVE PRIVILEGE ON DBMS TO custom")
    execute("GRANT PRIVILEGE MANAGEMENT ON DBMS TO custom")

    // When
    // Now revoke each sub-privilege in turn
    Seq(
      "SHOW PRIVILEGE",
      "ASSIGN PRIVILEGE",
      "REMOVE PRIVILEGE"
    ).foreach(privilege => execute(s"REVOKE $privilege ON DBMS FROM custom"))

    // Then
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      granted(adminAction("privilege_management")).role("custom").map
    ))
  }

  // Enforcement tests

  // SHOW PRIVILEGE

  val showPrivilegeCommands = Seq(
    "SHOW PRIVILEGES",
    "SHOW ALL PRIVILEGES",
    "SHOW ROLE custom PRIVILEGES"
  )

  test("should enforce show privilege privilege") {
    // GIVEN
    setupUserWithCustomRole("foo", "bar")

    // WHEN
    execute("GRANT SHOW PRIVILEGE ON DBMS TO custom")

    // THEN
    showPrivilegeCommands.foreach(command =>
      withClue(command) {
        executeOnSystem("foo", "bar", command)
      }
    )

    // WHEN
    execute("REVOKE SHOW PRIVILEGE ON DBMS FROM custom")

    // THEN
    showPrivilegeCommands.foreach(command =>
      withClue(command) {
        the[AuthorizationViolationException] thrownBy {
          executeOnSystem("foo", "bar", command)
        } should have message "Permission denied."
      }
    )
  }

  test("should fail when showing privileges when denied show privilege privilege") {
    // GIVEN
    setupUserWithCustomAdminRole("foo", "bar")

    // WHEN
    execute("DENY SHOW PRIVILEGE ON DBMS TO custom")

    // THEN
    showPrivilegeCommands.foreach(command =>
      withClue(command) {
        the[AuthorizationViolationException] thrownBy {
          executeOnSystem("foo", "bar", command)
        } should have message "Permission denied."
      }
    )
  }

  test("should show user privileges with correct privileges") {
    // GIVEN
    setupUserWithCustomRole("foo", "bar")

    // WHEN
    execute("GRANT SHOW PRIVILEGE ON DBMS TO custom")
    execute("GRANT SHOW USER ON DBMS TO custom")

    // THEN
    val result = new mutable.HashSet[Map[String, AnyRef]]
    executeOnSystem("foo", "bar", "SHOW USER neo4j privileges", resultHandler = (row, _) => {
      result.add(asPrivilegesResult(row))
    })
    result should be(defaultUserPrivileges)
  }

  test("should fail to show user privileges without show user privilege") {
    // GIVEN
    setupUserWithCustomRole("foo", "bar")

    // WHEN
    execute("GRANT SHOW PRIVILEGE ON DBMS TO custom")

    // THEN
    the[AuthorizationViolationException] thrownBy {
      executeOnSystem("foo", "bar", "SHOW USER neo4j privileges")
    } should have message "Permission denied."
  }

  test("should fail to show user privileges without show privilege privilege") {
    // GIVEN
    setupUserWithCustomRole("foo", "bar")

    // WHEN
    execute("GRANT SHOW USER ON DBMS TO custom")

    // THEN
    the[AuthorizationViolationException] thrownBy {
      executeOnSystem("foo", "bar", "SHOW USER neo4j privileges")
    } should have message "Permission denied."
  }

  test("should always be able to show your own privileges") {
    // GIVEN
    setupUserWithCustomAdminRole("foo", "bar")

    // WHEN
    execute("DENY SHOW PRIVILEGE ON DBMS TO custom")
    execute("DENY SHOW USER ON DBMS TO custom")

    // THEN
    executeOnSystem("foo", "bar", "SHOW USER foo PRIVILEGES")
  }

  // ASSIGN & REMOVE PRIVILEGE

  test("should enforce assign privilege privilege for GRANT dbms privilege") {
    // GIVEN
    setupUserWithCustomRole("foo", "bar")
    execute("CREATE ROLE otherRole")

    allPrivileges.foreach {
      case (command, actions) =>
        withClue(s"$command: \n") {
          // WHEN
          execute("GRANT ASSIGN PRIVILEGE ON DBMS TO custom")

          // THEN
          executeOnSystem("foo", "bar", s"GRANT $command TO otherRole")
          execute("SHOW ROLE otherRole PRIVILEGES").toSet should be(actions.map(p => granted(p).role("otherRole").map))

          // WHEN
          execute("REVOKE ASSIGN PRIVILEGE ON DBMS FROM custom")
          execute(s"REVOKE $command FROM otherRole")

          // THEN
          the[AuthorizationViolationException] thrownBy {
            executeOnSystem("foo", "bar", s"GRANT $command TO otherRole")
          } should have message "Permission denied."

          execute("SHOW ROLE otherRole PRIVILEGES").toSet should be(Set.empty)
        }
    }
  }

  test("should enforce assign privilege privilege for DENY dbms privilege") {
    // GIVEN
    setupUserWithCustomRole("foo", "bar")
    execute("CREATE ROLE otherRole")

    allPrivileges.foreach {
      case (command, actions) =>
        withClue(s"$command: \n") {
          // WHEN
          execute("GRANT ASSIGN PRIVILEGE ON DBMS TO custom")

          // THEN
          executeOnSystem("foo", "bar", s"DENY $command TO otherRole")
          execute("SHOW ROLE otherRole PRIVILEGES").toSet should be(actions.map(p => denied(p).role("otherRole").map))

          // WHEN
          execute("REVOKE ASSIGN PRIVILEGE ON DBMS FROM custom")
          execute(s"REVOKE $command FROM otherRole")

          // THEN
          the[AuthorizationViolationException] thrownBy {
            executeOnSystem("foo", "bar", s"DENY $command TO otherRole")
          } should have message "Permission denied."

          execute("SHOW ROLE otherRole PRIVILEGES").toSet should be(Set.empty)
        }
    }
  }

  test("should fail when granting and denying dbms privileges when denied assign privilege privilege") {
    // GIVEN
    setupUserWithCustomAdminRole("foo", "bar")
    execute("CREATE ROLE otherRole")

    // WHEN
    execute("DENY ASSIGN PRIVILEGE ON DBMS TO custom")

    allPrivilegeCommands.foreach { command =>
      withClue(s"$command: \n") {
        // THEN
        the[AuthorizationViolationException] thrownBy {
          executeOnSystem("foo", "bar", s"GRANT $command TO otherRole")
        } should have message "Permission denied."

        the[AuthorizationViolationException] thrownBy {
          executeOnSystem("foo", "bar", s"DENY $command TO otherRole")
        } should have message "Permission denied."

        execute("SHOW ROLE otherRole PRIVILEGES").toSet should be(Set.empty)
      }
    }
  }

  test(s"should enforce remove privilege privilege for dbms privileges") {
    // GIVEN
    setupUserWithCustomRole("foo", "bar")
    execute("CREATE ROLE otherRole")

    allPrivileges.foreach {
      case (command, actions) =>
        withClue(s"$command: \n") {
          // WHEN
          execute("GRANT REMOVE PRIVILEGE ON DBMS TO custom")
          execute(s"GRANT $command TO otherRole")

          // THEN
          executeOnSystem("foo", "bar", s"REVOKE $command FROM otherRole")
          execute("SHOW ROLE otherRole PRIVILEGES").toSet should be(Set.empty)

          // WHEN
          execute("REVOKE REMOVE PRIVILEGE ON DBMS FROM custom")
          execute(s"GRANT $command TO otherRole")

          // THEN
          the[AuthorizationViolationException] thrownBy {
            executeOnSystem("foo", "bar", s"REVOKE $command FROM otherRole")
          } should have message "Permission denied."

          execute("SHOW ROLE otherRole PRIVILEGES").toSet should be(actions.map(p => granted(p).role("otherRole").map))
          execute(s"REVOKE $command FROM otherRole")
        }
    }
  }

  test(s"should fail when revoking dbms privileges when denied remove privilege privilege") {
    // GIVEN
    setupUserWithCustomAdminRole("foo", "bar")
    execute("CREATE ROLE otherRole")

    // WHEN
    execute("DENY REMOVE PRIVILEGE ON DBMS TO custom")

    allPrivileges.foreach {
      case (command, actions) =>
        withClue(s"$command: \n") {
          execute(s"GRANT $command TO otherRole")

          // THEN
          the[AuthorizationViolationException] thrownBy {
            executeOnSystem("foo", "bar", s"REVOKE $command FROM otherRole")
          } should have message "Permission denied."

          execute("SHOW ROLE otherRole PRIVILEGES").toSet should be(actions.map(p => granted(p).role("otherRole").map))
          execute(s"REVOKE $command FROM otherRole")
        }
    }
  }

  // PRIVILEGE MANAGEMENT

  test("should enforce privilege management privilege") {
    // GIVEN
    setupUserWithCustomRole("foo", "bar")
    execute("CREATE ROLE otherRole")

    // WHEN
    execute("GRANT PRIVILEGE MANAGEMENT ON DBMS TO custom")
    execute("GRANT TRAVERSE ON GRAPH * NODES A TO otherRole")

    // THEN
    executeOnSystem("foo", "bar", "SHOW ROLE otherRole PRIVILEGES", resultHandler = (row, _) => {
      val res = Map(
        "access" -> row.get("access"),
        "action" -> row.get("action"),
        "resource" -> row.get("resource"),
        "graph" -> row.get("graph"),
        "segment" -> row.get("segment"),
        "role" -> row.get("role"),
      )
      res should be(granted(traverse).node("A").role("otherRole").map)
    }) should be(1)

    executeOnSystem("foo", "bar", "GRANT TRAVERSE ON GRAPH * NODES B TO otherRole")
    executeOnSystem("foo", "bar", "DENY TRAVERSE ON GRAPH * NODES C TO otherRole")
    executeOnSystem("foo", "bar", "REVOKE TRAVERSE ON GRAPH * NODES A FROM otherRole")

    execute("SHOW ROLE otherRole PRIVILEGES").toSet should be(
      Set(
        granted(traverse).node("B").role("otherRole").database("*").map,
        denied(traverse).node("C").role("otherRole").database("*").map
      )
    )

    // WHEN
    execute("REVOKE PRIVILEGE MANAGEMENT ON DBMS FROM custom")
    execute("REVOKE TRAVERSE ON GRAPH * NODES B FROM otherRole")
    execute("REVOKE TRAVERSE ON GRAPH * NODES C FROM otherRole")
    execute("GRANT TRAVERSE ON GRAPH * NODES A TO otherRole")

    // THEN
    the[AuthorizationViolationException] thrownBy {
      executeOnSystem("foo", "bar", "SHOW ROLE otherRole PRIVILEGES")
    } should have message "Permission denied."

    the[AuthorizationViolationException] thrownBy {
      executeOnSystem("foo", "bar", "GRANT TRAVERSE ON GRAPH * NODES B TO otherRole")
    } should have message "Permission denied."

    the[AuthorizationViolationException] thrownBy {
      executeOnSystem("foo", "bar", "DENY TRAVERSE ON GRAPH * NODES C TO otherRole")
    } should have message "Permission denied."

    the[AuthorizationViolationException] thrownBy {
      executeOnSystem("foo", "bar", "REVOKE TRAVERSE ON GRAPH * NODES A FROM otherRole")
    } should have message "Permission denied."

    execute("SHOW ROLE otherRole PRIVILEGES").toSet should be(
      Set(granted(traverse).node("A").role("otherRole").database("*").map)
    )
  }

  test("should fail privilege management when denied privilege management privilege") {
    // GIVEN
    setupUserWithCustomAdminRole("foo", "bar")
    execute("CREATE ROLE otherRole")

    // WHEN
    execute("GRANT SHOW PRIVILEGE ON DBMS TO custom")
    execute("GRANT ASSIGN PRIVILEGE ON DBMS TO custom")
    execute("GRANT REMOVE PRIVILEGE ON DBMS TO custom")
    execute("DENY PRIVILEGE MANAGEMENT ON DBMS TO custom")

    execute("GRANT TRAVERSE ON GRAPH * NODES A TO otherRole")

    // THEN
    the[AuthorizationViolationException] thrownBy {
      executeOnSystem("foo", "bar", "SHOW ROLE otherRole PRIVILEGES")
    } should have message "Permission denied."

    the[AuthorizationViolationException] thrownBy {
      executeOnSystem("foo", "bar", "GRANT TRAVERSE ON GRAPH * NODES B TO otherRole")
    } should have message "Permission denied."

    the[AuthorizationViolationException] thrownBy {
      executeOnSystem("foo", "bar", "DENY TRAVERSE ON GRAPH * NODES C TO otherRole")
    } should have message "Permission denied."

    the[AuthorizationViolationException] thrownBy {
      executeOnSystem("foo", "bar", "REVOKE TRAVERSE ON GRAPH * NODES A FROM otherRole")
    } should have message "Permission denied."

    execute("SHOW ROLE otherRole PRIVILEGES").toSet should be(
      Set(granted(traverse).node("A").role("otherRole").database("*").map)
    )
  }
}
