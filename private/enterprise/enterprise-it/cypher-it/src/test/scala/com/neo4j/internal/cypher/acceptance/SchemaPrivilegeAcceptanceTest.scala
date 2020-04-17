/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.internal.cypher.acceptance

import java.lang.Boolean.TRUE

import org.neo4j.configuration.Config
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME
import org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME
import org.neo4j.configuration.GraphDatabaseSettings.default_database
import org.neo4j.graphdb.QueryExecutionException
import org.neo4j.graphdb.security.AuthorizationViolationException
import org.neo4j.internal.kernel.api.security.PrivilegeAction
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException

class SchemaPrivilegeAcceptanceTest extends AdministrationCommandAcceptanceTestBase {

  test("should return empty counts to the outside for commands that update the system graph internally") {
    //TODO: ADD ANY NEW UPDATING COMMANDS HERE

    // GIVEN
    setup()
    execute("CREATE ROLE custom")
    execute("CREATE DATABASE foo")

    // Notice: They are executed in succession so they have to make sense in that order
    assertQueriesAndSubQueryCounts(List(
      "GRANT CREATE INDEX ON DATABASE * TO custom" -> 1,
      "REVOKE GRANT CREATE INDEX ON DATABASE * FROM custom" -> 1,
      "DENY CREATE INDEX ON DATABASE * TO custom" -> 1,
      "REVOKE DENY CREATE INDEX ON DATABASE * FROM custom" -> 1,

      "GRANT DROP INDEX ON DATABASE * TO custom" -> 1,
      "DENY DROP INDEX ON DATABASE * TO custom" -> 1,
      "REVOKE DROP INDEX ON DATABASE * FROM custom" -> 2,

      "GRANT INDEX MANAGEMENT ON DATABASES * TO custom" -> 1,
      "REVOKE GRANT INDEX MANAGEMENT ON DATABASES * FROM custom" -> 1,
      "DENY INDEX MANAGEMENT ON DATABASES * TO custom" -> 1,
      "REVOKE DENY INDEX MANAGEMENT ON DATABASES * FROM custom" -> 1,
      "GRANT INDEX MANAGEMENT ON DATABASES * TO custom" -> 1,
      "DENY INDEX MANAGEMENT ON DATABASES * TO custom" -> 1,
      "REVOKE INDEX MANAGEMENT ON DATABASES * FROM custom" -> 2,


      "GRANT CREATE CONSTRAINT ON DATABASE * TO custom" -> 1,
      "DENY CREATE CONSTRAINT ON DATABASE * TO custom" -> 1,
      "REVOKE CREATE CONSTRAINT ON DATABASE * FROM custom" -> 2,

      "GRANT DROP CONSTRAINT ON DATABASE * TO custom" -> 1,
      "REVOKE GRANT DROP CONSTRAINT ON DATABASE * FROM custom" -> 1,
      "DENY DROP CONSTRAINT ON DATABASE * TO custom" -> 1,
      "REVOKE DENY DROP CONSTRAINT ON DATABASE * FROM custom" -> 1,

      "GRANT CONSTRAINT MANAGEMENT ON DATABASES * TO custom" -> 1,
      "REVOKE CONSTRAINT MANAGEMENT ON DATABASES * FROM custom" -> 1,
      "DENY CONSTRAINT MANAGEMENT ON DATABASES * TO custom" -> 1,
      "REVOKE CONSTRAINT MANAGEMENT ON DATABASES * FROM custom" -> 1,
      "GRANT CONSTRAINT MANAGEMENT ON DATABASES * TO custom" -> 1,
      "DENY CONSTRAINT MANAGEMENT ON DATABASES * TO custom" -> 1,
      "REVOKE CONSTRAINT MANAGEMENT ON DATABASES * FROM custom" -> 2,


      "GRANT CREATE NEW LABEL ON DATABASE * TO custom" -> 1,
      "REVOKE GRANT CREATE NEW LABEL ON DATABASE * FROM custom" -> 1,
      "DENY CREATE NEW LABEL ON DATABASE * TO custom" -> 1,
      "REVOKE DENY CREATE NEW LABEL ON DATABASE * FROM custom" -> 1,

      "GRANT CREATE NEW TYPE ON DATABASE * TO custom" -> 1,
      "REVOKE CREATE NEW TYPE ON DATABASE * FROM custom" -> 1,
      "DENY CREATE NEW TYPE ON DATABASE * TO custom" -> 1,
      "REVOKE CREATE NEW TYPE ON DATABASE * FROM custom" -> 1,

      "GRANT CREATE NEW NAME ON DATABASE * TO custom" -> 1,
      "DENY CREATE NEW NAME ON DATABASE * TO custom" -> 1,
      "REVOKE CREATE NEW NAME ON DATABASE * FROM custom" -> 2,

      "GRANT NAME MANAGEMENT ON DATABASES * TO custom" -> 1,
      "REVOKE GRANT NAME MANAGEMENT ON DATABASES * FROM custom" -> 1,
      "DENY NAME MANAGEMENT ON DATABASES * TO custom" -> 1,
      "REVOKE DENY NAME MANAGEMENT ON DATABASES * FROM custom" -> 1,
      "GRANT NAME MANAGEMENT ON DATABASES * TO custom" -> 1,
      "DENY NAME MANAGEMENT ON DATABASES * TO custom" -> 1,
      "REVOKE NAME MANAGEMENT ON DATABASES * FROM custom" -> 2,


      "GRANT ALL DATABASE PRIVILEGES ON DATABASES foo TO custom" -> 1,
      "DENY ALL DATABASE PRIVILEGES ON DATABASES foo TO custom" -> 1,
      "REVOKE ALL DATABASE PRIVILEGES ON DATABASES foo FROM custom" -> 2
    ))
  }

  // Tests for granting, denying and revoking schema privileges

  test("should grant and revoke schema privileges") {
    // GIVEN
    setup()
    execute("CREATE DATABASE foo")
    execute("CREATE DATABASE bar")
    execute("CREATE ROLE role")

    schemaPrivileges.foreach {
      case (command, action) =>
        withClue(s"$command: \n") {
          // WHEN
          execute(s"GRANT $command ON DATABASE foo TO role")
          execute(s"GRANT $command ON DATABASE $$db TO role", Map("db" -> "bar"))
          execute(s"GRANT $command ON DATABASE * TO role")

          // THEN
          execute("SHOW ROLE role PRIVILEGES").toSet should be(Set(
            granted(action).database("foo").role("role").map,
            granted(action).database("bar").role("role").map,
            granted(action).role("role").map
          ))

          // WHEN
          execute(s"REVOKE GRANT $command ON DATABASE foo FROM role")
          execute(s"REVOKE GRANT $command ON DATABASE bar FROM role")
          execute(s"REVOKE GRANT $command ON DATABASE * FROM role")

          // THEN
          execute("SHOW ROLE role PRIVILEGES").toSet should be(Set.empty)
        }
    }
  }

  test("should grant and revoke schema privileges on multiple databases") {
    // GIVEN
    setup()
    execute("CREATE DATABASE foo")
    execute("CREATE DATABASE bar")
    execute("CREATE ROLE role")

    schemaPrivileges.foreach {
      case (command, action) =>
        withClue(s"$command: \n") {
          // WHEN
          execute(s"GRANT $command ON DATABASE foo, bar TO role")

          // THEN
          execute("SHOW ROLE role PRIVILEGES").toSet should be(Set(
            granted(action).database("foo").role("role").map,
            granted(action).database("bar").role("role").map,
          ))

          // WHEN
          execute(s"REVOKE GRANT $command ON DATABASE foo, bar FROM role")

          // THEN
          execute("SHOW ROLE role PRIVILEGES").toSet should be(Set.empty)
        }
    }
  }

  test("should deny and revoke schema privileges") {
    // GIVEN
    setup()
    execute("CREATE DATABASE foo")
    execute("CREATE DATABASE bar")
    execute("CREATE ROLE role")

    schemaPrivileges.foreach {
      case (command, action) =>
        withClue(s"$command: \n") {
          // WHEN
          execute(s"DENY $command ON DATABASE foo TO role")
          execute(s"DENY $command ON DATABASE $$db TO role", Map("db" -> "bar"))
          execute(s"DENY $command ON DATABASE * TO role")

          // THEN
          execute("SHOW ROLE role PRIVILEGES").toSet should be(Set(
            denied(action).database("foo").role("role").map,
            denied(action).database("bar").role("role").map,
            denied(action).role("role").map
          ))

          // WHEN
          execute(s"REVOKE DENY $command ON DATABASE foo FROM role")
          execute(s"REVOKE DENY $command ON DATABASE bar FROM role")
          execute(s"REVOKE DENY $command ON DATABASE * FROM role")

          // THEN
          execute("SHOW ROLE role PRIVILEGES").toSet should be(Set.empty)
        }
    }
  }

  test("should deny and revoke schema privileges on multiple databases") {
    // GIVEN
    setup()
    execute("CREATE DATABASE foo")
    execute("CREATE DATABASE bar")
    execute("CREATE ROLE role")

    schemaPrivileges.foreach {
      case (command, action) =>
        withClue(s"$command: \n") {
          // WHEN
          execute(s"DENY $command ON DATABASE foo, bar TO role")

          // THEN
          execute("SHOW ROLE role PRIVILEGES").toSet should be(Set(
            denied(action).database("foo").role("role").map,
            denied(action).database("bar").role("role").map,
          ))

          // WHEN
          execute(s"REVOKE DENY $command ON DATABASE foo, bar FROM role")

          // THEN
          execute("SHOW ROLE role PRIVILEGES").toSet should be(Set.empty)
        }
    }
  }

    test("should deny and revoke schema privileges on multiple databases with parameter") {
    // GIVEN
    setup()
    execute("CREATE DATABASE foo")
    execute("CREATE DATABASE bar")
    execute("CREATE ROLE role")

    schemaPrivileges.foreach {
      case (command, action) =>
        withClue(s"$command: \n") {
          // WHEN
          execute(s"DENY $command ON DATABASE $$dbParam, bar TO role", Map("dbParam" -> "foo"))

          // THEN
          execute("SHOW ROLE role PRIVILEGES").toSet should be(Set(
            denied(action).database("foo").role("role").map,
            denied(action).database("bar").role("role").map,
          ))

          // WHEN
          execute(s"REVOKE DENY $command ON DATABASE foo, $$dbParam FROM role", Map("dbParam" -> "bar"))

          // THEN
          execute("SHOW ROLE role PRIVILEGES").toSet should be(Set.empty)
        }
    }
  }

  test("should not revoke other index management privileges when revoking index management") {
    // GIVEN
    setup()
    execute("CREATE ROLE custom")
    execute("GRANT CREATE INDEX ON DATABASE * TO custom")
    execute("GRANT DROP INDEX ON DATABASE * TO custom")
    execute("GRANT INDEX MANAGEMENT ON DATABASE * TO custom")

    // WHEN
    execute("REVOKE INDEX MANAGEMENT ON DATABASE * FROM custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      granted(createIndex).role("custom").map,
      granted(dropIndex).role("custom").map
    ))
  }

  test("should not revoke other constraint management privileges when revoking constraint management") {
    // GIVEN
    setup()
    execute("CREATE ROLE custom")
    execute("GRANT CREATE CONSTRAINT ON DATABASE * TO custom")
    execute("GRANT DROP CONSTRAINT ON DATABASE * TO custom")
    execute("GRANT CONSTRAINT MANAGEMENT ON DATABASE * TO custom")

    // WHEN
    execute("REVOKE CONSTRAINT MANAGEMENT ON DATABASE * FROM custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      granted(createConstraint).role("custom").map,
      granted(dropConstraint).role("custom").map
    ))
  }

  test("should not revoke other name management privileges when revoking name management") {
    // GIVEN
    setup()
    execute("CREATE ROLE custom")
    execute("GRANT CREATE NEW NODE LABEL ON DATABASE * TO custom")
    execute("GRANT CREATE NEW RELATIONSHIP TYPE ON DATABASE * TO custom")
    execute("GRANT CREATE NEW PROPERTY NAME ON DATABASE * TO custom")
    execute("GRANT NAME MANAGEMENT ON DATABASE * TO custom")

    // WHEN
    execute("REVOKE NAME MANAGEMENT ON DATABASE * FROM custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      granted(createNodeLabel).role("custom").map,
      granted(createRelationshipType).role("custom").map,
      granted(createPropertyKey).role("custom").map
    ))
  }

  test("Should revoke sub-privilege even if index management exists") {
    // Given
    setup()
    execute("CREATE ROLE custom")
    execute("GRANT CREATE INDEX ON DATABASE * TO custom")
    execute("GRANT DROP INDEX ON DATABASE * TO custom")
    execute("GRANT INDEX MANAGEMENT ON DATABASE * TO custom")

    // When
    // Now revoke each sub-privilege in turn
    Seq(
      "CREATE INDEX",
      "DROP INDEX"
    ).foreach(privilege => execute(s"REVOKE $privilege ON DATABASE * FROM custom"))

    // Then
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      granted(indexManagement).role("custom").map
    ))
  }

  test("Should revoke sub-privilege even if constraint management exists") {
    // Given
    setup()
    execute("CREATE ROLE custom")
    execute("GRANT CREATE CONSTRAINT ON DATABASE * TO custom")
    execute("GRANT DROP CONSTRAINT ON DATABASE * TO custom")
    execute("GRANT CONSTRAINT MANAGEMENT ON DATABASE * TO custom")

    // When
    // Now revoke each sub-privilege in turn
    Seq(
      "CREATE CONSTRAINT",
      "DROP CONSTRAINT"
    ).foreach(privilege => execute(s"REVOKE $privilege ON DATABASE * FROM custom"))

    // Then
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      granted(constraintManagement).role("custom").map
    ))
  }

  test("Should revoke sub-privilege even if name management exists") {
    // Given
    setup()
    execute("CREATE ROLE custom")
    execute("GRANT CREATE NEW NODE LABEL ON DATABASE * TO custom")
    execute("GRANT CREATE NEW RELATIONSHIP TYPE ON DATABASE * TO custom")
    execute("GRANT CREATE NEW PROPERTY NAME ON DATABASE * TO custom")
    execute("GRANT NAME MANAGEMENT ON DATABASE * TO custom")

    // When
    // Now revoke each sub-privilege in turn
    Seq(
      "CREATE NEW NODE LABEL",
      "CREATE NEW RELATIONSHIP TYPE",
      "CREATE NEW PROPERTY NAME"
    ).foreach(privilege => execute(s"REVOKE $privilege ON DATABASE * FROM custom"))

    // Then
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      granted(nameManagement).role("custom").map
    ))
  }

  test("should grant index management privilege on custom default database") {
    // GIVEN
    val config = Config.defaults()
    config.set(default_database, "foo")
    setup(config)
    execute("CREATE ROLE role")

    // WHEN
    execute("GRANT INDEX ON DEFAULT DATABASE TO role")

    // THEN
    execute("SHOW ROLE role PRIVILEGES").toSet should be(Set(
      granted(indexManagement).database(DEFAULT).role("role").map
    ))

    // WHEN
    execute("REVOKE INDEX ON DEFAULT DATABASE FROM role")

    // THEN
    execute("SHOW ROLE role PRIVILEGES").toSet should be(Set.empty)
  }

  test("should grant constraint management privilege on custom default database") {
    // GIVEN
    val config = Config.defaults()
    config.set(default_database, "foo")
    setup(config)
    execute("CREATE ROLE role")

    // WHEN
    execute("GRANT CONSTRAINT ON DEFAULT DATABASE TO role")

    // THEN
    execute("SHOW ROLE role PRIVILEGES").toSet should be(Set(
      granted(constraintManagement).database(DEFAULT).role("role").map
    ))

    // WHEN
    execute("REVOKE CONSTRAINT ON DEFAULT DATABASE FROM role")

    // THEN
    execute("SHOW ROLE role PRIVILEGES").toSet should be(Set.empty)
  }

  test("should grant name management privilege on custom default database") {
    // GIVEN
    val config = Config.defaults()
    config.set(default_database, "foo")
    setup(config)
    execute("CREATE ROLE role")

    // WHEN
    execute("GRANT NAME ON DEFAULT DATABASE TO role")

    // THEN
    execute("SHOW ROLE role PRIVILEGES").toSet should be(Set(
      granted(nameManagement).database(DEFAULT).role("role").map
    ))

    // WHEN
    execute("REVOKE NAME ON DEFAULT DATABASE FROM role")

    // THEN
    execute("SHOW ROLE role PRIVILEGES").toSet should be(Set.empty)
  }

  test("should fail to grant all database privilege using * as parameter") {
    // Given
    setup()
    execute("CREATE ROLE custom")
    execute("CREATE DATABASE foo")

    the[InvalidArgumentsException] thrownBy {
      // When
      execute("GRANT ALL DATABASE PRIVILEGES ON DATABASE $db TO custom", Map("db" -> "*"))
      // Then
    } should have message "Failed to grant database_actions privilege to role 'custom': Parameterized database and graph names do not support wildcards."
  }

  test("should revoke sub-privilege even if all database privilege exists") {
    // Given
    setup()
    execute("CREATE ROLE custom")
    execute("CREATE DATABASE foo")
    execute("GRANT ACCESS ON DATABASE foo TO custom")
    execute("GRANT CREATE INDEX ON DATABASE foo TO custom")
    execute("GRANT DROP INDEX ON DATABASE foo TO custom")
    execute("GRANT INDEX ON DATABASE foo TO custom")
    execute("GRANT CREATE CONSTRAINT ON DATABASE foo TO custom")
    execute("GRANT DROP CONSTRAINT ON DATABASE foo TO custom")
    execute("GRANT CONSTRAINT ON DATABASE foo TO custom")
    execute("GRANT CREATE NEW LABEL ON DATABASE foo TO custom")
    execute("GRANT CREATE NEW TYPE ON DATABASE foo TO custom")
    execute("GRANT CREATE NEW PROPERTY NAME ON DATABASE foo TO custom")
    execute("GRANT NAME ON DATABASE foo TO custom")
    execute("GRANT ALL DATABASE PRIVILEGES ON DATABASE foo TO custom")

    // When
    // Now revoke each sub-privilege in turn
    Seq(
      "ACCESS",
      "CREATE INDEX",
      "DROP INDEX",
      "INDEX",
      "CREATE CONSTRAINT",
      "DROP CONSTRAINT",
      "CONSTRAINT",
      "CREATE NEW LABEL",
      "CREATE NEW TYPE",
      "CREATE NEW PROPERTY NAME",
      "NAME"
    ).foreach(privilege => execute(s"REVOKE $privilege ON DATABASE foo FROM custom"))

    // Then
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(granted(allDatabasePrivilege).database("foo").role("custom").map))
  }

  test("Should revoke compound TOKEN privileges from built-in roles") {
    // Given
    setup()
    execute("CREATE ROLE custom AS COPY OF admin")
    val expected = defaultRolePrivilegesFor("admin", "custom")

    // When && Then
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(expected)

    // When
    execute("REVOKE NAME MANAGEMENT ON DATABASES * FROM custom")

    // Then
    val expectedWithoutNameManagement = expected.filter(_ ("action") != PrivilegeAction.TOKEN.toString)
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(expectedWithoutNameManagement)
  }

  // Tests for actual behaviour of authorization rules for restricted users based on privileges

  // Index Management
  test("Should not allow index creation on non-existing tokens for normal user without token create privilege") {
    setup()
    setupUserWithCustomRole()
    execute("GRANT CREATE INDEX ON DATABASE * TO custom")

    // WHEN & THEN
    the[AuthorizationViolationException] thrownBy {
      executeOnDefault("joe", "soap", "CREATE INDEX FOR (u:User) ON (u.name)")
    } should have message "'create_label' operations are not allowed for user 'joe' with roles [PUBLIC, custom]."

    // THEN
    assert(graph.getMaybeIndex("User", Seq("name")).isEmpty)
  }

  test("Should allow index creation on already existing tokens for normal user without token create privilege") {
    setup()
    setupUserWithCustomRole()
    execute("GRANT CREATE INDEX ON DATABASE * TO custom")
    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE (:User {name: 'Me'})")

    // WHEN
    executeOnDefault("joe", "soap", "CREATE INDEX FOR (u:User) ON (u.name)") should be(0)

    // THEN
    assert(graph.getMaybeIndex("User", Seq("name")).isDefined)
  }

  test("Should not allow index creation for normal user without index create privilege") {
    setup()
    setupUserWithCustomRole()
    execute("GRANT NAME MANAGEMENT ON DATABASE * TO custom")

    // WHEN & THEN
    the[AuthorizationViolationException] thrownBy {
      executeOnDefault("joe", "soap", "CREATE INDEX FOR (u:User) ON (u.name)")
    } should have message "Schema operations are not allowed for user 'joe' with roles [PUBLIC, custom]."
  }

  test("Should not allow index create for normal user with only index drop privilege") {
    // Given
    setup()
    setupUserWithCustomRole()
    execute("GRANT NAME MANAGEMENT ON DATABASE * TO custom")
    execute("GRANT DROP INDEX ON DATABASE * TO custom")

    // WHEN & THEN
    the[AuthorizationViolationException] thrownBy {
      executeOnDefault("joe", "soap", "CREATE INDEX FOR (u:User) ON (u.name)")
    } should have message "Schema operation 'create_index' is not allowed for user 'joe' with roles [PUBLIC, custom]."
  }

  test("Should not allow index drop for normal user with only index create privilege") {
    // Given
    setup()
    setupUserWithCustomRole()
    execute("GRANT NAME MANAGEMENT ON DATABASE * TO custom")
    execute("GRANT CREATE INDEX ON DATABASE * TO custom")
    executeOnDefault("joe", "soap", "CREATE INDEX my_index FOR (u:User) ON (u.name)") should be(0)

    // WHEN & THEN
    the[AuthorizationViolationException] thrownBy {
      executeOnDefault("joe", "soap", "DROP INDEX my_index")
    } should have message "Schema operation 'drop_index' is not allowed for user 'joe' with roles [PUBLIC, custom]."
  }

  test("Should allow index creation for normal user with index create privilege") {
    setup()
    setupUserWithCustomRole()
    execute("GRANT NAME MANAGEMENT ON DATABASE * TO custom")
    execute("GRANT CREATE INDEX ON DATABASE * TO custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      granted(access).role("custom").map,
      granted(createIndex).role("custom").map,
      granted(nameManagement).role("custom").map
    ))

    // WHEN & THEN
    executeOnDefault("joe", "soap", "CREATE INDEX FOR (u:User) ON (u.name)") should be(0)
  }

  test("Should allow index dropping for normal user with index drop privilege") {
    setup()
    selectDatabase(DEFAULT_DATABASE_NAME)
    graph.createIndexWithName("my_index", "Label", "prop")
    setupUserWithCustomRole()
    execute("GRANT DROP INDEX ON DATABASE * TO custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      granted(access).role("custom").map,
      granted(dropIndex).role("custom").map
    ))

    // WHEN
    executeOnDefault("joe", "soap", "DROP INDEX my_index") should be(0)

    // THEN
    graph.getMaybeIndex("Label", Seq("prop")) should be(None)
  }

  test("Should allow index creation and dropping for normal user with index management privilege") {
    setup()
    setupUserWithCustomRole()
    execute("GRANT NAME MANAGEMENT ON DATABASE * TO custom")
    execute("GRANT INDEX ON DATABASE * TO custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      granted(access).role("custom").map,
      granted(nameManagement).role("custom").map,
      granted(indexManagement).role("custom").map
    ))

    // WHEN
    executeOnDefault("joe", "soap", "CREATE INDEX my_index FOR (u:User) ON (u.name)") should be(0)

    // THEN
    graph.getMaybeIndex("User", Seq("name")).isDefined should be(true)

    // WHEN
    executeOnDefault("joe", "soap", "DROP INDEX my_index") should be(0)

    // THEN
    graph.getMaybeIndex("User", Seq("name")).isDefined should be(false)
  }

  test("Should allow index creation for normal user with all database privileges") {
    setup()
    setupUserWithCustomRole()
    execute("CREATE DATABASE foo")
    execute("GRANT ALL PRIVILEGES ON DATABASE foo TO custom")

    // WHEN & THEN
    executeOn("foo", "joe", "soap", "CREATE INDEX FOR (u:User) ON (u.name)") should be(0)
  }

  test("Should not allow index creation for normal user with all database privileges and explicit deny") {
    setup()
    setupUserWithCustomRole()
    execute("CREATE DATABASE foo")
    execute("GRANT ALL PRIVILEGES ON DATABASE * TO custom")
    execute("DENY CREATE INDEX ON DATABASE foo TO custom")

    // WHEN & THEN
    the[AuthorizationViolationException] thrownBy {
      executeOn("foo", "joe", "soap", "CREATE INDEX FOR (u:User) ON (u.name)")
    } should have message "Schema operation 'create_index' is not allowed for user 'joe' with roles [PUBLIC, custom]."
  }

  test("should have index management privilege on new default after switch of default database") {
    // GIVEN
    val newDefaultDatabase = "foo"
    val config = Config.defaults(GraphDatabaseSettings.auth_enabled, TRUE)
    setup(config, impermanent = false)
    setupUserWithCustomRole("alice", "abc", "role")
    execute("GRANT NAME MANAGEMENT ON DATABASE * TO role")
    execute(s"CREATE database $newDefaultDatabase")

    // WHEN: Grant on default database
    execute(s"GRANT INDEX MANAGEMENT ON DEFAULT DATABASE TO role")

    // THEN: Get privilege on default
    execute("SHOW ROLE role PRIVILEGES").toSet should be(Set(
      granted(access).role("role").map,
      granted(nameManagement).role("role").map,
      granted(indexManagement).database(DEFAULT).role("role").map
    ))

    // WHEN: creating index on default
    executeOn(DEFAULT_DATABASE_NAME, "alice", "abc", "CREATE INDEX neo_index FOR (n:Label) ON (n.prop)") should be(0)

    // THEN
    graph.getMaybeIndex("Label", Seq("prop")).isDefined should be(true)

    // WHEN: creating index on foo
    the[AuthorizationViolationException] thrownBy {
      executeOn(newDefaultDatabase, "alice", "abc", "CREATE INDEX foo_index FOR (n:Label) ON (n.prop)")
    } should have message "Schema operations are not allowed for user 'alice' with roles [PUBLIC, role]."

    // THEN
    graph.getMaybeIndex("Label", Seq("prop")).isDefined should be(false)

    // WHEN: switch default database and create index on foo
    config.set(default_database, newDefaultDatabase)
    restart(config)
    selectDatabase(newDefaultDatabase)
    graph.createIndexWithName("foo_index", "Label", "prop")

    // Confirm default database
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute(s"SHOW DEFAULT DATABASE").toSet should be(Set(defaultDb(newDefaultDatabase)))

    // THEN: confirm privilege
    execute("SHOW ROLE role PRIVILEGES").toSet should be(Set(
      granted(access).role("role").map,
      granted(nameManagement).role("role").map,
      granted(indexManagement).database(DEFAULT).role("role").map
    ))

    // WHEN: dropping index on default
    the[AuthorizationViolationException] thrownBy {
      executeOn(DEFAULT_DATABASE_NAME, "alice", "abc", "DROP INDEX neo_index")
    } should have message "Schema operations are not allowed for user 'alice' with roles [PUBLIC, role]."

    // THEN
    graph.getMaybeIndex("Label", Seq("prop")).isEmpty should be(false)

    // WHEN: dropping index on foo
    executeOn(newDefaultDatabase, "alice", "abc", "DROP INDEX foo_index") should be(0)

    // THEN
    graph.getMaybeIndex("Label", Seq("prop")).isEmpty should be(true)
  }

  // Constraint Management
  test("Should not allow constraint creation on non-existing tokens for normal user without token create privilege") {
    setup()
    setupUserWithCustomRole()
    execute("GRANT CREATE CONSTRAINT ON DATABASE * TO custom")

    // WHEN & THEN
    the[AuthorizationViolationException] thrownBy {
      executeOnDefault("joe", "soap", "CREATE CONSTRAINT ON (n:User) ASSERT exists(n.name)")
    } should have message "'create_label' operations are not allowed for user 'joe' with roles [PUBLIC, custom]."

    // THEN
    assert(graph.getMaybeNodeConstraint("User", Seq("name")).isEmpty)
  }

  test("Should allow constraint creation on already existing tokens for normal user without token create privilege") {
    setup()
    setupUserWithCustomRole()
    execute("GRANT CREATE CONSTRAINT ON DATABASE * TO custom")
    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE (:User {name: 'Me'})")

    // WHEN
    executeOnDefault("joe", "soap", "CREATE CONSTRAINT ON (n:User) ASSERT exists(n.name)") should be(0)

    // THEN
    assert(graph.getMaybeNodeConstraint("User", Seq("name")).isDefined)
  }

  test("Should not allow constraint creation for normal user without constraint create privilege") {
    setup()
    setupUserWithCustomRole()
    execute("GRANT NAME MANAGEMENT ON DATABASE * TO custom")

    // WHEN & THEN
    the[AuthorizationViolationException] thrownBy {
      executeOnDefault("joe", "soap", "CREATE CONSTRAINT ON (n:User) ASSERT exists(n.name)")
    } should have message "Schema operations are not allowed for user 'joe' with roles [PUBLIC, custom]."
  }

  test("Should not allow constraint create for normal user with only constraint drop privilege") {
    // Given
    setup()
    setupUserWithCustomRole()
    execute("GRANT NAME MANAGEMENT ON DATABASE * TO custom")
    execute("GRANT DROP CONSTRAINT ON DATABASE * TO custom")

    // WHEN & THEN
    the[AuthorizationViolationException] thrownBy {
      executeOnDefault("joe", "soap", "CREATE CONSTRAINT ON (n:User) ASSERT exists(n.name)")
    } should have message "Schema operation 'create_constraint' is not allowed for user 'joe' with roles [PUBLIC, custom]."
  }

  test("Should not allow constraint drop for normal user with only constraint create privilege") {
    // Given
    setup()
    setupUserWithCustomRole()
    execute("GRANT NAME MANAGEMENT ON DATABASE * TO custom")
    execute("GRANT CREATE CONSTRAINT ON DATABASE * TO custom")
    executeOnDefault("joe", "soap", "CREATE CONSTRAINT my_constraint ON (n:User) ASSERT exists(n.name)") should be(0)

    // WHEN & THEN
    the[AuthorizationViolationException] thrownBy {
      executeOnDefault("joe", "soap", "DROP CONSTRAINT my_constraint")
    } should have message "Schema operation 'drop_constraint' is not allowed for user 'joe' with roles [PUBLIC, custom]."
  }

  test("Should allow constraint creation for normal user with constraint create privilege") {
    setup()
    setupUserWithCustomRole()
    execute("GRANT NAME MANAGEMENT ON DATABASE * TO custom")
    execute("GRANT CREATE CONSTRAINT ON DATABASE * TO custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      granted(access).role("custom").map,
      granted(createConstraint).role("custom").map,
      granted(nameManagement).role("custom").map
    ))

    // WHEN & THEN
    executeOnDefault("joe", "soap", "CREATE CONSTRAINT ON (n:User) ASSERT exists(n.name)") should be(0)
  }

  test("Should allow constraint dropping for normal user with constraint drop privilege") {
    setup()
    selectDatabase(DEFAULT_DATABASE_NAME)
    graph.createNodeExistenceConstraintWithName("my_constraint", "Label", "prop")
    setupUserWithCustomRole()
    execute("GRANT DROP CONSTRAINT ON DATABASE * TO custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      granted(access).role("custom").map,
      granted(dropConstraint).role("custom").map
    ))

    // WHEN
    executeOnDefault("joe", "soap", "DROP CONSTRAINT my_constraint") should be(0)

    // THEN
    graph.getMaybeNodeConstraint("Label", Seq("prop")) should be(None)
  }

  test("Should allow constraint creation and dropping for normal user with constraint management privilege") {
    setup()
    setupUserWithCustomRole()
    execute("GRANT NAME MANAGEMENT ON DATABASE * TO custom")
    execute("GRANT CONSTRAINT ON DATABASE * TO custom")

    // THEN
    execute("SHOW ROLE custom PRIVILEGES").toSet should be(Set(
      granted(access).role("custom").map,
      granted(nameManagement).role("custom").map,
      granted(constraintManagement).role("custom").map
    ))

    // WHEN
    executeOnDefault("joe", "soap", "CREATE CONSTRAINT my_constraint ON (u:User) ASSERT exists(u.name)") should be(0)

    // THEN
    graph.getMaybeNodeConstraint("User", Seq("name")).isDefined should be(true)

    // WHEN
    executeOnDefault("joe", "soap", "DROP CONSTRAINT my_constraint") should be(0)

    // THEN
    graph.getMaybeNodeConstraint("User", Seq("name")).isDefined should be(false)
  }

  test("Should allow constraint creation for normal user with all database privileges") {
    setup()
    setupUserWithCustomRole()
    execute("CREATE DATABASE foo")
    execute("GRANT ALL PRIVILEGES ON DATABASE foo TO custom")

    // WHEN & THEN
    executeOn("foo", "joe", "soap", "CREATE CONSTRAINT ON (n:User) ASSERT exists(n.name)") should be(0)
  }

  test("Should not allow constraint creation for normal user with all database privileges and explicit deny") {
    setup()
    setupUserWithCustomRole()
    execute("CREATE DATABASE foo")
    execute("GRANT ALL PRIVILEGES ON DATABASE * TO custom")
    execute("DENY CREATE CONSTRAINT ON DATABASE foo TO custom")

    // WHEN & THEN
    the[AuthorizationViolationException] thrownBy {
      executeOn("foo", "joe", "soap", "CREATE CONSTRAINT ON (n:User) ASSERT exists(n.name)")
    } should have message "Schema operation 'create_constraint' is not allowed for user 'joe' with roles [PUBLIC, custom]."
  }

  test("should have constraint management privilege on new default after switch of default database") {
    // GIVEN
    val newDefaultDatabase = "foo"
    val config = Config.defaults(GraphDatabaseSettings.auth_enabled, TRUE)
    setup(config, impermanent = false)
    setupUserWithCustomRole("alice", "abc", "role")
    execute("GRANT NAME MANAGEMENT ON DATABASE * TO role")
    execute(s"CREATE database $newDefaultDatabase")

    // Confirm default database
    execute(s"SHOW DEFAULT DATABASE").toSet should be(Set(defaultDb(DEFAULT_DATABASE_NAME)))

    // WHEN: Grant on default database
    execute(s"GRANT CONSTRAINT MANAGEMENT ON DEFAULT DATABASE TO role")

    // THEN: Get privilege on default
    execute("SHOW ROLE role PRIVILEGES").toSet should be(Set(
      granted(access).role("role").map,
      granted(nameManagement).role("role").map,
      granted(constraintManagement).database(DEFAULT).role("role").map
    ))

    // WHEN: creating constraint on default
    executeOn(DEFAULT_DATABASE_NAME, "alice", "abc", "CREATE CONSTRAINT neo_constraint ON (n:Label) ASSERT exists(n.prop)") should be(0)

    // THEN
    graph.getMaybeNodeConstraint("Label", Seq("prop")).isDefined should be(true)

    // WHEN: creating constraint on foo
    the[AuthorizationViolationException] thrownBy {
      executeOn(newDefaultDatabase, "alice", "abc", "CREATE CONSTRAINT foo_constraint ON (n:Label) ASSERT exists(n.prop)")
    } should have message "Schema operations are not allowed for user 'alice' with roles [PUBLIC, role]."

    // THEN
    graph.getMaybeNodeConstraint("Label", Seq("prop")).isDefined should be(false)

    // WHEN: switch default database and create constraint on foo
    config.set(default_database, newDefaultDatabase)
    restart(config)
    selectDatabase(newDefaultDatabase)
    graph.createNodeExistenceConstraintWithName("foo_constraint", "Label", "prop")

    // Confirm default database
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute(s"SHOW DEFAULT DATABASE").toSet should be(Set(defaultDb(newDefaultDatabase)))

    // THEN: confirm privilege
    execute("SHOW ROLE role PRIVILEGES").toSet should be(Set(
      granted(access).role("role").map,
      granted(nameManagement).role("role").map,
      granted(constraintManagement).database(DEFAULT).role("role").map
    ))

    // WHEN: dropping constraint on default
    the[AuthorizationViolationException] thrownBy {
      executeOn(DEFAULT_DATABASE_NAME, "alice", "abc", "DROP CONSTRAINT neo_constraint")
    } should have message "Schema operations are not allowed for user 'alice' with roles [PUBLIC, role]."

    // THEN
    graph.getMaybeNodeConstraint("Label", Seq("prop")).isEmpty should be(false)

    // WHEN: dropping constraint on foo
    executeOn(newDefaultDatabase, "alice", "abc", "DROP CONSTRAINT foo_constraint") should be(0)

    // THEN
    graph.getMaybeNodeConstraint("Label", Seq("prop")).isEmpty should be(true)
  }

  // Name Management
  test("Should allow label creation for normal user with label create privilege") {
    setup()
    setupUserWithCustomRole()
    execute("GRANT CREATE NEW LABEL ON DATABASE * TO custom")

    // WHEN & THEN
    executeOnDefault("joe", "soap", "CALL db.createLabel('A')") should be(0)
  }

  test("Should not allow label creation for normal user with explicit deny") {
    setup()
    setupUserWithCustomRole()
    execute("GRANT WRITE ON GRAPH * TO custom")
    execute("GRANT NAME MANAGEMENT ON DATABASE * TO custom")
    execute("DENY CREATE NEW LABEL ON DATABASE * TO custom")

    // WHEN & THEN
    the[AuthorizationViolationException] thrownBy {
      executeOnDefault("joe", "soap", "CREATE (n:User) RETURN n")
    } should have message "'create_label' operations are not allowed for user 'joe' with roles [PUBLIC, custom]."

    // WHEN & THEN
    the[QueryExecutionException] thrownBy {
      executeOnDefault("joe", "soap", "CALL db.createLabel('A')")
    } should have message "'create_label' operations are not allowed for user 'joe' with roles [PUBLIC, custom] restricted to TOKEN_WRITE."
  }

  test("Should allow type creation for normal user with type create privilege") {
    setup()
    setupUserWithCustomRole()
    execute("GRANT CREATE NEW TYPE ON DATABASE * TO custom")

    // WHEN & THEN
    executeOnDefault("joe", "soap", "CALL db.createRelationshipType('A')") should be(0)
  }

  test("Should not allow type creation for normal user with explicit deny") {
    setup()
    setupUserWithCustomRole()
    execute("GRANT WRITE ON GRAPH * TO custom")
    execute("GRANT NAME MANAGEMENT ON DATABASE * TO custom")
    execute("DENY CREATE NEW TYPE ON DATABASE * TO custom")

    // WHEN & THEN
    the[AuthorizationViolationException] thrownBy {
      executeOnDefault("joe", "soap", "CREATE ()-[n:Rel]->() RETURN n")
    } should have message "'create_reltype' operations are not allowed for user 'joe' with roles [PUBLIC, custom]."

    // WHEN & THEN
    the[QueryExecutionException] thrownBy {
      executeOnDefault("joe", "soap", "CALL db.createRelationshipType('A')")
    } should have message "'create_reltype' operations are not allowed for user 'joe' with roles [PUBLIC, custom] restricted to TOKEN_WRITE."
  }

  test("Should allow property key creation for normal user with name creation privilege") {
    setup()
    setupUserWithCustomRole()
    execute("GRANT CREATE NEW NAME ON DATABASE * TO custom")
    selectDatabase(DEFAULT_DATABASE_NAME)

    // WHEN & THEN
    executeOnDefault("joe", "soap", "CALL db.createProperty('age')") should be(0)
  }

  test("Should not allow property key creation for normal user with explicit deny") {
    setup()
    setupUserWithCustomRole()
    execute("GRANT WRITE ON GRAPH * TO custom")
    execute("GRANT NAME MANAGEMENT ON DATABASE * TO custom")
    execute("DENY CREATE NEW NAME ON DATABASE * TO custom")

    // WHEN & THEN
    the[AuthorizationViolationException] thrownBy {
      executeOnDefault("joe", "soap", "CREATE (n:User {name: 'Alice'}) RETURN n")
    } should have message "'create_propertykey' operations are not allowed for user 'joe' with roles [PUBLIC, custom]."

    // WHEN & THEN
    the[AuthorizationViolationException] thrownBy {
      executeOnDefault("joe", "soap", "CREATE ()-[n:Rel {prop: 'value'}]->() RETURN n")
    } should have message "'create_propertykey' operations are not allowed for user 'joe' with roles [PUBLIC, custom]."

    // WHEN & THEN
    the[QueryExecutionException] thrownBy {
      executeOnDefault("joe", "soap", "CALL db.createProperty('age')")
    } should have message "'create_propertykey' operations are not allowed for user 'joe' with roles [PUBLIC, custom] restricted to TOKEN_WRITE."
  }

  test("Should not allow property key creation for normal user with only label creation privilege") {
    setup()
    setupUserWithCustomRole()
    execute("GRANT WRITE ON GRAPH * TO custom")
    execute("GRANT CREATE NEW LABEL ON DATABASE * TO custom")

    // WHEN & THEN
    the[AuthorizationViolationException] thrownBy {
      executeOnDefault("joe", "soap", "CREATE (n:User {name: 'Alice'}) RETURN n.name")
    } should have message "'create_propertykey' operations are not allowed for user 'joe' with roles [PUBLIC, custom]."
  }

  test("Should not allow property key creation for normal user with only type creation privilege") {
    setup()
    setupUserWithCustomRole()
    execute("GRANT WRITE ON GRAPH * TO custom")
    execute("GRANT CREATE NEW TYPE ON DATABASE * TO custom")

    // WHEN & THEN
    the[AuthorizationViolationException] thrownBy {
      executeOnDefault("joe", "soap", "CREATE ()-[r:Rel {prop: 'value'}]->() RETURN r.prop")
    } should have message "'create_propertykey' operations are not allowed for user 'joe' with roles [PUBLIC, custom]."
  }

  test("Should allow all creation for normal user with name management privilege") {
    setup()
    setupUserWithCustomRole()
    execute("GRANT WRITE ON GRAPH * TO custom")
    execute("GRANT NAME MANAGEMENT ON DATABASE * TO custom")

    // WHEN & THEN
    executeOnDefault("joe", "soap", "CREATE (n:User {name: 'Alice'})-[:KNOWS {since: 2019}]->(:User {name: 'Bob'}) RETURN n.name", resultHandler = (row, _) => {
      row.get("n.name") should be("Alice")
    }) should be(1)
  }

  test("should have name management privilege on new default after switch of default database") {
    // GIVEN
    val newDefaultDatabase = "foo"
    val config = Config.defaults(GraphDatabaseSettings.auth_enabled, TRUE)
    setup(config, impermanent = false)
    setupUserWithCustomRole("alice", "abc", "role")
    execute("GRANT WRITE ON GRAPH * TO role")
    execute(s"CREATE database $newDefaultDatabase")

    // Confirm default database
    execute(s"SHOW DEFAULT DATABASE").toSet should be(Set(defaultDb(DEFAULT_DATABASE_NAME)))

    // WHEN: Grant on default database
    execute(s"GRANT NAME MANAGEMENT ON DEFAULT DATABASE TO role")

    // THEN: Get privilege on default
    execute("SHOW ROLE role PRIVILEGES").toSet should be(Set(
      granted(access).role("role").map,
      granted(write).node("*").role("role").map,
      granted(write).relationship("*").role("role").map,
      granted(nameManagement).database(DEFAULT).role("role").map
    ))

    // WHEN: creating on default
    executeOn(DEFAULT_DATABASE_NAME, "alice", "abc", "CREATE (n:Label1)-[:Type1]->({prop1: 1}) RETURN n") should be(1)

    // THEN
    execute("MATCH (n:Label1) RETURN n").isEmpty should be(false)

    // WHEN: creating on foo
    val exception1 = the[AuthorizationViolationException] thrownBy {
      executeOn(newDefaultDatabase, "alice", "abc", "CREATE (n:Label1)-[:Type1]->({prop1: 1}) RETURN n")
    }
    exception1.getMessage should (
      be("'create_label' operations are not allowed for user 'alice' with roles [PUBLIC, role].") or (
        be("'create_reltype' operations are not allowed for user 'alice' with roles [PUBLIC, role].") or
          be("'create_propertykey' operations are not allowed for user 'alice' with roles [PUBLIC, role]."))
      )

    // THEN
    execute("MATCH (n:Label1) RETURN n").isEmpty should be(true)

    // WHEN: switch default database
    config.set(default_database, newDefaultDatabase)
    restart(config)

    // Confirm default database
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute(s"SHOW DEFAULT DATABASE").toSet should be(Set(defaultDb(newDefaultDatabase)))

    // THEN: confirm privilege
    execute("SHOW ROLE role PRIVILEGES").toSet should be(Set(
      granted(access).role("role").map,
      granted(write).node("*").role("role").map,
      granted(write).relationship("*").role("role").map,
      granted(nameManagement).database(DEFAULT).role("role").map
    ))

    // WHEN: creating on default
    val exception2 = the[AuthorizationViolationException] thrownBy {
      executeOn(DEFAULT_DATABASE_NAME, "alice", "abc", "CREATE (n:Label2)-[:Type2]->({prop2: 1}) RETURN n")
    }
    exception2.getMessage should (
      be("'create_label' operations are not allowed for user 'alice' with roles [PUBLIC, role].") or (
        be("'create_reltype' operations are not allowed for user 'alice' with roles [PUBLIC, role].") or
          be("'create_propertykey' operations are not allowed for user 'alice' with roles [PUBLIC, role]."))
      )

    // THEN
    execute("MATCH (n:Label2) RETURN n").isEmpty should be(true)

    // WHEN: creating on foo
    executeOn(newDefaultDatabase, "alice", "abc", "CREATE (n:Label2)-[:Type2]->({prop2: 1}) RETURN n") should be(1)

    // THEN
    execute("MATCH (n:Label2) RETURN n").isEmpty should be(false)
  }

  test("Should allow all creation for normal user with all database privileges") {
    setup()
    setupUserWithCustomRole()
    execute("GRANT WRITE ON GRAPH * TO custom")
    execute("GRANT ALL ON DATABASE * TO custom")

    // WHEN & THEN
    executeOnDefault("joe", "soap", "CREATE (n:User {name: 'Alice'})-[:KNOWS {since: 2019}]->(:User {name: 'Bob'}) RETURN n.name", resultHandler = (row, _) => {
      row.get("n.name") should be("Alice")
    }) should be(1)
  }

  // Disable normal database creation because we need different settings on each test
  override protected def initTest() {}
}
