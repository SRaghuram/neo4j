/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.internal.cypher.acceptance

import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import com.neo4j.configuration.EnterpriseEditionSettings
import org.neo4j.configuration.Config
import org.neo4j.configuration.GraphDatabaseInternalSettings.block_create_drop_database
import org.neo4j.configuration.GraphDatabaseInternalSettings.block_start_stop_database
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME
import org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME
import org.neo4j.configuration.GraphDatabaseSettings.default_database
import org.neo4j.configuration.connectors.BoltConnector
import org.neo4j.configuration.helpers.SocketAddress
import org.neo4j.dbms.api.DatabaseExistsException
import org.neo4j.dbms.api.DatabaseLimitReachedException
import org.neo4j.dbms.api.DatabaseNotFoundException
import org.neo4j.exceptions.DatabaseAdministrationException
import org.neo4j.exceptions.InvalidArgumentException
import org.neo4j.exceptions.SyntaxException
import org.neo4j.graphdb.DatabaseShutdownException
import org.neo4j.graphdb.config.Setting
import org.neo4j.graphdb.security.AuthorizationViolationException
import org.neo4j.internal.kernel.api.security.AuthenticationResult
import org.neo4j.internal.kernel.api.security.LoginContext
import org.neo4j.kernel.DeadlockDetectedException
import org.neo4j.kernel.api.KernelTransaction

class MultiDatabaseAdministrationCommandAcceptanceTest extends AdministrationCommandAcceptanceTestBase {
  test("should return empty counts to the outside for commands that update the system graph internally") {
    //TODO: ADD ANY NEW UPDATING COMMANDS HERE

    // GIVEN
    setup()

    // Notice: They are executed in succession so they have to make sense in that order
    assertQueriesAndSubQueryCounts(List(
      "CREATE DATABASE foo" -> 1,
      "CREATE DATABASE foo2 IF NOT EXISTS" -> 1,
      "CREATE OR REPLACE DATABASE foo" -> 2,
      "CREATE OR REPLACE DATABASE foo3" -> 1,
      "STOP DATABASE foo" -> 1,
      "START DATABASE foo" -> 1,
      "DROP DATABASE foo" -> 1,
      "DROP DATABASE foo2 IF EXISTS" -> 1,
      "DROP DATABASE foo3 DUMP DATA" -> 1
    ))
  }

  test("should fail at startup when config setting for default database name is invalid") {
    // GIVEN
    val startOfError = "Error evaluating value for setting 'dbms.default_database'. "

    // Empty name
    the[IllegalArgumentException] thrownBy {
      // WHEN
      Config.defaults(default_database, "")
      // THEN
    } should have message startOfError + "Failed to validate '' for 'dbms.default_database': The provided database name is empty."

    // Starting on invalid characterN
    the[IllegalArgumentException] thrownBy {
      // WHEN
      Config.defaults(default_database, "_default")
      // THEN
    } should have message startOfError + "Failed to validate '_default' for 'dbms.default_database': Database name '_default' is not starting with an ASCII alphabetic character."

    // Has prefix 'system'
    the[IllegalArgumentException] thrownBy {
      // WHEN
      Config.defaults(default_database, "system-mine")
      // THEN
    } should have message startOfError + "Failed to validate 'system-mine' for 'dbms.default_database': Database name 'system-mine' is invalid, due to the prefix 'system'."

    // Contains invalid characters
    the[IllegalArgumentException] thrownBy {
      // WHEN
      Config.defaults(default_database, "mydbwith_and%")
      // THEN
    } should have message startOfError + "Failed to validate 'mydbwith_and%' for 'dbms.default_database': Database name 'mydbwith_and%' contains illegal characters. Use simple ascii characters, numbers, dots and dashes."

    // Too short name
    the[IllegalArgumentException] thrownBy {
      // WHEN
      Config.defaults(default_database, "me")
      // THEN
    } should have message startOfError + "Failed to validate 'me' for 'dbms.default_database': The provided database name must have a length between 3 and 63 characters."

    // Too long name
    val name = "ihaveallooootoflettersclearlymorethanishould-ihaveallooootoflettersclearlymorethanishould"
    the[IllegalArgumentException] thrownBy {
      // WHEN
      Config.defaults(default_database, name)
      // THEN
    } should have message startOfError + "Failed to validate '" + name + "' for 'dbms.default_database': The provided database name must have a length between 3 and 63 characters."
  }

  test("should not fail at startup when config setting block_create_drop_database is set to true ") {
    val config = Config.defaults()
    config.set(block_create_drop_database, java.lang.Boolean.TRUE)
    setup(config)
  }

  test("should fail CREATE DATABASE when config setting block_create_drop_database is set to true ") {
    // GIVEN
    val config = Config.defaults()
    config.set(block_create_drop_database, java.lang.Boolean.TRUE)
    setup(config)
    selectDatabase(SYSTEM_DATABASE_NAME)

    // WHEN & THEN (existing database)
    val exception1 = the[UnsupportedOperationException] thrownBy {
      execute("CREATE DATABASE neo4j")
    }
    exception1.getMessage should include("CREATE DATABASE is not supported")

    // WHEN & THEN (non-existing database)
    val exception2 = the[UnsupportedOperationException] thrownBy {
      execute("CREATE DATABASE database")
    }
    exception2.getMessage should include("CREATE DATABASE is not supported")
  }

  test("should fail CREATE DATABASE IF NOT EXISTS when config setting block_create_drop_database is set to true ") {
    // GIVEN
    val config = Config.defaults()
    config.set(block_create_drop_database, java.lang.Boolean.TRUE)
    setup(config)
    selectDatabase(SYSTEM_DATABASE_NAME)

    // WHEN & THEN (existing database)
    val exception1 = the[UnsupportedOperationException] thrownBy {
      execute("CREATE DATABASE neo4j IF NOT EXISTS")
    }
    exception1.getMessage should include("CREATE DATABASE is not supported")

    // WHEN & THEN (non-existing database)
    val exception2 = the[UnsupportedOperationException] thrownBy {
      execute("CREATE DATABASE database IF NOT EXISTS")
    }
    exception2.getMessage should include("CREATE DATABASE is not supported")
  }

  test("should fail CREATE OR REPLACE DATABASE when config setting block_create_drop_database is set to true ") {
    // GIVEN
    val config = Config.defaults()
    config.set(block_create_drop_database, java.lang.Boolean.TRUE)
    setup(config)
    selectDatabase(SYSTEM_DATABASE_NAME)

    // WHEN & THEN (existing database)
    val exception1 = the[UnsupportedOperationException] thrownBy {
      execute("CREATE OR REPLACE DATABASE neo4j")
    }
    exception1.getMessage should include("CREATE DATABASE is not supported")

    // WHEN & THEN (non-existing database)
    val exception2 = the[UnsupportedOperationException] thrownBy {
      execute("CREATE OR REPLACE DATABASE database")
    }
    exception2.getMessage should include("CREATE DATABASE is not supported")
  }

  test("should fail DROP DATABASE when config setting block_create_drop_database is set to true ") {
    // GIVEN
    val config = Config.defaults()
    config.set(block_create_drop_database, java.lang.Boolean.TRUE)
    setup(config)
    selectDatabase(SYSTEM_DATABASE_NAME)

    // WHEN & THEN (existing datbase)
    val exception1 = the[UnsupportedOperationException] thrownBy {
      execute("DROP DATABASE neo4j")
    }
    exception1.getMessage should include("DROP DATABASE is not supported")

    // WHEN & THEN (non-existing datbase)
    val exception2 = the[UnsupportedOperationException] thrownBy {
      execute("DROP DATABASE database")
    }
    exception2.getMessage should include("DROP DATABASE is not supported")
  }

  test("should fail DROP DATABASE IF EXISTS when config setting block_create_drop_database is set to true ") {
    // GIVEN
    val config = Config.defaults()
    config.set(block_create_drop_database, java.lang.Boolean.TRUE)
    setup(config)
    selectDatabase(SYSTEM_DATABASE_NAME)

    // WHEN & THEN (existing database)
    val exception1 = the[UnsupportedOperationException] thrownBy {
      execute("DROP DATABASE neo4j IF EXISTS")
    }
    exception1.getMessage should include("DROP DATABASE is not supported")

    // WHEN & THEN (non-existing database)
    val exception2 = the[UnsupportedOperationException] thrownBy {
      execute("DROP DATABASE database IF EXISTS")
    }
    exception2.getMessage should include("DROP DATABASE is not supported")
  }

  test("should not fail to stop and start database when config setting start_stop_drop_database is default ") {
    // GIVEN
    val config = Config.defaults()
    setup(config)
    execute("CREATE DATABASE foo")

    // WHEN & THEN
    execute("STOP DATABASE foo")
    execute("START DATABASE foo")
  }

  test("should not fail to stop and start database when config setting start_stop_drop_database is set to false") {
    // GIVEN
    val config = Config.defaults()
    config.set(block_start_stop_database, java.lang.Boolean.FALSE)
    setup(config)
    execute("CREATE DATABASE foo")

    // WHEN & THEN
    execute("STOP DATABASE foo")
    execute("START DATABASE foo")
  }

  test("should fail to stop database when config setting start_stop_drop_database is set to true") {
    // GIVEN
    val config = Config.defaults()
    config.set(block_start_stop_database, java.lang.Boolean.TRUE)
    setup(config)
    execute("CREATE DATABASE foo")

    // WHEN & THEN
    val exception = the[UnsupportedOperationException] thrownBy {
      execute("STOP DATABASE foo")
    }
    exception.getMessage should include("STOP DATABASE is not supported")
  }

  test("should fail to stop stopped database when config setting start_stop_drop_database is set to true") {
    // GIVEN
    val config = Config.defaults()
    setup(config)
    execute("CREATE DATABASE foo")
    execute("STOP DATABASE foo")

    config.set(block_start_stop_database, java.lang.Boolean.TRUE)
    restart(config)

    // WHEN & THEN
    val exception = the[UnsupportedOperationException] thrownBy {
      execute("STOP DATABASE foo")
    }
    exception.getMessage should include("STOP DATABASE is not supported")
  }

  test("should fail to start database when config setting start_stop_drop_database is set to true") {
    // GIVEN
    val config = Config.defaults()
    config.set(block_start_stop_database, java.lang.Boolean.TRUE)
    setup(config)
    execute("CREATE DATABASE foo")

    // WHEN & THEN
    val exception = the[UnsupportedOperationException] thrownBy {
      execute("START DATABASE foo")
    }
    exception.getMessage should include("START DATABASE is not supported")
  }

  test("should fail to start stopped database when config setting start_stop_drop_database is set to true") {
    // GIVEN
    val config = Config.defaults()
    config.set(block_start_stop_database, java.lang.Boolean.FALSE)
    setup(config, impermanent = false)
    execute("CREATE DATABASE foo")
    execute("STOP DATABASE foo")

    config.set(block_start_stop_database, java.lang.Boolean.TRUE)
    restart(config)

    // WHEN & THEN
    val exception = the[UnsupportedOperationException] thrownBy {
      execute("START DATABASE foo")
    }
    exception.getMessage should include("START DATABASE is not supported")
  }

  // Tests for showing databases

  test(s"should show database $DEFAULT_DATABASE_NAME") {
    // GIVEN
    setup()

    // WHEN
    val result = execute(s"SHOW DATABASE $DEFAULT_DATABASE_NAME")

    // THEN
    result.toList should be(List(db(DEFAULT_DATABASE_NAME, default = true, systemDefault = true)))
  }

  test(s"should show database $DEFAULT_DATABASE_NAME with parameter") {
    // GIVEN
    setup()

    // WHEN
    val result = execute("SHOW DATABASE $db", Map("db" -> DEFAULT_DATABASE_NAME))

    // THEN
    result.toList should be(List(db(DEFAULT_DATABASE_NAME, default = true, systemDefault = true)))
  }

  test("should show database with yield") {
    // GIVEN
    setup()

    // WHEN
    val result = execute("SHOW DATABASE $db YIELD name, address, role", Map("db" -> DEFAULT_DATABASE_NAME))

    // THEN
    result.toList should be(List(Map("name" -> "neo4j",
      "address" -> "localhost:7687",
      "role" -> "standalone")))
  }

  test("should show database with yield and where") {
    // GIVEN
    setup()

    // WHEN
    val result = execute("SHOW DATABASE $db YIELD name, address, role WHERE name = 'neo4j'", Map("db" -> DEFAULT_DATABASE_NAME))

    // THEN
    result.toList should be(List(Map("name" -> "neo4j",
      "address" -> "localhost:7687",
      "role" -> "standalone")))
  }

  test("should show database with yield and where 2") {
    // GIVEN
    setup()

    // WHEN
    val result = execute("SHOW DATABASES YIELD name, address, role WHERE name = 'neo4j'")

    // THEN
    result.toList should be(List(Map("name" -> "neo4j",
      "address" -> "localhost:7687",
      "role" -> "standalone")))
  }

  test("should show database with yield and skip") {
    // GIVEN
    setup()
    execute("CREATE DATABASE baz")
    execute("GRANT ACCESS ON DATABASE baz TO PUBLIC")
    execute("CREATE DATABASE bar")
    execute("GRANT ACCESS ON DATABASE bar TO PUBLIC")

    // WHEN
    val result = execute("SHOW DATABASES YIELD name ORDER BY name SKIP 2")

    // THEN
    result.toList should be(List(Map("name" -> "neo4j"),Map("name" -> "system")))
  }

  test("should show database with yield and limit") {
    // GIVEN
    setup()
    execute("CREATE DATABASE baz")
    execute("GRANT ACCESS ON DATABASE baz TO PUBLIC")
    execute("CREATE DATABASE bar")
    execute("GRANT ACCESS ON DATABASE bar TO PUBLIC")

    // WHEN
    val result = execute("SHOW DATABASES YIELD name ORDER BY name LIMIT 1")

    // THEN
    result.toList should be(List(Map("name" -> "bar")))
  }

  test("should show database with yield and order by asc") {
    // GIVEN
    setup()
    execute("CREATE DATABASE baz")
    execute("GRANT ACCESS ON DATABASE baz TO PUBLIC")
    execute("CREATE DATABASE bar")
    execute("GRANT ACCESS ON DATABASE bar TO PUBLIC")

    // WHEN
    val result = execute("SHOW DATABASES YIELD name ORDER BY name ASC")

    // THEN
    result.toList should be(List(Map("name" -> "bar"),Map("name" -> "baz"),Map("name" -> "neo4j"),Map("name" -> "system")))
  }

  test("should show database with yield and order by desc") {
    // GIVEN
    setup()
    execute("CREATE DATABASE baz")
    execute("GRANT ACCESS ON DATABASE baz TO PUBLIC")
    execute("CREATE DATABASE bar")
    execute("GRANT ACCESS ON DATABASE bar TO PUBLIC")

    // WHEN
    val result = execute("SHOW DATABASES YIELD name ORDER BY name DESC")

    // THEN
    result.toList should be(List(Map("name" -> "system"),Map("name" -> "neo4j"),Map("name" -> "baz"),Map("name" -> "bar")))
  }

  test("should show database with yield and return order by desc") {
    // GIVEN
    setup()
    execute("CREATE DATABASE baz")
    execute("CREATE DATABASE bar")

    // WHEN
    val result = execute("SHOW DATABASES YIELD name RETURN name ORDER BY name DESC")

    // THEN
    result.toList should be(List(Map("name" -> "system"),Map("name" -> "neo4j"),Map("name" -> "baz"),Map("name" -> "bar")))
  }

  test("should show database with yield and return with skip and limit") {
    // GIVEN
    setup()
    execute("CREATE DATABASE baz")
    execute("CREATE DATABASE bar")

    // WHEN
    val result = execute("SHOW DATABASES YIELD name RETURN name ORDER BY name DESC SKIP 1 LIMIT 2")

    // THEN
    result.toList should be(List(Map("name" -> "neo4j"),Map("name" -> "baz")))
  }

  test("should count database with yield and return") {
    // GIVEN
    setup()
    execute("CREATE DATABASE baz")
    execute("CREATE DATABASE bar")

    // WHEN
    val result = execute("SHOW DATABASES YIELD * RETURN count(*)")

    // THEN
    result.toList should be(List(Map("count(*)" -> 4)))
  }

  test("should show default DBMS database") {
    // GIVEN
    setup()

    // WHEN
    val result = execute("SHOW DEFAULT DBMS DATABASE")

    // THEN
    result.head("name") shouldBe "neo4j"
  }

  test("should show default DBMS database even when it is stopped") {
    // GIVEN
    setup()
    execute(s"STOP DATABASE $DEFAULT_DATABASE_NAME")

    // WHEN
    val result = execute("SHOW DEFAULT DBMS DATABASE")

    // THEN
    result.head("name") shouldBe "neo4j"
  }

  test("should show default DBMS database for user with custom default database") {
    // GIVEN
    setup()
    execute("CREATE DATABASE foo")
    execute("GRANT ACCESS ON DATABASE foo TO PUBLIC")
    execute("CREATE USER user SET PASSWORD '123' CHANGE NOT REQUIRED SET DEFAULT DATABASE foo")

    // WHEN
    executeOnSystem("user", "123", "SHOW DEFAULT DBMS DATABASE", resultHandler = (row, _) => {
      // THEN
      row.get("name") should be ("neo4j")
    }) should be (1)
  }

  test("should show default DBMS database with YIELD and WHERE") {
    // GIVEN
    setup()

    // WHEN
    val result = execute("SHOW DEFAULT DBMS DATABASE YIELD name WHERE name = 'neo4j'")

    // THEN
    result.size should be (1)
    result.toList.head should be (Map("name" -> "neo4j"))
  }

  test("should show default database where multiple databases exist") {
    // GIVEN
    setup()
    execute("CREATE DATABASE foo")
    execute("GRANT ACCESS ON DATABASE foo TO PUBLIC")
    execute("CREATE USER user SET PASSWORD '123' CHANGE NOT REQUIRED SET DEFAULT DATABASE foo")

    // WHEN
    executeOnSystem("user", "123", "SHOW DEFAULT DATABASE", resultHandler = (row, _) => {
      // THEN
      row.get("name") should be ("foo")
    }) should be (1)
  }

  test("should show default database with YIELD and aliases") {
    // GIVEN
    setup()

    // WHEN
    val result = execute("SHOW DEFAULT DATABASE YIELD name as foo WHERE foo = 'neo4j' RETURN foo").toList

    // THEN
    result.size shouldBe 1
    result.head("foo") shouldBe "neo4j"
  }

  test("should show database neo4j with YIELD and aliases") {
    // GIVEN
    setup()

    // WHEN
    val result = execute("SHOW DATABASE neo4j YIELD name WHERE name = 'neo4j' RETURN name as foo").toList

    // THEN
    result.size shouldBe 1
    result.head("foo") shouldBe "neo4j"
  }

  test("should show default database using where clause") {
    // GIVEN
    setup()

    // WHEN
    val result = execute("SHOW DATABASES WHERE default").toList

    // THEN
    result.size shouldBe 1
    result.head("name") shouldBe "neo4j"
    result.head("default") shouldBe true
  }

  test("should not show database with invalid yield") {
    // GIVEN
    setup()

    // WHEN
    val exception = the[SyntaxException] thrownBy {
      execute("SHOW DATABASE $db YIELD foo, bar, baz", Map("db" -> DEFAULT_DATABASE_NAME))
    }

    // THEN
    exception.getMessage should startWith("Variable `foo` not defined")
    exception.getMessage should include("(line 1, column 25 (offset: 24))")

  }

  test("should not show database with invalid where") {
    // GIVEN
    setup()

    // WHEN
    val exception = the[SyntaxException] thrownBy {
      execute("SHOW DATABASE $db WHERE foo = 'bar'", Map("db" -> DEFAULT_DATABASE_NAME))
    }

    // THEN
    exception.getMessage should startWith("Variable `foo` not defined")
    exception.getMessage should include("(line 1, column 25 (offset: 24))")
  }

  test("should not show database with yield and invalid where") {
    // GIVEN
    setup()

    // WHEN
    val exception = the[SyntaxException] thrownBy {
      execute("SHOW DATABASE $db YIELD name, address, role WHERE foo = 'bar'", Map("db" -> DEFAULT_DATABASE_NAME))
    }

    // THEN
    exception.getMessage should startWith("Variable `foo` not defined")
    exception.getMessage should include("(line 1, column 51 (offset: 50))")
  }

  test("should not show database with yield and invalid skip") {
    // GIVEN
    setup()

    // WHEN
    val exception = the[SyntaxException] thrownBy {
      execute("SHOW DATABASES YIELD name ORDER BY name SKIP -1")
    }

    // THEN
    exception.getMessage should startWith("Invalid input. '-1' is not a valid value. Must be a non-negative integer")
    exception.getMessage should include("(line 1, column 46 (offset: 45))")
  }

  test("should not show database with yield and invalid limit") {
    // GIVEN
    setup()

    // WHEN
    val exception = the[SyntaxException] thrownBy {
      execute("SHOW DATABASES YIELD name ORDER BY name LIMIT -1")
    }

    // THEN
    exception.getMessage should startWith("Invalid input. '-1' is not a valid value. Must be a non-negative integer")
    exception.getMessage should include("(line 1, column 47 (offset: 46))")
  }

  test("should not show database with yield and return and invalid skip") {
    // GIVEN
    setup()

    // WHEN
    val exception = the[SyntaxException] thrownBy {
      execute("SHOW DATABASES YIELD name ORDER BY name RETURN name SKIP -1")
    }

    // THEN
    exception.getMessage should startWith("Invalid input. '-1' is not a valid value. Must be a non-negative integer")
    exception.getMessage should include("(line 1, column 58 (offset: 57))")
  }

  test("should not show database with yield and return and invalid limit") {
    // GIVEN
    setup()

    // WHEN
    val exception = the[SyntaxException] thrownBy {
      execute("SHOW DATABASES YIELD name ORDER BY name RETURN name LIMIT -1")
    }

    // THEN
    exception.getMessage should startWith("Invalid input. '-1' is not a valid value. Must be a non-negative integer")
    exception.getMessage should include("(line 1, column 59 (offset: 58))")
  }

  test("should not show database with invalid order by") {
    // GIVEN
    setup()

    // WHEN
    val exception = the[SyntaxException] thrownBy {
      execute("SHOW DEFAULT DATABASE YIELD name ORDER BY bar")
    }

    // THEN
    exception.getMessage should startWith("Variable `bar` not defined")
    exception.getMessage should include("(line 1, column 43 (offset: 42))")
  }

  test("should not show database with aliasing and invalid return / aggregation") {
    // GIVEN
    setup()

    // WHEN
    val exception = the[SyntaxException] thrownBy {
      execute("SHOW DEFAULT DATABASE YIELD name as nm, role, currentStatus as cs RETURN count(cs), nm ORDER BY role")
    }

    // THEN
    exception.getMessage should startWith("In a WITH/RETURN with DISTINCT or an aggregation, it is not possible to access variables declared before the WITH/RETURN: role")
    exception.getMessage should include("(line 1, column 97 (offset: 96))")
  }

  test("should not show database when not using the system database") {
    //GIVEN
    setup()
    selectDatabase(DEFAULT_DATABASE_NAME)

    // WHEN
    val exception = the[DatabaseAdministrationException] thrownBy {
      execute("SHOW DEFAULT DATABASE YIELD * WHERE name = $name", "name"-> "db1")
    }

    // THEN
    exception.getMessage shouldBe "This is an administration command and it should be executed against the system database: SHOW DEFAULT DATABASE"
  }

  test("Should always show system even without access") {
    // GIVEN
    setup(defaultConfig)
    clearPublicRole()

    execute("CREATE USER foo SET PASSWORD 'bar' CHANGE NOT REQUIRED")

    executeOnSystem("foo", "bar", "SHOW DATABASES", resultHandler = (row, _) => {
      // THEN
      row.get("name") should be ("system")
    }) should be (1)
  }

  test("Should show database granted access through public role") {
    // GIVEN
    setup(defaultConfig)
    clearPublicRole()

    execute("CREATE USER foo SET PASSWORD 'bar' CHANGE NOT REQUIRED")
    execute("CREATE DATABASE baz")
    execute("GRANT ACCESS ON DATABASE baz TO PUBLIC")

    val expected = Seq("baz", "system")
    executeOnSystem("foo","bar", "SHOW DATABASES", resultHandler = (row, index) => {
      // THEN
      row.get("name") should be(expected(index))
    }) should be (2)
  }

  test("Should show only databases that a user has been given access to") {
    // GIVEN
    setup(defaultConfig)
    clearPublicRole()

    execute("CREATE USER foo SET PASSWORD 'bar' CHANGE NOT REQUIRED")
    execute("CREATE DATABASE baz")
    execute("CREATE ROLE blub")
    execute("GRANT ROLE blub TO foo")
    execute("GRANT ACCESS ON DATABASE baz TO blub")

    val expected = Seq("baz", "system")
    executeOnSystem("foo","bar", "SHOW DATABASES", resultHandler = (row, index) => {
      // THEN
      row.get("name") should be(expected(index))
    }) should be (2)
  }

  test("Should show default database that a user has been given access to") {
    // GIVEN
    setup(defaultConfig)
    clearPublicRole()

    execute("CREATE USER foo SET PASSWORD 'bar' CHANGE NOT REQUIRED")
    execute("CREATE DATABASE baz")
    execute("CREATE ROLE blub")
    execute("GRANT ROLE blub TO foo")
    execute("GRANT ACCESS ON DEFAULT DATABASE TO blub")

    executeOnSystem("foo", "bar", "SHOW DATABASES", resultHandler = (row, _) => {
      row.get("name") should (be ("system") or be ("neo4j"))
    }) should be (2)

    executeOnSystem("foo", "bar", "SHOW DEFAULT DATABASE", resultHandler = (row, _) => {
      // THEN
      row.get("name") should be("neo4j")
    }) should be (1)
  }

  test("Should show default database that a user has explicit access on") {
    // GIVEN
    setup(defaultConfig)
    clearPublicRole()

    execute("CREATE USER foo SET PASSWORD 'bar' CHANGE NOT REQUIRED")
    execute("CREATE ROLE blub")
    execute("GRANT ROLE blub TO foo")
    execute("GRANT ACCESS ON DATABASE neo4j TO blub")

    executeOnSystem("foo", "bar", "SHOW DATABASES", resultHandler = (row, _) => {
      row.get("name") should (be ("system") or be ("neo4j"))
    }) should be (2)

    executeOnSystem("foo", "bar", "SHOW DEFAULT DATABASE", resultHandler = (row, _) => {
      // THEN
      row.get("name") should be("neo4j")
    }) should be (1)
  }

  test("Should show all databases if user granted access on all") {
    // GIVEN
    setup(defaultConfig)
    clearPublicRole()

    execute("CREATE DATABASE baz")
    execute("CREATE USER foo SET PASSWORD 'bar' CHANGE NOT REQUIRED")
    execute("CREATE ROLE blub")
    execute("GRANT ROLE blub TO foo")
    execute("GRANT ACCESS ON DATABASE * TO blub")

    val expected = Seq("baz", "neo4j", "system")
    executeOnSystem("foo","bar", "SHOW DATABASES", resultHandler = (row, index) => {
      // THEN
      row.get("name") should be(expected(index))
    }) should be (3)
  }

  test("Should not show denied database") {
    // GIVEN
    setup(defaultConfig)
    clearPublicRole()

    execute("CREATE DATABASE baz")
    execute("CREATE USER foo SET PASSWORD 'bar' CHANGE NOT REQUIRED")
    execute("CREATE ROLE blub")
    execute("GRANT ROLE blub TO foo")
    execute("GRANT ACCESS ON DATABASE * TO blub")
    execute("DENY ACCESS ON DATABASE baz TO blub")

    val expected = Seq("neo4j", "system")
    executeOnSystem("foo","bar", "SHOW DATABASES", resultHandler = (row, index) => {
      // THEN
      row.get("name") should be(expected(index))
    }) should be (2)
  }

  test("Should show databases granted through different roles") {
    // GIVEN
    setup(defaultConfig)
    clearPublicRole()

    execute("CREATE DATABASE baz")
    execute("CREATE USER foo SET PASSWORD 'bar' CHANGE NOT REQUIRED")
    execute("CREATE ROLE blub")
    execute("CREATE ROLE blob")
    execute("GRANT ROLE blub TO foo")
    execute("GRANT ROLE blob TO foo")
    execute("GRANT ACCESS ON DATABASE neo4j TO blub")
    execute("GRANT ACCESS ON DATABASE baz TO blob")

    val expected = Seq("baz", "neo4j", "system")
    executeOnSystem("foo","bar", "SHOW DATABASES", resultHandler = (row, index) => {
      // THEN
      row.get("name") should be(expected(index))
    }) should be (3)
  }

  test("Should aggregate show databases over different roles") {
    // GIVEN
    setup(defaultConfig)
    clearPublicRole()

    execute("CREATE DATABASE baz")
    execute("CREATE USER foo SET PASSWORD 'bar' CHANGE NOT REQUIRED")
    execute("CREATE ROLE blub")
    execute("CREATE ROLE blob")
    execute("GRANT ROLE blub TO foo")
    execute("GRANT ROLE blob TO foo")
    execute("GRANT ACCESS ON DATABASE * TO blub")
    execute("DENY ACCESS ON DATABASE baz TO blob")

    val expected = Seq("neo4j", "system")
    executeOnSystem("foo","bar", "SHOW DATABASES", resultHandler = (row, index) => {
      // THEN
      row.get("name") should be(expected(index))
    }) should be (2)
  }

  test("Should not show database if access both granted and denied") {
    // GIVEN
    setup(defaultConfig)
    clearPublicRole()

    execute("CREATE DATABASE baz")
    execute("CREATE USER foo SET PASSWORD 'bar' CHANGE NOT REQUIRED")
    execute("CREATE ROLE blub")
    execute("GRANT ROLE blub TO foo")
    execute("GRANT ACCESS ON DATABASE baz TO blub")
    execute("DENY ACCESS ON DATABASE baz TO blub")

    val expected = Seq("system")
    executeOnSystem("foo","bar", "SHOW DATABASES", resultHandler = (row, index) => {
      // THEN
      row.get("name") should be(expected(index))
    }) should be (1)
  }

  test("should show custom default database") {
    // GIVEN
    val config = Config.defaults()
    config.set(default_database, "foo")
    setup(config)

    // WHEN
    val result = execute("SHOW DATABASE foo")

    // THEN
    result.toList should be(List(db("foo", default = true, systemDefault = true)))

    // WHEN
    val result2 = execute(s"SHOW DATABASE $DEFAULT_DATABASE_NAME")

    // THEN
    result2.toList should be(empty)
  }

  test("should show user specific default database") {
    // GIVEN
    val config = Config.defaults()
    config.set[java.lang.Boolean](GraphDatabaseSettings.auth_enabled, true)
    setup(config)
    execute("CREATE DATABASE foo WAIT")
    execute("GRANT ACCESS ON DATABASE foo TO PUBLIC")
    execute("CREATE USER bar SET PASSWORD '123' CHANGE NOT REQUIRED SET DEFAULT DATABASE foo")

    // WHEN
    executeOnSystem("bar", "123", "SHOW DATABASE foo", resultHandler = (row, _) => {
      // THEN
      row.get("name") should be("foo")
      row.get("default") shouldBe true
      row.get("systemDefault") shouldBe false
    }) should be (1)

    // WHEN
    executeOnSystem("bar", "123", "SHOW DATABASE neo4j", resultHandler = (row, _) => {
      // THEN
      row.get("name") should be("neo4j")
      row.get("default") shouldBe false
      row.get("systemDefault") shouldBe true
    }) should be (1)
  }

  test("should show default database when default database is stopped in SHOW USERS") {
    // GIVEN
    val config = Config.defaults()
    config.set[java.lang.Boolean](GraphDatabaseSettings.auth_enabled, true)
    setup(config)
    execute("CREATE DATABASE foo WAIT")
    execute("GRANT ACCESS ON DATABASE foo TO PUBLIC")
    execute("CREATE USER bar SET PASSWORD '123' SET DEFAULT DATABASE foo")
    execute("STOP DATABASE foo")

    // WHEN
    val result = execute("SHOW USERS WHERE user = 'bar'")

    // THEN
    result.toSet should be(Set(user("bar", defaultDatabase = "foo")))
  }

  test("should retain user default database when database is created") {
    // GIVEN
    val config = Config.defaults()
    config.set[java.lang.Boolean](GraphDatabaseSettings.auth_enabled, true)
    setup(config)
    val username = "foo"
    execute(s"CREATE USER $username SET PASSWORD $$password SET DEFAULT DATABASE $$database", Map("password" -> "pass", "database" -> "bar"))

    // THEN
    execute("SHOW USERS").toSet shouldBe Set(defaultUser, user(username, defaultDatabase = "bar"))

    // WHEN
    execute("CREATE DATABASE bar WAIT")

    // THEN
    execute("SHOW USERS").toSet shouldBe Set(defaultUser, user(username, defaultDatabase = "bar"))
    testUserLogin(username, "pass", AuthenticationResult.PASSWORD_CHANGE_REQUIRED)
  }

  test("should retain user default database when database is dropped") {
    // GIVEN
    val config = Config.defaults()
    config.set[java.lang.Boolean](GraphDatabaseSettings.auth_enabled, true)
    setup(config)
    val username = "foo"
    execute("CREATE DATABASE bar")
    execute(s"CREATE USER $username SET PASSWORD $$password SET DEFAULT DATABASE $$database", Map("password" -> "pass", "database" -> "bar"))

    // WHEN
    execute("DROP DATABASE bar")

    // THEN
    execute("SHOW USERS").toSet shouldBe Set(defaultUser, user(username, defaultDatabase = "bar"))
    testUserLogin(username, "pass", AuthenticationResult.PASSWORD_CHANGE_REQUIRED)
  }

  test("should show database for switch of default database") {
    // GIVEN
    val config = Config.defaults()
    setup(config)
    execute("CREATE database foo")

    // WHEN
    val neoResult = execute(s"SHOW DATABASE $DEFAULT_DATABASE_NAME")
    val fooResult = execute("SHOW DATABASE foo")

    // THEN
    neoResult.toSet should be(Set(db(DEFAULT_DATABASE_NAME, default = true, systemDefault = true)))
    fooResult.toSet should be(Set(db("foo")))

    // GIVEN
    config.set(default_database, "foo")
    initSystemGraph(config)

    // WHEN
    val neoResult2 = execute(s"SHOW DATABASE $DEFAULT_DATABASE_NAME")
    val fooResult2 = execute("SHOW DATABASE foo")

    // THEN
    neoResult2.toSet should be(Set(db(DEFAULT_DATABASE_NAME)))
    fooResult2.toSet should be(Set(db("foo", default = true, systemDefault = true)))
  }

  test("should start a stopped database when it becomes default") {
    // GIVEN
    val config = Config.defaults()
    setup(config)

    execute("CREATE DATABASE foo")
    execute("STOP DATABASE foo")

    // WHEN
    val result = execute("SHOW DATABASE foo")

    // THEN
    result.toSet should be(Set(db("foo", status = offlineStatus)))

    // GIVEN
    config.set(default_database, "foo")
    initSystemGraph(config)

    // WHEN
    val result2 = execute("SHOW DATABASE foo")

    // THEN
    // The new default database should be started when the system is restarted
    result2.toSet should be(Set(db("foo", default = true, systemDefault = true)))
  }

  test("should show database using mixed case name") {
    // GIVEN
    setup()
    execute("CREATE DATABASE FOO")

    // WHEN
    val result = execute("SHOW DATABASE FOO")

    // THEN
    result.toList should be(List(db("foo")))
  }

  test("should show default databases") {
    // GIVEN
    setup()

    // WHEN
    val result = execute("SHOW DATABASES")

    // THEN
    result.toSet should be(Set(db(DEFAULT_DATABASE_NAME, default = true, systemDefault = true), db(SYSTEM_DATABASE_NAME)))
  }

  test("should show custom default and system databases") {
    // GIVEN
    val config = Config.defaults()
    config.set(default_database, "foo")
    setup(config)

    // WHEN
    val result = execute("SHOW DATABASES")

    // THEN
    result.toSet should be(Set(db("foo", default = true, systemDefault = true), db(SYSTEM_DATABASE_NAME)))
  }

  test("should show databases for switch of default database") {
    // GIVEN
    val config = Config.defaults()
    setup(config)
    execute("CREATE database foo")

    // WHEN
    val result = execute("SHOW DATABASES")

    // THEN
    result.toSet should be(Set(db(DEFAULT_DATABASE_NAME, default = true, systemDefault = true), db("foo"), db(SYSTEM_DATABASE_NAME)))

    // GIVEN
    config.set(default_database, "foo")
    initSystemGraph(config)

    // WHENtt
    val result2 = execute("SHOW DATABASES")

    // THEN
    result2.toSet should be(Set(db(DEFAULT_DATABASE_NAME), db("foo", default = true, systemDefault = true), db(SYSTEM_DATABASE_NAME)))
  }

  test("should show default database") {
    // GIVEN
    setup()

    // WHEN
    val result = execute("SHOW DEFAULT DATABASE")

    // THEN
    result.toList should be(List(defaultDb(status = onlineStatus)))
  }

  test("should show custom default database using show default database command") {
    // GIVEN
    val config = Config.defaults()
    config.set(default_database, "foo")
    setup(config)

    // WHEN
    val result = execute("SHOW DEFAULT DATABASE")

    // THEN
    result.toList should be(List(defaultDb(name = "foo")))

    // WHEN
    val oldDefaultName = DEFAULT_DATABASE_NAME
    val result2 = execute(s"SHOW DATABASE $oldDefaultName")

    // THEN
    result2.toList should be(empty)
  }

  test("should show correct default database for switch of default database") {
    // GIVEN
    val config = Config.defaults()
    setup(config)

    // WHEN
    val result = execute("SHOW DEFAULT DATABASE")

    // THEN
    result.toSet should be(Set(defaultDb()))

    // GIVEN
    execute("CREATE database foo")
    config.set(default_database, "foo")
    initSystemGraph(config)

    // WHEN
    val result2 = execute("SHOW DEFAULT DATABASE")

    // THEN
    result2.toSet should be(Set(defaultDb(name = "foo")))
  }

  test("should fail when showing databases when credentials expired") {
    setup()
    setupUserWithCustomRole()
    execute("ALTER USER joe SET PASSWORD CHANGE REQUIRED")

    Seq(
      "SHOW DEFAULT DATABASE",
      "SHOW DATABASES",
      "SHOW DATABASE system",
      "CALL db.indexes()"
    ).foreach {
      query =>
        the[AuthorizationViolationException] thrownBy {
          // WHEN
          executeOnSystem("joe", "soap", query)
          // THEN
        } should have message String.format("Permission denied." + PASSWORD_CHANGE_REQUIRED_MESSAGE)
    }
  }

  test("should not fail when calling procedure allows expired credentials") {
    defaultConfig.set(BoltConnector.enabled.asInstanceOf[Setting[Any]], true)
    defaultConfig.set(BoltConnector.listen_address.asInstanceOf[Setting[Any]], new SocketAddress("localhost",0))
    try {
      setup()
      setupUserWithCustomRole()
      execute("ALTER USER joe SET PASSWORD CHANGE REQUIRED")

      Seq(
        "CALL db.ping",
        "CALL dbms.cluster.routing.getRoutingTable({}, \"neo4j\")",
        "CALL dbms.routing.getRoutingTable({}, \"neo4j\")"
      ).foreach {
        query =>
            // WHEN
            executeOnSystem("joe", "soap", query)
            // THEN should have no authorization error
      }
    } finally {
      defaultConfig.set(BoltConnector.enabled.asInstanceOf[Setting[Any]], false)
    }
  }

  // Tests for creating databases

  test("should create database in systemdb") {
    setup()

    // WHEN
    execute("CREATE DATABASE `f.o-o123`")

    // THEN
    val result = execute("SHOW DATABASE `f.o-o123`")
    result.toList should be(List(db("f.o-o123")))
  }

  test("should create database with parameter") {
    setup()

    // WHEN
    execute("CREATE DATABASE $db", Map("db" -> "f.o-o123"))

    // THEN
    val result = execute("SHOW DATABASE `f.o-o123`")
    result.toList should be(List(db("f.o-o123")))
  }

  test("should create database with dots in name in systemdb") {
    setup(defaultConfig)

    // WHEN
    execute("CREATE DATABASE f.oo123")

    // THEN
    val result = execute("SHOW DATABASE f.oo123")
    result.toList should be(List(db("f.oo123")))
  }

  test("should create database using if not exists") {
    setup()

    // WHEN
    execute("CREATE DATABASE `f.o-o123` IF NOT EXISTS")

    // THEN
    val result = execute("SHOW DATABASE `f.o-o123`")
    result.toList should be(List(db("f.o-o123")))
  }

  test("should create database in systemdb when max number of databases is not reached") {

    val config = Config.defaults()
    config.set(EnterpriseEditionSettings.max_number_of_databases, java.lang.Long.valueOf(3L))
    setup(config)

    // WHEN
    execute("CREATE DATABASE `foo`")

    // THEN
    val result = execute("SHOW DATABASE `foo`")
    result.toList should be(List(db("foo")))
    execute("SHOW DATABASES").toList.size should equal(3)
  }

  test("should fail to create database in systemdb when max number of databases is already reached") {

    val config = Config.defaults()
    config.set(EnterpriseEditionSettings.max_number_of_databases, java.lang.Long.valueOf(2L))
    setup(config)

    // WHEN
    the[DatabaseLimitReachedException] thrownBy {
      // WHEN
      execute("CREATE DATABASE `foo`")
      // THEN
    } should have message "Failed to create the specified database 'foo': The total limit of databases is already reached. " +
      "To create more you need to either drop databases or change the limit via the config setting 'dbms.max_databases'"

    // THEN
    execute("SHOW DATABASES").toList.size should equal(2)

    // WHEN
    the[DatabaseLimitReachedException] thrownBy {
      // WHEN
      execute("CREATE DATABASE $db", Map("db" -> "foo"))
      // THEN
    } should have message "Failed to create the specified database 'foo': The total limit of databases is already reached. " +
      "To create more you need to either drop databases or change the limit via the config setting 'dbms.max_databases'"

    // THEN
    execute("SHOW DATABASES").toList.size should equal(2)
  }

  test("should create database using mixed case name") {
    setup()

    // WHEN
    execute("CREATE DATABASE FoO")

    // THEN
    val result = execute("SHOW DATABASE fOo")
    result.toList should be(List(db("foo")))
  }

  test("should create default database on re-start after being dropped") {
    // GIVEN
    setup()
    setupUserWithCustomRole(access = false)
    execute(s"GRANT ACCESS ON DATABASE $DEFAULT_DATABASE_NAME TO custom")
    execute(s"GRANT MATCH {*} ON GRAPH $DEFAULT_DATABASE_NAME NODES * (*) TO custom")

    // WHEN
    execute(s"DROP DATABASE $DEFAULT_DATABASE_NAME")

    // THEN
    the[DatabaseNotFoundException] thrownBy {
      executeOnDBMSDefault("joe", "soap", "MATCH (n) RETURN n.name")
    } should have message DEFAULT_DATABASE_NAME

    // WHEN
    initSystemGraph(defaultConfig)
    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE (:B {name:'b'})")

    // THEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute(s"SHOW DATABASE $DEFAULT_DATABASE_NAME").toSet should be(Set(db(DEFAULT_DATABASE_NAME, default = true, systemDefault = true)))
    executeOnDBMSDefault("joe", "soap", "MATCH (n) RETURN n.name") should be(0)
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute(s"SHOW USER joe PRIVILEGES").toSet should be(publicPrivileges("joe"))
  }

  test("should have access on a created database") {
    // GIVEN
    setup()
    execute("CREATE USER baz SET PASSWORD 'bar' CHANGE NOT REQUIRED")
    execute("GRANT ROLE editor TO baz")

    // WHEN
    execute("CREATE DATABASE foo")

    // THEN
    executeOn("foo", "baz", "bar", "MATCH (n) RETURN n") should be(0)
  }

  test("should be able to drop and recreate the same database") {
    // GIVEN
    setup()
    execute("CREATE DATABASE foo")
    execute("SHOW DATABASE foo").toList should be(List(db("foo")))

    // WHEN
    execute("DROP DATABASE foo")
    execute("SHOW DATABASE foo").toList should be(List.empty)

    // WHEN
    execute("CREATE DATABASE foo")
    execute("SHOW DATABASE foo").toList should be(List(db("foo")))
  }

  test("should be able to drop database with dump additional command") {
    // GIVEN
    setup()
    execute("CREATE DATABASE foo")
    execute("SHOW DATABASE foo").toList should be(List(db("foo")))

    // WHEN / THEN
    selectDatabase("system")
    execute("DROP DATABASE foo DUMP DATA")
    execute("SHOW DATABASE foo").toList should be(List.empty)
  }

  test("should have no access on a re-created database") {
    // GIVEN
    setup()
    execute("CREATE DATABASE foo")
    selectDatabase("foo")
    execute("CREATE (:A {name:'a'})")
    setupUserWithCustomRole(access = false)

    // WHEN
    execute("GRANT ACCESS ON DATABASE foo TO custom")
    execute("GRANT MATCH {*} ON GRAPH foo NODES * (*) TO custom")

    // THEN
    execute(s"SHOW USER joe PRIVILEGES").toSet should be(publicPrivileges("joe") ++ Set(
      granted(access).database("foo").user("joe").role("custom").map,
      granted(matchPrivilege).node("*").graph("foo").user("joe").role("custom").map
    ))
    executeOn("foo", "joe", "soap", "MATCH (n) RETURN n.name",
      resultHandler = (row, _) => row.get("n.name") should be("a")) should be(1)

    // WHEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("DROP DATABASE foo")

    // THEN
    the[DatabaseNotFoundException] thrownBy {
      executeOn("foo", "joe", "soap", "MATCH (n) RETURN n.name")
    } should have message "foo"

    // WHEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute("CREATE DATABASE foo")
    selectDatabase("foo")
    execute("CREATE (:B {name:'b'})")

    // THEN
    the[AuthorizationViolationException] thrownBy {
      executeOn("foo", "joe", "soap", "MATCH (n) RETURN n.name")
    } should have message "Database access is not allowed for user 'joe' with roles [PUBLIC, custom]."
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute(s"SHOW USER joe PRIVILEGES").toSet should be(publicPrivileges("joe"))
  }

  test("should have access on a re-created default database") {
    // GIVEN
    setup()
    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE (:A {name:'a'})")
    setupUserWithCustomRole(access = false)

    // WHEN
    execute(s"GRANT ACCESS ON DATABASE $DEFAULT_DATABASE_NAME TO custom")
    execute(s"GRANT MATCH {*} ON GRAPH $DEFAULT_DATABASE_NAME NODES * (*) TO custom")

    // THEN
    execute(s"SHOW USER joe PRIVILEGES").toSet should be(publicPrivileges("joe") ++ Set(
      granted(access).database(DEFAULT_DATABASE_NAME).user("joe").role("custom").map,
      granted(matchPrivilege).node("*").graph(DEFAULT_DATABASE_NAME).user("joe").role("custom").map,
    ))
    executeOnDBMSDefault("joe", "soap", "MATCH (n) RETURN n.name",
      resultHandler = (row, _) => row.get("n.name") should be("a")) should be(1)

    // WHEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute(s"DROP DATABASE $DEFAULT_DATABASE_NAME")

    // THEN
    the[DatabaseNotFoundException] thrownBy {
      executeOnDBMSDefault("joe", "soap", "MATCH (n) RETURN n.name")
    } should have message DEFAULT_DATABASE_NAME

    // WHEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute(s"CREATE DATABASE $DEFAULT_DATABASE_NAME")
    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE (:B {name:'b'})")

    // THEN
    selectDatabase(GraphDatabaseSettings.SYSTEM_DATABASE_NAME)
    execute(s"SHOW DATABASE $DEFAULT_DATABASE_NAME").toSet should be(Set(db(DEFAULT_DATABASE_NAME, default = true, systemDefault = true)))
    executeOnDBMSDefault("joe", "soap", "MATCH (n) RETURN n.name") should be(0)

    selectDatabase(SYSTEM_DATABASE_NAME)
    execute(s"SHOW USER joe PRIVILEGES").toSet should be(publicPrivileges("joe"))
  }

  test("should fail when creating an already existing database") {
    setup()

    // GIVEN
    execute("CREATE DATABASE foo")

    the[DatabaseExistsException] thrownBy {
      // WHEN
      execute("CREATE DATABASE foo")
      // THEN
    } should have message "Failed to create the specified database 'foo': Database already exists."

    the[DatabaseExistsException] thrownBy {
      // WHEN
      execute("CREATE DATABASE $db", Map("db" -> "foo"))
      // THEN
    } should have message "Failed to create the specified database 'foo': Database already exists."
  }

  test("should do nothing when creating an already existing database using if not exists") {
    // GIVEN
    setup()
    execute("CREATE DATABASE foo")
    execute("STOP DATABASE foo")

    // WHEN
    execute("CREATE DATABASE foo IF NOT EXISTS")

    // THEN
    execute("SHOW DATABASE foo").toList should be(List(db("foo", status = offlineStatus)))
  }

  test("should do nothing when creating an already existing database using if not exists with max number of databases reached") {
    // GIVEN
    val config = Config.defaults()
    config.set(EnterpriseEditionSettings.max_number_of_databases, java.lang.Long.valueOf(3L))
    setup(config)
    execute("CREATE DATABASE foo")
    execute("STOP DATABASE foo")

    // WHEN
    execute("CREATE DATABASE foo IF NOT EXISTS")

    // THEN
    execute("SHOW DATABASE foo").toList should be(List(db("foo", status = offlineStatus)))
  }

  test("should fail when creating a database with invalid name") {
    setup()

    // Empty name
    testCreateDbWithInvalidName("", "The provided database name is empty.")

    // Starting on invalid character
    testCreateDbWithInvalidName("_default", "Database name '_default' is not starting with an ASCII alphabetic character.")

    // Has prefix 'system'
    testCreateDbWithInvalidName("system-mine", "Database name 'system-mine' is invalid, due to the prefix 'system'.")

    // Contains invalid characters
    testCreateDbWithInvalidName("myDbWith_and%",
      "Database name 'mydbwith_and%' contains illegal characters. Use simple ascii characters, numbers, dots and dashes.")

    // Too short name
    testCreateDbWithInvalidName("me", "The provided database name must have a length between 3 and 63 characters.")

    // Too long name
    val name = "ihaveallooootoflettersclearlymorethanishould-ihaveallooootoflettersclearlymorethanishould"
    testCreateDbWithInvalidName(name, "The provided database name must have a length between 3 and 63 characters.")
  }

  private def testCreateDbWithInvalidName(name: String, expectedErrorMessage: String): Unit = {

    val e = the[InvalidArgumentException] thrownBy {
      execute(s"CREATE DATABASE `$name`")
    }

    e should have message expectedErrorMessage
    e.status().code().toString should be("Status.Code[Neo.ClientError.Statement.ArgumentError]")

    val e2 = the[InvalidArgumentException] thrownBy {
      execute("CREATE DATABASE $db", Map("db" -> name))
    }

    e2 should have message expectedErrorMessage
    e2.status().code().toString should be("Status.Code[Neo.ClientError.Statement.ArgumentError]")

  }

  test("should replace existing database (even with max number of databases reached)") {
    // GIVEN
    val config = Config.defaults()
    config.set(EnterpriseEditionSettings.max_number_of_databases, java.lang.Long.valueOf(3L))
    setup(config)

    // WHEN: creation
    execute("CREATE OR REPLACE DATABASE `foo`")
    execute("STOP DATABASE foo")

    // THEN
    execute("SHOW DATABASE `foo`").toList should be(List(db("foo", status = offlineStatus)))
    execute("SHOW DATABASES").toList.size should equal(3)

    // WHEN: replacing
    execute("CREATE OR REPLACE DATABASE `foo`")

    // THEN
    execute("SHOW DATABASE `foo`").toList should be(List(db("foo")))
    execute("SHOW DATABASES").toList.size should equal(3)
  }

  test("should replace default database") {
    // GIVEN
    setup()
    execute(s"STOP DATABASE $DEFAULT_DATABASE_NAME")

    // WHEN
    execute(s"CREATE OR REPLACE DATABASE $DEFAULT_DATABASE_NAME")

    // THEN
    execute(s"SHOW DATABASE $DEFAULT_DATABASE_NAME").toList should be(List(db(DEFAULT_DATABASE_NAME, default = true, systemDefault = true)))
    execute("SHOW DATABASES").toList.size should equal(2)
  }

  test("should fail to create database when max number of databases reached using replace") {
    // GIVEN
    val config = Config.defaults()
    config.set(EnterpriseEditionSettings.max_number_of_databases, java.lang.Long.valueOf(2L))
    setup(config)

    the[DatabaseLimitReachedException] thrownBy {
      // WHEN
      execute("CREATE OR REPLACE DATABASE `foo`")
      // THEN
    } should have message "Failed to create the specified database 'foo': The total limit of databases is already reached. " +
      "To create more you need to either drop databases or change the limit via the config setting 'dbms.max_databases'"

    // THEN
    execute("SHOW DATABASES").toList.size should equal(2)
    execute("SHOW DATABASE `foo`").toList should be(List.empty)
  }

  test("should get syntax exception when using both replace and if not exists") {
    // GIVEN
    setup()

    // WHEN
    val exception = the[SyntaxException] thrownBy {
      execute("CREATE OR REPLACE DATABASE foo IF NOT EXISTS")
    }

    // THEN
    exception.getMessage should include("Failed to create the specified database 'foo': cannot have both `OR REPLACE` and `IF NOT EXISTS`.")

    // WHEN
    val exception2 = the[SyntaxException] thrownBy {
      execute("CREATE OR REPLACE DATABASE $db IF NOT EXISTS", Map("db" -> "foo"))
    }

    // THEN
    exception2.getMessage should include("Failed to create the specified database '$db': cannot have both `OR REPLACE` and `IF NOT EXISTS`.")
  }

  // Tests for dropping databases

  test("should create and drop databases") {
    setup()

    // GIVEN
    execute("CREATE DATABASE foo")
    execute("CREATE DATABASE bar")
    execute("CREATE DATABASE baz")

    // WHEN
    val result = execute("SHOW DATABASES")

    // THEN
    result.toList should contain allOf(db("foo"), db("bar"), db("baz"))

    // WHEN
    execute("DROP DATABASE baz") //online database
    execute("STOP DATABASE bar")
    execute("DROP DATABASE bar") //offline database

    // THEN
    val result2 = execute("SHOW DATABASES")
    val databaseNames: Set[String] = result2.columnAs("name").toSet
    databaseNames should contain("foo")
    databaseNames should not contain allOf("bar", "baz")

    // WHEN
    execute("CREATE DATABASE bar")

    // THEN
    execute("SHOW DATABASES").toList should contain allOf(db("foo"), db("bar"))
  }

  test("should drop database with parameter") {
    // GIVEN
    setup()
    execute("CREATE DATABASE foo")

    // WHEN
    execute("DROP DATABASE $db", Map("db" -> "foo"))

    // THEN
    val result2 = execute("SHOW DATABASES")
    val databaseNames: Set[String] = result2.columnAs("name").toSet
    databaseNames should not contain "foo"
  }

  test("should drop existing database using if exists") {
    // GIVEN
    setup()
    execute("CREATE DATABASE foo")

    // WHEN
    execute("DROP DATABASE foo IF EXISTS")

    // THEN
    execute("SHOW DATABASE foo").toSet should be(Set.empty)
  }

  test("should drop database using mixed case name") {
    // GIVEN
    setup()
    execute("CREATE DATABASE foo")

    // WHEN
    execute("DROP DATABASE FOO")

    // THEN
    val result2 = execute("SHOW DATABASES")
    val databaseNames: Set[String] = result2.columnAs("name").toSet
    databaseNames should not contain "foo"
  }

  test("should have no access on a dropped database") {
    // GIVEN
    setup()
    execute("CREATE DATABASE baz")
    execute("CREATE USER foo SET PASSWORD 'bar' CHANGE NOT REQUIRED")
    execute("GRANT ROLE editor TO foo")

    // WHEN
    execute("DROP DATABASE baz")
    execute("SHOW DATABASES").toSet should be(Set(db(DEFAULT_DATABASE_NAME, default = true, systemDefault = true), db(SYSTEM_DATABASE_NAME)))

    // THEN
    the[DatabaseNotFoundException] thrownBy {
      executeOn("baz", "foo", "bar", "MATCH (n) RETURN n")
    } should have message "baz"
  }

  test("should fail when dropping a non-existing database") {
    setup()

    the[DatabaseNotFoundException] thrownBy {
      // WHEN
      execute("DROP DATABASE foo")
      // THEN
    } should have message "Failed to delete the specified database 'foo': Database does not exist."

    // using parameter
    the[DatabaseNotFoundException] thrownBy {
      // WHEN
      execute("DROP DATABASE $db", Map("db" -> "foo"))
      // THEN
    } should have message "Failed to delete the specified database 'foo': Database does not exist."

    // THEN
    execute("SHOW DATABASE foo").toSet should be(Set.empty)

    // and an invalid (non-existing) one
    the[DatabaseNotFoundException] thrownBy {
      // WHEN
      execute("DROP DATABASE ``")
      // THEN
    } should have message "Failed to delete the specified database '': Database does not exist."

    // and an invalid (non-existing) one using parameter
    the[DatabaseNotFoundException] thrownBy {
      // WHEN
      execute("DROP DATABASE $db", Map("db" -> ""))
      // THEN
    } should have message "Failed to delete the specified database '': Database does not exist."

    // THEN
    execute("SHOW DATABASE ``").toSet should be(Set.empty)
  }

  test("should do nothing when dropping a non-existing database using if exists") {
    setup()

    // WHEN
    execute("DROP DATABASE foo IF EXISTS")

    // THEN
    execute("SHOW DATABASE foo").toSet should be(Set.empty)

    // and an invalid (non-existing) one

    // WHEN
    execute("DROP DATABASE `` IF EXISTS")

    // THEN
    execute("SHOW DATABASE ``").toSet should be(Set.empty)
  }

  test("should fail when dropping a dropped database") {
    setup()

    // GIVEN
    execute("CREATE DATABASE foo")
    execute("STOP DATABASE foo")
    execute("DROP DATABASE foo")

    the[DatabaseNotFoundException] thrownBy {
      // WHEN
      execute("DROP DATABASE foo")
      // THEN
    } should have message "Failed to delete the specified database 'foo': Database does not exist."

    // THEN
    execute("SHOW DATABASE foo").toSet should be(Set.empty)
  }

  test("should do nothing when dropping a dropped database using if exists") {
    setup()

    // GIVEN
    execute("CREATE DATABASE foo")
    execute("STOP DATABASE foo")
    execute("DROP DATABASE foo")

    // WHEN
    execute("DROP DATABASE foo IF EXISTS")

    // THEN
    execute("SHOW DATABASE foo").toSet should be(Set.empty)
  }

  test("should fail on dropping system database") {
    setup()

    the[DatabaseAdministrationException] thrownBy {
      // WHEN
      execute(s"DROP DATABASE $SYSTEM_DATABASE_NAME")
      // THEN
    } should have message "Not allowed to delete system database."

    // THEN
    execute(s"SHOW DATABASE $SYSTEM_DATABASE_NAME").toSet should be(Set(db(SYSTEM_DATABASE_NAME)))

    the[DatabaseAdministrationException] thrownBy {
      // WHEN
      execute("DROP DATABASE $db IF EXISTS", Map("db" -> SYSTEM_DATABASE_NAME))
      // THEN
    } should have message "Not allowed to delete system database."

    // THEN
    execute(s"SHOW DATABASE $SYSTEM_DATABASE_NAME").toSet should be(Set(db(SYSTEM_DATABASE_NAME)))
  }

  test("should drop default database") {
    setup()

    // WHEN
    execute(s"DROP DATABASE $DEFAULT_DATABASE_NAME")

    // THEN
    execute("SHOW DATABASES").toSet should be(Set(db(SYSTEM_DATABASE_NAME)))

    // WHEN
    execute(s"CREATE DATABASE $DEFAULT_DATABASE_NAME")

    // THEN
    execute("SHOW DATABASES").toSet should be(Set(db(DEFAULT_DATABASE_NAME, default = true, systemDefault = true), db(SYSTEM_DATABASE_NAME)))
  }

  test("should drop custom default database") {
    // GIVEN
    val config = Config.defaults()
    config.set(default_database, "foo")
    setup(config)

    // WHEN
    execute("DROP DATABASE foo")

    // THEN
    execute("SHOW DATABASES").toSet should be(Set(db(SYSTEM_DATABASE_NAME)))

    // WHEN
    execute("CREATE DATABASE foo")

    // THEN
    execute("SHOW DATABASES").toSet should be(Set(db("foo", default = true, systemDefault = true), db(SYSTEM_DATABASE_NAME)))
  }

  // Tests for starting databases

  test("should start database on create") {
    setup()

    // WHEN
    execute("CREATE DATABASE foo")

    // THEN
    val result = execute("SHOW DATABASE foo")
    result.toList should be(List(db("foo", status = onlineStatus)))
  }

  test("should fail when starting a non-existing database") {
    setup()

    the[DatabaseNotFoundException] thrownBy {
      // WHEN
      execute("START DATABASE foo")
      // THEN
    } should have message "Failed to start the specified database 'foo': Database does not exist."

    // using parameter
    the[DatabaseNotFoundException] thrownBy {
      // WHEN
      execute("START DATABASE $db", Map("db" -> "foo"))
      // THEN
    } should have message "Failed to start the specified database 'foo': Database does not exist."

    // and an invalid (non-existing) one
    the[DatabaseNotFoundException] thrownBy {
      // WHEN
      execute("START DATABASE ``")
      // THEN
    } should have message "Failed to start the specified database '': Database does not exist."

    // and an invalid (non-existing) one using parameter
    the[DatabaseNotFoundException] thrownBy {
      // WHEN
      execute("START DATABASE $db", Map("db" -> ""))
      // THEN
    } should have message "Failed to start the specified database '': Database does not exist."
  }

  test("should fail when starting a dropped database") {
    setup()

    // GIVEN
    execute("CREATE DATABASE foo")
    execute("STOP DATABASE foo")
    execute("DROP DATABASE foo")

    the[DatabaseNotFoundException] thrownBy {
      // WHEN
      execute("START DATABASE foo")
      // THEN
    } should have message "Failed to start the specified database 'foo': Database does not exist."

    // THEN
    val result = execute("SHOW DATABASE foo")
    result.toList should be(List.empty)
  }

  test("should be able to start a started database") {
    setup()

    // GIVEN
    execute("CREATE DATABASE foo")

    // WHEN
    execute("START DATABASE foo")

    // THEN
    val result2 = execute("SHOW DATABASE foo")
    result2.toList should be(List(db("foo")))
  }

  test("should re-start database") {
    setup()

    // GIVEN
    execute("CREATE DATABASE foo")
    execute("STOP DATABASE foo")

    // WHEN
    execute("START DATABASE foo")

    // THEN
    val result = execute("SHOW DATABASE foo")
    result.toList should be(List(db("foo")))
  }

  test("should re-start database with parameter") {
    // GIVEN
    setup()
    execute("CREATE DATABASE foo")
    execute("STOP DATABASE foo")

    // WHEN
    execute("START DATABASE $db", Map("db" -> "foo"))

    // THEN
    val result = execute("SHOW DATABASE foo")
    result.toList should be(List(db("foo")))
  }

  test("should re-start database using mixed case name") {
    setup()

    // GIVEN
    execute("CREATE DATABASE foo")
    execute("STOP DATABASE foo")

    // WHEN
    execute("START DATABASE FOO")

    // THEN
    val result = execute("SHOW DATABASE foo")
    result.toList should be(List(db("foo")))
  }

  test("should have access on a re-started database") {
    // GIVEN
    setup()
    execute("CREATE DATABASE foo")
    execute("STOP DATABASE foo")

    execute("CREATE USER baz SET PASSWORD 'bar' CHANGE NOT REQUIRED")
    execute("GRANT ROLE editor TO baz")

    the[DatabaseShutdownException] thrownBy {
      executeOn("foo", "baz", "bar", "MATCH (n) RETURN n")
    } should have message "This database is shutdown."

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("START DATABASE foo")

    // THEN
    executeOn("foo", "baz", "bar", "MATCH (n) RETURN n") should be(0)
  }

  // Tests for stopping databases

  test("should stop database") {
    setup()

    // GIVEN
    execute("CREATE DATABASE foo")

    // WHEN
    execute("STOP DATABASE foo")

    // THEN
    val result2 = execute("SHOW DATABASE foo")
    result2.toList should be(List(db("foo", status = offlineStatus)))
  }

  test("should stop database with parameter") {
    setup()

    // GIVEN
    execute("CREATE DATABASE foo")

    // WHEN
    execute("STOP DATABASE $db", Map("db" -> "foo"))

    // THEN
    val result = execute("SHOW DATABASE foo")
    result.toList should be(List(db("foo", status = offlineStatus)))
  }

  test("should stop database using mixed case name") {
    setup()

    // GIVEN
    execute("CREATE DATABASE foo")

    // WHEN
    execute("STOP DATABASE FoO")

    // THEN
    val result = execute("SHOW DATABASE foo")
    result.toList should be(List(db("foo", status = offlineStatus)))
  }

  test("should have no access on a stopped database") {
    // GIVEN
    setup()
    execute("CREATE DATABASE baz")
    execute("CREATE USER foo SET PASSWORD 'bar' CHANGE NOT REQUIRED")
    execute("GRANT ROLE editor TO foo")

    // WHEN
    execute("STOP DATABASE baz")

    // THEN
    the[DatabaseShutdownException] thrownBy {
      executeOn("baz", "foo", "bar", "MATCH (n) RETURN n")
    } should have message "This database is shutdown."

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("START DATABASE baz")

    // THEN
    executeOn("baz", "foo", "bar", "MATCH (n) RETURN n") should be(0)
  }

  test("should fail when stopping a non-existing database") {
    setup()

    the[DatabaseNotFoundException] thrownBy {
      // WHEN
      execute("STOP DATABASE foo")
      // THEN
    } should have message "Failed to stop the specified database 'foo': Database does not exist."

    // using parameter
    the[DatabaseNotFoundException] thrownBy {
      // WHEN
      execute("STOP DATABASE $db", Map("db" -> "foo"))
      // THEN
    } should have message "Failed to stop the specified database 'foo': Database does not exist."

    // and an invalid (non-existing) one
    the[DatabaseNotFoundException] thrownBy {
      // WHEN
      execute("STOP DATABASE ``")
      // THEN
    } should have message "Failed to stop the specified database '': Database does not exist."

    // and an invalid (non-existing) one using parameter
    the[DatabaseNotFoundException] thrownBy {
      // WHEN
      execute("STOP DATABASE $db", Map("db" -> ""))
      // THEN
    } should have message "Failed to stop the specified database '': Database does not exist."
  }

  test("should fail when stopping a dropped database") {
    setup()

    // GIVEN
    execute("CREATE DATABASE foo")
    execute("STOP DATABASE foo")
    execute("DROP DATABASE foo")

    the[DatabaseNotFoundException] thrownBy {
      // WHEN
      execute("STOP DATABASE foo")
      // THEN
    } should have message "Failed to stop the specified database 'foo': Database does not exist."

    // THEN
    val result = execute("SHOW DATABASE foo")
    result.toList should be(List.empty)
  }

  test("should fail on stopping system database") {
    setup()

    the[DatabaseAdministrationException] thrownBy {
      // WHEN
      execute(s"STOP DATABASE $SYSTEM_DATABASE_NAME")
      // THEN
    } should have message "Not allowed to stop system database."

    // THEN
    execute(s"SHOW DATABASE $SYSTEM_DATABASE_NAME").toSet should be(Set(db(SYSTEM_DATABASE_NAME)))

    the[DatabaseAdministrationException] thrownBy {
      // WHEN
      execute("STOP DATABASE $db", Map("db" -> SYSTEM_DATABASE_NAME))
      // THEN
    } should have message "Not allowed to stop system database."

    // THEN
    execute(s"SHOW DATABASE $SYSTEM_DATABASE_NAME").toSet should be(Set(db(SYSTEM_DATABASE_NAME)))
  }

  test("should stop default database") {
    // GIVEN
    setup()

    // WHEN
    execute(s"STOP DATABASE $DEFAULT_DATABASE_NAME")

    // THEN
    execute("SHOW DATABASES").toSet should be(Set(
      db(DEFAULT_DATABASE_NAME, status = offlineStatus, default = true, systemDefault = true), db(SYSTEM_DATABASE_NAME)))
  }

  test("should stop custom default database") {
    // GIVEN
    val config = Config.defaults()
    config.set(default_database, "foo")
    setup(config)

    // WHEN
    execute("STOP DATABASE foo")

    // THEN
    execute("SHOW DATABASES").toSet should be(Set(db("foo", status = offlineStatus, default = true, systemDefault = true),
      db(SYSTEM_DATABASE_NAME)))
  }

  test("should have no access on a stopped default database") {
    // GIVEN
    setup()
    execute("CREATE USER foo SET PASSWORD 'bar' CHANGE NOT REQUIRED")
    execute("GRANT ROLE editor TO foo")

    // WHEN
    execute(s"STOP DATABASE $DEFAULT_DATABASE_NAME")
    execute(s"SHOW DATABASE $DEFAULT_DATABASE_NAME").toSet should be(Set(db(DEFAULT_DATABASE_NAME, status = offlineStatus, default = true, systemDefault = true)))

    // THEN
    the[DatabaseShutdownException] thrownBy {
      executeOnDBMSDefault("foo", "bar", "MATCH (n) RETURN n")
    } should have message "This database is shutdown."

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute(s"START DATABASE $DEFAULT_DATABASE_NAME")

    // THEN
    executeOnDBMSDefault("foo", "bar", "MATCH (n) RETURN n") should be(0)
  }

  test("should be able to stop a stopped database") {
    setup()

    // GIVEN
    execute("CREATE DATABASE foo")
    execute("STOP DATABASE foo")

    // WHEN
    execute("STOP DATABASE foo")

    // THEN
    val result = execute("SHOW DATABASE foo")
    result.toList should be(List(db("foo", status = offlineStatus)))
  }

  test("should pass through deadlock exception") {
    val secondNodeLocked = new CountDownLatch(1)
    var exception2then1: Exception = null
    var exception1then2: Exception = null

    class StopDb2AndThenDb1 extends Runnable {
      override def run(): Unit = {
        val tx = graph.beginTransaction(KernelTransaction.Type.EXPLICIT, LoginContext.AUTH_DISABLED)
        tx.execute("STOP DATABASE db2")
        secondNodeLocked.countDown()
        try {
          tx.execute("STOP DATABASE db1")
        } catch {
          case e: Exception => exception2then1 = e
        }
      }
    }

    setup(defaultConfig)
    execute("CREATE DATABASE db1")
    execute("CREATE DATABASE db2")

    val tx = graph.beginTransaction(KernelTransaction.Type.EXPLICIT, LoginContext.AUTH_DISABLED)
    // take lock on db1 node
    tx.execute("STOP DATABASE db1")

    // take lock on db2 and wait for a lock on db1 in other transaction
    val other = new Thread(new StopDb2AndThenDb1)
    other.start()

    // and wait for those locks to be pending
    secondNodeLocked.await(10, TimeUnit.SECONDS)

    // try to take lock on db2
    try {
      tx.execute("STOP DATABASE db2")
    } catch {
      case e: Exception => exception1then2 = e
    }

    other.join(10000)

    assertThrows[DeadlockDetectedException] {
      if (exception2then1 != null) throw exception2then1
      if (exception1then2 != null) throw exception1then2
    }
  }

  // Disable normal database creation because we need different settings on each test
  override protected def initTest() {}
}
