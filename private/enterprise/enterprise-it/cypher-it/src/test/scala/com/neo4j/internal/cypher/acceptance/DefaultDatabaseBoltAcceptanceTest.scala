/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.internal.cypher.acceptance

import com.neo4j.causalclustering.common.Cluster
import com.neo4j.causalclustering.core.consensus.roles.Role
import com.neo4j.cypher.EnterpriseGraphDatabaseTestSupport
import com.neo4j.test.causalclustering.ClusterConfig
import org.neo4j.configuration.Config
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME
import org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME
import org.neo4j.configuration.connectors.BoltConnector
import org.neo4j.configuration.connectors.ConnectorPortRegister
import org.neo4j.configuration.helpers.SocketAddress
import org.neo4j.cypher.ExecutionEngineFunSuite
import org.neo4j.driver.AuthTokens
import org.neo4j.driver.Driver
import org.neo4j.driver.GraphDatabase
import org.neo4j.driver.Session
import org.neo4j.driver.SessionConfig
import org.neo4j.driver.Transaction
import org.neo4j.driver.exceptions.FatalDiscoveryException
import org.neo4j.driver.exceptions.TransientException
import org.neo4j.fabric.config.FabricSettings
import org.neo4j.graphdb.config.Setting
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade
import org.neo4j.kernel.internal.GraphDatabaseAPI
import org.neo4j.test.rule.TestDirectory.testDirectory

import java.net.URI
import java.util.function.BiConsumer
import scala.collection.JavaConverters.asScalaBufferConverter
import scala.collection.JavaConverters.mapAsJavaMapConverter
import scala.collection.JavaConverters.mapAsScalaMapConverter
import scala.util.Random

class DefaultDatabaseBoltAcceptanceTest extends ExecutionEngineFunSuite with EnterpriseGraphDatabaseTestSupport {
  private val username = "foo"
  private val password = "bar"
  private val fooDb = "foodb"
  private val fooDbRole = "foodbrole"

  var cluster: Cluster = _
  private val testDir = {
    val testDir = testDirectory()
    testDir.prepareDirectory(getClass, Random.alphanumeric.take(8).mkString(""))
    testDir
  }
  private val clusterConfig = ClusterConfig
      .clusterConfig()
      .withNumberOfCoreMembers(3)
      .withNumberOfReadReplicas(1)
      .withInstanceCoreParam(GraphDatabaseSettings.auth_enabled, _ => "true")
      .withInstanceCoreParam(GraphDatabaseSettings.routing_enabled, _ => "true")


  override def databaseConfig(): Map[Setting[_], Object] = {
    val dbConfig = super.databaseConfig()
    (dbConfig +
      (BoltConnector.enabled -> true) +
      (BoltConnector.listen_address -> new SocketAddress("localhost", 0)) +
      (GraphDatabaseSettings.auth_enabled -> true)
      )
      .asInstanceOf[Map[Setting[_], Object]]
  }

  private def withDriver(f: Transaction => Unit, databaseName: Option[String] = None): Unit = {
    withDriver(driverExecutor(databaseName)._2, f)
  }

  private def withDriver(boltAddress: String, f: Transaction => Unit, databaseName: Option[String]) {
    withDriver(driverExecutor(boltAddress, databaseName)._2, f)
  }

  private def withDriver(session: Session, f: Transaction => Unit): Unit = {
    val tx = session.beginTransaction()
    try {
      f(tx)
      tx.commit()
    } finally {
      session.close()
    }
  }

  private def driverExecutor(databaseName: Option[String]): (Driver, Session) = {
    val config = Config.newBuilder().set(databaseConfig().asJava).build()
    val connectorPortRegister =
      managementService.database(config.get(GraphDatabaseSettings.default_database)).asInstanceOf[GraphDatabaseAPI]
        .getDependencyResolver.resolveDependency(classOf[ConnectorPortRegister])
    if (config.get(BoltConnector.enabled)) {
      driverExecutor(connectorPortRegister.getLocalAddress(BoltConnector.NAME).toString(), databaseName)
    } else {
      throw new IllegalStateException("Bolt connector is not configured")
    }
  }

  private def driverExecutor(boltAddress: String, databaseName: Option[String]): (Driver, Session) = {
    val boltURI = URI.create(s"neo4j://$boltAddress/")
    val sessionConfig = databaseName.map(SessionConfig.forDatabase).getOrElse(SessionConfig.defaultConfig())
    val driver = GraphDatabase.driver(boltURI, AuthTokens.basic(username, password))
    (driver, driver.session(sessionConfig))
  }

  private def withCluster(clusterTest: Cluster => Unit): Unit = {
    cluster = ClusterConfig.createCluster(testDir.directory(Random.alphanumeric.take(8).mkString("")), clusterConfig)
    cluster.start()
    try {
      clusterTest(cluster)
    } finally {
      cluster.shutdown()
    }
  }

  implicit class TransactionExtension(t: Transaction) {
    def execute(query: String): List[Map[String, AnyRef]] = t.run(query).list(r => r.asMap().asScala.toMap).asScala.toList
  }

  Seq[java.lang.Boolean](java.lang.Boolean.FALSE, java.lang.Boolean.TRUE).foreach(fabricIsEnabled => {

    ignore(s"Should get system default database when logging in if no default database is set with fabric: $fabricIsEnabled") {
      restartWithConfig(
        databaseConfig() + (FabricSettings.enabled_by_default -> fabricIsEnabled)
      )

      // GIVEN
      selectDatabase(SYSTEM_DATABASE_NAME)
      execute(s"CREATE USER $username SET PASSWORD '$password' CHANGE NOT REQUIRED")
      execute(s"CREATE ROLE $fooDbRole")
      execute(s"GRANT ROLE $fooDbRole TO $username")

      // WHEN
      withDriver({ tx =>
        // THEN
        tx.execute("SHOW CURRENT USER YIELD defaultDatabase").toSet shouldBe
          Set(Map("defaultDatabase" -> DEFAULT_DATABASE_NAME))
      }, Some(SYSTEM_DATABASE_NAME))
      withDriver(tx => tx.execute("MATCH (n) RETURN n.foo").toSet shouldBe Set())
    }

    ignore(s"Should get user's default database when logging in and a default database has been set with fabric: $fabricIsEnabled") {
      restartWithConfig(
        databaseConfig() + (FabricSettings.enabled_by_default -> fabricIsEnabled)
      )

      // GIVEN
      selectDatabase(SYSTEM_DATABASE_NAME)
      execute(s"CREATE DATABASE $fooDb")
      execute(s"CREATE USER $username SET PASSWORD '$password' CHANGE NOT REQUIRED SET DEFAULT DATABASE $fooDb")
      execute(s"CREATE ROLE $fooDbRole")
      execute(s"GRANT ROLE $fooDbRole TO $username")
      execute(s"GRANT ALL ON DATABASE $fooDb TO $fooDbRole")
      execute(s"GRANT ALL ON GRAPH $fooDb TO $fooDbRole")

      selectDatabase(fooDb)
      execute("CREATE (n:Foo{foo:'foo'}) RETURN n").toList

      // WHEN
      withDriver({ tx =>
        // THEN
        tx.execute("SHOW CURRENT USER YIELD defaultDatabase").toSet shouldBe
          Set(Map("defaultDatabase" -> fooDb))
      }, Some(SYSTEM_DATABASE_NAME))
      withDriver(tx => tx.execute("MATCH (n) RETURN n.foo").toSet shouldBe Set(Map("n.foo" -> "foo")))
    }

    ignore(s"Should fail when logging in and a default database has been set but is stopped with fabric: $fabricIsEnabled") {
      restartWithConfig(
        databaseConfig() + (FabricSettings.enabled_by_default -> fabricIsEnabled)
      )

      // GIVEN
      selectDatabase(SYSTEM_DATABASE_NAME)
      execute(s"CREATE DATABASE $fooDb")
      execute(s"CREATE USER $username SET PASSWORD '$password' CHANGE NOT REQUIRED SET DEFAULT DATABASE $fooDb")
      execute(s"STOP DATABASE $fooDb WAIT")

      selectDatabase(DEFAULT_DATABASE_NAME)
      execute("CREATE (n:Foo{foo:'foo'}) RETURN n").toList

      // WHEN
      withDriver({ tx =>
        // THEN
        tx.execute("SHOW CURRENT USER YIELD defaultDatabase").toSet shouldBe
          Set(Map("defaultDatabase" -> fooDb))
      }, Some(SYSTEM_DATABASE_NAME))
      the[TransientException] thrownBy {
        withDriver(tx => tx.execute("MATCH (n) RETURN n.foo").toSet shouldBe Set(Map("n.foo" -> "foo")))
      } should have message s"Database '$fooDb' is unavailable."
    }

    ignore(s"Should fail when logging in and a default database has been set but not created with fabric: $fabricIsEnabled") {
      restartWithConfig(
        databaseConfig() + (FabricSettings.enabled_by_default -> fabricIsEnabled)
      )

      // GIVEN
      selectDatabase(SYSTEM_DATABASE_NAME)
      execute(s"CREATE USER $username SET PASSWORD '$password' CHANGE NOT REQUIRED SET DEFAULT DATABASE $fooDb")

      // WHEN
      withDriver({ tx =>
        // THEN
        tx.execute("SHOW CURRENT USER YIELD defaultDatabase").toSet shouldBe
          Set(Map("defaultDatabase" -> fooDb))
      }, Some(SYSTEM_DATABASE_NAME))
      the[FatalDiscoveryException] thrownBy {
        withDriver(tx => tx.execute("MATCH (n) RETURN n.foo").toSet shouldBe Set())
      } should have message s"Database does not exist. Database name: '$fooDb'."
    }
  })

  ignore("Updating default database during a user session does not change the default database for that session") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute(s"CREATE DATABASE $fooDb")
    execute(s"CREATE USER $username SET PASSWORD '$password' CHANGE NOT REQUIRED SET DEFAULT DATABASE $fooDb")
    execute(s"CREATE ROLE $fooDbRole")
    execute(s"GRANT ROLE $fooDbRole TO $username")
    execute(s"GRANT ALL ON DEFAULT DATABASE TO $fooDbRole")
    execute(s"GRANT ALL ON DEFAULT GRAPH TO $fooDbRole")

    selectDatabase(fooDb)
    execute("CREATE (n:Foo{foo:'foo'}) RETURN n").toList
    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE (n:Foo{foo:'neo4j'}) RETURN n").toList

    // WHEN
    val (driver, session) = driverExecutor(None)
    try {
      var tx = session.beginTransaction()
      tx.execute("MATCH (n) RETURN n.foo").toSet shouldBe Set(Map("n.foo" -> "foo"))
      tx.commit()

      selectDatabase(SYSTEM_DATABASE_NAME)
      execute(s"ALTER USER $username SET DEFAULT DATABASE $DEFAULT_DATABASE_NAME")

      // THEN
      tx = session.beginTransaction()
      tx.execute("MATCH (n) RETURN n.foo").toSet shouldBe Set(Map("n.foo" -> "foo"))
      tx.commit()
    } finally {
      session.close()
      driver.close()
    }
  }

  ignore("Updating default database during a user session where the default database has not previously been used does not changed it for that session") {
    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute(s"CREATE DATABASE $fooDb")
    execute(s"CREATE USER $username SET PASSWORD '$password' CHANGE NOT REQUIRED SET DEFAULT DATABASE $fooDb")
    execute(s"CREATE ROLE $fooDbRole")
    execute(s"GRANT ROLE $fooDbRole TO $username")
    execute(s"GRANT ALL ON DEFAULT DATABASE TO $fooDbRole")
    execute(s"GRANT ALL ON DEFAULT GRAPH TO $fooDbRole")

    selectDatabase(fooDb)
    execute("CREATE (n:Foo{foo:'foo'}) RETURN n").toList
    selectDatabase(DEFAULT_DATABASE_NAME)
    execute("CREATE (n:Foo{foo:'neo4j'}) RETURN n").toList

    // WHEN
    var (driver, session) = driverExecutor(Some(fooDb))
    try {
      var tx = session.beginTransaction()
      tx.execute("MATCH (n) RETURN n.foo").toSet shouldBe Set(Map("n.foo" -> "foo"))
      tx.commit()

      selectDatabase(SYSTEM_DATABASE_NAME)
      execute(s"ALTER USER $username SET DEFAULT DATABASE $DEFAULT_DATABASE_NAME")

      // THEN
      session.close()
      session = driver.session()
      tx = session.beginTransaction()
      tx.execute("MATCH (n) RETURN n.foo").toSet shouldBe Set(Map("n.foo" -> "foo"))
      tx.commit()
    } finally {
      driver.close()
    }
  }

  ignore("Should get user's default database when logging onto a cluster") {
    // GIVEN
    withCluster(cluster => {
      cluster.systemTx((_, tx) => {
        tx.execute(s"CREATE DATABASE $fooDb WAIT").close()
        tx.execute(s"CREATE USER $username SET PASSWORD '$password' CHANGE NOT REQUIRED SET DEFAULT DATABASE $fooDb").close()
        tx.execute(s"CREATE ROLE $fooDbRole").close()
        tx.execute(s"GRANT ROLE $fooDbRole TO $username").close()
        tx.execute(s"GRANT ALL ON DATABASE $fooDb TO $fooDbRole").close()
        tx.execute(s"GRANT ALL ON GRAPH $fooDb TO $fooDbRole").close()
        tx.commit()
      })

      val work: BiConsumer[GraphDatabaseFacade, org.neo4j.graphdb.Transaction] = (_, tx) => {
        tx.execute("CREATE (n:Foo{foo:'foo'}) RETURN n").close()
        tx.commit()
      }
      cluster.coreTx(fooDb, work)

      val boltAddress = cluster.randomCoreMember(true).get().boltAdvertisedAddress()

      // WHEN
      withDriver(boltAddress, { tx =>
        // THEN
        tx.execute("SHOW CURRENT USER YIELD defaultDatabase").toSet shouldBe
          Set(Map("defaultDatabase" -> fooDb))
      }, Some(SYSTEM_DATABASE_NAME))
      withDriver(boltAddress,
        tx => tx.execute("MATCH (n) RETURN n.foo").toSet shouldBe Set(Map("n.foo" -> "foo")), None)
    })
  }

  ignore("Should get user's default database and permissions when logging onto a cluster using fabric routing") {
    // GIVEN
    withCluster(cluster => {
      cluster.systemTx((_, tx) => {
        tx.execute(s"CREATE DATABASE $fooDb WAIT").close()
        tx.execute(s"CREATE USER $username SET PASSWORD '$password' CHANGE NOT REQUIRED SET DEFAULT DATABASE $fooDb").close()
        tx.execute(s"CREATE ROLE $fooDbRole").close()
        tx.execute(s"GRANT ROLE $fooDbRole TO $username").close()
        tx.execute(s"GRANT ALL ON DEFAULT DATABASE TO $fooDbRole").close()
        tx.execute(s"GRANT ALL ON DEFAULT GRAPH TO $fooDbRole").close()
        tx.commit()
      })

      val work: BiConsumer[GraphDatabaseFacade, org.neo4j.graphdb.Transaction] = (_, tx) => {
        tx.execute("CREATE (n:Foo{foo:'foo'}) RETURN n").close()
        tx.commit()
      }
      cluster.coreTx(fooDb, work)

      val boltAddress = cluster.getMemberWithAnyRole(fooDb, Role.FOLLOWER).boltAdvertisedAddress()

      // WHEN
      withDriver(boltAddress, { tx =>
        // THEN
        tx.execute("SHOW CURRENT USER YIELD defaultDatabase").toSet shouldBe
          Set(Map("defaultDatabase" -> fooDb))
      }, Some(SYSTEM_DATABASE_NAME))
      withDriver(boltAddress, tx => {
        tx.execute("MATCH (n) RETURN n.foo").toSet shouldBe Set(Map("n.foo" -> "foo"))
        tx.execute("CREATE (n{foo:'new'}) RETURN n.foo").toSet shouldBe Set(Map("n.foo" -> "new"))
      }, None)
    })
  }

}
