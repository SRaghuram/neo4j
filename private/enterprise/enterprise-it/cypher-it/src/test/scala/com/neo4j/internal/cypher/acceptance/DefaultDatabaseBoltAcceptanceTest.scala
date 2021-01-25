/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.internal.cypher.acceptance

import java.net.URI
import java.util.function.BiConsumer

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
import org.neo4j.fabric.FabricDatabaseManager
import org.neo4j.graphdb.config.Setting
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade
import org.neo4j.kernel.internal.GraphDatabaseAPI
import org.neo4j.test.rule.TestDirectory.testDirectory
import org.neo4j.util.FeatureToggles

import scala.collection.JavaConverters.asScalaBufferConverter
import scala.collection.JavaConverters.mapAsJavaMapConverter
import scala.collection.JavaConverters.mapAsScalaMapConverter
import scala.util.Random

class DefaultDatabaseBoltAcceptanceTest extends ExecutionEngineFunSuite with EnterpriseGraphDatabaseTestSupport {

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

  override def initTest(): Unit = {
    super.initTest()
  }

  private def withDriver(username: String, password: String, f: Transaction => Unit, databaseName: Option[String] = None): Unit = {
    withDriver(driverExecutor(username, password, databaseName)._2, f)
  }

  private def withDriver(boltAddress: String, username: String, password: String, f: Transaction => Unit, databaseName: Option[String]) {
    withDriver(driverExecutor(boltAddress, username, password, databaseName)._2, f)
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

  private def driverExecutor(username: String, password: String, databaseName: Option[String]): (Driver, Session) = {
    val config = Config.newBuilder().set(databaseConfig().asJava).build()
    val connectorPortRegister =
      managementService.database(config.get(GraphDatabaseSettings.default_database)).asInstanceOf[GraphDatabaseAPI]
        .getDependencyResolver.resolveDependency(classOf[ConnectorPortRegister])
    if (config.get(BoltConnector.enabled)) {
      driverExecutor(connectorPortRegister.getLocalAddress(BoltConnector.NAME).toString(), username, password, databaseName)
    } else {
      throw new IllegalStateException("Bolt connector is not configured")
    }
  }

  private def driverExecutor(boltAddress: String, username: String, password: String, databaseName: Option[String]): (Driver, Session) = {
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

  Seq[Boolean](false, true).foreach(fabricIsEnabled => {

    test(s"Should get system default database when logging in if no default database is set with fabric: $fabricIsEnabled") {
      FeatureToggles.set(classOf[FabricDatabaseManager], "fabric_by_default", fabricIsEnabled)
      initTest()

      // GIVEN
      selectDatabase(SYSTEM_DATABASE_NAME)
      execute("CREATE USER foo SET PASSWORD 'bar' CHANGE NOT REQUIRED")
      execute("CREATE ROLE foodbrole")
      execute("GRANT ROLE foodbrole to foo")

      // WHEN
      withDriver("foo", "bar", { tx =>
        // THEN
        tx.execute("SHOW CURRENT USER YIELD defaultDatabase").toSet shouldBe
          Set(Map("defaultDatabase" -> DEFAULT_DATABASE_NAME))
      }, Some("system"))
      withDriver("foo", "bar", tx => tx.execute("MATCH (n) RETURN n.foo").toSet shouldBe Set())
    }

    test(s"Should get user's default database when logging in and a default database has been set with fabric: $fabricIsEnabled") {
      FeatureToggles.set(classOf[FabricDatabaseManager], "fabric_by_default", fabricIsEnabled)
      initTest()

      // GIVEN
      selectDatabase(SYSTEM_DATABASE_NAME)
      execute("CREATE DATABASE foodb")
      execute("CREATE USER foo SET PASSWORD 'bar' CHANGE NOT REQUIRED SET DEFAULT DATABASE foodb")
      execute("CREATE ROLE foodbrole")
      execute("GRANT ROLE foodbrole to foo")
      execute("GRANT ALL ON DATABASE foodb to foodbrole")
      execute("GRANT ALL GRAPH PRIVILEGES ON GRAPH foodb to foodbrole")

      selectDatabase("foodb")
      execute("CREATE (n:Foo{foo:'foo'}) RETURN n").toList

      // WHEN
      withDriver("foo", "bar", { tx =>
        // THEN
        tx.execute("SHOW CURRENT USER YIELD defaultDatabase").toSet shouldBe
          Set(Map("defaultDatabase" -> "foodb"))
      }, Some("system"))
      withDriver("foo", "bar", tx => tx.execute("MATCH (n) RETURN n.foo").toSet shouldBe Set(Map("n.foo" -> "foo")))
    }

    test(s"Should fail when logging in and a default database has been set but is stopped with fabric: $fabricIsEnabled") {
      FeatureToggles.set(classOf[FabricDatabaseManager], "fabric_by_default", fabricIsEnabled)
      initTest()

      // GIVEN
      selectDatabase(SYSTEM_DATABASE_NAME)
      execute("CREATE DATABASE foodb")
      execute("CREATE USER foo SET PASSWORD 'bar' CHANGE NOT REQUIRED SET DEFAULT DATABASE foodb")
      execute("STOP DATABASE foodb WAIT")

      selectDatabase("neo4j")
      execute("CREATE (n:Foo{foo:'foo'}) RETURN n").toList

      // WHEN
      withDriver("foo", "bar", { tx =>
        // THEN
        tx.execute("SHOW CURRENT USER YIELD defaultDatabase").toSet shouldBe
          Set(Map("defaultDatabase" -> "foodb"))
      }, Some("system"))
      the[TransientException] thrownBy {
        withDriver("foo", "bar", tx => tx.execute("MATCH (n) RETURN n.foo").toSet shouldBe Set(Map("n.foo" -> "foo")))
      } should have message "Database 'foodb' is unavailable."
    }

    test(s"Should fail when logging in and a default database has been set but not created with fabric: $fabricIsEnabled") {
      FeatureToggles.set(classOf[FabricDatabaseManager], "fabric_by_default", fabricIsEnabled)
      initTest()

      // GIVEN
      selectDatabase(SYSTEM_DATABASE_NAME)
      execute("CREATE USER foo SET PASSWORD 'bar' CHANGE NOT REQUIRED SET DEFAULT DATABASE foodb")

      // WHEN
      withDriver("foo", "bar", { tx =>
        // THEN
        tx.execute("SHOW CURRENT USER YIELD defaultDatabase").toSet shouldBe
          Set(Map("defaultDatabase" -> "foodb"))
      }, Some("system"))
      the[FatalDiscoveryException] thrownBy {
        withDriver("foo", "bar", tx => tx.execute("MATCH (n) RETURN n.foo").toSet shouldBe Set())
      } should have message "Database does not exist. Database name: 'foodb'."
    }
  })

  test("updating default database during a user session does not change the default database for that session") {
    initTest()

    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE DATABASE foodb")
    execute("CREATE USER foo SET PASSWORD 'bar' CHANGE NOT REQUIRED SET DEFAULT DATABASE foodb")
    execute("CREATE ROLE foodbrole")
    execute("GRANT ROLE foodbrole to foo")
    execute("GRANT ALL ON DEFAULT DATABASE to foodbrole")
    execute("GRANT ALL GRAPH PRIVILEGES ON DEFAULT GRAPH to foodbrole")

    selectDatabase("foodb")
    execute("CREATE (n:Foo{foo:'foo'}) RETURN n").toList
    selectDatabase("neo4j")
    execute("CREATE (n:Foo{foo:'neo4j'}) RETURN n").toList

    // WHEN
    val (driver, session) = driverExecutor("foo", "bar", None)
    try {
      var tx = session.beginTransaction()
      tx.execute("MATCH (n) RETURN n.foo").toSet shouldBe Set(Map("n.foo" -> "foo"))
      tx.commit()

      selectDatabase(SYSTEM_DATABASE_NAME)
      execute(s"ALTER USER foo SET DEFAULT DATABASE $DEFAULT_DATABASE_NAME")

      // THEN
      tx = session.beginTransaction()
      tx.execute("MATCH (n) RETURN n.foo").toSet shouldBe Set(Map("n.foo" -> "foo"))
      tx.commit()
    } finally {
      session.close()
      driver.close()
    }
  }

  test("updating default database during a user session where the default database has not previously been used does not changed it for that session")
  {
    initTest()

    // GIVEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute("CREATE DATABASE foodb")
    execute("CREATE USER foo SET PASSWORD 'bar' CHANGE NOT REQUIRED SET DEFAULT DATABASE foodb")
    execute("CREATE ROLE foodbrole")
    execute("GRANT ROLE foodbrole to foo")
    execute("GRANT ALL ON DEFAULT DATABASE to foodbrole")
    execute("GRANT ALL GRAPH PRIVILEGES ON DEFAULT GRAPH to foodbrole")

    selectDatabase("foodb")
    execute("CREATE (n:Foo{foo:'foo'}) RETURN n").toList
    selectDatabase("neo4j")
    execute("CREATE (n:Foo{foo:'neo4j'}) RETURN n").toList

    // WHEN
    var (driver, session) = driverExecutor("foo", "bar", Some("foodb"))
    try {
      var tx = session.beginTransaction()
      tx.execute("MATCH (n) RETURN n.foo").toSet shouldBe Set(Map("n.foo" -> "foo"))
      tx.commit()

      selectDatabase(SYSTEM_DATABASE_NAME)
      execute(s"ALTER USER foo SET DEFAULT DATABASE $DEFAULT_DATABASE_NAME")

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

  test(s"Should get user's default database when logging onto a cluster") {
    // GIVEN
    withCluster(cluster => {
      cluster.systemTx((_, tx) => {
        tx.execute("CREATE DATABASE foodb WAIT").close()
        tx.execute("CREATE USER foo SET PASSWORD 'bar' CHANGE NOT REQUIRED SET DEFAULT DATABASE foodb").close()
        tx.execute("CREATE ROLE foodbrole").close()
        tx.execute("GRANT ROLE foodbrole to foo").close()
        tx.execute("GRANT ALL ON DATABASE foodb to foodbrole").close()
        tx.execute("GRANT ALL GRAPH PRIVILEGES ON GRAPH foodb to foodbrole").close()
        tx.commit()
      })

      val work: BiConsumer[GraphDatabaseFacade, org.neo4j.graphdb.Transaction] = (_, tx) => {
        tx.execute("CREATE (n:Foo{foo:'foo'}) RETURN n").close()
        tx.commit()
      }
      cluster.coreTx("foodb", work)

      val boltAddress = cluster.randomCoreMember(true).get().boltAdvertisedAddress()

      // WHEN
      withDriver(boltAddress, "foo", "bar", { tx =>
        // THEN
        tx.execute("SHOW CURRENT USER YIELD defaultDatabase").toSet shouldBe
          Set(Map("defaultDatabase" -> "foodb"))
      }, Some("system"))
      withDriver(boltAddress, "foo", "bar",
        tx => tx.execute("MATCH (n) RETURN n.foo").toSet shouldBe Set(Map("n.foo" -> "foo")), None)

    })
  }

  test(s"Should get user's default database and permissions when logging onto a cluster using fabric routing") {
    // GIVEN
    withCluster(cluster => {
      cluster.systemTx((_, tx) => {
        tx.execute("CREATE DATABASE foodb WAIT").close()
        tx.execute("CREATE USER foo SET PASSWORD 'bar' CHANGE NOT REQUIRED SET DEFAULT DATABASE foodb").close()
        tx.execute("CREATE ROLE foodbrole").close()
        tx.execute("GRANT ROLE foodbrole to foo").close()
        tx.execute("GRANT ALL ON DEFAULT DATABASE to foodbrole").close()
        tx.execute("GRANT ALL GRAPH PRIVILEGES ON DEFAULT GRAPH to foodbrole").close()
        tx.commit()
      })

      val work: BiConsumer[GraphDatabaseFacade, org.neo4j.graphdb.Transaction] = (_, tx) => {
        tx.execute("CREATE (n:Foo{foo:'foo'}) RETURN n").close()
        tx.commit()
      }
      cluster.coreTx("foodb", work)

      val boltAddress = cluster.getMemberWithAnyRole("foodb", Role.FOLLOWER).boltAdvertisedAddress()

      // WHEN
      withDriver(boltAddress, "foo", "bar", { tx =>
        // THEN
        tx.execute("SHOW CURRENT USER YIELD defaultDatabase").toSet shouldBe
          Set(Map("defaultDatabase" -> "foodb"))
      }, Some("system"))
      withDriver(boltAddress, "foo", "bar", tx => {
        tx.execute("MATCH (n) RETURN n.foo").toSet shouldBe Set(Map("n.foo" -> "foo"))
        tx.execute("CREATE (n{foo:'new'}) RETURN n.foo").toSet shouldBe Set(Map("n.foo" -> "new"))
      }, None)
    })
  }

}
