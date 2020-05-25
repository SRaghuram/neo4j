/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.internal.cypher.acceptance

import java.io.File
import java.util

import com.neo4j.dbms.EnterpriseSystemGraphComponent
import com.neo4j.server.security.enterprise.auth.InMemoryRoleRepository
import com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles
import com.neo4j.server.security.enterprise.systemgraph.EnterpriseSecurityGraphComponent
import com.neo4j.test.TestEnterpriseDatabaseManagementServiceBuilder
import org.neo4j.collection.Dependencies
import org.neo4j.configuration.Config
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.dbms.api.DatabaseManagementService
import org.neo4j.dbms.database.SystemGraphComponent
import org.neo4j.dbms.database.SystemGraphComponents
import org.neo4j.logging.Log
import org.neo4j.server.security.auth.InMemoryUserRepository
import org.neo4j.server.security.systemgraph.UserSecurityGraphComponent
import org.neo4j.test.TestDatabaseManagementServiceBuilder
import org.scalactic.source
import org.scalatest.FunSuiteLike
import org.scalatest.Tag
import org.scalatest.mockito.MockitoSugar

trait EnterpriseComponentVersionTestSupport extends MockitoSugar with FunSuiteLike {
  self: AdministrationCommandAcceptanceTestBase =>

  val VERSION_40 = "Neo4j 4.0"
  val VERSION_41D1 = "Neo4j 4.1.0-Drop01"
  val VERSION_41 = "Neo4j 4.1"
  var _configSupplier: () => Config = () => Config.defaults()
  var _version: String = VERSION_41
  var _expectToFail: Option[Class[_]] = None
  var _verbose = false

  def setVersion(version: String, expectToFail: Option[Class[_]] = None, verbose: Boolean = false): Unit = {
    _version = version
    _expectToFail = expectToFail
    _verbose = verbose
  }

  def withVersion(version: String)(block: => Any): Unit = {
    setVersion(version)
    block
    setVersion(VERSION_41)
  }

  override protected def createDatabaseFactory(databaseRootDir: File): TestDatabaseManagementServiceBuilder = {
    val builder = new TestDBMSBuilder(databaseRootDir)
    _configSupplier = ()  => builder.getConfig
    builder
  }

  class TestSystemGraphComponents(val components: SystemGraphComponent*) extends SystemGraphComponents {
    for (component <- components) {
      super.register(component)
    }

    override def register(component: SystemGraphComponent): Unit = {
      // Do nothing in tests
    }
  }

  private class TestDBMSBuilder(rootDir: File) extends TestEnterpriseDatabaseManagementServiceBuilder(rootDir) {
    lazy val getConfig: Config = config.fromConfig(fromConfig).set(GraphDatabaseSettings.neo4j_home, homeDirectory.toPath.toAbsolutePath).build

    override def build: DatabaseManagementService = {
      val cfg = getConfig
      val userRepository = new InMemoryUserRepository
      val securityLog = mock[Log]
      val communityComponent = new UserSecurityGraphComponent(securityLog, userRepository, userRepository, cfg)
      val testSystemGraphComponents = new TestSystemGraphComponents(new EnterpriseSystemGraphComponent(cfg), communityComponent)
      // We explicitly do not add enterpriseComponents to the component initializer, so we can initialize per test class
      val deps = new Dependencies(dependencies)
      deps.satisfyDependencies(testSystemGraphComponents)
      dependencies = deps
      newDatabaseManagementService(cfg, databaseDependencies)
    }
  }

  override protected def test(testName: String, testTags: Tag*)(testFun: => Any /* Assertion */)(implicit pos: source.Position): Unit = {
    val versionToTest = _version
    val expectToFail = _expectToFail
    val verbose = _verbose
    val longerName = (versionToTest, expectToFail) match {
      case (VERSION_41, _) => testName
      case (_, None) => s"[${versionToTest}] $testName"
      case (_, Some(_)) => s"[FAIL: ${versionToTest}] $testName"
    }
    super.test(longerName, testTags: _*) {
      initializeEnterpriseSecurityGraphComponent(_configSupplier(), versionToTest, verbose)
      if (expectToFail.isDefined) {
        try {
          testFun
          fail(s"Expected exception to be thrown, but none was for '$longerName'")
        } catch {
          case e: Exception if e.getClass.isAssignableFrom(expectToFail.get) =>
            if(verbose) println("Got expected exception: " + e)
          case x =>
            fail(s"Expected to get exception ${expectToFail.get}, but instead got: " + x)
        }
      } else {
        testFun
      }
    }
  }

  override def initializeEnterpriseSecurityGraphComponent(config: Config): Unit = {
    // Disable normal setup so each test can initialize with different versions
  }

  def initializeEnterpriseSecurityGraphComponent(config: Config, version: String, verbose: Boolean): Unit = {
    doInitializeEnterpriseSecurityGraphComponent(config, version, verbose)
  }

  def doInitializeEnterpriseSecurityGraphComponent(config: Config, version: String, verbose: Boolean): Unit = {
    if (true || verbose) println("Initializing version: " + version)
    config.set(GraphDatabaseSettings.allow_single_automatic_upgrade, java.lang.Boolean.FALSE)
    val userRepository = new InMemoryUserRepository
    val roleRepository = new InMemoryRoleRepository
    val enterpriseComponent = new EnterpriseSecurityGraphComponent(mock[Log], roleRepository, userRepository, config)
    val builder = enterpriseComponent.findSecurityGraphComponentVersion(version)
    val tx = graphOps.beginTx
    try {
      val users = new util.HashSet[String]()
      users.add("neo4j")
      builder.initializePrivileges(tx, PredefinedRoles.roles, util.Map.of(PredefinedRoles.ADMIN, users))
      tx.commit()
    } finally {
      if (tx != null) tx.close()
    }
  }
}
