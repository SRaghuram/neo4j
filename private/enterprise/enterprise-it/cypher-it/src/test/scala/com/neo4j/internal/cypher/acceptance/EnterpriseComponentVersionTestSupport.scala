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

  val VERSION_40: String = "Neo4j 4.0"
  val VERSION_41D1: String = "Neo4j 4.1.0-Drop01"
  val VERSION_41: String = "Neo4j 4.1"
  val CURRENT_VERSION: String = VERSION_41
  val allSystemGraphVersions: Array[String] = Array(VERSION_40, VERSION_41D1, VERSION_41)
  var _configSupplier: () => Config = () => Config.defaults()
  var _version: Option[String] = None
  var _expectToFailWith: Option[Class[_]] = None
  var _verbose: Boolean = false

  private def setVersion(version: Option[String], expectToFailWith: Option[Class[_]] = None, verbose: Boolean = false): Unit = {
    _version = version
    _expectToFailWith = expectToFailWith
    _verbose = verbose
  }

  def withVersion(version: String)(block: => Any): Unit = {
    setVersion(Some(version))
    block
    setVersion(None)
  }

  def unsupportedWhenNotLatest(version: String): Option[Class[_]] = if (version == CURRENT_VERSION) None else Some(classOf[UnsupportedOperationException])

  val allSupported: String => Option[Class[_]] = _ => None

  def withVersions(versions: String*)(expectToFail: String => Option[Class[_]])(block: => Any): Unit = {
    versions.foreach { version =>
      setVersion(Some(version), expectToFail(version))
      block
      setVersion(None, None)
    }
  }

  def withAllSystemGraphVersions(expectToFail: String => Option[Class[_]])(block: => Any): Unit = withVersions(allSystemGraphVersions: _*)(expectToFail)(block)

  def translatePrivileges(privileges: Set[Map[String, AnyRef]], version: String): Set[Map[String, AnyRef]] = version match {
    case "Neo4j 4.0" => translatePrivilegesTo40(privileges)
    case "Neo4j 4.1" => translatePrivilegesTo41(privileges)
    case "Neo4j 4.1.0-Drop01" => translatePrivilegesTo41(privileges)
    case _ => throw new IllegalArgumentException(s"Unsupported version: $version")
  }

  def makeCombinedFrom(acc: Set[Map[String, AnyRef]], m: Map[String, AnyRef], combined: String, other: String): Set[Map[String, AnyRef]] = {
    val read: Option[Map[String, AnyRef]] = acc.find(p => p("action") == other && p("segment") == m("segment"))
    acc + read.map(r => r + ("action" -> combined)).getOrElse(m)
    if (read.isDefined) {
      acc - read.get + (m + ("action" -> combined))
    } else {
      acc + m
    }
  }

  def translatePrivilegesTo41(privileges: Set[Map[String, AnyRef]]): Set[Map[String, AnyRef]] = privileges.foldLeft(Set.empty[Map[String, AnyRef]]) {
    case (acc, m: Map[String, AnyRef]) if m("action") == "schema" => acc ++ Set(m + ("action" -> "index"), m + ("action" -> "constraint"))
    case (acc, m: Map[String, AnyRef]) if m("action") == "write" => acc + (m + ("resource" -> "graph"))
    case (acc, m: Map[String, AnyRef]) if m("action") == "traverse" => makeCombinedFrom(acc, m + ("resource" -> "all_properties"), "match", "read")
    case (acc, m: Map[String, AnyRef]) if m("action") == "read" => makeCombinedFrom(acc, m, "match", "traverse")
    case (acc, x) => acc + x
  }

  def translatePrivilegesTo40(privileges: Set[Map[String, AnyRef]]): Set[Map[String, AnyRef]] = privileges.foldLeft(Set.empty[Map[String, AnyRef]]) {
    case (acc, m: Map[String, AnyRef]) if m("action") == "index" => makeCombinedFrom(acc, m, "schema", "constraint")
    case (acc, m: Map[String, AnyRef]) if m("action") == "constraint" => makeCombinedFrom(acc, m, "schema", "index")
    case (acc, m: Map[String, AnyRef]) if m("action") == "write" => acc + (m + ("resource" -> "all_properties"))
    case (acc, m: Map[String, AnyRef]) if m("action") == "match" => acc ++ Set(m + ("action" -> "traverse", "resource" -> "graph"), m + ("action" -> "read"))
    case (acc, x) => acc + x
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
    val versionToTest = _version.getOrElse(CURRENT_VERSION)
    val expectToFailWith = _expectToFailWith
    val verbose = _verbose
    val longerName = (versionToTest, expectToFailWith) match {
      case (CURRENT_VERSION, _) => testName
      case (_, None) => s"[$versionToTest] $testName"
      case (_, Some(_)) => s"[FAIL: $versionToTest] $testName"
    }
    super.test(longerName, testTags: _*) {
      initializeEnterpriseSecurityGraphComponent(_configSupplier(), versionToTest, verbose)
      if (expectToFailWith.isDefined) {
        try {
          testFun
          fail(s"Expected exception to be thrown, but none was for '$longerName'")
        } catch {
          case e: Exception if e.getClass.isAssignableFrom(expectToFailWith.get) =>
            if(verbose) println("Got expected exception: " + e)
          case x =>
            fail(s"Expected to get exception ${expectToFailWith.get}, but instead got: " + x)
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
    if (verbose) println("Initializing version: " + version)
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
