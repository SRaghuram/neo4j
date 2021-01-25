/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.internal.cypher.acceptance

import java.nio.file.Path
import java.util

import com.neo4j.dbms.EnterpriseSystemGraphComponent
import com.neo4j.server.security.enterprise.auth.InMemoryRoleRepository
import com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles
import com.neo4j.server.security.enterprise.systemgraph.EnterpriseSecurityGraphComponent
import com.neo4j.server.security.enterprise.systemgraph.EnterpriseSecurityGraphComponentVersion
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

  val CURRENT_VERSION: EnterpriseSecurityGraphComponentVersion = EnterpriseSecurityGraphComponentVersion.
    values()(EnterpriseSecurityGraphComponentVersion.LATEST_ENTERPRISE_SECURITY_COMPONENT_VERSION)
  val allSystemGraphVersions: Array[EnterpriseSecurityGraphComponentVersion] = Array(
    EnterpriseSecurityGraphComponentVersion.ENTERPRISE_SECURITY_40,
    EnterpriseSecurityGraphComponentVersion.ENTERPRISE_SECURITY_41D1,
    EnterpriseSecurityGraphComponentVersion.ENTERPRISE_SECURITY_41,
    EnterpriseSecurityGraphComponentVersion.ENTERPRISE_SECURITY_42D4,
    EnterpriseSecurityGraphComponentVersion.ENTERPRISE_SECURITY_42D6,
    EnterpriseSecurityGraphComponentVersion.ENTERPRISE_SECURITY_42D7,
    EnterpriseSecurityGraphComponentVersion.ENTERPRISE_SECURITY_42P1,
    EnterpriseSecurityGraphComponentVersion.ENTERPRISE_SECURITY_43D1)
  var _configSupplier: () => Config = () => Config.defaults()
  var _version: Option[EnterpriseSecurityGraphComponentVersion] = None
  var _expectToFailWith: Option[Class[_]] = None
  var _verbose: Boolean = false

  private def setVersion(version: Option[EnterpriseSecurityGraphComponentVersion], expectToFailWith: Option[Class[_]] = None, verbose: Boolean = false): Unit = {
    _version = version
    _expectToFailWith = expectToFailWith
    _verbose = verbose
  }

  def withVersion(version: EnterpriseSecurityGraphComponentVersion)(block: => Any): Unit = {
    setVersion(Some(version))
    block
    setVersion(None)
  }

  def unsupportedBefore41(version: EnterpriseSecurityGraphComponentVersion): Option[Class[_]] = if (version == EnterpriseSecurityGraphComponentVersion.ENTERPRISE_SECURITY_40 ||
                                                                   version == EnterpriseSecurityGraphComponentVersion.ENTERPRISE_SECURITY_41D1)
    Some(classOf[UnsupportedOperationException]) else None

  val allSupported: EnterpriseSecurityGraphComponentVersion => Option[Class[_]] = _ => None

  def withVersions(versions: EnterpriseSecurityGraphComponentVersion*)(expectToFail: EnterpriseSecurityGraphComponentVersion => Option[Class[_]])(block: => Any): Unit = {
    versions.foreach { version =>
      setVersion(Some(version), expectToFail(version))
      block
      setVersion(None, None)
    }
  }

  def withAllSystemGraphVersions(expectToFail: EnterpriseSecurityGraphComponentVersion => Option[Class[_]])(block: => Any): Unit = withVersions(allSystemGraphVersions: _*)(expectToFail)(block)

  def defaultAdminPrivilegesFor(replace: String, version: EnterpriseSecurityGraphComponentVersion): Set[Map[String, AnyRef]] = {
    val adminPrivileges = version match {
      case EnterpriseSecurityGraphComponentVersion.ENTERPRISE_SECURITY_40 => Set(
        granted(access).role("admin").map,
        granted(read).role("admin").node("*").map,
        granted(traverse).role("admin").node("*").map,
        granted(read).role("admin").relationship("*").map,
        granted(traverse).role("admin").relationship("*").map,
        granted(write + ("resource" -> "all_properties")).role("admin").node("*").map,
        granted(write + ("resource" -> "all_properties")).role("admin").relationship("*").map,
        granted(nameManagement).role("admin").map,
        granted(adminAction("schema")).role("admin").map,
        granted(adminAction("admin")).role("admin").map
      )
      case EnterpriseSecurityGraphComponentVersion.ENTERPRISE_SECURITY_41D1 | EnterpriseSecurityGraphComponentVersion.ENTERPRISE_SECURITY_41 |
           EnterpriseSecurityGraphComponentVersion.ENTERPRISE_SECURITY_42D4 | EnterpriseSecurityGraphComponentVersion.ENTERPRISE_SECURITY_42D6 |
           EnterpriseSecurityGraphComponentVersion.ENTERPRISE_SECURITY_42D7 | EnterpriseSecurityGraphComponentVersion.ENTERPRISE_SECURITY_42P1 => Set(
        granted(access).role("admin").map,
        granted(matchPrivilege).role("admin").node("*").map,
        granted(matchPrivilege).role("admin").relationship("*").map,
        granted(write).role("admin").node("*").map,
        granted(write).role("admin").relationship("*").map,
        granted(nameManagement).role("admin").map,
        granted(indexManagement).role("admin").map,
        granted(constraintManagement).role("admin").map,
        granted(adminAction("admin")).role("admin").map
      )
      case EnterpriseSecurityGraphComponentVersion.ENTERPRISE_SECURITY_43D1 => defaultRolePrivileges
      case _            => throw new IllegalArgumentException(s"Unsupported version: $version")
    }
    adminPrivileges.foldLeft(Set.empty[Map[String, AnyRef]]) {
      case (acc, row) if row("role") == "admin" =>
        acc + row.map {
          case (k, _) if k == "role" => (k, replace)
          case (k, v) => (k, v)
        }
      case (acc, _) => acc
    }
  }

  override protected def createDatabaseFactory(databaseRootDir: Path): TestDatabaseManagementServiceBuilder = {
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

  private class TestDBMSBuilder(rootDir: Path) extends TestEnterpriseDatabaseManagementServiceBuilder(rootDir) {
    lazy val getConfig: Config = config.fromConfig(fromConfig).set(GraphDatabaseSettings.neo4j_home, homeDirectory.toAbsolutePath).build

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
          case x: Throwable =>
            fail(s"Expected to get exception ${expectToFailWith.get}, but instead got: " + x.getMessage, x)
        }
      } else {
        testFun
      }
    }
  }

  override def initializeEnterpriseSecurityGraphComponent(config: Config): Unit = {
    // Disable normal setup so each test can initialize with different versions
  }

  def initializeEnterpriseSecurityGraphComponent(config: Config, version: EnterpriseSecurityGraphComponentVersion, verbose: Boolean): Unit = {
    doInitializeEnterpriseSecurityGraphComponent(config, version, verbose)
  }

  def doInitializeEnterpriseSecurityGraphComponent(config: Config, version: EnterpriseSecurityGraphComponentVersion, verbose: Boolean): Unit = {
    if (verbose) println("Initializing version: " + version)
    config.set(GraphDatabaseSettings.allow_single_automatic_upgrade, java.lang.Boolean.FALSE)
    val userRepository = new InMemoryUserRepository
    val roleRepository = new InMemoryRoleRepository
    val enterpriseComponent = new EnterpriseSecurityGraphComponent(mock[Log], roleRepository, userRepository, config)
    initializeSystemGraphConstraints(enterpriseComponent)
    val builder = enterpriseComponent.findSecurityGraphComponentVersion(version.getDescription)
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

  def initializeSystemGraphConstraints(enterpriseSecurityGraphComponent: EnterpriseSecurityGraphComponent): Unit = {
    val tx = graphOps.beginTx()
    try {
      enterpriseSecurityGraphComponent.initializeSystemGraphConstraints(tx)
      tx.commit()
    } finally {
      if (tx != null) tx.close()
    }
  }
}
