/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.cypher.security

import java.io.IOException

import com.neo4j.bench.common.Neo4jConfigBuilder
import com.neo4j.bench.common.model.Neo4jConfig
import com.neo4j.bench.micro.benchmarks.cypher.AbstractCypherBenchmark
import com.neo4j.bench.micro.data.{DataGeneratorConfig, PropertyDefinition, RelationshipDefinition}
import com.neo4j.server.security.enterprise.auth.EnterpriseAuthAndUserManager
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.graphdb.Label
import org.neo4j.internal.kernel.api.security.LoginContext
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException
import org.neo4j.kernel.api.security.AuthToken
import org.neo4j.kernel.api.security.exception.InvalidAuthTokenException
import org.neo4j.kernel.internal.GraphDatabaseAPI

import scala.collection.mutable

abstract class AbstractSecurityBenchmark extends AbstractCypherBenchmark {

  override def benchmarkGroup = "Security"

  val users: mutable.Map[String, LoginContext] = mutable.Map[String, LoginContext]()
  val neo4jConfig: Neo4jConfig = Neo4jConfigBuilder.empty()
    .withSetting(GraphDatabaseSettings.auth_enabled, "true").build()

  override protected def afterDatabaseStart(config: DataGeneratorConfig): Unit = {
    val authManager = db.asInstanceOf[GraphDatabaseAPI].getDependencyResolver.resolveDependency(classOf[EnterpriseAuthAndUserManager])

    val labels: Array[Label] = config.labels()
    val nodeProperties: Array[PropertyDefinition] = config.nodeProperties()
    val relTypes: Array[RelationshipDefinition] = config.outRelationships()
    val relProperties: Array[PropertyDefinition] = config.relationshipProperties()

    try {
      // Role with explicit privileges to read everything in the graph
      systemDb().executeTransactionally("CREATE ROLE WhiteRole")
      labels.foreach { label =>
        systemDb().executeTransactionally(s"GRANT TRAVERSE ON GRAPH * NODES ${label.name()} TO WhiteRole")
        nodeProperties.foreach(p => systemDb().executeTransactionally(s"GRANT READ {${p.key()}} ON GRAPH * NODES ${label.name()} TO WhiteRole"))
      }

      relTypes.foreach { relDefinition =>
        val relType = relDefinition.`type`().name()
        systemDb().executeTransactionally(s"GRANT TRAVERSE ON GRAPH * RELATIONSHIPS $relType TO WhiteRole")
        relProperties.foreach(p => systemDb().executeTransactionally(s"GRANT READ {${p.key()}} ON GRAPH * RELATIONSHIPS $relType TO WhiteRole"))
      }

      // Role that denies unused graph elements
      systemDb().executeTransactionally("CREATE ROLE BlackRole")
      systemDb().executeTransactionally("DENY TRAVERSE ON GRAPH * ELEMENTS BLACK TO BlackRole")
      systemDb().executeTransactionally("DENY READ {blackProp} ON GRAPH * ELEMENTS BLACK TO BlackRole")

      // User with grants
      systemDb().executeTransactionally("CREATE USER white SET PASSWORD 'abc123' CHANGE NOT REQUIRED")
      systemDb().executeTransactionally("GRANT ROLE WhiteRole TO white")

      // User with grants and denies
      systemDb().executeTransactionally("CREATE USER black SET PASSWORD 'foo' CHANGE NOT REQUIRED")
      systemDb().executeTransactionally("GRANT ROLE WhiteRole TO black")
      systemDb().executeTransactionally("GRANT ROLE BlackRole TO black")

      users += ("white" -> authManager.login(AuthToken.newBasicAuthToken("white", "abc123")))
      users += ("black" -> authManager.login(AuthToken.newBasicAuthToken("black", "foo")))
      users += ("full" -> LoginContext.AUTH_DISABLED)
    } catch {
      case e@(_: IOException | _: InvalidArgumentsException | _: InvalidAuthTokenException) =>
        throw new RuntimeException(e.getMessage, e)
    }
  }
}
