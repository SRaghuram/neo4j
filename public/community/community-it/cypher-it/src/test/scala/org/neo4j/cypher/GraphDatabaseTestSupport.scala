/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher

import java.io.File
import java.nio.file.Files

import org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME
import org.neo4j.cypher.internal.javacompat.GraphDatabaseCypherService
import org.neo4j.cypher.internal.v4_0.util.test_helpers.{CypherFunSuite, CypherTestSupport}
import org.neo4j.dbms.api.DatabaseManagementService
import org.neo4j.graphdb._
import org.neo4j.graphdb.config.Setting
import org.neo4j.internal.kernel.api.TokenRead
import org.neo4j.internal.kernel.api.helpers.Indexes
import org.neo4j.internal.kernel.api.procs._
import org.neo4j.internal.kernel.api.security.LoginContext
import org.neo4j.kernel.GraphDatabaseQueryService
import org.neo4j.kernel.api.KernelTransaction.Type
import org.neo4j.kernel.api.procedure.{CallableProcedure, CallableUserAggregationFunction, CallableUserFunction, GlobalProcedures}
import org.neo4j.kernel.api.{Kernel, KernelTransaction}
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge
import org.neo4j.kernel.impl.coreapi.InternalTransaction
import org.neo4j.logging.{LogProvider, NullLogProvider}
import org.neo4j.monitoring.Monitors
import org.neo4j.test.TestDatabaseManagementServiceBuilder
import org.scalatest.matchers.{MatchResult, Matcher}

import scala.collection.JavaConverters._
import scala.collection.Map

trait GraphDatabaseTestSupport extends CypherTestSupport with GraphIcing {
  self: CypherFunSuite  =>

  var graphOps: GraphDatabaseService = _
  var graph: GraphDatabaseCypherService = _
  var managementService: DatabaseManagementService = _
  var nodes: List[Node] = _
  protected var tx: InternalTransaction = _

  def databaseConfig(): Map[Setting[_],Object] = Map()

  def logProvider: LogProvider = NullLogProvider.getInstance()

  override protected def initTest() {
    super.initTest()
    startGraphDatabase()
  }

  protected def startGraphDatabase(config: Map[Setting[_], Object] = databaseConfig()): Unit = {
    managementService = graphDatabaseFactory(Files.createTempDirectory("test").getParent.toFile).impermanent().setConfig(config.asJava).setInternalLogProvider(logProvider).build()
    graphOps = managementService.database(DEFAULT_DATABASE_NAME)
    graph = new GraphDatabaseCypherService(graphOps)
    onNewGraphDatabase()
  }

  protected def onNewGraphDatabase(): Unit = ()
  protected def onDeletedGraphDatabase(): Unit = ()

  protected def beginTransaction(`type`: KernelTransaction.Type, loginContext: LoginContext): InternalTransaction = {
    if (tx != null) {
      throw new TransactionFailureException("Failed to start a new transaction. Already have an open transaction in `tx` in this test.")
    }
    tx = graph.beginTransaction(`type`, loginContext)
    tx
  }

  // Runs code inside of a transaction. Will mark the transaction as successful if no exception is thrown
  protected def inTestTx[T](f: InternalTransaction => T, txType: Type = Type.`implicit`): T = withTx(f, txType)

  protected def inTestTx[T](f: => T): T = inTestTx(_ => f)

  protected def withTx[T](f: InternalTransaction => T, txType: Type = Type.`implicit`): T = {
    if (tx == null) {
      graph.withTx(f, txType)
    } else {
      // Reuse the open transaction
      // (NOTE: Transaction Type is meaningless after the removal of placebo transaction anyway, so we do not care to check if it differs)
      f(tx)
    }
  }

  protected def startGraphDatabase(storeDir: File): Unit = {
    managementService = graphDatabaseFactory(storeDir).impermanent().build()
    graphOps = managementService.database(DEFAULT_DATABASE_NAME)
    graph = new GraphDatabaseCypherService(graphOps)
  }

  protected final def graphDatabaseFactory(databaseRootDir: File): TestDatabaseManagementServiceBuilder = {
    val factory = createDatabaseFactory(databaseRootDir)
    this match {
      case custom: FakeClock =>
        factory.setClock(custom.clock)
      case _ =>
    }
    factory
  }

  /**
   * Override this method when you need an enterprise ManagementServiceBuilder
   */
  protected def createDatabaseFactory(databaseRootDir: File): TestDatabaseManagementServiceBuilder = new TestDatabaseManagementServiceBuilder(databaseRootDir)

  protected def restartWithConfig(config: Map[Setting[_], Object] = databaseConfig()): Unit = {
    managementService.shutdown()
    startGraphDatabase(config)
  }

  override protected def stopTest() {
    try {
      if (tx != null) {
        tx.close()
        tx = null
      }
      super.stopTest()
    }
    finally {
      if (managementService != null) {
        managementService.shutdown()
      }
      graphOps = null
      graph = null
      managementService = null
      nodes = null
      onDeletedGraphDatabase()
    }
  }

  def assertInTx(f: => Option[String]) {
    inTestTx { f match {
        case Some(error) => fail(error)
        case _           =>
      }
    }
  }

  def resampleIndexes(): Unit = {
    graph.withTx( tx => {
      tx.execute("CALL db.resampleOutdatedIndexes")
      assertWithKernelTx(ktx => Indexes.awaitResampling(ktx.schemaRead(), 300))
    })
  }

  def assertWithKernelTx(f: KernelTransaction => Unit): Unit = {
    val kernel = graph.getDependencyResolver.resolveDependency(classOf[Kernel])
    var ktx: KernelTransaction = null
    try {
      ktx = kernel.beginTransaction( Type.explicit, LoginContext.AUTH_DISABLED )
      f(ktx)
    } finally {
      ktx.commit()
    }
  }

  def tokenReader[T](f: TokenRead => T): T = {
    f(kernelTransaction().tokenRead())
  }

  def kernelTransaction(): KernelTransaction = {
    val bridge = graph.getDependencyResolver.resolveDependency(classOf[ThreadToStatementContextBridge])
    val transaction = bridge.getKernelTransactionBoundToThisThread(true, graph.getGraphDatabaseService.databaseId())
    transaction
  }

  def nodeId(n: Node) = inTestTx {
    n.getId
  }

  def relationshipId(r: Relationship) = inTestTx {
    r.getId
  }

  def labels(n: Node) = inTestTx {
    n.getLabels.iterator().asScala.map(_.toString).toSet
  }

  def countNodes() = graph.withTx(tx => {
    tx.getAllNodes.asScala.size
  })

  def countRelationships() = graph.withTx( tx => {
    tx.getAllRelationships.asScala.size
  })

  def createNode(): Node = createNode(Map[String, Any]())

  def createNode(name: String): Node = createNode(Map[String, Any]("name" -> name))

  def createNode(props: Map[String, Any]): Node = {
    inTestTx { tx =>
      val node = tx.createNode()

      props.foreach((kv) => node.setProperty(kv._1, kv._2))
      node
    }
  }

  def createLabeledNode(props: Map[String, Any], labels: String*): Node = {
    val n = createNode()

    inTestTx {
      labels.foreach {
        name => n.addLabel(Label.label(name))
      }

      props.foreach {
        case (k, v) => n.setProperty(k, v)
      }
    }

    n
  }

  def createLabeledNode(labels: String*): Node = createLabeledNode(Map[String, Any](), labels: _*)

  def createNode(values: (String, Any)*): Node = createNode(values.toMap)

  def deleteAllEntities() = {
    withTx( tx => {
      val relIterator = tx.getAllRelationships.iterator()

      while (relIterator.hasNext) {
        relIterator.next().delete()
      }
      val nodeIterator = tx.getAllNodes.iterator()
      while (nodeIterator.hasNext) {
        nodeIterator.next().delete()
      }
    })

  }

  def nodeIds = nodes.map(_.getId).toArray

  val REL = RelationshipType.withName("REL")

  def relate(a: Node, b: Node): Relationship = relate(a, b, "REL")

  def relate(a: Node, b: Node, pk: (String, Any)*): Relationship = relate(a, b, "REL", pk.toMap)

  def relate(n1: Node, n2: Node, relType: String, name: String): Relationship = relate(n1, n2, relType, Map("name" -> name))

  def relate(a: Node, b: Node, c: Node*) {
    (Seq(a, b) ++ c).reduce((n1, n2) => {
      relate(n1, n2)
      n2
    })
  }

  def relate(n1: Node, n2: Node, relType: String, props: Map[String, Any] = Map()): Relationship = inTestTx {
    val r = n1.createRelationshipTo(n2, RelationshipType.withName(relType))

    props.foreach((kv) => r.setProperty(kv._1, kv._2))
    r
  }

  def relate(x: ((String, String), String)): Relationship = inTestTx {
    x match {
      case ((from, relType), to) => {
        val f = node(from)
        val t = node(to)
        f.createRelationshipTo(t, RelationshipType.withName(relType))
      }
    }
  }

  def node(name: String): Node = {
    nodes.find(_.getProperty("name") == name).get
  }

  def relType(name: String): RelationshipType = graph.withTx( tx => tx.getAllRelationshipTypes.asScala.find(_.name() == name).get )

  def createNodes(names: String*): List[Node] = {
    nodes = names.map(x => createNode(Map("name" -> x))).toList
    nodes
  }

  def createDiamond(): (Node, Node, Node, Node) = {
    //    Graph:
    //             (a)
    //             / \
    //            v   v
    //          (b)  (c)
    //           \   /
    //            v v
    //            (d)

    val a = createNode("a")
    val b = createNode("b")
    val c = createNode("c")
    val d = createNode("d")

    relate(a, b)
    relate(b, d)
    relate(a, c)
    relate(c, d)
    (a, b, c, d)
  }

  def registerProcedure[T <: CallableProcedure](qualifiedName: String)(f: ProcedureSignature.Builder => T): T = {
    val parts = qualifiedName.split('.')
    val namespace = parts.reverse.tail.reverse
    val name = parts.last
    registerProcedure(namespace: _*)(name)(f)
  }

  def registerProcedure[T <: CallableProcedure](namespace: String*)(name: String)(f: ProcedureSignature.Builder => T): T = {
    val builder = ProcedureSignature.procedureSignature(namespace.toArray, name)
    val proc = f(builder)
    kernelAPI.registerProcedure(proc)
    proc
  }

  def registerUserDefinedFunction[T <: CallableUserFunction](qualifiedName: String)(f: UserFunctionSignature.Builder => T): T = {
    val parts = qualifiedName.split('.')
    val namespace = parts.reverse.tail.reverse
    val name = parts.last
    registerUserFunction(namespace: _*)(name)(f)
  }

  def registerUserDefinedAggregationFunction[T <: CallableUserAggregationFunction](qualifiedName: String)(f: UserFunctionSignature.Builder => T): T = {
    val parts = qualifiedName.split('.')
    val namespace = parts.reverse.tail.reverse
    val name = parts.last
    registerUserAggregationFunction(namespace: _*)(name)(f)
  }

  def registerUserFunction[T <: CallableUserFunction](namespace: String*)(name: String)(f: UserFunctionSignature.Builder => T): T = {
    val builder = UserFunctionSignature.functionSignature(namespace.toArray, name)
    val func = f(builder)
    kernelAPI.registerUserFunction(func)
    func
  }

  def registerUserAggregationFunction[T <: CallableUserAggregationFunction](namespace: String*)(name: String)(f: UserFunctionSignature.Builder => T): T = {
    val builder = UserFunctionSignature.functionSignature(namespace.toArray, name)
    val func = f(builder)
    kernelAPI.registerUserAggregationFunction(func)
    func
  }

  def getUserFunctionHandle(qualifiedName: String) = {
    val parts = qualifiedName.split('.')
    val namespace = parts.reverse.tail.reverse
    val name = parts.last
    val procs = graph.getDependencyResolver.resolveDependency(classOf[GlobalProcedures])
    procs.function(new QualifiedName(namespace, name))
  }

  def kernelMonitors: Monitors = graph.getDependencyResolver.resolveDependency(classOf[Monitors])

  private def kernelAPI: Kernel = graph.getDependencyResolver.resolveDependency(classOf[Kernel])

  case class haveConstraints(expectedConstraints: String*) extends Matcher[GraphDatabaseQueryService] {
    def apply(graph: GraphDatabaseQueryService): MatchResult = {
      inTestTx {
        val constraintNames = graph.schema().getConstraints.asScala.toList.map(i => s"${i.getConstraintType}:${i.getLabel}(${i.getPropertyKeys.asScala.toList.mkString(",")})")
        val result = expectedConstraints.forall(i => constraintNames.contains(i.toString))
        MatchResult(
          result,
          s"Expected graph to have constraints ${expectedConstraints.mkString(", ")}, but it was ${constraintNames.mkString(", ")}",
          s"Expected graph to not have constraints ${expectedConstraints.mkString(", ")}, but it did."
        )
      }
    }
  }
}
