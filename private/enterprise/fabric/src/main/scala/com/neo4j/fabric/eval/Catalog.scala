/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.eval

import com.neo4j.fabric.config.FabricConfig
import com.neo4j.fabric.util.Errors
import com.neo4j.fabric.util.Errors.show
import org.neo4j.cypher.internal.v4_0.ast.CatalogName
import org.neo4j.cypher.internal.v4_0.util.InputPosition
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.IntegralValue

import scala.collection.JavaConverters._

object Catalog {

  sealed trait Entry

  sealed trait Graph extends Entry

  case class RemoteGraph(graph: FabricConfig.Graph) extends Graph

  trait View extends Entry {
    val arity: Int
    val signature: Seq[Arg[_]]

    def eval(args: Seq[AnyValue]): Graph

    def checkArity(args: Seq[AnyValue]): Unit =
      if (args.size != arity) Errors.wrongArity(arity, args.size, InputPosition.NONE)

    def cast[T <: AnyValue](a: Arg[T], v: AnyValue, args: Seq[AnyValue]): T =
      try a.tpe.cast(v)
      catch {
        case e: ClassCastException => Errors.wrongType(show(signature), show(args))
      }
  }

  case class View1[A1 <: AnyValue](a1: Arg[A1])(f: A1 => Graph) extends View {
    val arity = 1
    val signature = Seq(a1)

    def eval(args: Seq[AnyValue]): Graph = {
      checkArity(args)
      f(cast(a1, args(0), args))
    }
  }

  case class Arg[T <: AnyValue](name: String, tpe: Class[T])

  val empty: Catalog = Catalog(Map.empty)

  def merge(a: Catalog, b: Catalog): Catalog = Catalog(a.entries ++ b.entries)

  def fromConfig(config: FabricConfig): Catalog =
    fromDatabase(config.getDatabase)

  private def fromDatabase(database: FabricConfig.Database): Catalog = {

    val name = database.getName.name
    val direct = database.getGraphs.asScala
      .map(fromGraph(name, _))
      .foldLeft(empty)(merge)

    val functions = Catalog(Map(
      CatalogName(name, "graph") -> View1(Arg("gid", classOf[IntegralValue]))(sid =>
        direct.resolve(graphName(name, sid.longValue()), Seq()))
    ))

    direct ++ functions
  }

  private def fromGraph(database: String, graph: FabricConfig.Graph): Catalog = {
    val remoteGraph = RemoteGraph(graph)
    val mapping = Map(graphName(database, graph.getId) -> remoteGraph)

    val mappingWithGraphName =
      if (graph.getName != null) {
        mapping + (CatalogName(database, graph.getName) -> remoteGraph)
      } else {
        mapping
      }

    Catalog(mappingWithGraphName)
  }

  private def graphName(database: String, sid: Long) =
    CatalogName(database, "graph" + sid)

}

case class Catalog(entries: Map[CatalogName, Catalog.Entry]) {

  def resolve(name: CatalogName): Catalog.Graph =
    resolve(name, Seq())

  def resolve(name: CatalogName, args: Seq[AnyValue]): Catalog.Graph = {
    entries.get(name) match {
      case None => Errors.entityNotFound("Catalog entry", show(name))

      case Some(g: Catalog.Graph) =>
        if (args.nonEmpty) Errors.wrongArity(0, args.size, InputPosition.NONE)
        else g

      case Some(v: Catalog.View) => v.eval(args)
    }
  }

  def ++(that: Catalog): Catalog = Catalog.merge(this, that)
}
