/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.planning

import org.neo4j.cypher.internal.ast.GraphSelection
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.rendering.QueryRenderer

sealed trait Use {
  def graphSelection: GraphSelection
  def position: InputPosition
}

object Use {

  final case class Default(graphSelection: GraphSelection) extends Use {
    def position: InputPosition = graphSelection.position
  }
  final case class Declared(graphSelection: GraphSelection) extends Use {
    def position: InputPosition = graphSelection.position
  }
  final case class Inherited(use: Use)(pos: InputPosition) extends Use {
    def graphSelection: GraphSelection = use.graphSelection
    def position: InputPosition = pos
  }

  @scala.annotation.tailrec
  def show(use: Use): Any = use match {
    case s: Default  => show(s.graphSelection) + " (transaction default)"
    case d: Declared => show(d.graphSelection)
    case i: Inherited => show(root(i))
  }

  def show(graphSelection: GraphSelection): String =
    QueryRenderer.pretty(graphSelection.expression)

  @scala.annotation.tailrec
  private def root(use: Use): Use = use match {
    case i: Inherited => root(i.use)
    case u            => u
  }
}


