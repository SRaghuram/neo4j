/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.planning

import org.neo4j.cypher.internal.ast

sealed trait Use {
  def graphSelection: ast.GraphSelection
}

object Use {

  final case class Declared(graphSelection: ast.GraphSelection) extends Use
  final case class Inherited(use: Use) extends Use {
    def graphSelection: ast.GraphSelection = use.graphSelection
  }
}


