/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.planning

import org.neo4j.cypher.internal.ast

sealed trait StatementType

object StatementType {

  case object Query extends StatementType {
    override def toString: String = "Query"
  }
  case object SchemaCommand extends StatementType {
    override def toString: String = "Schema modification"
  }
  case object AdminCommand extends StatementType {
    override def toString: String = "Administration command"
  }

  // Java access helpers
  val QUERY: StatementType = Query
  val SCHEMA_COMMAND: StatementType = SchemaCommand
  val ADMIN_COMMAND: StatementType = AdminCommand

  def of(statement: ast.Statement): StatementType =
    statement match {
      case _: ast.Query => Query
      case _: ast.Command => SchemaCommand
      case _: ast.AdministrationCommand => AdminCommand
    }
}
