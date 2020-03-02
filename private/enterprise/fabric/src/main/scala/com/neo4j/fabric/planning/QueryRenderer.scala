/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.planning

import org.neo4j.cypher.internal.ast.Clause
import org.neo4j.cypher.internal.ast.Query
import org.neo4j.cypher.internal.ast.SingleQuery
import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.ast.prettifier.ExpressionStringifier
import org.neo4j.cypher.internal.ast.prettifier.Prettifier
import org.neo4j.cypher.internal.util.InputPosition

object QueryRenderer {

  private val renderer = Prettifier(ExpressionStringifier(alwaysParens = true, alwaysBacktick = true))
  private val pos = InputPosition.NONE

  def render(clauses: Seq[Clause]): String =
    render(Query(None, SingleQuery(clauses)(pos))(pos))

  def render(statement: Statement): String =
    renderer.asString(statement)
}
