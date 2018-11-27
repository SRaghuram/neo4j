/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.queryReduction

import org.neo4j.cypher.internal.queryReduction.ast.ASTNodeHelper._
import org.neo4j.cypher.internal.v3_5.ast._

class StatementGTRInput(initialStatement: Statement) extends GTRInput[Statement](initialStatement) {
  override def depth: Int = getDepth(currentTree)
  override def size: Int = getSize(currentTree)
  override def getDDInput(level: Int) = StatementLevelDDInput(currentTree, level)
  override def getBTInput(level: Int) = StatementLevelBTInput(currentTree, level)
}
