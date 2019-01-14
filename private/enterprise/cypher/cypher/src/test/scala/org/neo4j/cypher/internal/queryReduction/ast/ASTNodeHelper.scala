/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.queryReduction.ast

import org.neo4j.cypher.internal.v3_5.util._

object ASTNodeHelper {

  def getDepth(node: ASTNode): Int = {
    val children = getChildren(node)
    if (children.isEmpty) {
      0
    } else {
      children.map(getDepth).reduce(Math.max) + 1
    }
  }

  def countNodesOnLevel(node: ASTNode, level: Int): Int = {
    val children = getChildren(node)
    if (level == 0) {
      1
    } else {
      children.map(countNodesOnLevel(_, level - 1)).sum
    }
  }

  def getSize(node: ASTNode): Int = {
    getChildren(node).map(getSize).fold(1)(_ + _)
  }

  def forallNodes(node: ASTNode)(predicate: ASTNode => Boolean): Boolean = {
    getChildren(node).map(forallNodes(_)(predicate)).fold(predicate(node))(_ && _)
  }

  def existsNode(node: ASTNode)(predicate: ASTNode => Boolean): Boolean = {
    getChildren(node).map(existsNode(_)(predicate)).fold(predicate(node))(_ || _)
  }

}
