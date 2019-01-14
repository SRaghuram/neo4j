/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.queryReduction

import org.neo4j.cypher.internal.queryReduction.DDmin.Oracle

import scala.collection.mutable

object GTR {

  def apply[I](input: GTRInput[I])(test: Oracle[I]): I = {
    for(i <- 1 to input.depth) {
      // ddmin
      val ddLevelInput = input.getDDInput(i)
      val newDDTree = DDmin(ddLevelInput)(test)
      input.updateTree(newDDTree)
      // bt
      val btLevelInput = input.getBTInput(i)
      val newBTTree = BT(btLevelInput)(test)
      input.updateTree(newBTTree)
    }
    input.currentTree
  }
}

object GTRStar {

  private val cache = mutable.Map[Any, OracleResult]()

  def apply[I](input: GTRInput[I])(test: Oracle[I]): I = {
    def runWithCache(i: I): OracleResult = {
      val key = i
      if (cache.contains(key)) {
        cache(key)
      } else {
        // No cached value available
        val res = test(i)
        // Cache the result
        cache(key) = res
        res
      }
    }

    cache.clear()

    var nbNodesBefore = 0
    var nbNodesAfter = input.size

    do {
      nbNodesBefore = nbNodesAfter
      GTR(input)(runWithCache)
      nbNodesAfter = input.size
    } while(nbNodesAfter < nbNodesBefore)

    input.currentTree
  }
}


abstract class GTRInput[I](initialTree: I) {
  var currentTree: I = initialTree

  def updateTree(tree: I): Unit = {
    currentTree = tree
  }

  def depth : Int
  def size : Int
  def getDDInput(level: Int): DDInput[I]
  def getBTInput(level: Int): BTInput[I, _]
}
