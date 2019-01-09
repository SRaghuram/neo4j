/*
 * Copyright (c) 2002-2018 "Neo4j,"
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

import java.util.concurrent.{Callable, Executors}

import org.neo4j.graphdb.Node

class CypherIsolationIntegrationTest extends ExecutionEngineFunSuite {

  val THREADS = 50
  val UPDATES = 100

  test("Should work around read isolation limitations using multiple set") {
    // Given
    val n = createNode("x" -> 0L, "y" -> 0L, "z" -> 0L)

    // When
    val unlocked = updateAndCount(n, "x", "MATCH (n) SET n.x = n.x + 1")
    val locked1 = updateAndCount(n, "y", "MATCH (n) SET n._LOCK_ = true SET n.y = n.y + 1")
    val locked2 = updateAndCount(n, "z", "MATCH (n) SET n._LOCK_ = true SET n.z = n.z + 1 REMOVE n._LOCK_")

    // Then
    unlocked should equal(THREADS * UPDATES)
    locked1 should equal(THREADS * UPDATES)
    locked2 should equal(THREADS * UPDATES)
  }

  def updateAndCount(node: Node, property: String, query: String): Long = {

    val executor = Executors.newFixedThreadPool(THREADS)

    val futures = (1 to THREADS) map { x =>
      executor.submit(new Callable[Unit] {
        override def call(): Unit = {
          for (x <- 1 to UPDATES) {
            execute(query)
          }
        }})
      }

    try {
      futures.foreach(_.get())
    } finally executor.shutdown()

    graph.inTx {
      node.getProperty(property).asInstanceOf[Long]
    }
  }

}
