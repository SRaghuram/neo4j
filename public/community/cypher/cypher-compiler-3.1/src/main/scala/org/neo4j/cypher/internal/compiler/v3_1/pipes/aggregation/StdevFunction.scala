/*
 * Copyright (c) 2002-2019 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.cypher.internal.compiler.v3_1.pipes.aggregation

import org.neo4j.cypher.internal.compiler.v3_1._
import commands.expressions.{Expression, NumericHelper}
import pipes.QueryState

class StdevFunction(val value: Expression, val population:Boolean)
  extends AggregationFunction
  with NumericExpressionOnly
  with NumericHelper {

  def name = if (population) "STDEVP" else "STDEV"

  // would be cool to not have to keep a temporary list to do multiple passes
  // this will blow up RAM over a big data set (not lazy!)
  // but I don't think it's currently possible with the way aggregation works
  private var temp = Vector[Double]()
  private var count:Int = 0
  private var total:Double = 0

  def result: Any = {
    if(count < 2) {
      0.0
    } else {
      val avg = total/count
      val variance = if(population) {
        val sumOfDeltas = temp.foldLeft(0.0)((acc, e) => {val delta = e - avg; acc + (delta * delta) })
        sumOfDeltas / count
      } else {
        val sumOfDeltas = temp.foldLeft(0.0)((acc, e) => {val delta = e - avg; acc + (delta * delta) })
        sumOfDeltas / (count - 1)
      }
      math.sqrt(variance)
    }
  }

  def apply(data: ExecutionContext)(implicit state: QueryState) {
    actOnNumber(value(data), (number) => {
      count += 1
      total += asDouble(number)
      temp = temp :+ asDouble(number)
    })
  }
}
