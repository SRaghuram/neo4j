/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.cypher.internal.v4_0.expressions.functions

import org.neo4j.cypher.internal.v4_0.expressions._
import org.neo4j.cypher.internal.v4_0.util.InputPosition
import org.neo4j.kernel.impl.query.FunctionInformation

object Function {
  private val knownFunctions: Seq[Function] = Vector(
    Abs,
    Acos,
    Asin,
    Atan,
    Atan2,
    Avg,
    Ceil,
    Coalesce,
    Collect,
    Ceil,
    Cos,
    Cot,
    Count,
    Degrees,
    Distance,
    E,
    EndNode,
    Exists,
    Exp,
    Filename,
    Floor,
    Haversin,
    Head,
    Id,
    Labels,
    Last,
    Left,
    Length,
    Linenumber,
    Log,
    Log10,
    LTrim,
    Max,
    Min,
    Nodes,
    Pi,
    PercentileCont,
    PercentileDisc,
    Point,
    Keys,
    Radians,
    Rand,
    Range,
    Reduce,
    Relationships,
    Replace,
    Reverse,
    Right,
    Round,
    RTrim,
    Sign,
    Sin,
    Size,
    Sqrt,
    Split,
    StartNode,
    StdDev,
    StdDevP,
    Substring,
    Sum,
    Tail,
    Tan,
    ToBoolean,
    ToFloat,
    ToInteger,
    ToLower,
    ToString,
    ToUpper,
    Properties,
    Trim,
    Type
  )

  lazy val lookup: Map[String, Function] = knownFunctions.map { f => (f.name.toLowerCase, f) }.toMap

  lazy val functionInfo: List[FunctionInformation] = {
    lookup.values.flatMap {
      case f: TypeSignatures =>
        f.signatures.flatMap {
          case signature: FunctionTypeSignature if !signature.deprecated =>
            val info: FunctionInfo = new FunctionInfo(f) {
              def getDescription: String = signature.toString

              def getSignature: String = signature.toString
            }
            Seq(info)
        }
      case func: FunctionWithInfo =>
        Seq(new FunctionInfo(func) {
          def getDescription: String = func.getDescription

          def getSignature: String = func.getSignatureAsString
        })
    }.toList
  }
}

abstract case class FunctionInfo(f: Function) extends FunctionInformation {
  override def getFunctionName: String = f.name

  override def isAggregationFunction: java.lang.Boolean = f match {
      case _: AggregatingFunction => true
      case _ => false
    }

  override def toString: String = f.name + " || " + getSignature + " || " + getDescription + " || " + isAggregationFunction
}

abstract class Function {
  def name: String

  def asFunctionName(implicit position: InputPosition): FunctionName = FunctionName(name)(position)

  def asInvocation(argument: Expression, distinct: Boolean = false)(implicit position: InputPosition): FunctionInvocation =
    FunctionInvocation(asFunctionName, distinct = distinct, IndexedSeq(argument))(position)

  def asInvocation(lhs: Expression, rhs: Expression)(implicit position: InputPosition): FunctionInvocation =
    FunctionInvocation(asFunctionName, distinct = false, IndexedSeq(lhs, rhs))(position)
}

trait FunctionWithInfo {
  def getSignatureAsString: String

  def getDescription: String
}

abstract class AggregatingFunction extends Function
