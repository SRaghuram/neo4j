/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.cypher.internal.expressions

import org.neo4j.cypher.internal.util.InputPosition

case class GetDegree(
                      node: Expression,
                      relType: Option[RelTypeName],
                      dir: SemanticDirection
                    )(val position: InputPosition) extends Expression {

  override def asCanonicalStringVal: String = {
    val lArrow = if (dir == SemanticDirection.INCOMING) "<" else ""
    val rArrow = if (dir == SemanticDirection.OUTGOING) ">" else ""

    s"size((${node.asCanonicalStringVal})$lArrow-${relType.map(t => s"[:${t.name}]").getOrElse("")}-$rArrow())"
  }
}
