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
package org.neo4j.cypher.internal.v4_0.ast.semantics

import org.neo4j.cypher.internal.v4_0.util.{CypherException, InputPosition, SyntaxException}

class SyntaxExceptionCreator(queryText: String, preParserOffset: Option[InputPosition]) extends ((String, InputPosition) => CypherException) {
  override def apply(message: String, position: InputPosition): CypherException = {
    val adjustedPosition = position.withOffset(preParserOffset)
    new SyntaxException(s"$message ($adjustedPosition)", queryText, adjustedPosition)
  }
}

object SyntaxExceptionCreator {
  def throwOnError(mkException: SyntaxExceptionCreator): (Seq[SemanticErrorDef]) => Unit =
    (errors: Seq[SemanticErrorDef]) => errors.foreach(e => throw mkException(e.msg, e.position))
}
