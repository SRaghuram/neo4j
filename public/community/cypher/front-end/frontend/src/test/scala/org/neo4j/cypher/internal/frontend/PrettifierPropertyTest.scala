/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.cypher.internal.frontend

import org.neo4j.cypher.internal.ast.generator.AstGenerator
import org.neo4j.cypher.internal.ast.generator.AstShrinker
import org.neo4j.cypher.internal.ast.prettifier.{ExpressionStringifier, Prettifier}
import org.neo4j.cypher.internal.parser.CypherParser
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.scalatest.prop.GeneratorDrivenPropertyChecks

class PrettifierPropertyTest extends CypherFunSuite
  with GeneratorDrivenPropertyChecks
  with PrettifierTestUtils {

  val pr = Prettifier(ExpressionStringifier(alwaysParens = true, alwaysBacktick = true))

  val parser = new CypherParser

  val gen = new AstGenerator(simpleStrings = false)

  implicit val config: PropertyCheckConfiguration = PropertyCheckConfiguration(minSuccessful = 500)

  import AstShrinker.shrinkQuery

  test("Prettifier output should parse to the same ast") {
    forAll(gen._query) { query =>
      roundTripCheck(query)
    }
  }
}
