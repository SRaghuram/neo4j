/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.cypher.internal.v3_5.ast.semantics.functions

import org.neo4j.cypher.internal.v3_5.expressions.Expression.SemanticContext
import org.neo4j.cypher.internal.v3_5.util.symbols._

class PercentileContTest extends FunctionTestBase("percentileCont") {

  override val context = SemanticContext.Results

  test("shouldHandleAllSpecializations") {
    testValidTypes(CTInteger, CTInteger)(CTFloat)
    testValidTypes(CTInteger, CTFloat)(CTFloat)
    testValidTypes(CTFloat, CTInteger)(CTFloat)
    testValidTypes(CTFloat, CTFloat)(CTFloat)
  }

  test("shouldHandleCombinedSpecializations") {
    testValidTypes(CTFloat | CTInteger, CTFloat | CTInteger)(CTFloat)
  }

  test("shouldFailIfWrongArguments") {
    testInvalidApplication(CTFloat)("Insufficient parameters for function 'percentileCont'")
    testInvalidApplication(CTFloat, CTFloat, CTFloat)("Too many parameters for function 'percentileCont'")
  }

  test("shouldFailTypeCheckWhenAddingIncompatible") {
    testInvalidApplication(CTInteger, CTBoolean)(
      "Type mismatch: expected Float but was Boolean"
    )
    testInvalidApplication(CTBoolean, CTInteger)(
      "Type mismatch: expected Float but was Boolean"
    )
  }
}
