/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.physicalplanning.ast

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.ast.prettifier.ExpressionStringifier
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class RuntimeAstTest extends CypherFunSuite with AstConstructionTestSupport {

  private val stringifier = ExpressionStringifier(_.asCanonicalStringVal)

  test("collect all should render nicely") {
    stringifier(CollectAll(varFor("x"))(pos)) should equal("collect_all(x)")
  }
}
