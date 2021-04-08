/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.physicalplanning.ast

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.ast.prettifier.ExpressionStringifier
import org.neo4j.cypher.internal.expressions.CollectAll
import org.neo4j.cypher.internal.expressions.GetDegree
import org.neo4j.cypher.internal.expressions.SemanticDirection.BOTH
import org.neo4j.cypher.internal.expressions.SemanticDirection.INCOMING
import org.neo4j.cypher.internal.expressions.SemanticDirection.OUTGOING
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class RuntimeAstTest extends CypherFunSuite with AstConstructionTestSupport {

  private val stringifier = ExpressionStringifier(_.asCanonicalStringVal)

  test("collect all should render nicely") {
    stringifier(CollectAll(varFor("x"))(pos)) should equal("collect_all(x)")
  }

  test("get degree should render nicely") {
    stringifier(GetDegree(varFor("x"), None, OUTGOING)(pos)) should equal("size((x)-->())")
    stringifier(GetDegree(varFor("x"), None, INCOMING)(pos)) should equal("size((x)<--())")
    stringifier(GetDegree(varFor("x"), None, BOTH)(pos)) should equal("size((x)--())")
    stringifier(GetDegree(varFor("x"), Some(relTypeName("Rel")), OUTGOING)(pos)) should equal("size((x)-[:Rel]->())")
    stringifier(GetDegree(varFor("x"), Some(relTypeName("Rel")), INCOMING)(pos)) should equal("size((x)<-[:Rel]-())")
    stringifier(GetDegree(varFor("x"), Some(relTypeName("Rel")), BOTH)(pos)) should equal("size((x)-[:Rel]-())")
  }
}
