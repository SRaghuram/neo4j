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
package org.neo4j.cypher.internal.parser

import org.neo4j.cypher.internal.ast
import org.neo4j.cypher.internal.ast.AllConstraints
import org.neo4j.cypher.internal.ast.DeprecatedSyntax
import org.neo4j.cypher.internal.ast.ExistsConstraints
import org.neo4j.cypher.internal.ast.NewSyntax
import org.neo4j.cypher.internal.ast.NodeExistsConstraints
import org.neo4j.cypher.internal.ast.NodeKeyConstraints
import org.neo4j.cypher.internal.ast.OldValidSyntax
import org.neo4j.cypher.internal.ast.RelExistsConstraints
import org.neo4j.cypher.internal.ast.ShowConstraintsClause
import org.neo4j.cypher.internal.ast.ShowIndexesClause
import org.neo4j.cypher.internal.ast.UniqueConstraints

/* Tests for listing indexes and constraints */
class ShowSchemaCommandParserTest extends SchemaCommandsParserTestBase {

  // Show indexes

  Seq("INDEX", "INDEXES").foreach { indexKeyword =>

    // No explicit output

    test(s"SHOW $indexKeyword") {
      yields(_ => query(ShowIndexesClause(all = true, brief = false, verbose = false, None, hasYield = false)(pos)))
    }

    test(s"SHOW ALL $indexKeyword") {
      yields(_ => query(ShowIndexesClause(all = true, brief = false, verbose = false, None, hasYield = false)(pos)))
    }

    test(s"SHOW BTREE $indexKeyword") {
      yields(_ => query(ShowIndexesClause(all = false, brief = false, verbose = false, None, hasYield = false)(pos)))
    }

    test(s"USE db SHOW $indexKeyword") {
      yields(_ => query(use(varFor("db")), ShowIndexesClause(all = true, brief = false, verbose = false, None, hasYield = false)(pos)))
    }

    // Brief output (deprecated)

    test(s"SHOW $indexKeyword BRIEF") {
      yields(_ => query(ShowIndexesClause(all = true, brief = true, verbose = false, None, hasYield = false)(pos)))
    }

    test(s"SHOW $indexKeyword BRIEF OUTPUT") {
      yields(_ => query(ShowIndexesClause(all = true, brief = true, verbose = false, None, hasYield = false)(pos)))
    }

    test(s"SHOW ALL $indexKeyword BRIEF") {
      yields(_ => query(ShowIndexesClause(all = true, brief = true, verbose = false, None, hasYield = false)(pos)))
    }

    test(s"SHOW  ALL $indexKeyword BRIEF OUTPUT") {
      yields(_ => query(ShowIndexesClause(all = true, brief = true, verbose = false, None, hasYield = false)(pos)))
    }

    test(s"SHOW BTREE $indexKeyword BRIEF") {
      yields(_ => query(ShowIndexesClause(all = false, brief = true, verbose = false, None, hasYield = false)(pos)))
    }

    // Verbose output (deprecated)

    test(s"SHOW $indexKeyword VERBOSE") {
      yields(_ => query(ShowIndexesClause(all = true, brief = false, verbose = true, None, hasYield = false)(pos)))
    }

    test(s"SHOW ALL $indexKeyword VERBOSE") {
      yields(_ => query(ShowIndexesClause(all = true, brief = false, verbose = true, None, hasYield = false)(pos)))
    }

    test(s"SHOW BTREE $indexKeyword VERBOSE OUTPUT") {
      yields(_ => query(ShowIndexesClause(all = false, brief = false, verbose = true, None, hasYield = false)(pos)))
    }
  }

  // Show indexes filtering

  test("SHOW INDEX WHERE uniqueness = 'UNIQUE'") {
    yields(_ => query(ShowIndexesClause(all = true, brief = false, verbose = false, Some(where(equals(varFor("uniqueness"), literalString("UNIQUE")))), hasYield = false)(pos)))
  }

  test("SHOW INDEXES YIELD populationPercent") {
    yields(_ => query(ShowIndexesClause(all = true, brief = false, verbose = false, None, hasYield = true)(pos), yieldClause(returnItems(variableReturnItem("populationPercent")))))
  }

  test("SHOW BTREE INDEXES YIELD *") {
    yields(_ => query(ShowIndexesClause(all = false, brief = false, verbose = false, None, hasYield = true)(pos), yieldClause(returnAllItems)))
  }

  test("USE db SHOW BTREE INDEXES YIELD name, populationPercent AS pp WHERE pp < 50.0 RETURN name") {
    yields(_ => query(
      use(varFor("db")),
      ShowIndexesClause(all = false, brief = false, verbose = false, None, hasYield = true)(pos),
      yieldClause(returnItems(variableReturnItem("name"), aliasedReturnItem("populationPercent", "pp")),
        where = Some(where(lessThan(varFor("pp"), literalFloat(50.0))))),
      return_(variableReturnItem("name"))
    ))
  }

  test("SHOW INDEXES YIELD name AS INDEX, type AS OUTPUT") {
    yields(_ => query(ShowIndexesClause(all = true, brief = false, verbose = false, None, hasYield = true)(pos),
      yieldClause(returnItems(aliasedReturnItem("name", "INDEX"), aliasedReturnItem("type", "OUTPUT")))))
  }

  test("SHOW INDEXES WHERE name = 'GRANT'") {
    yields(_ => query(ShowIndexesClause(all = true, brief = false, verbose = false,
      Some(where(equals(varFor("name"), literalString("GRANT")))), hasYield = false)(pos)))
  }

  // Negative tests for show indexes

  test("SHOW ALL BTREE INDEXES") {
    failsToParse
  }

  test("SHOW INDEX OUTPUT") {
    failsToParse
  }

  test("SHOW INDEX YIELD") {
    failsToParse
  }

  test("SHOW INDEX VERBOSE BRIEF OUTPUT") {
    failsToParse
  }

  test("SHOW INDEXES BRIEF YIELD *") {
    failsToParse
  }

  test("SHOW INDEXES VERBOSE YIELD *") {
    failsToParse
  }

  test("SHOW INDEXES BRIEF RETURN *") {
    failsToParse
  }

  test("SHOW INDEXES VERBOSE RETURN *") {
    failsToParse
  }

  test("SHOW INDEXES BRIEF WHERE uniqueness = 'UNIQUE'") {
    failsToParse
  }

  test("SHOW INDEXES VERBOSE WHERE uniqueness = 'UNIQUE'") {
    failsToParse
  }

  test("SHOW INDEXES YIELD * YIELD *") {
    failsToParse
  }

  test("SHOW INDEXES WHERE uniqueness = 'UNIQUE' YIELD *") {
    failsToParse
  }

  test("SHOW INDEXES WHERE uniqueness = 'UNIQUE' RETURN *") {
    failsToParse
  }

  test("SHOW INDEXES YIELD a b RETURN *") {
    failsToParse
  }

  test("SHOW INDEXES YIELD * WITH * MATCH (n) RETURN n") {
    // Can't parse WITH after SHOW
    failsToParse
  }

  test("UNWIND range(1,10) as b SHOW INDEXES YIELD * RETURN *") {
    // Can't parse SHOW after UNWIND
    failsToParse
  }

  test("SHOW INDEXES WITH name, type RETURN *") {
    // Can't parse WITH after SHOW
    failsToParse
  }

  test("SHOW INDEXES RETURN *") {
    failsToParse
  }

  test("SHOW NODE INDEXES") {
    failsToParse
  }

  test("SHOW REL INDEXES") {
    failsToParse
  }

  test("SHOW RELATIONSHIP INDEXES") {
    failsToParse
  }

  // Show constraints

  private val oldConstraintTypes = Seq(
    ("", AllConstraints),
    ("ALL", AllConstraints),
    ("UNIQUE", UniqueConstraints),
    ("NODE KEY", NodeKeyConstraints),
    ("EXIST", ExistsConstraints(OldValidSyntax)),
    ("EXISTS", ExistsConstraints(DeprecatedSyntax)),
    ("NODE EXIST", NodeExistsConstraints(OldValidSyntax)),
    ("NODE EXISTS", NodeExistsConstraints(DeprecatedSyntax)),
    ("RELATIONSHIP EXIST", RelExistsConstraints(OldValidSyntax)),
    ("RELATIONSHIP EXISTS", RelExistsConstraints(DeprecatedSyntax)),
  )

  private val newExistenceConstraintType = Seq(
    ("PROPERTY EXISTENCE", ExistsConstraints(NewSyntax)),
    ("PROPERTY EXIST", ExistsConstraints(NewSyntax)),
    ("EXISTENCE", ExistsConstraints(NewSyntax)),
    ("NODE PROPERTY EXISTENCE", NodeExistsConstraints(NewSyntax)),
    ("NODE PROPERTY EXIST", NodeExistsConstraints(NewSyntax)),
    ("NODE EXISTENCE", NodeExistsConstraints(NewSyntax)),
    ("RELATIONSHIP PROPERTY EXISTENCE", RelExistsConstraints(NewSyntax)),
    ("RELATIONSHIP PROPERTY EXIST", RelExistsConstraints(NewSyntax)),
    ("RELATIONSHIP EXISTENCE", RelExistsConstraints(NewSyntax)),
    ("REL PROPERTY EXISTENCE", RelExistsConstraints(NewSyntax)),
    ("REL PROPERTY EXIST", RelExistsConstraints(NewSyntax)),
    ("REL EXISTENCE", RelExistsConstraints(NewSyntax)),
    ("REL EXIST", RelExistsConstraints(NewSyntax)),
  )

  Seq("CONSTRAINT", "CONSTRAINTS").foreach {
    constraintKeyword =>
      (oldConstraintTypes ++ newExistenceConstraintType).foreach {
        case (constraintTypeKeyword, constraintType) =>

          test(s"SHOW $constraintTypeKeyword $constraintKeyword") {
            yields(_ => query(ShowConstraintsClause(constraintType, brief = false, verbose = false, None, hasYield = false)(pos)))
          }

          test(s"USE db SHOW $constraintTypeKeyword $constraintKeyword") {
            yields(_ => query(use(varFor("db")), ShowConstraintsClause(constraintType, brief = false, verbose = false, None, hasYield = false)(pos)))
          }

      }

      // Brief/verbose output (deprecated)

      oldConstraintTypes.foreach {
        case (constraintTypeKeyword, constraintType) =>

          test(s"SHOW $constraintTypeKeyword $constraintKeyword BRIEF") {
            yields(_ => query(ShowConstraintsClause(constraintType, brief = true, verbose = false, None, hasYield = false)(pos)))
          }

          test(s"SHOW $constraintTypeKeyword $constraintKeyword BRIEF OUTPUT") {
            yields(_ => query(ShowConstraintsClause(constraintType, brief = true, verbose = false, None, hasYield = false)(pos)))
          }

          test(s"SHOW $constraintTypeKeyword $constraintKeyword VERBOSE") {
            yields(_ => query(ShowConstraintsClause(constraintType, brief = false, verbose = true, None, hasYield = false)(pos)))
          }

          test(s"SHOW $constraintTypeKeyword $constraintKeyword VERBOSE OUTPUT") {
            yields(_ => query(ShowConstraintsClause(constraintType, brief = false, verbose = true, None, hasYield = false)(pos)))
          }
      }
  }

  // Show constraints filtering

  test("SHOW CONSTRAINT WHERE entityType = 'RELATIONSHIP'") {
    yields(_ => query(ShowConstraintsClause(AllConstraints, brief = false, verbose = false, Some(where(equals(varFor("entityType"), literalString("RELATIONSHIP")))), hasYield = false)(pos)))
  }

  test("SHOW REL PROPERTY EXISTENCE CONSTRAINTS YIELD labelsOrTypes") {
    yields(_ => query(ShowConstraintsClause(RelExistsConstraints(NewSyntax), brief = false, verbose = false, None, hasYield = true)(pos), yieldClause(returnItems(variableReturnItem("labelsOrTypes")))))
  }

  test("SHOW UNIQUE CONSTRAINTS YIELD *") {
    yields(_ => query(ShowConstraintsClause(UniqueConstraints, brief = false, verbose = false, None, hasYield = true)(pos), yieldClause(returnAllItems)))
  }

  test("USE db SHOW NODE KEY CONSTRAINTS YIELD name, properties AS pp WHERE size(pp) > 1 RETURN name") {
    yields(_ => query(
      use(varFor("db")),
      ShowConstraintsClause(NodeKeyConstraints, brief = false, verbose = false, None, hasYield = true)(pos),
      yieldClause(returnItems(variableReturnItem("name"), aliasedReturnItem("properties", "pp")),
        where = Some(where(greaterThan(function("size", varFor("pp")), literalInt(1))))),
      return_(variableReturnItem("name"))
    ))
  }

  test("SHOW EXISTENCE CONSTRAINTS YIELD name AS CONSTRAINT, type AS OUTPUT") {
    yields(_ => query(ShowConstraintsClause(ExistsConstraints(NewSyntax), brief = false, verbose = false, None, hasYield = true)(pos),
      yieldClause(returnItems(aliasedReturnItem("name", "CONSTRAINT"), aliasedReturnItem("type", "OUTPUT")))))
  }

  test("SHOW NODE EXIST CONSTRAINTS WHERE name = 'GRANT'") {
    yields(_ => query(ShowConstraintsClause(NodeExistsConstraints(OldValidSyntax), brief = false, verbose = false,
      Some(where(equals(varFor("name"), literalString("GRANT")))), hasYield = false)(pos)))
  }

  // Negative tests for show constraints

  test("SHOW ALL EXISTS CONSTRAINTS") {
    failsToParse
  }

  test("SHOW UNIQUENESS CONSTRAINTS") {
    failsToParse
  }

  test("SHOW NODE CONSTRAINTS") {
    failsToParse
  }

  test("SHOW EXISTS NODE CONSTRAINTS") {
    failsToParse
  }

  test("SHOW NODES EXIST CONSTRAINTS") {
    failsToParse
  }

  test("SHOW RELATIONSHIP CONSTRAINTS") {
    failsToParse
  }

  test("SHOW EXISTS RELATIONSHIP CONSTRAINTS") {
    failsToParse
  }

  test("SHOW RELATIONSHIPS EXIST CONSTRAINTS") {
    failsToParse
  }

  test("SHOW REL EXISTS CONSTRAINTS") {
    failsToParse
  }

  test("SHOW KEY CONSTRAINTS") {
    failsToParse
  }

  test("SHOW CONSTRAINTS OUTPUT") {
    failsToParse
  }

  test("SHOW CONSTRAINTS VERBOSE BRIEF OUTPUT") {
    failsToParse
  }

  newExistenceConstraintType.foreach {
    case (constraintTypeKeyword, _) =>
      test(s"SHOW $constraintTypeKeyword CONSTRAINTS BRIEF") {
        failsToParse
      }

      test(s"SHOW $constraintTypeKeyword CONSTRAINT BRIEF OUTPUT") {
        failsToParse
      }

      test(s"SHOW $constraintTypeKeyword CONSTRAINT VERBOSE") {
        failsToParse
      }

      test(s"SHOW $constraintTypeKeyword CONSTRAINTS VERBOSE OUTPUT") {
        failsToParse
      }
  }

  test("SHOW CONSTRAINT YIELD") {
    failsToParse
  }

  test("SHOW CONSTRAINTS BRIEF YIELD *") {
    failsToParse
  }

  test("SHOW CONSTRAINTS VERBOSE YIELD *") {
    failsToParse
  }

  test("SHOW CONSTRAINTS BRIEF WHERE entityType = 'NODE'") {
    failsToParse
  }

  test("SHOW CONSTRAINTS VERBOSE WHERE entityType = 'NODE'") {
    failsToParse
  }

  test("SHOW CONSTRAINTS YIELD * YIELD *") {
    failsToParse
  }

  test("SHOW CONSTRAINTS WHERE entityType = 'NODE' YIELD *") {
    failsToParse
  }

  test("SHOW CONSTRAINTS YIELD a b RETURN *") {
    failsToParse
  }

  test("SHOW CONSTRAINTS YIELD * WITH * MATCH (n) RETURN n") {
    // Can't parse WITH after SHOW
    failsToParse
  }

  test("UNWIND range(1,10) as b SHOW CONSTRAINTS YIELD * RETURN *") {
    // Can't parse SHOW after UNWIND
    failsToParse
  }

  test("SHOW CONSTRAINTS WITH name, type RETURN *") {
    // Can't parse WITH after SHOW
    failsToParse
  }

  test("SHOW EXISTS CONSTRAINT WHERE name = 'foo'") {
    failsToParse
  }

  test("SHOW NODE EXISTS CONSTRAINT WHERE name = 'foo'") {
    failsToParse
  }

  test("SHOW RELATIONSHIP EXISTS CONSTRAINT WHERE name = 'foo'") {
    failsToParse
  }

  test("SHOW EXISTS CONSTRAINT YIELD *") {
    failsToParse
  }

  test("SHOW NODE EXISTS CONSTRAINT YIELD *") {
    failsToParse
  }

  test("SHOW RELATIONSHIP EXISTS CONSTRAINT YIELD name") {
    failsToParse
  }

}
