/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.internal.cypher.planner

import com.neo4j.internal.cypher.acceptance.AdministrationCommandAcceptanceTestBase
import org.neo4j.cypher.internal.plandescription.Arguments.{Database, Qualifier, Role, User}
import org.neo4j.cypher.internal.plandescription.{Argument, Children, NoChildren, PlanDescriptionImpl}
import org.neo4j.cypher.internal.v4_0.ast.prettifier.Prettifier
import org.neo4j.cypher.internal.v4_0.util.attribution.Id
import org.neo4j.cypher.internal.v4_0.util.test_helpers.WindowsStringSafe

class AdministrationCommandPlannerTestBase extends AdministrationCommandAcceptanceTestBase {
  implicit val windowsSafe: WindowsStringSafe.type = WindowsStringSafe

  def planDescription(name: String, arguments: Seq[Argument] = Seq.empty, children: Children = NoChildren) =
    PlanDescriptionImpl(Id.INVALID_ID, name, children, arguments, Set.empty)

  def databaseArg(name: String) = Database(Prettifier.escapeName(name))

  def userArg(name: String) = User(Prettifier.escapeName(name))

  def roleArg(name: String) = Role(Prettifier.escapeName(name))

  def qualifierArg(label: String, name: String) = Qualifier(label + " " + Prettifier.escapeName(name))
}
