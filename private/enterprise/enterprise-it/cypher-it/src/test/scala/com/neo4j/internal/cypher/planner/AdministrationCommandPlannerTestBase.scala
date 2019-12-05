/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.internal.cypher.planner

import com.neo4j.internal.cypher.acceptance.AdministrationCommandAcceptanceTestBase
import org.neo4j.cypher.internal.plandescription.Arguments._
import org.neo4j.cypher.internal.plandescription._
import org.neo4j.cypher.internal.ast.prettifier.Prettifier
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.cypher.internal.util.test_helpers.WindowsStringSafe

class AdministrationCommandPlannerTestBase extends AdministrationCommandAcceptanceTestBase {
  implicit val windowsSafe: WindowsStringSafe.type = WindowsStringSafe

  // Argument helpers

  def databaseArg(name: String) = Database(Prettifier.escapeName(name))

  def userArg(name: String) = User(Prettifier.escapeName(name))

  def roleArg(name: String) = Role(Prettifier.escapeName(name))

  def qualifierArg(label: String, name: String) = Qualifier(label + " " + Prettifier.escapeName(name))

  // Plan helpers

  private def planDescription(name: String, arguments: Seq[Argument] = Seq.empty, children: Children = NoChildren) =
    PlanDescriptionImpl(Id.INVALID_ID, name, children, arguments, Set.empty)

  def logPlan(source: InternalPlanDescription): PlanDescriptionImpl = planDescription("LogSystemCommand", children = SingleChild(source))

  def managementPlan(name: String, arguments: Seq[Argument], source: InternalPlanDescription): PlanDescriptionImpl = planDescription(name, arguments, SingleChild(source))
  def managementPlan(name: String, source: InternalPlanDescription): PlanDescriptionImpl = planDescription(name, children = SingleChild(source))
  def managementPlan(name: String, arguments: Seq[Argument] = Seq.empty): PlanDescriptionImpl = planDescription(name, arguments)

  def databasePrivilegePlan(name: String, action: String, database: String, roleName: String, source: InternalPlanDescription): PlanDescriptionImpl = planDescription(name, Seq(DatabaseAction(action), databaseArg(database), roleArg(roleName)), SingleChild(source))
  def databasePrivilegePlan(name: String, action: String, roleName: String, source: InternalPlanDescription): PlanDescriptionImpl = planDescription(name, Seq(DatabaseAction(action), Database("*"), roleArg(roleName)), SingleChild(source))

  def rolePrivilegePlan(name: String, action: String, roleName: String, source: InternalPlanDescription): PlanDescriptionImpl = planDescription(name, Seq(DbmsAction(action), roleArg(roleName)), SingleChild(source))
  def userPrivilegePlan(name: String, action: String, username: String, source: InternalPlanDescription): PlanDescriptionImpl = planDescription(name, Seq(DbmsAction(action), userArg(username)), SingleChild(source))

  def graphPrivilegePlan(name: String, database: String, qualifier: Qualifier, roleName: String, source: InternalPlanDescription): PlanDescriptionImpl = planDescription(name, Seq(databaseArg(database), qualifier, roleArg(roleName)), SingleChild(source))
  def graphPrivilegePlan(name: String, qualifier: Qualifier, roleName: String, source: InternalPlanDescription): PlanDescriptionImpl = planDescription(name, Seq(Database("*"), qualifier, roleArg(roleName)), SingleChild(source))

  def helperPlan(name: String, arguments: Seq[Argument], source: InternalPlanDescription): PlanDescriptionImpl = planDescription(name, arguments, SingleChild(source))
  def helperPlan(name: String, source: InternalPlanDescription): PlanDescriptionImpl = planDescription(name, children = SingleChild(source))

  def assertDbmsAdminPlan(action: String): PlanDescriptionImpl = planDescription("AssertDbmsAdmin", Seq(DbmsAction(action)))
  def assertDatabaseAdminPlan(action: String, database: String): PlanDescriptionImpl = planDescription("AssertDatabaseAdmin", Seq(DatabaseAction(action), databaseArg(database)))
}
