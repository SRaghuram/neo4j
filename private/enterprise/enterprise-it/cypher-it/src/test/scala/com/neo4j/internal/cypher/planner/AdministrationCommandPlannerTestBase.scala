/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.internal.cypher.planner

import com.neo4j.internal.cypher.acceptance.AdministrationCommandAcceptanceTestBase
import org.neo4j.cypher.internal.ast.prettifier.Prettifier
import org.neo4j.cypher.internal.plandescription.Argument
import org.neo4j.cypher.internal.plandescription.Arguments.Database
import org.neo4j.cypher.internal.plandescription.Arguments.DatabaseAction
import org.neo4j.cypher.internal.plandescription.Arguments.DbmsAction
import org.neo4j.cypher.internal.plandescription.Arguments.Qualifier
import org.neo4j.cypher.internal.plandescription.Arguments.Resource
import org.neo4j.cypher.internal.plandescription.Arguments.Role
import org.neo4j.cypher.internal.plandescription.Arguments.User
import org.neo4j.cypher.internal.plandescription.Children
import org.neo4j.cypher.internal.plandescription.InternalPlanDescription
import org.neo4j.cypher.internal.plandescription.NoChildren
import org.neo4j.cypher.internal.plandescription.PlanDescriptionImpl
import org.neo4j.cypher.internal.plandescription.SingleChild
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.cypher.internal.util.test_helpers.WindowsStringSafe

class AdministrationCommandPlannerTestBase extends AdministrationCommandAcceptanceTestBase {
  implicit val windowsSafe: WindowsStringSafe.type = WindowsStringSafe

  // Argument helpers

  def databaseArg(name: String) = Database(Prettifier.escapeName(name))

  def userArg(name: String) = User(Prettifier.escapeName(name))

  def roleArg(name: String) = Role(Prettifier.escapeName(name))

  def qualifierArg(label: String, name: String) = Qualifier(s"$label ${Prettifier.escapeName(name)}")

  def resourceArg(name: String) = Resource(s"PROPERTY ${Prettifier.escapeName(name)}")
  def allResourceArg() = Resource("ALL PROPERTIES")

  def graphPrivilegeArg(name: String) = Database(s"GRAPH ${Prettifier.escapeName(name)}")

  def databasePrivilegeArg(name: String) = Database(s"DATABASE ${Prettifier.escapeName(name)}")

  def userPrivilegeArg(name: String) = User(s"USER ${Prettifier.escapeName(name)}")

  def rolePrivilegeArg(name: String) = Role(s"ROLE ${Prettifier.escapeName(name)}")

  // Plan helpers

  private def planDescription(name: String, arguments: Seq[Argument] = Seq.empty, children: Children = NoChildren) =
    PlanDescriptionImpl(Id.INVALID_ID, name, children, arguments, Set.empty)

  def logPlan(source: InternalPlanDescription): PlanDescriptionImpl = planDescription("LogSystemCommand", children = SingleChild(source))

  def managementPlan(name: String, arguments: Seq[Argument], source: InternalPlanDescription): PlanDescriptionImpl = planDescription(name, arguments, SingleChild(source))
  def managementPlan(name: String, source: InternalPlanDescription): PlanDescriptionImpl = planDescription(name, children = SingleChild(source))
  def managementPlan(name: String, arguments: Seq[Argument] = Seq.empty): PlanDescriptionImpl = planDescription(name, arguments)

  def databasePrivilegePlan(name: String, action: String, database: String, roleName: String, source: InternalPlanDescription): PlanDescriptionImpl =
    planDescription(name, Seq(DatabaseAction(action), databasePrivilegeArg(database), rolePrivilegeArg(roleName)), SingleChild(source))
  def databasePrivilegePlan(name: String, action: String, allDatabases: Boolean, roleName: String, source: InternalPlanDescription): PlanDescriptionImpl = {
    val databaseArg = if (allDatabases) "ALL DATABASES" else "DEFAULT DATABASE"
    planDescription(name, Seq(DatabaseAction(action), Database(databaseArg), rolePrivilegeArg(roleName)), SingleChild(source))
  }

  def databasePrivilegePlan(name: String, action: String, database: String, qualifier: Qualifier, roleName: String, source: InternalPlanDescription): PlanDescriptionImpl =
    planDescription(name, Seq(DatabaseAction(action), databasePrivilegeArg(database), qualifier, rolePrivilegeArg(roleName)), SingleChild(source))
  def databasePrivilegePlan(name: String, action: String, allDatabases: Boolean, qualifier: Qualifier, roleName: String, source: InternalPlanDescription): PlanDescriptionImpl = {
    val databaseArg = if (allDatabases) "ALL DATABASES" else "DEFAULT DATABASE"
    planDescription(name, Seq(DatabaseAction(action), Database(databaseArg), qualifier, rolePrivilegeArg(roleName)), SingleChild(source))
  }

  def dbmsPrivilegePlan(name: String, action: String, roleName: String, source: InternalPlanDescription): PlanDescriptionImpl = planDescription(name, Seq(DbmsAction(action), rolePrivilegeArg(roleName)), SingleChild(source))

  def graphPrivilegePlan(name: String, database: String, qualifier: Qualifier, roleName: String, source: InternalPlanDescription): PlanDescriptionImpl = planDescription(name, Seq(graphPrivilegeArg(database), qualifier, rolePrivilegeArg(roleName)), SingleChild(source))
  def graphPrivilegePlan(name: String, qualifier: Qualifier, roleName: String, source: InternalPlanDescription): PlanDescriptionImpl = planDescription(name, Seq(Database("ALL GRAPHS"), qualifier, rolePrivilegeArg(roleName)), SingleChild(source))

  def graphPrivilegePlan(name: String, database: String, resource: Resource, qualifier: Qualifier, roleName: String, source: InternalPlanDescription): PlanDescriptionImpl =
    planDescription(name, Seq(graphPrivilegeArg(database), resource, qualifier, rolePrivilegeArg(roleName)), SingleChild(source))
  def graphPrivilegePlan(name: String, resource: Resource, qualifier: Qualifier, roleName: String, source: InternalPlanDescription): PlanDescriptionImpl =
    planDescription(name, Seq(Database("ALL GRAPHS"), resource, qualifier, rolePrivilegeArg(roleName)), SingleChild(source))

  def helperPlan(name: String, arguments: Seq[Argument], source: InternalPlanDescription): PlanDescriptionImpl = planDescription(name, arguments, SingleChild(source))
  def helperPlan(name: String, source: InternalPlanDescription): PlanDescriptionImpl = planDescription(name, children = SingleChild(source))

  def assertDbmsAdminPlan(actions: String*): PlanDescriptionImpl = planDescription("AssertDbmsAdmin", actions.map(a => DbmsAction(a)))
  def assertDatabaseAdminPlan(action: String, database: String): PlanDescriptionImpl = planDescription("AssertDatabaseAdmin", Seq(DatabaseAction(action), databaseArg(database)))
}
