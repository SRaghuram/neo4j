/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.internal.cypher.planner

import com.neo4j.internal.cypher.acceptance.AdministrationCommandAcceptanceTestBase
import org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME
import org.neo4j.cypher.internal.ast.prettifier.ExpressionStringifier
import org.neo4j.cypher.internal.plandescription.Argument
import org.neo4j.cypher.internal.plandescription.Arguments.Details
import org.neo4j.cypher.internal.plandescription.Children
import org.neo4j.cypher.internal.plandescription.InternalPlanDescription
import org.neo4j.cypher.internal.plandescription.LogicalPlan2PlanDescriptionTest.details
import org.neo4j.cypher.internal.plandescription.NoChildren
import org.neo4j.cypher.internal.plandescription.PlanDescriptionImpl
import org.neo4j.cypher.internal.plandescription.SingleChild
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.cypher.internal.util.test_helpers.WindowsStringSafe

class AdministrationCommandPlannerTestBase extends AdministrationCommandAcceptanceTestBase {
  implicit val windowsSafe: WindowsStringSafe.type = WindowsStringSafe

  override protected def initTest() {
    super.initTest()
    selectDatabase(SYSTEM_DATABASE_NAME)
  }

  // Argument helpers

  def databaseArg(name: String) = details(ExpressionStringifier.backtick(name))

  def userArg(name: String) = userPrivilegeArg(name)

  def roleArg(name: String) = rolePrivilegeArg(name)

  def qualifierArg(label: String, name: String) = details(s"$label ${ExpressionStringifier.backtick(name)}")

  def scopeArg(label: String, name: String) = details(s"$label ${ExpressionStringifier.backtick(name)}")

  def resourceArg(name: String) = details(s"PROPERTY ${ExpressionStringifier.backtick(name)}")

  def allResourceArg() = details("ALL PROPERTIES")

  def graphPrivilegeArg(name: String) = details(s"GRAPH ${ExpressionStringifier.backtick(name)}")

  def databasePrivilegeArg(name: String) = details(s"DATABASE ${ExpressionStringifier.backtick(name)}")

  def userPrivilegeArg(name: String) = details(s"USER ${ExpressionStringifier.backtick(name)}")

  def rolePrivilegeArg(name: String) = details(s"ROLE ${ExpressionStringifier.backtick(name)}")

  def roleAndUserPrivilegeArg(role: String, user: String) = Details(rolePrivilegeArg(role).info ++ userPrivilegeArg(user).info)

  // Plan helpers

  private def planDescription(name: String, arguments: Seq[Argument] = Seq.empty, children: Children = NoChildren) =
    PlanDescriptionImpl(Id.INVALID_ID, name, children, arguments, Set.empty)

  def logPlan(source: InternalPlanDescription): PlanDescriptionImpl = planDescription("LogSystemCommand", children = SingleChild(source))

  def managementPlan(name: String, arguments: Seq[Argument], source: InternalPlanDescription): PlanDescriptionImpl = planDescription(name, arguments, SingleChild(source))
  def managementPlan(name: String, source: InternalPlanDescription): PlanDescriptionImpl = planDescription(name, children = SingleChild(source))
  def managementPlan(name: String, arguments: Seq[Argument] = Seq.empty): PlanDescriptionImpl = planDescription(name, arguments)

  def databasePrivilegePlan(name: String, action: String, database: String, roleName: String, source: InternalPlanDescription): PlanDescriptionImpl =
    planDescription(name, Seq(details(action +: (databasePrivilegeArg(database).info.map(_.prettifiedString) ++ rolePrivilegeArg(roleName).info.map(_.prettifiedString)))), SingleChild(source))
  def databasePrivilegePlan(name: String, action: String, database: Details, parameterRoleName: String, source: InternalPlanDescription): PlanDescriptionImpl =
    planDescription(name, Seq(details(action +: database.info.map(_.prettifiedString) :+ s"ROLE $parameterRoleName")), SingleChild(source))
  def databasePrivilegePlan(name: String, action: String, allDatabases: Boolean, roleName: String, source: InternalPlanDescription): PlanDescriptionImpl = {
    val databaseArg = if (allDatabases) "ALL DATABASES" else "DEFAULT DATABASE"
    planDescription(name, Seq(details(Seq(action, databaseArg) ++ rolePrivilegeArg(roleName).info.map(_.prettifiedString))), SingleChild(source))
  }

  def databasePrivilegePlan(name: String, action: String, database: String, qualifier: Details, roleName: String, source: InternalPlanDescription): PlanDescriptionImpl =
    planDescription(name, Seq(details(action +: (databasePrivilegeArg(database).info.map(_.prettifiedString) ++ qualifier.info.map(_.prettifiedString) ++ rolePrivilegeArg(roleName).info.map(_.prettifiedString)))), SingleChild(source))
  def databasePrivilegePlan(name: String, action: String, database: Details, qualifier: Details, parameterRoleName: String, source: InternalPlanDescription): PlanDescriptionImpl =
    planDescription(name, Seq(details(action +: (database.info.map(_.prettifiedString) ++ qualifier.info.map(_.prettifiedString)) :+ s"ROLE $parameterRoleName")), SingleChild(source))
  def databasePrivilegePlan(name: String, action: String, allDatabases: Boolean, qualifier: Details, roleName: String, source: InternalPlanDescription): PlanDescriptionImpl = {
    val databaseArg = if (allDatabases) "ALL DATABASES" else "DEFAULT DATABASE"
    planDescription(name, Seq(details(Seq(action, databaseArg) ++ qualifier.info.map(_.prettifiedString) ++ rolePrivilegeArg(roleName).info.map(_.prettifiedString))), SingleChild(source))
  }

  def dbmsPrivilegePlan(name: String, action: String, roleName: String, source: InternalPlanDescription): PlanDescriptionImpl = planDescription(name, Seq(details(action +: rolePrivilegeArg(roleName).info.map(_.prettifiedString))), SingleChild(source))
  def dbmsPrivilegePlan(name: String, action: String, roleName: Details, source: InternalPlanDescription): PlanDescriptionImpl = planDescription(name, Seq(details(action +: roleName.info.map(_.prettifiedString))), SingleChild(source))

  def graphPrivilegePlan(name: String, graph: String, qualifier: Details, roleName: String, source: InternalPlanDescription): PlanDescriptionImpl =
    planDescription(name, Seq(Details(graphPrivilegeArg(graph).info ++ qualifier.info ++ rolePrivilegeArg(roleName).info)), SingleChild(source))
  def graphPrivilegePlan(name: String, graph: Details, qualifier: Details, parameterRoleName: String, source: InternalPlanDescription): PlanDescriptionImpl =
    planDescription(name, Seq(details((graph.info.map(_.prettifiedString) ++ qualifier.info.map(_.prettifiedString)) :+ s"ROLE $parameterRoleName")), SingleChild(source))
  def graphPrivilegePlanForAllGraphs(name: String, qualifier: Details, roleName: String, source: InternalPlanDescription): PlanDescriptionImpl =
    planDescription(name, Seq(details("ALL GRAPHS" +: (qualifier.info.map(_.prettifiedString) ++ rolePrivilegeArg(roleName).info.map(_.prettifiedString)))), SingleChild(source))

  def graphPrivilegePlan(name: String, graph: String, resource: Details, qualifier: Details, roleName: String, source: InternalPlanDescription): PlanDescriptionImpl =
    planDescription(name, Seq(Details(graphPrivilegeArg(graph).info ++ resource.info ++ qualifier.info ++ rolePrivilegeArg(roleName).info)), SingleChild(source))
  def graphPrivilegePlan(name: String, graph: Details, resource: Details, qualifier: Details, parameterRoleName: String, source: InternalPlanDescription): PlanDescriptionImpl =
    planDescription(name, Seq(details((graph.info.map(_.prettifiedString) ++ resource.info.map(_.prettifiedString) ++ qualifier.info.map(_.prettifiedString)) :+ s"ROLE $parameterRoleName")), SingleChild(source))
  def graphPrivilegePlanForAllGraphs(name: String, resource: Details, qualifier: Details, roleName: String, source: InternalPlanDescription): PlanDescriptionImpl =
    planDescription(name, Seq(details("ALL GRAPHS" +: (resource.info.map(_.prettifiedString) ++ qualifier.info.map(_.prettifiedString) ++ rolePrivilegeArg(roleName).info.map(_.prettifiedString)))), SingleChild(source))

  def helperPlan(name: String, arguments: Seq[Argument], source: InternalPlanDescription): PlanDescriptionImpl = planDescription(name, arguments, SingleChild(source))
  def helperPlan(name: String, source: InternalPlanDescription): PlanDescriptionImpl = planDescription(name, children = SingleChild(source))

  def assertDbmsAdminPlan(actions: String*): PlanDescriptionImpl = planDescription("AssertDbmsAdmin", Seq(details(actions)))
  def assertDbmsAdminOrSelfPlan(user: Details, actions: String*): PlanDescriptionImpl = planDescription("AssertDbmsAdminOrSelf", Seq(details(actions ++ user.info.map(_.prettifiedString))))
  def assertDatabaseAdminPlan(action: String, database: Details): PlanDescriptionImpl = planDescription("AssertDatabaseAdmin", Seq(details(action +: database.info.map(_.prettifiedString))))
}
