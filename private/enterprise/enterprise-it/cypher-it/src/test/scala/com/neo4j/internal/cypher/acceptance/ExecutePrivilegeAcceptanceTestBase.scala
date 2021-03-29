/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.internal.cypher.acceptance

abstract class ExecutePrivilegeAcceptanceTestBase extends AdministrationCommandAcceptanceTestBase {

  val listUsersProcGlob = "dbms.security.listUsers"
  val labelsProcGlob = "db.labels"
  val propertyProcGlob = "db.property*"

  val defaultRole: String = "default"
}
