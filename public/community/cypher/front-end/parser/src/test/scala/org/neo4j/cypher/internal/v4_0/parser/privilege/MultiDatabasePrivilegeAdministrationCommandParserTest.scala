/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.cypher.internal.v4_0.parser.privilege

import org.neo4j.cypher.internal.v4_0.ast
import org.neo4j.cypher.internal.v4_0.parser.AdministrationCommandParserTestBase

class MultiDatabasePrivilegeAdministrationCommandParserTest extends AdministrationCommandParserTestBase {

  // Access privilege

  Seq(
    ("GRANT", "TO", grantDatabasePrivilege: databasePrivilegeFunc),
    ("DENY", "TO", denyDatabasePrivilege: databasePrivilegeFunc),
    ("REVOKE GRANT", "FROM", revokeGrantDatabasePrivilege: databasePrivilegeFunc),
    ("REVOKE DENY", "FROM", revokeDenyDatabasePrivilege: databasePrivilegeFunc),
    ("REVOKE", "FROM", revokeDatabasePrivilege: databasePrivilegeFunc)
  ).foreach {
    case (command: String, preposition: String, privilege: databasePrivilegeFunc) =>

      test(s"$command ACCESS ON DATABASE * $preposition role") {
        yields(privilege(ast.AccessDatabaseAction, ast.AllGraphsScope() _, Seq("role")))
      }

      test(s"$command ACCESS ON DATABASES * $preposition role") {
        yields(privilege(ast.AccessDatabaseAction, ast.AllGraphsScope() _, Seq("role")))
      }

      test(s"$command ACCESS ON DATABASE * $preposition role1, role2") {
        yields(privilege(ast.AccessDatabaseAction, ast.AllGraphsScope() _, Seq("role1", "role2")))
      }

      test(s"$command ACCESS ON DATABASE foo $preposition role") {
        yields(privilege(ast.AccessDatabaseAction, ast.NamedGraphScope("foo") _, Seq("role")))
      }

      test(s"$command ACCESS ON DATABASE foo $preposition role1, role2") {
        yields(privilege(ast.AccessDatabaseAction, ast.NamedGraphScope("foo") _, Seq("role1", "role2")))
      }

      test(s"$command ACCESS ON GRAPH * $preposition role") {
        failsToParse
      }

      test(s"$command ACCESS ON DATABASES foo, bar $preposition role") {
        failsToParse
      }

  }
}
