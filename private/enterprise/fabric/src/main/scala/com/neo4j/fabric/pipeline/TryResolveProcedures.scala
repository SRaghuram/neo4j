/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.pipeline

import org.neo4j.cypher.internal.v4_0.util.{Rewriter, bottomUp}
import com.neo4j.fabric.util.Rewritten._
import org.neo4j.cypher.internal.logical.plans.{QualifiedName, ResolvedCall, ResolvedFunctionInvocation}
import org.neo4j.cypher.internal.planner.spi.ProcedureSignatureResolver
import org.neo4j.cypher.internal.v4_0.ast.{AliasedReturnItem, Query, Return, ReturnItems, SingleQuery, UnresolvedCall}
import org.neo4j.cypher.internal.v4_0.expressions.{FunctionInvocation, Variable}

import scala.util.Try

case class TryResolveProcedures(signatures: ProcedureSignatureResolver) extends Rewriter {

  override def apply(input: AnyRef): AnyRef =
    input
      .rewritten
      .bottomUp {
        // Try resolving procedures
        case unresolved: UnresolvedCall =>
          Try(ResolvedCall(signatures.procedureSignature)(unresolved).coerceArguments)
            .getOrElse(unresolved)
        // Try resolving functions
        case function: FunctionInvocation if function.needsToBeResolved =>
          val name = QualifiedName(function)
          signatures.functionSignature(name)
            .map(sig => ResolvedFunctionInvocation(name, Some(sig), function.args)(function.position).coerceArguments)
            .getOrElse(function)
      }
      .rewritten
      .bottomUp {
        // Expand implicit yields and add return
        case q @ Query(None, part @ SingleQuery(Seq(resolved: ResolvedCall))) =>
          val expanded = resolved.withFakedFullDeclarations
          val aliases = expanded.callResults.map { item =>
            val copy1 = Variable(item.variable.name)(item.variable.position)
            val copy2 = Variable(item.variable.name)(item.variable.position)
            AliasedReturnItem(copy1, copy2)(resolved.position)
          }
          val projection = Return(distinct = false, ReturnItems(includeExisting = false, aliases)(resolved.position),
            None, None, None)(resolved.position)
          q.copy(part = part.copy(clauses = Seq(expanded, projection))(part.position))(q.position)
      }

}
