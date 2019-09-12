/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.planner

import org.neo4j.cypher.internal.v4_0.ast.semantics.{FeatureError, SemanticError, SemanticErrorDef}
import org.neo4j.cypher.internal.v4_0.util.{ASTNode, InputPosition}

object Errors {

  trait HasErrors extends Throwable {
    def update(upd: SemanticErrorDef => SemanticErrorDef): HasErrors
  }

  case class InvalidQueryException(errors: Seq[SemanticErrorDef]) extends RuntimeException(
    s"Invalid query\n${errors.map(e => s"- ${e.msg} [at ${e.position}]").mkString("\n")}"
  ) with HasErrors {
    override def update(upd: SemanticErrorDef => SemanticErrorDef): InvalidQueryException = copy(errors.map(upd))
  }

  case class EvaluationFailedException(errors: Seq[SemanticErrorDef]) extends RuntimeException(
    s"Evaluation failed\n${errors.map(e => s"- ${e.msg} [at ${e.position}]").mkString("\n")}"
  ) with HasErrors {
    override def update(upd: SemanticErrorDef => SemanticErrorDef): EvaluationFailedException = copy(errors.map(upd))
  }
  def semantic(msg: String, node: ASTNode): SemanticError = SemanticError(msg, node.position)

  def invalidOnError(errors: Seq[SemanticErrorDef]): Unit = if (errors.nonEmpty) invalid(errors)

  def invalid(errors: Seq[SemanticErrorDef]): Nothing = throw InvalidQueryException(errors)

  def invalid(error: SemanticErrorDef): Nothing = invalid(Seq(error))

  def failure(errors: Seq[SemanticErrorDef]): Nothing = throw EvaluationFailedException(errors)

  def failure(error: SemanticErrorDef): Nothing = failure(Seq(error))

  def notFound(kind: String, needle: String, pos: InputPosition): Nothing = failure(SemanticError(s"$kind not found: $needle", pos))

  def unexpected(exp: String, got: String, pos: InputPosition): Nothing = invalid(SemanticError(s"Expected: $exp, got: $got", pos))

  def unexpected(exp: String, got: String, in: String, pos: InputPosition): Nothing = invalid(SemanticError(s"Expected: $exp, got: $got, in: $in", pos))

  def unexpected(exp: String, got: ASTNode): Nothing = unexpected(exp, AstShow.show(got), got.position)

  def wrongArity(exp: Int, got: Int, in: String, pos: InputPosition): Nothing = unexpected(s"$exp arguments", s"$got arguments", in, pos)

  def unimplemented(context: String, value: Any): Nothing = throw new NotImplementedError(s"$context not implemented: $value")

  def error(msg: String): Nothing = throw new Error(msg)

  /** Attaches position info to exceptions, if it is missing */
  def errorContext[T](node: ASTNode)(block: => T): T =
    try block catch {
      case e: HasErrors => throw e.update {
        case SemanticError(msg, InputPosition.NONE, refs @ _*) => SemanticError(msg, node.position, refs: _*)
        case FeatureError(msg, InputPosition.NONE)             => FeatureError(msg, node.position)
        case o                                                 => o
      }
    }
}
