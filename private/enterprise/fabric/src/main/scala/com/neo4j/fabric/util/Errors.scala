/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.util

import com.neo4j.fabric.eval.Catalog
import org.neo4j.cypher.internal.v4_0.ast.{CatalogDDL, CatalogName}
import org.neo4j.cypher.internal.v4_0.ast.semantics.{FeatureError, SemanticError, SemanticErrorDef}
import org.neo4j.cypher.internal.v4_0.util.symbols.CypherType
import org.neo4j.cypher.internal.v4_0.util.{ASTNode, InputPosition}
import org.neo4j.exceptions._
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Value

/**
  * The errors in this file are in 2 categories: "Open Cypher" and "Neo4j".
  * <p>
  * The main feature of "Neo4j" errors is that they have status codes. Any error without a status code that occurs during cypher statement execution
  * and makes to Bolt server will be mapped to a generic [[org.neo4j.kernel.api.exceptions.Status.Statement.ExecutionFailed]] that is an equivalent of HTTP 500
  * and indicates a generic failure on the server side.
  * <p>
  * "Open Cypher" errors can be used, but they need to be mapped to "Neo4j" ones unless "Statement Execution Failed" is a desirable outcome.
  */
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
  def openCypherSemantic(msg: String, node: ASTNode): SemanticError = SemanticError(msg, node.position)

  def openCypherInvalidOnError(errors: Seq[SemanticErrorDef]): Unit = if (errors.nonEmpty) openCypherInvalid(errors)

  def openCypherInvalid(errors: Seq[SemanticErrorDef]): Nothing = throw InvalidQueryException(errors)

  def openCypherInvalid(error: SemanticErrorDef): Nothing = openCypherInvalid(Seq(error))

  def openCypherFailure(errors: Seq[SemanticErrorDef]): Nothing = throw EvaluationFailedException(errors)

  def openCypherFailure(error: SemanticErrorDef): Nothing = openCypherFailure(Seq(error))

  def openCypherUnexpected(exp: String, pos: InputPosition): Nothing = openCypherInvalid(SemanticError(s"Expected: $exp", pos))

  def openCypherUnexpected(exp: String, got: String, pos: InputPosition): Nothing = openCypherInvalid(SemanticError(s"Expected: $exp, got: $got", pos))

  def openCypherUnexpected(exp: String, got: String, in: String, pos: InputPosition): Nothing = openCypherInvalid(SemanticError(s"Expected: $exp, got: $got, in: $in", pos))

  def openCypherUnexpected(exp: String, got: ASTNode): Nothing = openCypherUnexpected(exp, got.position)

  def openCypherUnknownFunction(qualifiedName : String, pos: InputPosition): Nothing = openCypherFailure(SemanticError(s"Unknown function '$qualifiedName'", pos))

  def wrongType(exp: String, got: String): Nothing = throw new CypherTypeException(s"Expected: $exp, got: $got")

  def wrongArity(exp: Int, got: Int, pos: InputPosition): Nothing = syntax(s"$exp arguments", s"$got arguments", pos)

  def syntax(msg: String, query: String, pos: InputPosition): Nothing = throw new SyntaxException(msg, query, pos.offset)

  def semantic(message: String) = throw new InvalidSemanticsException(message)

  def ddlNotSupported(ddl: CatalogDDL) = throw new DatabaseAdministrationException(
    s"This is an administration command and it should be executed against the system database: ${ddl.name}")

  def notSupported(feature:String) = semantic(s"$feature not supported in Fabric database")

  def entityNotFound(kind: String, needle: String): Nothing = throw new EntityNotFoundException(s"$kind not found: $needle")

  /** Attaches position and query info to exceptions, if it is missing */
  def errorContext[T](query: String, node: ASTNode)(block: => T): T =
    try block catch {
      case e: HasErrors => throw e.update {
        case SemanticError(msg, InputPosition.NONE, _*) => syntax(msg, query, node.position)
        case SemanticError(msg, pos, _*) => syntax(msg, query, pos)
        case FeatureError(msg, InputPosition.NONE) => syntax(msg, query, node.position)
        case FeatureError(msg, pos) => syntax(msg, query, pos)
        case o => o
      }
    }

  def show(n: CatalogName): String = n.parts.mkString(".")

  def show(t: CypherType): String = t.toNeoTypeString

  def show(av: AnyValue): String = av match {
    case v: Value => v.prettyPrint()
    case x        => x.getTypeName
  }

  def show(a: Catalog.Arg[_]) = s"${a.name}: ${a.tpe.getSimpleName}"

  def show(seq: Seq[_]): String = seq.map {
    case v: AnyValue       => show(v)
    case a: Catalog.Arg[_] => show(a)
  }.mkString("(", ",", ")")
}
