/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.compiled.expressions

import org.neo4j.codegen.TypeReference
import org.neo4j.codegen.api.Block
import org.neo4j.codegen.api.Eq
import org.neo4j.codegen.api.Field
import org.neo4j.codegen.api.IntermediateRepresentation
import org.neo4j.codegen.api.IntermediateRepresentation.block
import org.neo4j.codegen.api.IntermediateRepresentation.condition
import org.neo4j.codegen.api.IntermediateRepresentation.declareAndAssign
import org.neo4j.codegen.api.IntermediateRepresentation.load
import org.neo4j.codegen.api.IntermediateRepresentation.noValue
import org.neo4j.codegen.api.IntermediateRepresentation.ternary
import org.neo4j.codegen.api.IntermediateRepresentation.typeRefOf
import org.neo4j.codegen.api.Load
import org.neo4j.codegen.api.LocalVariable
import org.neo4j.values.AnyValue

/**
 *
 *  A code chain representing an intermediate representation.
 *  - Only do null checks when needed
 *  - Only evaluate code when needed.
 *
 * Example: exists(n.prop)
 * ----------
 * 1) code chain for "n"
 *
 * 1.1) if n is nullable
 *    -> CodeLink("getLongAt(offset)", true)(START)
 * 1.2) else
 *    -> CodeLink("getLongAt(offset)", false)(START)
 *
 * ----------
 * 2) code chain for "n.prop". Declare and assign "n" and do null check if n is nullable.
 *
 * 2.1) if n is nullable:
 *        -> CodeLink("n.prop", true)(
 *            NullCheckLink("n == null")(
 *             CodeLink("val n = getLongAt(offset)", true)(START)
 *            )
 *           )
 * 2.2) else:
 *        -> CodeLink("n.prop", true)(
 *             CodeLink("val n = getLongAt(offset)", false)(START)
 *           )
 * ----------
 * 3) code chain for exists(n.prop). Evaluate "n.prop" code chain and assign to existsInput.
 *
 * 3.1) if n is nullable:
 *        -> CodeLink("val existsInput = { val n = getLongAt(0); if (n == null) null else n.prop }; existsInput != null", false)(START)
 *
 * 3.2) else:
 *        -> CodeLink("val existsInput = { val n = getLongAt(0); n.prop }; existsInput != null", false)(START)
 */
sealed trait CodeChain {
  def codeSet: Set[IntermediateRepresentation]
  def nullCheckSet: Set[IntermediateRepresentation]
  def fields: Set[Field]
  def variables: Set[LocalVariable]

  /**
   *
   * A: if(a == null) null else { if(a.prop == null) null { } }
   * B: if (b) { ... }
   *
   * B.rebaseOn(A) =>
   *  if(a == null) null else { if(a.prop == null) null { if (b) { ... }} }
   *
   */
  def rebaseOn(newBase: CodeChain): CodeChain

  /**
   * Extract the intermediate representation, local fields and variables
   */
  def extract(namer: VariableNamer): IRInfo

  /**
   * Add CodeLink on top of current CodeChain
   */
  def withCode(isNullable: Boolean,
               ir: IntermediateRepresentation): CodeLink = withCode(isNullable, Seq(ir), Set.empty, Set.empty)

  /**
   * Add CodeLink on top of current CodeChain
   */
  def withCode(isNullable: Boolean,
               irInfo: IRInfo): CodeLink = withCode(isNullable, Seq(irInfo.code), irInfo.fields, irInfo.localVariables)

  /**
   * Add CodeLink on top of current CodeChain
   */
  def withCode(isNullable: Boolean, ir: Seq[IntermediateRepresentation], fields: Set[Field], variables: Set[LocalVariable]): CodeLink

  /**
   * Add NullCheckLink if it does not already exist in the CodeChain
   */
  def withNullCheck(check: IntermediateRepresentation): CodeChain

  /**
   * Returns a list of all the links in the CodeChain
   */
  def toList(): List[CodeChain]

  /**
   * If we have the following code:
   * if(a) {
   *    if(b) {
   *      if(c) {
   *        ...
   *      }
   *    }
   * }
   *
   * pruneAt("if(c)") would yield:
   *
   * if(c) {
   *    ...
   * }
   *
   */
  def pruneAt(first: CodeChain): CodeChain

  /**
   * Check equality on both current link and inner code chain
   */
  def deepEq(codeChain: CodeChain): Boolean
}

sealed trait AbstractCodeChain extends CodeChain {
  override def extract(namer: VariableNamer): IRInfo = extractRec(Seq.empty, Set.empty, Set.empty, namer)

  /**
   * Recursive method to extract the intermediate representation, local fields and variables
   */
  def extractRec(code: Seq[IntermediateRepresentation],
                 fields: Set[Field],
                 variables: Set[LocalVariable],
                 namer: VariableNamer): IRInfo

  override def toList(): List[CodeChain] = toList(List.empty)

  /**
   * Recursive method to get a list of all the links in the CodeChain
   */
  def toList(tail: List[CodeChain]): List[CodeChain]

  override def withCode(isNullable: Boolean, ir: Seq[IntermediateRepresentation], fields: Set[Field], variables: Set[LocalVariable]): CodeLink =
    CodeLink(ir, isNullable, fields, variables)(this)

  override def withNullCheck(check: IntermediateRepresentation): CodeChain = {
    if (nullCheckSet(check)) {
      this
    } else {
      NullCheckLink(check)(this)
    }
  }
}

abstract class OuterCodeChain extends AbstractCodeChain {
  def inner: AbstractCodeChain

  override def deepEq(codeChain: CodeChain): Boolean =
    codeChain match {
      case occ: OuterCodeChain => this == occ && inner.deepEq(occ.inner)
      case _ => false
    }

  override def toList(tail: List[CodeChain]): List[CodeChain] = inner.toList(this :: tail)
}

case object START extends AbstractCodeChain {
  override def deepEq(codeChain: CodeChain) = codeChain == this
  override def fields: Set[Field] = Set.empty
  override def variables: Set[LocalVariable] = Set.empty
  override def rebaseOn(newBase: CodeChain): CodeChain = newBase
  override def codeSet: Set[IntermediateRepresentation] = Set.empty
  override def nullCheckSet: Set[IntermediateRepresentation] = Set.empty
  override def toList(tail: List[CodeChain]): List[CodeChain] = this :: tail
  override def pruneAt(first: CodeChain): CodeChain = START

  override def extractRec(code: Seq[IntermediateRepresentation],
                          fields: Set[Field],
                          variables: Set[LocalVariable],
                          namer: VariableNamer): IRInfo = {
    val codeIr = if (code.size == 1) code.head else block(code: _*)

    IRInfo(codeIr, fields, variables)
  }
}

case class CodeLink(code: Seq[IntermediateRepresentation], isNullable: Boolean, fields: Set[Field] = Set.empty, variables: Set[LocalVariable])
                   (override val inner: AbstractCodeChain) extends OuterCodeChain {

  override def rebaseOn(newBase: CodeChain): CodeLink =
    inner.rebaseOn(newBase).withCode(isNullable, code, fields, variables)

  override def pruneAt(first: CodeChain): CodeChain = {
    val newInner = if (this.eq(first)) {
      START
    } else {
      inner.pruneAt(first)
    }

    newInner.withCode(isNullable, code, fields, variables)
  }

  override lazy val codeSet: Set[IntermediateRepresentation] = inner.codeSet ++ code

  override def nullCheckSet: Set[IntermediateRepresentation] = inner.nullCheckSet

  def assignTo(typ: TypeReference, variableName: String): CodeLink =
    inner.withCode(isNullable, CodeChainExpressionCompiler.unnestAssignBlock(typ, variableName, code), fields, variables)

  def assignToAndNullCheck(typ: TypeReference, variableName: String, nullValue: IntermediateRepresentation): CodeChain =
    assignToAndNullCheck(typ, Load(variableName, typ), nullValue)

  def assignToAndNullCheck(typ: TypeReference, load: Load, nullValue: IntermediateRepresentation): CodeChain = {
    val assigned = assignTo(typ, load.variable)
    if (assigned.isNullable) {
      assigned.withNullCheck(Eq(load, nullValue))
    } else {
      assigned
    }
  }

  override def extractRec(x: Seq[IntermediateRepresentation],
                          y: Set[Field],
                          z: Set[LocalVariable],
                          namer: VariableNamer): IRInfo =
    inner.extractRec(code ++ x, y ++ fields, z ++ variables, namer)
}

case class NullCheckLink(check: IntermediateRepresentation)(override val inner: AbstractCodeChain) extends OuterCodeChain {
  override def fields: Set[Field] = Set.empty

  override def variables: Set[LocalVariable] = Set.empty

  override def extractRec(x: Seq[IntermediateRepresentation],
                          y: Set[Field],
                          z: Set[LocalVariable],
                          namer: VariableNamer): IRInfo = {
    val elseCode = if (x.size == 1) x.head else block(x: _*)

    val code = if(elseCode.isInstanceOf[Block]) {
      val retName = namer.nextVariableName("retVal")
      block(
        declareAndAssign(typeRefOf[AnyValue], retName, noValue),
        condition(IntermediateRepresentation.not(check))(IntermediateRepresentation.assign(retName, elseCode)),
        load[AnyValue](retName),
      )
    } else {
      ternary(check, noValue, elseCode)
    }

    inner.extractRec(Seq(code), y, z, namer)
  }

  override def pruneAt(first: CodeChain): CodeChain = {
    val newInner = if (this.eq(first)) {
      START
    } else {
      inner.pruneAt(first)
    }

    newInner.withNullCheck(check)
  }

  override def rebaseOn(newBase: CodeChain): CodeChain =
    inner.rebaseOn(newBase).withNullCheck(check)

  override def codeSet: Set[IntermediateRepresentation] = inner.codeSet

  override lazy val nullCheckSet: Set[IntermediateRepresentation] = inner.nullCheckSet + check
}

case class IRInfo(code: IntermediateRepresentation, fields: Set[Field] = Set.empty, localVariables: Set[LocalVariable] = Set.empty)