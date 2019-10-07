/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel

import org.neo4j.codegen.api.CodeGeneration.{ByteCodeGeneration, CodeSaver}
import org.neo4j.codegen.api.IntermediateRepresentation._
import org.neo4j.codegen.api.{IntermediateRepresentation, Load}
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.runtime.compiled.expressions.VariableNamer
import org.neo4j.cypher.internal.runtime.morsel.operators.MorselUnitTest
import org.neo4j.cypher.internal.v4_0.util.symbols.{CTAny, CTInteger, CTNode, CTRelationship, CTString}
import org.scalatest.matchers.{MatchResult, Matcher}
import org.neo4j.cypher.internal.runtime.morsel.OperatorExpressionCompilerTest.matchIR

class OperatorExpressionCompilerTest extends MorselUnitTest {

  def createOperatorExpressionCompiler(slots: SlotConfiguration, maybeInputSlots: Option[SlotConfiguration] = None): OperatorExpressionCompiler = {
    val inputSlots = maybeInputSlots.getOrElse(slots)
    val readOnly = true
    val codeGenerationMode = ByteCodeGeneration(new CodeSaver(false, false))
    val namer = new VariableNamer

    val expressionCompiler = new OperatorExpressionCompiler(slots, inputSlots, readOnly, codeGenerationMode, namer) // NOTE: We assume slots is the same within an entire pipeline
    expressionCompiler
  }

  val aSlotConfiguration =
    SlotConfiguration.empty
      .newLong("x", nullable = false, CTNode)
      .newLong("y", nullable = false, CTRelationship)
      .newLong("z", nullable = true, CTNode)
      .newReference("a", nullable = false, CTInteger)
      .newReference("b", nullable = false, CTString)
      .newReference("c", nullable = true, CTAny)

  test("should map long slot to local") {
    // Given
    val slots = aSlotConfiguration
    val oec = createOperatorExpressionCompiler(slots)

    0 until 3 foreach { i =>
      // When
      oec.getLongAt(i)
      val getSecondTimeIr = oec.getLongAt(i)

      // Then
      val local = longSlotLocal(i)
      oec.getAllLocalsForLongSlots shouldEqual (0 to i).map(j => (j, longSlotLocal(j)))
      getSecondTimeIr should matchIR(Load(local))
    }
  }

  test("should map ref slot to local") {
    // Given
    val slots = aSlotConfiguration
    val oec = createOperatorExpressionCompiler(slots)

    0 until 3 foreach { i =>
      // When
      oec.getRefAt(i)
      val getSecondTimeIr = oec.getRefAt(i)

      // Then
      val local = refSlotLocal(i)
      oec.getAllLocalsForRefSlots shouldEqual (0 to i).map(j => (j, refSlotLocal(j))) // Each iteration should add one more local
      getSecondTimeIr should matchIR(Load(local))
    }
  }

  test("should handle isolated scope") {
    // Given
    val slots = aSlotConfiguration
    val oec = createOperatorExpressionCompiler(slots)

    val long0 = longSlotLocal(0)
    val ref0 = refSlotLocal(0)
    val long1 = longSlotLocal(1)
    val ref1 = refSlotLocal(1)

    // When
    oec.getLongAt(0)
    oec.getRefAt(0)

    oec.beginScope("scope1")

    // Then should still see locals in parent scope
    oec.getLongAt(0) should matchIR(Load(long0))
    oec.getRefAt(0) should matchIR(Load(ref0))

    // When
    oec.getLongAt(1)
    oec.getRefAt(1)

    // Then should see locals in scope1
    oec.getLongAt(1) should matchIR(Load(long1))
    oec.getRefAt(1) should matchIR(Load(ref1))

    // Then getAll... should _only_ return locals in scope1
    oec.getAllLocalsForLongSlots shouldEqual Seq((1, long1))
    oec.getAllLocalsForRefSlots shouldEqual Seq((1, ref1))

    // When
    val continuationState = oec.endScope(mergeIntoParentScope = false)

    // Then coninuationState should have 3 fields
    continuationState.fields should have size 3 // long1 + ref1 + boolean state flag

    // Then getAll... should _only_ return locals in root scope
    oec.getAllLocalsForLongSlots shouldEqual Seq((0, long0))
    oec.getAllLocalsForRefSlots shouldEqual Seq((0, ref0))
  }

  test("should handle merged scope") {
    // Given
    val slots = aSlotConfiguration
    val oec = createOperatorExpressionCompiler(slots)

    val long0 = longSlotLocal(0)
    val ref0 = refSlotLocal(0)
    val long1 = longSlotLocal(1)
    val ref1 = refSlotLocal(1)

    // When
    oec.getLongAt(0)
    oec.getRefAt(0)

    oec.beginScope("scope1")

    // Then should still see locals in parent scope
    oec.getLongAt(0) should matchIR(Load(long0))
    oec.getRefAt(0) should matchIR(Load(ref0))

    // When
    oec.getLongAt(1)
    oec.getRefAt(1)

    // Then should see locals in scope1
    oec.getLongAt(1) should matchIR(Load(long1))
    oec.getRefAt(1) should matchIR(Load(ref1))

    // Then getAll... should _only_ return locals in scope1
    oec.getAllLocalsForLongSlots shouldEqual Seq((1, long1))
    oec.getAllLocalsForRefSlots shouldEqual Seq((1, ref1))

    // When
    val continuationState = oec.endScope(mergeIntoParentScope = true)

    // Then coninuationState should have 3 fields
    continuationState.fields should have size 3 // long1 + ref1 + boolean state flag

    // Then getAll... should return locals from root scope _and_ scope1
    oec.getAllLocalsForLongSlots shouldEqual Seq((0, long0), (1, long1))
    oec.getAllLocalsForRefSlots shouldEqual Seq((0, ref0), (1, ref1))
  }

  test("should handle merge in nested isolated scope") {
    // Given
    val slots = aSlotConfiguration
    val oec = createOperatorExpressionCompiler(slots)

    val long0 = longSlotLocal(0)
    val ref0 = refSlotLocal(0)
    val long1 = longSlotLocal(1)
    val ref1 = refSlotLocal(1)
    val long2 = longSlotLocal(2)
    val ref2 = refSlotLocal(2)

    // When
    oec.getLongAt(0)
    oec.getRefAt(0)

    oec.beginScope("scope1")

    // Then should still see locals in parent scope
    oec.getLongAt(0) should matchIR(Load(long0))
    oec.getRefAt(0) should matchIR(Load(ref0))

    // When
    oec.getLongAt(1)
    oec.getRefAt(1)

    // Then should see locals in scope1
    oec.getLongAt(1) should matchIR(Load(long1))
    oec.getRefAt(1) should matchIR(Load(ref1))

    // Then getAll... should _only_ return locals in scope1
    oec.getAllLocalsForLongSlots shouldEqual Seq((1, long1))
    oec.getAllLocalsForRefSlots shouldEqual Seq((1, ref1))

    // When
    oec.beginScope("scope2")

    // Then should still see locals in parent scopes
    oec.getLongAt(0) should matchIR(Load(long0))
    oec.getRefAt(0) should matchIR(Load(ref0))
    oec.getLongAt(1) should matchIR(Load(long1))
    oec.getRefAt(1) should matchIR(Load(ref1))

    // When
    oec.getLongAt(2)
    oec.getRefAt(2)

    // Then should see locals in scope2
    oec.getLongAt(2) should matchIR(Load(long2))
    oec.getRefAt(2) should matchIR(Load(ref2))

    // When
    val continuationState2 = oec.endScope(mergeIntoParentScope = true)

    // Then coninuationState2 should have 3 fields
    continuationState2.fields should have size 3 // long2 + ref2 + boolean state flag

    // Then getAll... should _only_ return locals in scope1 + scope2
    oec.getAllLocalsForLongSlots shouldEqual Seq((1, long1), (2, long2))
    oec.getAllLocalsForRefSlots shouldEqual Seq((1, ref1), (2, ref2))

    // When
    val continuationState1 = oec.endScope(mergeIntoParentScope = false)

    // Then coninuationState1 should have 5 fields
    continuationState1.fields should have size 5 // long1 + ref1 + long2 + ref2 + boolean state flag

    // Then getAll... should _only_ return locals in root scope
    oec.getAllLocalsForLongSlots shouldEqual Seq((0, long0))
    oec.getAllLocalsForRefSlots shouldEqual Seq((0, ref0))
  }

  test("should handle merge in nested scopes") {
    // Given
    val slots = aSlotConfiguration
    val oec = createOperatorExpressionCompiler(slots)

    val long0 = longSlotLocal(0)
    val ref0 = refSlotLocal(0)
    val long1 = longSlotLocal(1)
    val ref1 = refSlotLocal(1)
    val long2 = longSlotLocal(2)
    val ref2 = refSlotLocal(2)

    // When
    oec.getLongAt(0)
    oec.getRefAt(0)

    oec.beginScope("scope1")

    // Then should still see locals in parent scope
    oec.getLongAt(0) should matchIR(Load(long0))
    oec.getRefAt(0) should matchIR(Load(ref0))

    // When
    oec.getLongAt(1)
    oec.getRefAt(1)

    // Then should see locals in scope1
    oec.getLongAt(1) should matchIR(Load(long1))
    oec.getRefAt(1) should matchIR(Load(ref1))

    // Then getAll... should _only_ return locals in scope1
    oec.getAllLocalsForLongSlots shouldEqual Seq((1, long1))
    oec.getAllLocalsForRefSlots shouldEqual Seq((1, ref1))

    // When
    oec.beginScope("scope2")

    // Then should still see locals in parent scopes
    oec.getLongAt(0) should matchIR(Load(long0))
    oec.getRefAt(0) should matchIR(Load(ref0))
    oec.getLongAt(1) should matchIR(Load(long1))
    oec.getRefAt(1) should matchIR(Load(ref1))

    // When
    oec.getLongAt(2)
    oec.getRefAt(2)

    // Then should see locals in scope2
    oec.getLongAt(2) should matchIR(Load(long2))
    oec.getRefAt(2) should matchIR(Load(ref2))

    // When
    val continuationState2 = oec.endScope(mergeIntoParentScope = true)

    // Then coninuationState2 should have 3 fields
    continuationState2.fields should have size 3 // long2 + ref2 + boolean state flag

    // Then getAll... should _only_ return locals in scope1 + scope2
    oec.getAllLocalsForLongSlots shouldEqual Seq((1, long1), (2, long2))
    oec.getAllLocalsForRefSlots shouldEqual Seq((1, ref1), (2, ref2))

    // When
    val continuationState1 = oec.endScope(mergeIntoParentScope = true)

    // Then coninuationState1 should have 5 fields
    continuationState1.fields should have size 5 // long1 + ref1 + long2 + ref2 + boolean state flag

    // Then getAll... should return locals from all scopes
    oec.getAllLocalsForLongSlots shouldEqual Seq((0, long0), (1, long1), (2, long2))
    oec.getAllLocalsForRefSlots shouldEqual Seq((0, ref0), (1, ref1), (2, ref2))
  }

  private def longSlotLocal(offset: Int): String =
    "longSlot" + offset

  private def refSlotLocal(offset: Int): String =
    "refSlot" + offset
}

object OperatorExpressionCompilerTest {
  def matchIR(ir: IntermediateRepresentation): IrMatcher = IrMatcher(ir)
}

case class IrMatcher(expected: IntermediateRepresentation) extends Matcher[IntermediateRepresentation] {
  override def apply(actual: IntermediateRepresentation): MatchResult = {
    val matches = expected == actual
    val rawFailureMessage = s"Expected a\n  ${expected}\nbut got a\n  ${actual}"
    MatchResult(matches,
      rawFailureMessage,
      rawNegatedFailureMessage = rawFailureMessage)
  }
}

