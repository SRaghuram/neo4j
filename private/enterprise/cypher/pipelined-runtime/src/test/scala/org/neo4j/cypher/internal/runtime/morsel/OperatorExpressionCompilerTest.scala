/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel

import org.neo4j.codegen.api.CodeGeneration.{ByteCodeGeneration, CodeGenerationMode, CodeSaver}
import org.neo4j.codegen.api.IntermediateRepresentation._
import org.neo4j.codegen.api.{IntermediateRepresentation, Load}
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.physicalplanning.ast.{SlottedCachedProperty, SlottedCachedPropertyWithoutPropertyToken}
import org.neo4j.cypher.internal.runtime.compiled.expressions.VariableNamer
import org.neo4j.cypher.internal.runtime.morsel.OperatorExpressionCompilerTest.matchIR
import org.neo4j.cypher.internal.runtime.morsel.operators.MorselUnitTest
import org.neo4j.cypher.internal.runtime.morsel.operators.OperatorCodeGenHelperTemplates.{INPUT_MORSEL, UNINITIALIZED_LONG_SLOT_VALUE, UNINITIALIZED_REF_SLOT_VALUE}
import org.neo4j.cypher.internal.v4_0.expressions.{NODE_TYPE, PropertyKeyName}
import org.neo4j.cypher.internal.v4_0.util.InputPosition.NONE
import org.neo4j.cypher.internal.v4_0.util.symbols.{CTAny, CTInteger, CTNode, CTRelationship, CTString}
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Value
import org.scalatest.matchers.{MatchResult, Matcher}

class OperatorExpressionCompilerTest extends MorselUnitTest {

  def createOperatorExpressionCompiler(slots: SlotConfiguration, maybeInputSlots: Option[SlotConfiguration] = None): TestOperatorExpressionCompiler = {
    val inputSlots = maybeInputSlots.getOrElse(slots)
    val readOnly = true
    val codeGenerationMode = ByteCodeGeneration(new CodeSaver(false, false))
    val namer = new VariableNamer

    val expressionCompiler = new TestOperatorExpressionCompiler(slots, inputSlots, readOnly, codeGenerationMode, namer)
    expressionCompiler
  }

  val cachedProperties: Array[SlottedCachedProperty] =
    Array(
      SlottedCachedPropertyWithoutPropertyToken("a", PropertyKeyName("prop")(NONE), 0, false, "prop", 0, NODE_TYPE),
      SlottedCachedPropertyWithoutPropertyToken("b", PropertyKeyName("prop")(NONE), 1, false, "prop", 1, NODE_TYPE),
      SlottedCachedPropertyWithoutPropertyToken("c", PropertyKeyName("prop")(NONE), 2, false, "prop", 2, NODE_TYPE)
    )

  val getFromStoreIr = print(constant("getFromStore"))
  val setLongIr = constant(42L)
  val setRefIr = constant("hello")

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

  test("should map cached ref slot to local - get from input context") {
    // Given
    val slots = aSlotConfiguration.copy()
      .newCachedProperty(cachedProperties(0))
      .newCachedProperty(cachedProperties(1))
      .newCachedProperty(cachedProperties(2))

    val oec = createOperatorExpressionCompiler(slots)

    0 until 3 foreach { i =>
      oec.resetCounts()

      // When
      val getFirstTimeIr = oec.getCachedPropertyAt(cachedProperties(i), getFromStoreIr)

      // Then first time IR should get from input context (i.e. not from store and not from local)
      oec.initCachedPropertyFromContextCount shouldEqual 1
      oec.initCachedPropertyFromStoreCount shouldEqual 0
      oec.loadLocalCachedPropertyCount shouldEqual 0

      // When
      val getSecondTimeIr = oec.getCachedPropertyAt(cachedProperties(i), getFromStoreIr)

      // Then second time IR should get from local
      oec.initCachedPropertyFromContextCount shouldEqual 1
      oec.initCachedPropertyFromStoreCount shouldEqual 0
      oec.loadLocalCachedPropertyCount shouldEqual 1

      // Then
      val local = refSlotLocal(i)
      oec.getAllLocalsForCachedProperties shouldEqual (0 to i).map(j => (j, refSlotLocal(j))) // Each iteration should add one more cached property local
      oec.getAllLocalsForRefSlots shouldEqual (0 to i).map(j => (j, refSlotLocal(j))) // Each iteration should add one more local
    }
  }

  test("should map cached ref slot to local - get from store") {
    // Given
    val slots = aSlotConfiguration.copy()
      .newCachedProperty(cachedProperties(0))
      .newCachedProperty(cachedProperties(1))
      .newCachedProperty(cachedProperties(2))

    // Input slot configuration does not have cached properties
    val oec = createOperatorExpressionCompiler(slots, Some(aSlotConfiguration))

    0 until 3 foreach { i =>
      oec.resetCounts()

      // When
      val getFirstTimeIr = oec.getCachedPropertyAt(cachedProperties(i), getFromStoreIr)

      // Then first time IR should get from store (i.e. not from context and not from local)
      oec.initCachedPropertyFromContextCount shouldEqual 0
      oec.initCachedPropertyFromStoreCount shouldEqual 1
      oec.loadLocalCachedPropertyCount shouldEqual 0

      // When
      val getSecondTimeIr = oec.getCachedPropertyAt(cachedProperties(i), getFromStoreIr)

      // Then second time IR should get from local (i.e. not from context and not from store)
      oec.initCachedPropertyFromContextCount shouldEqual 0
      oec.initCachedPropertyFromStoreCount shouldEqual 1
      oec.loadLocalCachedPropertyCount shouldEqual 1

      // Then
      val local = refSlotLocal(i)
      oec.getAllLocalsForCachedProperties shouldEqual (0 to i).map(j => (j, refSlotLocal(j))) // Each iteration should add one more cached property local
      oec.getAllLocalsForRefSlots shouldEqual (0 to i).map(j => (j, refSlotLocal(j))) // Each iteration should add one more local
      getFirstTimeIr should matchIR(block(assign(local, getFromStoreIr), cast[Value](load(local))))
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
    val continuationState = oec.endInitializationScope(mergeIntoParentScope = false)

    // Then continuationState should have 3 fields
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
    val continuationState = oec.endInitializationScope(mergeIntoParentScope = true)

    // Then continuationState should have 3 fields
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
    val continuationState2 = oec.endInitializationScope(mergeIntoParentScope = true)

    // Then continuationState2 should have 3 fields
    continuationState2.fields should have size 3 // long2 + ref2 + boolean state flag

    // Then getAll... should _only_ return locals in scope1 + scope2
    oec.getAllLocalsForLongSlots shouldEqual Seq((1, long1), (2, long2))
    oec.getAllLocalsForRefSlots shouldEqual Seq((1, ref1), (2, ref2))

    // When
    val continuationState1 = oec.endInitializationScope(mergeIntoParentScope = false)

    // Then continuationState1 should have 5 fields
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
    val continuationState2 = oec.endInitializationScope(mergeIntoParentScope = true)

    // Then continuationState2 should have 3 fields
    continuationState2.fields should have size 3 // long2 + ref2 + boolean state flag

    // Then getAll... should _only_ return locals in scope1 + scope2
    oec.getAllLocalsForLongSlots shouldEqual Seq((1, long1), (2, long2))
    oec.getAllLocalsForRefSlots shouldEqual Seq((1, ref1), (2, ref2))

    // When
    val continuationState1 = oec.endInitializationScope(mergeIntoParentScope = true)

    // Then continuationState1 should have 5 fields
    continuationState1.fields should have size 5 // long1 + ref1 + long2 + ref2 + boolean state flag

    // Then getAll... should return locals from all scopes
    oec.getAllLocalsForLongSlots shouldEqual Seq((0, long0), (1, long1), (2, long2))
    oec.getAllLocalsForRefSlots shouldEqual Seq((0, ref0), (1, ref1), (2, ref2))
  }

  test("should handle merge in nested scopes with cached property") {
    // Given
    val slots = aSlotConfiguration.copy().newCachedProperty(cachedProperties(2))
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

    // Then getAll... should return locals in root scope
    oec.getAllLocalsForLongSlots shouldEqual Seq((0, long0))
    oec.getAllLocalsForRefSlots shouldEqual Seq((0, ref0))
    oec.getAllLocalsForCachedProperties shouldBe empty

    // When
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
    oec.getAllLocalsForCachedProperties shouldBe empty

    // When
    oec.beginScope("scope2")

    // Then should still see locals in parent scopes
    oec.getLongAt(0) should matchIR(Load(long0))
    oec.getRefAt(0) should matchIR(Load(ref0))
    oec.getLongAt(1) should matchIR(Load(long1))
    oec.getRefAt(1) should matchIR(Load(ref1))

    // When
    oec.getLongAt(2)
    oec.getCachedPropertyAt(cachedProperties(2), getFromStoreIr)

    // Then should see locals in scope2
    oec.getLongAt(2) should matchIR(Load(long2))
    oec.getRefAt(2) should matchIR(Load(ref2))

    // Then getAll... should _only_ return locals in scope2
    oec.getAllLocalsForLongSlots shouldEqual Seq((2, long2))
    oec.getAllLocalsForRefSlots shouldEqual Seq((2, ref2))
    oec.getAllLocalsForCachedProperties shouldEqual Seq((2, ref2))

    // When
    val continuationState2 = oec.endInitializationScope(mergeIntoParentScope = true)

    // Then continuationState2 should have 3 fields
    continuationState2.fields should have size 3 // long2 + ref2 + boolean state flag

    // Then getAll... should _only_ return locals in scope1 + scope2
    oec.getAllLocalsForLongSlots shouldEqual Seq((1, long1), (2, long2))
    oec.getAllLocalsForRefSlots shouldEqual Seq((1, ref1), (2, ref2))
    oec.getAllLocalsForCachedProperties shouldEqual Seq((2, ref2))

    // When
    val continuationState1 = oec.endInitializationScope(mergeIntoParentScope = true)

    // Then continuationState1 should have 5 fields
    continuationState1.fields should have size 5 // long1 + ref1 + long2 + ref2 + boolean state flag

    // Then getAll... should return locals from all scopes
    oec.getAllLocalsForLongSlots shouldEqual Seq((0, long0), (1, long1), (2, long2))
    oec.getAllLocalsForRefSlots shouldEqual Seq((0, ref0), (1, ref1), (2, ref2))
    oec.getAllLocalsForCachedProperties shouldEqual Seq((2, ref2))
  }

  test("should handle writeLocalsToSlots in nested scope") {
    val cachedProp9 = SlottedCachedPropertyWithoutPropertyToken("r9", PropertyKeyName("prop")(NONE), 9, false, "prop", 9, NODE_TYPE)

    // Given
    val slots = SlotConfiguration.empty
      .newLong("l0", nullable = false, CTNode)
      .newLong("l1", nullable = false, CTRelationship)
      .newLong("l2", nullable = true, CTNode)
      .newLong("l3", nullable = false, CTNode)
      .newLong("l4", nullable = false, CTRelationship)
      .newLong("l5", nullable = true, CTNode)
      .newLong("l6", nullable = false, CTNode)
      .newLong("l7", nullable = false, CTRelationship)
      .newLong("l8", nullable = true, CTNode)
      .newReference("r0", nullable = false, CTInteger)
      .newReference("r1", nullable = false, CTString)
      .newReference("r2", nullable = true, CTAny)
      .newReference("r3", nullable = false, CTInteger)
      .newReference("r4", nullable = false, CTString)
      .newReference("r5", nullable = true, CTAny)
      .newReference("r6", nullable = false, CTInteger)
      .newReference("r7", nullable = false, CTString)
      .newReference("r8", nullable = true, CTAny)
      .newCachedProperty(cachedProp9)

    val oec = createOperatorExpressionCompiler(slots)

    // When
    oec.getLongAt(0)
    oec.getLongAt(1)
    oec.getRefAt(0)
    oec.getRefAt(1)
    oec.setLongAt(1, setLongIr)
    oec.setLongAt(2, setLongIr)
    oec.setRefAt(1, setRefIr)
    oec.setRefAt(2, setRefIr)

    oec.beginScope("scope1")

    oec.getLongAt(3)
    oec.getLongAt(4)
    oec.getRefAt(3)
    oec.getRefAt(4)
    oec.setLongAt(5, setLongIr)
    oec.setLongAt(4, setLongIr)
    oec.setRefAt(5, setRefIr)
    oec.setRefAt(4, setRefIr)

    oec.getLongAt(5)

    oec.beginScope("scope2")

    oec.getLongAt(6)
    oec.getLongAt(7)
    oec.getRefAt(7)
    oec.getRefAt(6)
    oec.setLongAt(7, setLongIr)
    oec.setLongAt(8, setLongIr)
    oec.setRefAt(7, setRefIr)
    oec.setRefAt(8, setRefIr)

    oec.getRefAt(8)
    oec.getCachedPropertyAt(cachedProp9, getFromStoreIr)

    // When
    val writeIR = oec.writeLocalsToSlots()

    // Then
    writeIR shouldEqual block(
      oec.setLongInExecutionContext(1, load(longSlotLocal(1))),
      oec.setLongInExecutionContext(2, load(longSlotLocal(2))),
      oec.setLongInExecutionContext(4, load(longSlotLocal(4))),
      oec.setLongInExecutionContext(5, load(longSlotLocal(5))),
      oec.setLongInExecutionContext(7, load(longSlotLocal(7))),
      oec.setLongInExecutionContext(8, load(longSlotLocal(8))),
      oec.setRefInExecutionContext(1, load(refSlotLocal(1))),
      oec.setRefInExecutionContext(2, load(refSlotLocal(2))),
      oec.setRefInExecutionContext(4, load(refSlotLocal(4))),
      oec.setRefInExecutionContext(5, load(refSlotLocal(5))),
      oec.setRefInExecutionContext(7, load(refSlotLocal(7))),
      oec.setRefInExecutionContext(8, load(refSlotLocal(8))),
      oec.setRefInExecutionContext(9, load(refSlotLocal(9))),
    )

    // When
    val localState2 = oec.endScope()

    // Then
    localState2.locals shouldEqual List(
      variable[Long]("longSlot6", oec.getLongFromExecutionContext(6, loadField(INPUT_MORSEL))),
      variable[Long]("longSlot7", oec.getLongFromExecutionContext(7, loadField(INPUT_MORSEL))),
      variable[Long]("longSlot8", UNINITIALIZED_LONG_SLOT_VALUE),
      variable[AnyValue]("refSlot6", oec.getRefFromExecutionContext(6, loadField(INPUT_MORSEL))),
      variable[AnyValue]("refSlot7", oec.getRefFromExecutionContext(7, loadField(INPUT_MORSEL))),
      variable[AnyValue]("refSlot8", UNINITIALIZED_REF_SLOT_VALUE),
      variable[AnyValue]("refSlot9", UNINITIALIZED_REF_SLOT_VALUE), // Cached properties are currently always initialized at runtime
    )

    // When
    val localState1 = oec.endScope()

    // Then
    localState1.locals shouldEqual List(
      variable[Long]("longSlot3", oec.getLongFromExecutionContext(3, loadField(INPUT_MORSEL))),
      variable[Long]("longSlot4", oec.getLongFromExecutionContext(4, loadField(INPUT_MORSEL))),
      variable[Long]("longSlot5", UNINITIALIZED_LONG_SLOT_VALUE),
      variable[AnyValue]("refSlot3", oec.getRefFromExecutionContext(3, loadField(INPUT_MORSEL))),
      variable[AnyValue]("refSlot4", oec.getRefFromExecutionContext(4, loadField(INPUT_MORSEL))),
      variable[AnyValue]("refSlot5", UNINITIALIZED_REF_SLOT_VALUE),
    )

    // When
    val localState0 = oec.endScope()

    // Then
    localState0.locals shouldEqual List(
      variable[Long]("longSlot0", oec.getLongFromExecutionContext(0, loadField(INPUT_MORSEL))),
      variable[Long]("longSlot1", oec.getLongFromExecutionContext(1, loadField(INPUT_MORSEL))),
      variable[Long]("longSlot2", UNINITIALIZED_LONG_SLOT_VALUE),
      variable[AnyValue]("refSlot0", oec.getRefFromExecutionContext(0, loadField(INPUT_MORSEL))),
      variable[AnyValue]("refSlot1", oec.getRefFromExecutionContext(1, loadField(INPUT_MORSEL))),
      variable[AnyValue]("refSlot2", UNINITIALIZED_REF_SLOT_VALUE),
    )
  }

  private def longSlotLocal(offset: Int): String =
    "longSlot" + offset

  private def refSlotLocal(offset: Int): String =
    "refSlot" + offset
}

object OperatorExpressionCompilerTest {
  def matchIR(ir: IntermediateRepresentation): IrMatcher = IrMatcher(ir)
  def matchBeginsWithIR(ir: IntermediateRepresentation): BeginsWithIrMatcher = BeginsWithIrMatcher(ir)
}

class TestOperatorExpressionCompiler(slots: SlotConfiguration, inputSlots: SlotConfiguration, readOnly: Boolean,
                                     codeGenerationMode: CodeGenerationMode, namer: VariableNamer)
  extends OperatorExpressionCompiler(slots, inputSlots, readOnly, codeGenerationMode, namer) {

  var initCachedPropertyFromStoreCount = 0
  var initCachedPropertyFromContextCount = 0
  var loadLocalCachedPropertyCount = 0

  override protected def didInitializeCachedPropertyFromStore(): Unit =
    initCachedPropertyFromStoreCount += 1
  override protected def didInitializeCachedPropertyFromContext(): Unit =
    initCachedPropertyFromContextCount += 1
  override protected def didLoadLocalCachedProperty(): Unit =
    loadLocalCachedPropertyCount += 1

  def resetCounts(): Unit = {
    initCachedPropertyFromStoreCount = 0
    initCachedPropertyFromContextCount = 0
    loadLocalCachedPropertyCount = 0
  }
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

case class BeginsWithIrMatcher(expected: IntermediateRepresentation) extends Matcher[IntermediateRepresentation] {
  override def apply(actual: IntermediateRepresentation): MatchResult = {
    val matches1 = expected.getClass == actual.getClass
    val matches2 = matches1 && expected.asInstanceOf[Product].productArity == actual.asInstanceOf[Product].productArity
    val matches = matches2

    val rawFailureMessage = s"Expected a\n  ${expected}\nbut got a\n  ${actual}"
    MatchResult(matches,
      rawFailureMessage,
      rawNegatedFailureMessage = rawFailureMessage)
  }
}

