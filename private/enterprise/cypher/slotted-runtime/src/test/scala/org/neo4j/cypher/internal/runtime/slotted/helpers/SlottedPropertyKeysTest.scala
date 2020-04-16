/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.slotted.helpers

import org.mockito.Mockito.when
import org.neo4j.cypher.internal.runtime.DbAccess
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class SlottedPropertyKeysTest extends CypherFunSuite {
  test("should find resolved keys") {
    // given
    val keys = new SlottedPropertyKeys(Seq(3 -> 30, 1 -> 10, 2 -> 20), Seq.empty)

    // when
    val dbAccess = mock[DbAccess]

    // then
    keys.accept(dbAccess, 1) shouldBe true
    keys.offset shouldBe 10
    keys.accept(dbAccess, 3) shouldBe true
    keys.offset shouldBe 30
    keys.accept(dbAccess, 2) shouldBe true
    keys.offset shouldBe 20
    keys.accept(dbAccess, 4) shouldBe false
  }

  test("should resolve keys") {
    // given
    val keys = new SlottedPropertyKeys(Seq(4 -> 40, 1 -> 10, 2 -> 20), Seq("3" -> 30, "5" -> 50))

    // when
    val dbAccess = mock[DbAccess]
    when(dbAccess.propertyKey("3")).thenReturn(3)
    when(dbAccess.propertyKey("5")).thenReturn(5)

    // then
    keys.accept(dbAccess, 1) shouldBe true
    keys.offset shouldBe 10
    keys.accept(dbAccess, 3) shouldBe true
    keys.offset shouldBe 30
    keys.accept(dbAccess, 5) shouldBe true
    keys.offset shouldBe 50
    keys.accept(dbAccess, 2) shouldBe true
    keys.offset shouldBe 20
    keys.accept(dbAccess, 4) shouldBe true
    keys.offset shouldBe 40
  }

  test("should partially resolve keys") {
    // given
    val keys = new SlottedPropertyKeys(Seq(4 -> 40, 1 -> 10, 2 -> 20), Seq("3" -> 30, "5" -> 50))

    // when
    val dbAccess = mock[DbAccess]
    when(dbAccess.propertyKey("3")).thenReturn(3)
    when(dbAccess.propertyKey("5")).thenReturn(-1)

    // then
    keys.accept(dbAccess, 1) shouldBe true
    keys.offset shouldBe 10
    keys.accept(dbAccess, 3) shouldBe true
    keys.offset shouldBe 30
    keys.accept(dbAccess, 5) shouldBe false
    keys.accept(dbAccess, 2) shouldBe true
    keys.offset shouldBe 20
    keys.accept(dbAccess, 4) shouldBe true
    keys.offset shouldBe 40
  }

}
