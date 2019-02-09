/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.runtime.spec

import org.neo4j.cypher.internal.v4_0.util.test_helpers.{CypherFunSuite, TestName}
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values

class RowsMatcherTest extends CypherFunSuite with TestName
{
  test("AnyRowsMatcher") {
    AnyRowsMatcher.matches(Array[String](), NO_ROWS) should be(true)
    AnyRowsMatcher.matches(Array("a"), NO_ROWS) should be(true)
    AnyRowsMatcher.matches(Array("a"), Array(row(1))) should be(true)
    AnyRowsMatcher.matches(Array("X", "Y", "Z"), Array(
      row(1, 0, 4),
      row(4, 0, 4),
      row(1, 2, 4),
      row(2, 0, 4)
    )) should be(true)
  }

  test("NoRowsMatcher") {
    NoRowsMatcher.matches(Array[String](), NO_ROWS) should be(true)
    NoRowsMatcher.matches(Array("a"), NO_ROWS) should be(true)
    NoRowsMatcher.matches(Array("a"), Array(row(1))) should be(false)
    NoRowsMatcher.matches(Array("X", "Y", "Z"), Array(
      row(1, 0, 4),
      row(4, 0, 4),
      row(1, 2, 4),
      row(2, 0, 4)
    )) should be(false)
  }

  test("EqualInAnyOrder") {
    EqualInAnyOrder(NO_ROWS).matches(Array[String](), NO_ROWS) should be(true)
    EqualInAnyOrder(NO_ROWS).matches(Array("a"), NO_ROWS) should be(true)
    EqualInAnyOrder(NO_ROWS).matches(Array("a"), Array(row(1))) should be(false)
    EqualInAnyOrder(Array(row(1))).matches(Array("a"), Array(row(1))) should be(true)
    EqualInAnyOrder(Array(row(2))).matches(Array("a"), Array(row(1))) should be(false)
    EqualInAnyOrder(Array(row(1, 0, 4))).matches(Array("X", "Y", "Z"), Array(row(1, 0, 4))) should be(true)
    EqualInAnyOrder(Array(row(1, 0, 5))).matches(Array("X", "Y", "Z"), Array(row(1, 0, 4))) should be(false)

    val rows = Array(
      row(1, 0, 4),
      row(4, 0, 4),
      row(4, 0, 4),
      row(1, 2, 4),
      row(2, 0, 4))

    EqualInAnyOrder(rows).matches(Array("X", "Y", "Z"), rows) should be(true)
    EqualInAnyOrder(rows).matches(Array("X", "Y", "Z"), rows.tail :+ rows.head) should be(true)
    EqualInAnyOrder(rows).matches(Array("X", "Y", "Z"), rows.reverse) should be(true)
    EqualInAnyOrder(rows).matches(Array("X", "Y", "Z"), rows :+ rows.head) should be(false)
  }

  test("EqualInOrder") {
    EqualInOrder(NO_ROWS).matches(Array[String](), NO_ROWS) should be(true)
    EqualInOrder(NO_ROWS).matches(Array("a"), NO_ROWS) should be(true)
    EqualInOrder(NO_ROWS).matches(Array("a"), Array(row(1))) should be(false)
    EqualInOrder(Array(row(1))).matches(Array("a"), Array(row(1))) should be(true)
    EqualInOrder(Array(row(2))).matches(Array("a"), Array(row(1))) should be(false)
    EqualInOrder(Array(row(1, 0, 4))).matches(Array("X", "Y", "Z"), Array(row(1, 0, 4))) should be(true)
    EqualInOrder(Array(row(1, 0, 5))).matches(Array("X", "Y", "Z"), Array(row(1, 0, 4))) should be(false)

    val rows = Array(
      row(1, 0, 4),
      row(4, 0, 4),
      row(4, 0, 4),
      row(1, 2, 4),
      row(2, 0, 4))

    EqualInAnyOrder(rows).matches(Array("X", "Y", "Z"), rows) should be(true)
    EqualInOrder(rows).matches(Array("X", "Y", "Z"), rows.tail :+ rows.head) should be(false)
    EqualInOrder(rows).matches(Array("X", "Y", "Z"), rows.reverse) should be(false)
    EqualInOrder(rows).matches(Array("X", "Y", "Z"), rows :+ rows.head) should be(false)
  }

  test("GroupBy") {
    assertOkSingleRow(new GroupBy("a"))

    val rows = Array(
      row(1, 0, 4),
      row(4, 0, 4),
      row(4, 0, 4),
      row(1, 2, 4),
      row(2, 2, 4))

    new GroupBy("a").matches(Array("a", "b", "c"), rows) should be(false)
    new GroupBy("b").matches(Array("a", "b", "c"), rows) should be(true)
    new GroupBy("c").matches(Array("a", "b", "c"), rows) should be(true)
  }

  test("Ascending") {
    assertOkSingleRow(new Ascending("a"))

    val rows = Array(
      row(1, 0, 1),
      row(4, 0, 2),
      row(4, 0, 3),
      row(1, 2, 4),
      row(2, 2, 5))

    new Ascending("a").matches(Array("a", "b", "c"), rows) should be(false)
    new Ascending("b").matches(Array("a", "b", "c"), rows) should be(true)
    new Ascending("c").matches(Array("a", "b", "c"), rows) should be(true)
  }

  test("Descending") {
    assertOkSingleRow(new Descending("a"))

    val rows = Array(
      row(5, 1, 5),
      row(4, 1, 4),
      row(4, 1, 3),
      row(1, 0, 2),
      row(2, 0, 1))

    new Descending("a").matches(Array("a", "b", "c"), rows) should be(false)
    new Descending("b").matches(Array("a", "b", "c"), rows) should be(true)
    new Descending("c").matches(Array("a", "b", "c"), rows) should be(true)
  }

  test("ComplexOrder") {
    val rows = Array(
      row(5, 1, 4),
      row(4, 1, 4),
      row(4, 1, 5),
      row(4, 0, 5),
      row(2, 6, 5),
      row(2, 5, 5),
      row(2, 4, 5),
      row(2, 3, 6),
      row(2, 2, 6),
      row(2, 1, 6))

    new GroupBy("a").desc("b").asc("c").matches(Array("a", "b", "c"), rows) should be(true)
    new GroupBy("c").desc("a").matches(Array("a", "b", "c"), rows) should be(true)
    new GroupBy("c").groupBy("b").matches(Array("a", "b", "c"), rows) should be(true)
    new Ascending("c").desc("a").matches(Array("a", "b", "c"), rows) should be(true)
    new Ascending("c").desc("b").matches(Array("a", "b", "c"), rows) should be(false)
    new Descending("a").desc("b").asc("c").matches(Array("a", "b", "c"), rows) should be(true)
  }

  test("ComplexOrder2") {
    val rows = Array(
      row(1, 1, 1),
      row(1, 1, 2),
      row(1, 2, 1),
      row(1, 2, 2),
      row(1, 3, 1),
      row(1, 3, 2),
      row(2, 1, 1),
      row(2, 1, 2),
      row(2, 2, 1),
      row(2, 2, 2),
      row(2, 3, 1),
      row(2, 3, 2),
      row(3, 1, 1),
      row(3, 1, 2),
      row(3, 2, 1),
      row(3, 2, 2),
      row(3, 3, 1),
      row(3, 3, 2))

    new GroupBy("a").groupBy("b").groupBy("c").matches(Array("a", "b", "c"), rows) should be(true)
    new Ascending("a").asc("b").asc("c").matches(Array("a", "b", "c"), rows) should be(true)
    new Descending("a").desc("b").desc("c").matches(Array("a", "b", "c"), rows.reverse) should be(true)
  }

  private def assertOkSingleRow(columnAMatcher: RowOrderMatcher): Unit = {
    columnAMatcher.matches(Array[String](), NO_ROWS) should be(true)
    columnAMatcher.matches(Array("a"), NO_ROWS) should be(true)
    columnAMatcher.matches(Array("a"), Array(row(1))) should be(true)
    columnAMatcher.matches(Array("a", "b", "c"), Array(row(1, 0, 4))) should be(true)
    columnAMatcher.matches(Array("x", "a", "c"), Array(row(1, 0, 4))) should be(true)
    intercept[IllegalArgumentException](columnAMatcher.matches(Array("X"), Array(row(1))))
  }

  private val NO_ROWS = IndexedSeq[Array[AnyValue]]()

  private def row(values: Any*): Array[AnyValue] =
    values.map(Values.of).toArray
}
