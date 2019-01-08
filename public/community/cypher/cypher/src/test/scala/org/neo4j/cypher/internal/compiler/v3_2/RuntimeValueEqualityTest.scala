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
package org.neo4j.cypher.internal.compiler.v3_2

import java.util
import java.util.Arrays.asList
import java.util.Collections.singletonMap

import org.neo4j.cypher.internal.codegen.CompiledEquivalenceUtils
import org.neo4j.cypher.internal.compiler.v3_2.commands.predicates.Equivalent
import org.neo4j.cypher.internal.frontend.v3_2.test_helpers.CypherFunSuite
import org.neo4j.graphdb.spatial.{CRS => JavaCRS, Coordinate => JavaCoordinate, Point => JavaPoint}

import scala.collection.JavaConverters._

/*
This test file tests both the compiled runtime and the interpreted runtime,
to make sure that both have the same semantics
 */
class RuntimeValueEqualityTest extends CypherFunSuite {
  shouldNotMatch(23.toByte, 23.5)

  shouldMatch(1.0, 1L)
  shouldMatch(1.0, 1)
  shouldMatch(1.0, 1.0)
  shouldMatch(0.9, 0.9)
  shouldMatch(Math.PI, Math.PI)
  shouldNotMatch(Math.PI, Math.nextUp(Math.PI))
  shouldMatch(1.1, 1.1)
  shouldMatch(0, 0)
  //  shouldMatch(Double.NaN, Double.NaN)
  shouldMatch(Integer.MAX_VALUE.toDouble, Integer.MAX_VALUE)
  shouldMatch(Long.MaxValue.toDouble, Long.MaxValue)
  shouldMatch(Int.MaxValue.toDouble + 1, Int.MaxValue.toLong + 1)
  shouldMatch(Double.PositiveInfinity, Double.PositiveInfinity)
  shouldMatch(Double.NegativeInfinity, Double.NegativeInfinity)
  shouldMatch(true, true)
  shouldMatch(false, false)
  shouldNotMatch(true, false)
  shouldNotMatch(false, true)
  shouldNotMatch(true, 0)
  shouldNotMatch(false, 0)
  shouldNotMatch(true, 1)
  shouldNotMatch(false, 1)
  shouldNotMatch(false, "false")
  shouldNotMatch(true, "true")
  shouldMatch(42.toByte, 42.toByte)
  shouldMatch(42.toByte, 42.toShort)
  shouldNotMatch(42.toByte, 42 + 256)
  shouldMatch(43.toByte, 43)
  shouldMatch(43.toByte, 43.toLong)
  shouldMatch(23.toByte, 23.0d)
  shouldMatch(23.toByte, 23.0f)
  shouldNotMatch(23.toByte, 23.5f)
  shouldMatch(11.toShort, 11.toByte)
  shouldMatch(42.toShort, 42.toShort)
  shouldNotMatch(42.toShort, 42 + 65536)
  shouldMatch(43.toShort, 43)
  shouldMatch(43.toShort, 43.toLong)
  shouldMatch(23.toShort, 23.0f)
  shouldMatch(23.toShort, 23.0d)
  shouldNotMatch(23.toShort, 23.5)
  shouldNotMatch(23.toShort, 23.5f)
  shouldMatch(11, 11.toByte)
  shouldMatch(42, 42.toShort)
  shouldNotMatch(42, 42 + 4294967296L)
  shouldMatch(43, 43)
  shouldMatch(Integer.MAX_VALUE, Integer.MAX_VALUE)
  shouldMatch(43, 43.toLong)
  shouldMatch(23, 23.0)
  shouldNotMatch(23, 23.5)
  shouldNotMatch(23, 23.5f)
  shouldMatch(11L, 11.toByte)
  shouldMatch(42L, 42.toShort)
  shouldMatch(43L, 43)
  shouldMatch(43L, 43.toLong)
  shouldMatch(87L, 87.toLong)
  shouldMatch(Long.MaxValue, Long.MaxValue)
  shouldMatch(Int.MaxValue, Int.MaxValue.toLong)
  shouldMatch(23L, 23.0)
  shouldNotMatch(23L, 23.5)
  shouldNotMatch(23L, 23.5f)
  shouldMatch(9007199254740992L, 9007199254740992D)
  shouldNotMatch(4611686018427387905L, 4611686018427387900L)
  shouldMatch(11f, 11.toByte)
  shouldMatch(42f, 42.toShort)
  shouldMatch(43f, 43)
  shouldMatch(43f, 43.toLong)
  shouldMatch(23f, 23.0)
  shouldNotMatch(23f, 23.5)
  shouldNotMatch(23f, 23.5f)
  shouldNotMatch(3.14f, 3.15d)
  shouldMatch(11d, 11.toByte)
  shouldMatch(42d, 42.toShort)
  shouldMatch(43d, 43)
  shouldMatch(43d, 43.toLong)
  shouldMatch(23d, 23.0)
  shouldNotMatch(23d, 23.5)
  shouldNotMatch(23d, 23.5f)
  shouldMatch(3.14f, 3.14f)

  /*
  weird, but correct floating point behaviour
  `3.14d` is `1.1001000111101011100001010001111010111000010100011111 * 2^1` (binary)
  `3.14f` is `1.10010001111010111000011 * 2^1` (binary) which is widened to the following double:
             `1.1001000111101011100001100000000000000000000000000000 * 2^1` (binary) (edited)

  as you can see, these two numbers are not equal:
   `(double) 3.14d`: `1.1001000111101011100001010001111010111000010100011111 * 2^1` (binary)
   `(double) 3.14f`: `1.1001000111101011100001100000000000000000000000000000 * 2^1` (binary)
   */
  shouldNotMatch(3.14f, 3.14d)
  shouldNotMatch(3.14d, 3.14f)
  shouldNotMatch(Math.PI, Math.PI.toFloat)
  shouldNotMatch(Math.PI.toFloat, Math.PI)
  shouldMatch(3.14d, 3.14d)
  shouldMatch("A", "A")
  shouldMatch('A', 'A')
  shouldMatch('A', "A")
  shouldMatch("A", 'A')
  shouldNotMatch("AA", 'A')
  shouldNotMatch("a", "A")
  shouldNotMatch("A", "a")
  shouldNotMatch("0", 0)
  shouldNotMatch('0', 0)

  // Lists and arrays
  shouldMatch(Array[Int](1, 2, 3), Array[Int](1, 2, 3))
  shouldMatch(Array[Array[Int]](Array(1), Array(2, 2), Array(3, 3, 3)), Array[Array[Double]](Array(1.0), Array(2.0, 2.0), Array(3.0, 3.0, 3.0)))
  shouldMatch(Array[Int](1, 2, 3), Array[Long](1, 2, 3))
  shouldMatch(Array[Int](1, 2, 3), Array[Double](1.0, 2.0, 3.0))

  shouldMatch(Array[String]("A", "B", "C"), Array[String]("A", "B", "C"))
  shouldMatch(Array[String]("A", "B", "C"), Array[Char]('A', 'B', 'C'))
  shouldMatch(Array[Char]('A', 'B', 'C'), Array[String]("A", "B", "C"))
  shouldMatch(Array[Int](1, 2, 3), asList(1, 2, 3))

  shouldMatch(asList(1, 2, 3), asList(1L, 2L, 3L))
  shouldMatch(asList(1, 2, 3, null), asList(1L, 2L, 3L, null))
  shouldMatch(Array[Int](1, 2, 3), asList(1L, 2L, 3L))
  shouldMatch(Array[Int](1, 2, 3), asList(1.0D, 2.0D, 3.0D))
  shouldMatch(Array[Any](1, Array[Int](2, 2), 3), asList(1.0D, asList(2.0D, 2.0D), 3.0D))
  shouldMatch(Array[String]("A", "B", "C"), asList("A", "B", "C"))
  shouldMatch(Array[String]("A", "B", "C"), asList('A', 'B', 'C'))
  shouldMatch(Array[Char]('A', 'B', 'C'), asList("A", "B", "C"))
  shouldMatch(new util.ArrayList[AnyRef](), Array.empty)
  shouldNotMatch(false, Array(false))
  shouldNotMatch(Array(false), false)
  shouldNotMatch(1, Array(1))
  shouldNotMatch("apa", Array("apa"))
  shouldNotMatch(Array(1), Array(Array(1)))

  // Maps
  shouldMatch(Map("a" -> 42).asJava, Map("a" -> 42).asJava)
  shouldMatch(Map("a" -> 42).asJava, Map("a" -> 42.0).asJava)
  shouldMatch(Map("a" -> 42).asJava, singletonMap("a", 42.0))
  shouldMatch(singletonMap("a", asList(41.0, 42.0)), Map("a" -> List(41,42).asJava).asJava)
  shouldMatch(Map("a" -> singletonMap("x", asList(41.0, 'c'.asInstanceOf[Character]))).asJava, singletonMap("a", Map("x" -> List(41, "c").asJava).asJava))

  // Geographic Values
  val crs = ImplementsJavaCRS("cartesian", "http://spatialreference.org/ref/sr-org/7203/", 7203)
  shouldMatch(ImplementsJavaPoint(32, 43, crs), ImplementsJavaPoint(32.0, 43.0, crs))

  private def shouldMatch(v1: Any, v2: Any) {
    test(testName(v1, v2, "=")) {
      val eq1 = Equivalent(v1)
      val eq2 = Equivalent(v2)
      eq1.equals(v2) should equal(true)
      eq2.equals(v1) should equal(true)
      eq1.hashCode() should equal(eq2.hashCode())
      CompiledEquivalenceUtils.equals(v1, v2) shouldBe true
      CompiledEquivalenceUtils.equals(v2, v1) shouldBe true
      CompiledEquivalenceUtils.hashCode(v1) should equal(CompiledEquivalenceUtils.hashCode(v2))
    }
  }

  private def shouldNotMatch(v1: Any, v2: Any) {
    test(testName(v1, v2, "<>")) {
      CompiledEquivalenceUtils.equals(v1, v2) shouldBe false
      CompiledEquivalenceUtils.equals(v2, v1) shouldBe false
      val eq1 = Equivalent(v1)
      val eq2 = Equivalent(v2)
      eq1.equals(v2) should equal(false)
      eq2.equals(v1) should equal(false)
      // it is preferable that hash codes are different, but not necessary
    }
  }

  private def testName(v1: Any, v2: Any, operator: String): String = {
    s"$v1 (${v1.getClass.getSimpleName}) $operator $v2 (${v2.getClass.getSimpleName})\n"
  }
}

case class ImplementsJavaPoint(longitude: Double, latitude: Double, crs: JavaCRS) extends JavaPoint {
  override def getCRS: JavaCRS = crs

  override def getCoordinates: util.List[JavaCoordinate] = asList(new JavaCoordinate(longitude, latitude))

  override def getGeometryType: String = crs.getType
}

case class ImplementsJavaCRS(typ: String, href: String, code: Int) extends JavaCRS {
  override def getType: String = typ

  override def getHref: String = href

  override def getCode: Int = code
}

