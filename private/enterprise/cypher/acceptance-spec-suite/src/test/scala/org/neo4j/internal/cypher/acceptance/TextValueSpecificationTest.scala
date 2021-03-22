/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance

import org.junit.runner.RunWith
import org.neo4j.values.storable.TextValue
import org.neo4j.values.storable.Values.stringArray
import org.neo4j.values.storable.Values.stringValue
import org.neo4j.values.storable.Values.utf8Value
import org.neo4j.values.virtual.VirtualValues.fromArray
import org.scalacheck.Arbitrary
import org.scalacheck.Gen
import org.scalatest.Matchers
import org.scalatest.PropSpec
import org.scalatest.junit.JUnitRunner
import org.scalatest.prop.GeneratorDrivenPropertyChecks

import java.nio.charset.StandardCharsets

@RunWith(classOf[JUnitRunner])
class TextValueSpecificationTest extends PropSpec with GeneratorDrivenPropertyChecks with Matchers {

  private val substringGen = for {
    string <- Arbitrary.arbitrary[String]
    max = string.codePointCount(0, string.length)
    start <- Gen.chooseNum[Int](0, max)
    length <- Gen.chooseNum[Int](0, max)
  } yield (string, start, length)

  property("equals") {
    forAll { x: String =>
      stringValue(x).equals(utf8Value(x.getBytes(StandardCharsets.UTF_8))) shouldBe true
    }
  }

  property("length") {
    forAll { x: String =>
      stringValue(x).length() shouldBe x.length
      utf8Value(x.getBytes(StandardCharsets.UTF_8)).length() shouldBe x.length
    }
  }

  property("isEmpty") {
    forAll { x: String =>
      stringValue(x).isEmpty shouldBe x.isEmpty
      utf8Value(x.getBytes(StandardCharsets.UTF_8)).isEmpty shouldBe x.isEmpty
    }
  }

  property("hashCode") {
    forAll { x: String =>
      stringValue(x).hashCode() shouldBe utf8Value(x.getBytes(StandardCharsets.UTF_8)).hashCode()
    }
  }

  property("trim") {
    forAll { x: String =>
      val sValue = stringValue(x)
      val utf8StringValue = utf8Value(x.getBytes(StandardCharsets.UTF_8))
      equivalenceTest(sValue, utf8StringValue)
      equivalenceTest(sValue.trim(), utf8StringValue.ltrim().rtrim())
      equivalenceTest(utf8StringValue.trim(), sValue.ltrim().rtrim())
    }
  }

  property("reverse") {
    forAll { x: String =>
      val sValue = stringValue(x)
      val utf8StringValue = utf8Value(x.getBytes(StandardCharsets.UTF_8))
      equivalenceTest(sValue.reverse(), utf8StringValue.reverse())
    }
  }

  property("ltrim") {
    forAll { x: String =>
      equivalenceTest(stringValue(x).ltrim(), utf8Value(x.getBytes(StandardCharsets.UTF_8)).ltrim())
    }
  }

  property("rtrim") {
    forAll { x: String =>
      equivalenceTest(stringValue(x).rtrim(), utf8Value(x.getBytes(StandardCharsets.UTF_8)).rtrim())
    }
  }

  property("toLower") {
    forAll { x: String =>
      val value = stringValue(x)
      val utf8 = utf8Value(x.getBytes(StandardCharsets.UTF_8))
      equivalenceTest(stringValue(x.toLowerCase), value.toLower)
      equivalenceTest(value.toLower, utf8.toLower)
    }
  }

  property("toUpper") {
    forAll { x: String =>
      val value = stringValue(x)
      val utf8 = utf8Value(x.getBytes(StandardCharsets.UTF_8))
      equivalenceTest(stringValue(x.toUpperCase), value.toUpper)
      equivalenceTest(value.toUpper, utf8.toUpper)
    }
  }

  private val replaceGen = {
    for {
      x <- Arbitrary.arbitrary[String]
      find <- Gen.alphaStr
      replace <- Arbitrary.arbitrary[String]
    } yield (x, find, replace)
  }

  private val replaceSubstringGen = {
    for {
      x <- Arbitrary.arbitrary[String]
      start <- Gen.chooseNum(0, x.length)
      stop <- Gen.chooseNum(start, if (x.isEmpty) 0 else x.length)
      find = x.substring(start, stop)
      replace <- Arbitrary.arbitrary[String]
    } yield (x, find, replace)
  }

  property("replace") {
    forAll(Gen.oneOf(replaceGen, replaceSubstringGen)) {
      case (x: String, find: String, replace: String) =>
        val value = stringValue(x)
        val utf8 = utf8Value(x.getBytes(StandardCharsets.UTF_8))
        equivalenceTest(stringValue(x.replace(find, replace)), value.replace(find, replace))
        equivalenceTest(value.replace(find, replace), utf8.replace(find, replace))
    }
  }

  private val splitGen = {
    for {
     x <- Arbitrary.arbitrary[String]
     find <- Gen.alphaStr
    } yield (x,find)
  }

  private val splitWithHitsGen = {
    for {
     x <- Arbitrary.arbitrary[String]
     find <- Gen.alphaStr
     minSplits <- Gen.chooseNum(0, if (x.isEmpty) 0 else x.length - 1)
     positions <- Gen.pick(minSplits, Range(0, x.length))
   } yield {
     val stringWithHits = x.zipWithIndex.foldLeft(new StringBuilder) {
       case (builder, (charInX, index)) =>
         if (positions.contains(index)) {
           builder.append(find)
         }
         builder.append(charInX)
         builder
     }
     (stringWithHits.toString(), find)
   }
  }


  property("split") {
    forAll(Gen.oneOf(splitGen, splitWithHitsGen))  {
      case (x, find) =>
        val value = stringValue(x)
        val utf8 = utf8Value(x.getBytes(StandardCharsets.UTF_8))
        val split = x.split(find)
        value.split(find) shouldBe utf8.split(find)
        if (x != find) {
          fromArray(stringArray(split: _*)) shouldBe value.split(find)
        } else {
          value.split(find) shouldBe fromArray(stringArray("", ""))
        }
    }
  }

  property("compareTo") {
    forAll { (x: String, y: String) =>
      val stringX = stringValue(x)
      val stringY = stringValue(y)
      val utf8X = utf8Value(x.getBytes(StandardCharsets.UTF_8))
      val utf8Y = utf8Value(y.getBytes(StandardCharsets.UTF_8))
      val compare = Math.signum(stringX.compareTo(stringY))
      compare shouldBe Math.signum(stringX.compareTo(utf8Y))
      compare shouldBe Math.signum(utf8X.compareTo(stringY))
      compare shouldBe Math.signum(utf8X.compareTo(utf8Y))
      compare shouldBe Math.signum(-stringY.compareTo(utf8X))
      compare shouldBe Math.signum(-utf8Y.compareTo(stringX))
      compare shouldBe Math.signum(-utf8Y.compareTo(utf8X))
    }
  }

  property("compareTo between string value and utf8 value") {
    forAll { x: String =>
      val stringX = stringValue(x)
      val utf8X = utf8Value(x.getBytes(StandardCharsets.UTF_8))
      stringX.compareTo(stringX) shouldBe 0
      stringX.compareTo(utf8X) shouldBe 0
      utf8X.compareTo(stringX) shouldBe 0
      utf8X.compareTo(utf8X) shouldBe 0
    }
  }

  property("substring") {
    forAll(substringGen) {
      case (string, start, length) =>
        equivalenceTest(
          stringValue(string).substring(start, length),
          utf8Value(string.getBytes(StandardCharsets.UTF_8)).substring(start, length)
        )
    }
  }

  implicit override val generatorDrivenConfig: PropertyCheckConfiguration =
    PropertyCheckConfiguration(minSuccessful = 1000)

  private def equivalenceTest(t1: TextValue, t2: TextValue) = {
    t1 shouldBe t2
    t1.length() shouldBe t2.length()
    t1.hashCode() shouldBe t2.hashCode()
    t1.hashCode64() shouldBe t2.hashCode64()
  }
}
