/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance

import java.nio.charset.StandardCharsets

import org.neo4j.values.storable.TextValue
import org.neo4j.values.storable.Values.stringArray
import org.neo4j.values.storable.Values.stringValue
import org.neo4j.values.storable.Values.utf8Value
import org.neo4j.values.virtual.VirtualValues.fromArray
import org.scalacheck.Arbitrary
import org.scalacheck.Gen
import org.scalacheck.Prop.forAll
import org.scalacheck.Properties
import org.scalatest.prop.Configuration

object TextValueSpecificationTest extends Properties("TextValue") with Configuration {

  private val substringGen = for {
    string <- Arbitrary.arbitrary[String]
    max = string.codePointCount(0, string.length)
    start <- Gen.chooseNum[Int](0, max)
    length <- Gen.chooseNum[Int](0, max)
  } yield (string, start, length)

  property("equals") = forAll { x: String =>
    stringValue(x).equals(utf8Value(x.getBytes(StandardCharsets.UTF_8)))
  }

  property("length") = forAll { x: String =>
    stringValue(x).length() == utf8Value(x.getBytes(StandardCharsets.UTF_8)).length()
  }

  property("isEmpty") = forAll { x: String =>
    stringValue(x).isEmpty() == utf8Value(x.getBytes(StandardCharsets.UTF_8)).isEmpty()
  }

  property("hashCode") = forAll { x: String =>
    stringValue(x).hashCode() == utf8Value(x.getBytes(StandardCharsets.UTF_8)).hashCode()
  }

  property("trim") = forAll { x: String =>
    equivalent(stringValue(x).trim(), utf8Value(x.getBytes(StandardCharsets.UTF_8)).trim())
  }

  property("trim") = forAll { x: String => {
    val sValue = stringValue(x)
    val utf8StringValue = utf8Value(x.getBytes(StandardCharsets.UTF_8))
    equivalent(sValue.trim(), utf8StringValue.ltrim().rtrim()) &&
      equivalent(utf8StringValue.trim(), sValue.ltrim().rtrim())}
  }

  property("reverse") = forAll { x: String =>
    val sValue = stringValue(x)
    val utf8StringValue = utf8Value(x.getBytes(StandardCharsets.UTF_8))
    equivalent(sValue.reverse(), utf8StringValue.reverse())
  }

  property("ltrim") = forAll { x: String =>
    equivalent(stringValue(x).ltrim(), utf8Value(x.getBytes(StandardCharsets.UTF_8)).ltrim())
  }

  property("rtrim") = forAll { x: String =>
    equivalent(stringValue(x).rtrim(), utf8Value(x.getBytes(StandardCharsets.UTF_8)).rtrim())
  }

  property("toLower") = forAll { x: String => {
    val value = stringValue(x)
    val utf8 = utf8Value(x.getBytes(StandardCharsets.UTF_8))
    equivalent(stringValue(x.toLowerCase), value.toLower) &&
      equivalent(value.toLower, utf8.toLower)
  }}

  property("toUpper") = forAll { x: String => {
    val value = stringValue(x)
    val utf8 = utf8Value(x.getBytes(StandardCharsets.UTF_8))
    equivalent(stringValue(x.toUpperCase), value.toUpper) &&
      equivalent(value.toUpper, utf8.toUpper)
  }}

  private val replaceGen = for {x <- Arbitrary.arbitrary[String]
                                find <-Gen.alphaStr
                                replace <- Arbitrary.arbitrary[String]} yield (x,find, replace)
  property("replace") = forAll(replaceGen) {
    case (x: String, find: String, replace: String) =>
      val value = stringValue(x)
      val utf8 = utf8Value(x.getBytes(StandardCharsets.UTF_8))
      equivalent(stringValue(x.replace(find, replace)), value.replace(find, replace)) &&
        equivalent(value.replace(find, replace), utf8.replace(find, replace))
  }

  private val splitGen = for {x <- Arbitrary.arbitrary[String]
                              find <-Gen.alphaStr} yield (x,find)

  property("split") = forAll(splitGen)  {
    case (x, find) =>
      val value = stringValue(x)
      val utf8 = utf8Value(x.getBytes(StandardCharsets.UTF_8))
      val split = x.split(find)
      if (x != find) {
        fromArray(stringArray(split: _*)) == value.split(find) &&
          value.split(find) == utf8.split(find)
      } else {
        value.split(find) == utf8.split(find) && value.split(find) == fromArray(stringArray("", ""))
      }

  }

  property("compareTo") = forAll { (x: String, y: String) =>
    val stringX = stringValue(x)
    val stringY = stringValue(y)
    val utf8X = utf8Value(x.getBytes(StandardCharsets.UTF_8))
    val utf8Y = utf8Value(y.getBytes(StandardCharsets.UTF_8))
    val compare = Math.signum(stringX.compareTo(stringY))
    compare == Math.signum(stringX.compareTo(utf8Y)) &&
      compare == Math.signum(utf8X.compareTo(stringY)) &&
      compare == Math.signum(utf8X.compareTo(utf8Y)) &&
      compare == Math.signum(-stringY.compareTo(utf8X)) &&
      compare == Math.signum(-utf8Y.compareTo(stringX)) &&
      compare == Math.signum(-utf8Y.compareTo(utf8X))
  }

  property("compareTo") = forAll { x: String =>
    val stringX = stringValue(x)
    val utf8X = utf8Value(x.getBytes(StandardCharsets.UTF_8))
    stringX.compareTo(stringX) == 0 &&
      stringX.compareTo(utf8X) == 0 &&
      utf8X.compareTo(stringX) == 0 &&
      utf8X.compareTo(utf8X) == 0
  }

  property("substring") = forAll(substringGen) {
    case (string, start, length) =>
      equivalent(stringValue(string).substring(start, length),
        utf8Value(string.getBytes(StandardCharsets.UTF_8)).substring(start, length))
  }

  implicit override val generatorDrivenConfig: TextValueSpecificationTest.PropertyCheckConfiguration =
    PropertyCheckConfig(minSuccessful = 1000)

  private def equivalent(t1: TextValue, t2: TextValue) =
    t1.length() == t2.length() && t1 == t2 && t1.hashCode() == t2.hashCode()
}
