/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.compiled.expressions

import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.kernel.impl.util.ValueUtils
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values.NO_VALUE
import org.neo4j.values.storable.Values.intValue

import scala.collection.mutable.ArrayBuffer

class CompiledMapValueTest extends CypherFunSuite {

  test("should be able to get from map") {
    //given
    val map = mapValue("a" -> 1, "b" -> 2, "c" -> 3)

    //then
    map.get("a") should equal(intValue(1))
    map.get("b") should equal(intValue(2))
    map.get("c") should equal(intValue(3))
    map.get("d") should equal(NO_VALUE)
  }

  test("Should be able to check containsKey") {
    //given
    val map = mapValue((50 to 100).map(i => i.toString -> i):_*)

    //then
    (1 to 200).foreach(i => {
      map.containsKey(i.toString) should equal(i >= 50 && i <= 100)
    })
  }

  test("Should be able to use foreach") {
    //given
    val map = mapValue((50 to 100).map(i => i.toString -> i):_*)

    //when
    val buffer = ArrayBuffer.empty[(String, AnyValue)]
    map.foreach((k, v) => buffer.append(k -> v))

    //then
    buffer.toMap should equal((50 to 100).map(i => i.toString -> intValue(i)).toMap)
  }

  private def mapValue(items: (String, Any)*): CompiledMapValue = {
    val map = items.toMap
    val keys = map.keys.toArray.sorted
    val res = new CompiledMapValue(keys)
    keys.zipWithIndex.foreach {
      case (k, i) => res.internalPut(i, ValueUtils.of(map(k)))
    }
    res
  }
}
