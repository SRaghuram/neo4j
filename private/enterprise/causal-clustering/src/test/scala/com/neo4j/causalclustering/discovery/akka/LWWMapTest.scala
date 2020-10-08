/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka

import akka.actor.Address
import akka.cluster.UniqueAddress
import akka.cluster.ddata.LWWMap
import akka.cluster.ddata.LWWRegister
import akka.cluster.ddata.SelfUniqueAddress
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class LWWMapTest extends NeoSuite {

  "LWWMap" should {
    "respect first write wins semantics when merging with a reverse clock" in {
      Given("an LWWMap with initial entries")
      val clock = LWWRegister.reverseClock[Int]
      val address1 = SelfUniqueAddress(UniqueAddress(Address("akka", "localhost"), 1L))
      val address2 = SelfUniqueAddress(UniqueAddress(Address("akka", "localhost"), 2L))

      val initialMap = Map("a" -> 1, "b" -> 2).foldLeft(LWWMap.empty[String,Int]) {
        case (acc, (k, v)) => acc.put(address1, k, v, clock)
      }

      val secondMap = Map("a" -> 2, "b" -> 4, "c" -> 6).foldLeft(LWWMap.empty[String,Int]) {
        case (acc, (k, v)) => acc.put(address2, k, v, clock)
      }

      When("a second LWWMap with entries is merged")
      val result = initialMap merge secondMap

      Then("overlapping keys should not be updated")
      result.entries shouldBe Map("a" -> 1, "b" -> 2, "c" -> 6)
    }
  }
}
