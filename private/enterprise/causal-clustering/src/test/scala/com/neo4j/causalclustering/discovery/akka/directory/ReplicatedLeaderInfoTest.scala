/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.directory

import com.neo4j.causalclustering.core.consensus.LeaderInfo
import com.neo4j.causalclustering.discovery.akka.NeoSuite
import com.neo4j.causalclustering.identity.IdFactory
import com.neo4j.causalclustering.identity.MemberId
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ReplicatedLeaderInfoTest extends NeoSuite {

 "ReplicatedLeaderInfo values" should {

   "prefer higher term LeaderInfo values when merging" in {
     Given("High term leader info")
     val leaderInfoHigh = new ReplicatedLeaderInfo(new LeaderInfo(IdFactory.randomMemberId(),2L))

     And("Low term leader info")
     val leaderInfoLow = new ReplicatedLeaderInfo(new LeaderInfo(IdFactory.randomMemberId(),1L))

     When("Merging low into high")

     Then("should keep high")
     assert(leaderInfoHigh == leaderInfoHigh.mergeData(leaderInfoLow))

     And("should overwrite low")
     val newLeaderInfo = leaderInfoLow.mergeData(leaderInfoHigh)
     assert(leaderInfoLow != newLeaderInfo && leaderInfoHigh == newLeaderInfo)
   }

   "prefer stepping down LeaderInfo values when merging in the same term" in {
     Given("Stepping down leader info")
     val steppingDownleaderInfo = new ReplicatedLeaderInfo(new LeaderInfo(IdFactory.randomMemberId(),1L).stepDown())

     And("Normal leaderInfo")
     val leaderInfo = new ReplicatedLeaderInfo(new LeaderInfo(IdFactory.randomMemberId(),1L))

     When("Merging normal into stepping down")

     Then("should keep stepping down")
     assert(steppingDownleaderInfo == steppingDownleaderInfo.mergeData(leaderInfo))

     And("should overwrite normal")
     val newLeaderInfo = leaderInfo.mergeData(steppingDownleaderInfo)
     assert(leaderInfo != newLeaderInfo && steppingDownleaderInfo == newLeaderInfo)
   }
 }

}
