/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package cypher.features

import org.junit.Assert.fail
import org.junit.jupiter.api.Test
import org.opencypher.tools.tck.api.Scenario

abstract class EnterpriseBaseAcceptanceTest extends BaseFeatureTest {
  // these two should be empty on commit!
  // Use a substring match, for example "UnwindAcceptance"
  val featureToRun = ""
  val scenarioToRun = ""

  lazy val scenarios: Seq[Scenario] =  filterScenarios(BaseFeatureTestHolder.allAcceptanceScenarios, featureToRun, scenarioToRun)

  @Test
  def debugTokensNeedToBeEmpty(): Unit = {
    // besides the obvious reason this test is also here (and not using assert)
    // to ensure that any import optimizer doesn't remove the correct import for fail (used by the commented out methods further down)
    if (!scenarioToRun.equals(""))
      fail("scenarioToRun is only for debugging and should not be committed")

    if (!featureToRun.equals(""))
      fail("featureToRun is only for debugging and should not be committed")
  }
}
