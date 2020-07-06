/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package cypher.features

import com.neo4j.configuration.MetricsSettings.metrics_enabled
import cypher.features.Neo4jAdapter.defaultTestConfig
import org.junit.Assert.fail
import org.junit.jupiter.api.Test
import org.neo4j.graphdb.config.Setting
import org.opencypher.tools.tck.api.Scenario

abstract class EnterpriseBaseTCKTests extends BaseFeatureTest {

  // these two should be empty on commit!
  val featureToRun = "MiscellaneousErrorAcceptance"
  val scenarioToRun = ""

  lazy val scenarios: Seq[Scenario] = filterScenarios(BaseFeatureTestHolder.allTckScenarios, featureToRun, scenarioToRun)

  val enterpriseDefaultTestConfig: collection.Map[Setting[_], AnyRef] = defaultTestConfig + (metrics_enabled -> java.lang.Boolean.FALSE)

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
