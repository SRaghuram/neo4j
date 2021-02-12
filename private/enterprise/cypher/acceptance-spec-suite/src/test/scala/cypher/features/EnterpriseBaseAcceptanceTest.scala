/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package cypher.features

import com.neo4j.configuration.MetricsSettings.metrics_enabled
import cypher.features.Neo4jAdapter.defaultTestConfig
import org.junit.Assert.fail
import org.junit.jupiter.api.Test
import org.neo4j.configuration.GraphDatabaseInternalSettings.cypher_worker_count
import org.neo4j.graphdb.config.Setting
import org.opencypher.tools.tck.api.Scenario

abstract class EnterpriseBaseAcceptanceTest extends BaseFeatureTest {
  // these two should be empty on commit!
  // Use a substring match, for example "UnwindAcceptance"
  val categoryToRun = ""
  val featureToRun = ""
  val scenarioToRun = ""

  val enterpriseDefaultTestConfig: collection.Map[Setting[_], AnyRef] = defaultTestConfig +
    (metrics_enabled -> java.lang.Boolean.FALSE) +
    (cypher_worker_count -> java.lang.Integer.valueOf(-1))

  lazy val scenarios: Seq[Scenario] =  filterScenarios(BaseFeatureTestHolder.allAcceptanceScenarios, categoryToRun, featureToRun, scenarioToRun)

  @Test
  def debugTokensNeedToBeEmpty(): Unit = {
    // besides the obvious reason this test is also here (and not using assert)
    // to ensure that any import optimizer doesn't remove the correct import for fail (used by the commented out methods further down)
    if (!categoryToRun.equals(""))
      fail("categoryToRun is only for debugging and should not be committed")

    if (!scenarioToRun.equals(""))
      fail("scenarioToRun is only for debugging and should not be committed")

    if (!featureToRun.equals(""))
      fail("featureToRun is only for debugging and should not be committed")
  }
}
