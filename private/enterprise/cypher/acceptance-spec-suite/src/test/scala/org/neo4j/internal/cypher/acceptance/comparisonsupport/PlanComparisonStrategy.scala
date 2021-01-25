/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance.comparisonsupport

import org.neo4j.cypher.internal.RewindableExecutionResult
import org.neo4j.cypher.internal.plandescription.InternalPlanDescription
import org.scalatest.Assertions

import scala.util.Failure
import scala.util.Success
import scala.util.Try

sealed trait PlanComparisonStrategy extends Assertions {
  def compare(expectSucceed: TestConfiguration, scenario: TestScenario, result: RewindableExecutionResult): Unit
}

case object DoNotComparePlans extends PlanComparisonStrategy {
  override def compare(expectSucceed: TestConfiguration, scenario: TestScenario, result: RewindableExecutionResult): Unit = {}
}

case class ComparePlansWithPredicate(predicate: InternalPlanDescription => Boolean,
                                     expectPlansToFailPredicate: TestConfiguration = TestConfiguration.empty,
                                     predicateFailureMessage: String = "") extends PlanComparisonStrategy {
  override def compare(expectSucceed: TestConfiguration, scenario: TestScenario, result: RewindableExecutionResult): Unit = {
    val comparePlans = expectSucceed - expectPlansToFailPredicate
    if (comparePlans.containsScenario(scenario)) {
      if (!predicate(result.executionPlanDescription())) {
        fail(s"plan for ${scenario.name} did not fulfill predicate.\n$predicateFailureMessage\n${result.executionPlanString()}")
      }
    } else {
      if (predicate(result.executionPlanDescription())) {
        fail(s"plan for ${scenario.name} did unexpectedly fulfill predicate\n$predicateFailureMessage\n${result.executionPlanString()}")
      }
    }
  }
}

case class ComparePlansWithAssertion(assertion: InternalPlanDescription => Unit,
                                     expectPlansToFail: TestConfiguration = TestConfiguration.empty) extends PlanComparisonStrategy {
  override def compare(expectSucceed: TestConfiguration, scenario: TestScenario, result: RewindableExecutionResult): Unit = {
    val comparePlans = expectSucceed - expectPlansToFail
    if (comparePlans.containsScenario(scenario)) {
      withClue(s"plan for ${scenario.name}\n") {
        assertion(result.executionPlanDescription())
      }
    } else if (!Configs.Experimental.containsScenario(scenario)) {
      val tryResult = Try(assertion(result.executionPlanDescription()))
      tryResult match {
        case Success(_) =>
          fail(s"plan for ${scenario.name} did unexpectedly succeed \n${result.executionPlanString()}")
        case Failure(_) =>
        // Expected to fail
      }
    }
  }
}
