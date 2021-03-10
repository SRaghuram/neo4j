/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.spec.parallel

import java.lang.System.lineSeparator

import org.neo4j.cypher.internal.EnterpriseRuntimeContext
import org.neo4j.cypher.internal.PipelinedRuntime.PARALLEL
import org.neo4j.cypher.internal.logical.plans.Ascending
import org.neo4j.cypher.internal.logical.plans.IndexOrderNone
import org.neo4j.cypher.internal.runtime.spec.ENTERPRISE
import org.neo4j.cypher.internal.runtime.spec.ENTERPRISE.MORSEL_SIZE
import org.neo4j.cypher.internal.runtime.spec.Edition
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSuite
import org.neo4j.cypher.internal.runtime.spec.parallel.ParallelRuntimeSpecSuite.FUSING
import org.neo4j.cypher.internal.runtime.spec.parallel.ParallelRuntimeSpecSuite.NO_FUSING
import org.neo4j.cypher.internal.runtime.spec.parallel.ParallelRuntimeSpecSuite.SIZE_HINT
import org.neo4j.cypher.internal.runtime.spec.pipelined.AssertFusingSucceeded
import org.neo4j.cypher.internal.runtime.spec.pipelined.PipelinedDbHitsTestBase
import org.neo4j.cypher.internal.runtime.spec.pipelined.PipelinedDebugGeneratedSource
import org.neo4j.cypher.internal.runtime.spec.pipelined.PipelinedFusingNotificationTestBase
import org.neo4j.cypher.internal.runtime.spec.pipelined.ProfileNoTimeTestBase
import org.neo4j.cypher.internal.runtime.spec.pipelined.SchedulerTracerTestBase
import org.neo4j.cypher.internal.runtime.spec.stress.AggregationStressTestBase
import org.neo4j.cypher.internal.runtime.spec.stress.AllNodeScanStressTestBase
import org.neo4j.cypher.internal.runtime.spec.stress.ApplyStressTestBase
import org.neo4j.cypher.internal.runtime.spec.stress.ArgumentStressTestBase
import org.neo4j.cypher.internal.runtime.spec.stress.CartesianProductStressTestBase
import org.neo4j.cypher.internal.runtime.spec.stress.DirectedRelationshipIndexContainsScanStressTestBase
import org.neo4j.cypher.internal.runtime.spec.stress.DirectedRelationshipIndexEndsWithScanStressTestBase
import org.neo4j.cypher.internal.runtime.spec.stress.DirectedRelationshipIndexScanStressTestBase
import org.neo4j.cypher.internal.runtime.spec.stress.DirectedRelationshipIndexSeekExactStressTestBase
import org.neo4j.cypher.internal.runtime.spec.stress.DirectedRelationshipIndexSeekRangeStressTestBase
import org.neo4j.cypher.internal.runtime.spec.stress.DistinctStressTestBase
import org.neo4j.cypher.internal.runtime.spec.stress.ExpandAllStressTestBase
import org.neo4j.cypher.internal.runtime.spec.stress.ExpressionStressTestBase
import org.neo4j.cypher.internal.runtime.spec.stress.FilterStressTestBase
import org.neo4j.cypher.internal.runtime.spec.stress.LabelScanStressTestBase
import org.neo4j.cypher.internal.runtime.spec.stress.NodeByIdSeekStressTestBase
import org.neo4j.cypher.internal.runtime.spec.stress.NodeIndexContainsScanStressTestBase
import org.neo4j.cypher.internal.runtime.spec.stress.NodeIndexEndsWithScanStressTestBase
import org.neo4j.cypher.internal.runtime.spec.stress.NodeIndexScanStressTestBase
import org.neo4j.cypher.internal.runtime.spec.stress.NodeIndexSeekExactStressTestBase
import org.neo4j.cypher.internal.runtime.spec.stress.NodeIndexSeekRangeStressTestBase
import org.neo4j.cypher.internal.runtime.spec.stress.ProjectionStressTestBase
import org.neo4j.cypher.internal.runtime.spec.stress.UndirectedRelationshipIndexContainsScanStressTestBase
import org.neo4j.cypher.internal.runtime.spec.stress.UndirectedRelationshipIndexEndsWithScanStressTestBase
import org.neo4j.cypher.internal.runtime.spec.stress.UndirectedRelationshipIndexScanStressTestBase
import org.neo4j.cypher.internal.runtime.spec.stress.UndirectedRelationshipIndexSeekExactStressTestBase
import org.neo4j.cypher.internal.runtime.spec.stress.UndirectedRelationshipIndexSeekRangeStressTestBase
import org.neo4j.cypher.internal.runtime.spec.stress.UnwindStressTestBase
import org.neo4j.cypher.internal.runtime.spec.stress.VarExpandStressTestBase
import org.neo4j.cypher.internal.runtime.spec.stress.WorkloadTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.AggregationTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.AllNodeScanTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.AllNodeScanWithOtherOperatorsTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.AntiSemiApplyTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.ApplyTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.ArgumentTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.CachePropertiesTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.CartesianProductProvidedOrderTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.CartesianProductTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.DirectedRelationshipByIdSeekTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.DistinctTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.EagerTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.EmptyResultTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.EnterpriseNodeIndexSeekTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.ExpandAllTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.ExpandAllWithOtherOperatorsTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.ExpandIntoTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.ExpandIntoWithOtherOperatorsTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.ExpressionTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.FilterTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.LabelScanTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.LeftOuterHashJoinTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.LimitTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.MemoryManagementDisabledTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.MiscTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.NodeByIdSeekTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.NodeCountFromCountStoreTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.NodeHashJoinTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.NodeIndexContainsScanTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.NodeIndexEndsWithScanTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.NodeIndexPointDistanceSeekTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.NodeIndexScanTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.NodeIndexSeekTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.NodeIndexStartsWithSeekTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.OptionalExpandAllTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.OptionalExpandIntoTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.OptionalTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.ProfileRowsTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.ProfileTimeTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.ProjectionTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.ProvidedOrderTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.PruningVarLengthExpandTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.ReactiveResultStressTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.ReactiveResultTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.RelationshipCountFromCountStoreTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.RelationshipIndexContainsScanTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.RelationshipIndexEndsWithScanTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.RelationshipIndexScanTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.RelationshipIndexSeekTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.RelationshipTypeScanTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.RightOuterHashJoinTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.RollupApplyTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.SemiApplyTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.ShortestPathTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.SkipTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.SortTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.SubscriberErrorTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.Top1WithTiesTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.TopTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.UndirectedRelationshipByIdSeekTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.UnionTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.UnwindTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.ValueHashJoinTestBase
import org.neo4j.cypher.internal.runtime.spec.tests.VarLengthExpandTestBase
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.cypher.internal.util.test_helpers.TimeLimitedCypherTest
import org.neo4j.cypher.result.OperatorProfile
import org.neo4j.graphdb.Label.label
import org.neo4j.graphdb.RelationshipType
import org.scalatest.Outcome

import scala.util.Random

object ParallelRuntimeSpecSuite {
  val SIZE_HINT = 1000

  val FUSING: Edition[EnterpriseRuntimeContext] = ENTERPRISE.WITH_FUSING(ENTERPRISE.WITH_WORKERS(ENTERPRISE.DEFAULT))
  val NO_FUSING: Edition[EnterpriseRuntimeContext] = ENTERPRISE.WITH_NO_FUSING(ENTERPRISE.WITH_WORKERS(ENTERPRISE.DEFAULT))
}

trait ParallelRuntimeSpecSuite extends TimeLimitedCypherTest with AssertFusingSucceeded with PipelinedDebugGeneratedSource {
  self: RuntimeTestSuite[EnterpriseRuntimeContext] =>
  abstract override def withFixture(test: NoArgTest): Outcome = {
    withClue(s"Failed with MORSEL_SIZE = $MORSEL_SIZE${lineSeparator()}")(super.withFixture(test))
  }
}

// INPUT
class ParallelRuntimeInputTest extends ParallelInputTestBase(FUSING, PARALLEL) with ParallelRuntimeSpecSuite
class ParallelRuntimeInputNoFusingTest extends ParallelInputTestBase(NO_FUSING, PARALLEL) with ParallelRuntimeSpecSuite

// ALL NODE SCAN
class ParallelRuntimeAllNodeScanTest extends AllNodeScanTestBase(FUSING, PARALLEL, SIZE_HINT)
                                     with AllNodeScanWithOtherOperatorsTestBase[EnterpriseRuntimeContext]
                                     with ParallelRuntimeSpecSuite
class ParallelRuntimeAllNodeScanNoFusingTest extends AllNodeScanTestBase(NO_FUSING, PARALLEL, SIZE_HINT)
                                             with AllNodeScanWithOtherOperatorsTestBase[EnterpriseRuntimeContext]
                                             with ParallelRuntimeSpecSuite
class ParallelRuntimeAllNodeScanStressTest extends AllNodeScanStressTestBase(FUSING, PARALLEL) with ParallelRuntimeSpecSuite
class ParallelRuntimeAllNodeScanNoFusingStressTest extends AllNodeScanStressTestBase(NO_FUSING, PARALLEL) with ParallelRuntimeSpecSuite

// NODE BY ID SEEK
class ParallelRuntimeNodeByIdSeekTest extends NodeByIdSeekTestBase(FUSING, PARALLEL, SIZE_HINT) with ParallelRuntimeSpecSuite
class ParallelRuntimeNodeByIdSeekNoFusingTest extends NodeByIdSeekTestBase(NO_FUSING, PARALLEL, SIZE_HINT) with ParallelRuntimeSpecSuite
class ParallelRuntimeNodeByIdSeekStressTest extends NodeByIdSeekStressTestBase(FUSING, PARALLEL) with ParallelRuntimeSpecSuite
class ParallelRuntimeNodeByIdSeekNoFusingStressTest extends NodeByIdSeekStressTestBase(NO_FUSING, PARALLEL) with ParallelRuntimeSpecSuite

// RELATIONSHIP BY ID SEEK
class ParallelRuntimeDirectedRelationshipByIdSeekTest extends DirectedRelationshipByIdSeekTestBase(FUSING, PARALLEL, SIZE_HINT) with ParallelRuntimeSpecSuite
class ParallelRuntimeDirectedRelationshipByIdSeekNoFusingTest extends DirectedRelationshipByIdSeekTestBase(NO_FUSING, PARALLEL, SIZE_HINT) with ParallelRuntimeSpecSuite

//TYPE SCAN
class ParallelRuntimeRelationshipTypeScanTest extends RelationshipTypeScanTestBase(FUSING, PARALLEL, SIZE_HINT) with ParallelRuntimeSpecSuite
class ParallelRuntimeRelationshipTypeScanNoFusingTest extends RelationshipTypeScanTestBase(NO_FUSING, PARALLEL, SIZE_HINT) with ParallelRuntimeSpecSuite

// UNDIRECTED RELATIONSHIP BY ID SEEK
class ParallelRuntimeUndirectedRelationshipByIdSeekTest extends UndirectedRelationshipByIdSeekTestBase(FUSING, PARALLEL, SIZE_HINT) with ParallelRuntimeSpecSuite
class ParallelRuntimeUndirectedRelationshipByIdSeekNoFusingTest extends UndirectedRelationshipByIdSeekTestBase(NO_FUSING, PARALLEL, SIZE_HINT) with ParallelRuntimeSpecSuite

// NODE COUNT FROM COUNT STORE
class ParallelNodeCountFromCountStoreTest extends NodeCountFromCountStoreTestBase(FUSING, PARALLEL) with ParallelRuntimeSpecSuite
class ParallelNodeCountFromCountStoreNoFusingTest extends NodeCountFromCountStoreTestBase(NO_FUSING, PARALLEL) with ParallelRuntimeSpecSuite

// RELATIONSHIP COUNT FROM COUNT STORE
class ParallelRelationshipCountFromCountStoreTest extends RelationshipCountFromCountStoreTestBase(FUSING, PARALLEL) with ParallelRuntimeSpecSuite
class ParallelRelationshipCountFromCountStoreNoFusingTest extends RelationshipCountFromCountStoreTestBase(NO_FUSING, PARALLEL) with ParallelRuntimeSpecSuite

// LABEL SCAN
class ParallelRuntimeLabelScanTest extends LabelScanTestBase(FUSING, PARALLEL, SIZE_HINT) with ParallelRuntimeSpecSuite
class ParallelRuntimeLabelScanNoFusingTest extends LabelScanTestBase(NO_FUSING, PARALLEL, SIZE_HINT) with ParallelRuntimeSpecSuite
class ParallelRuntimeLabelScanStressTest extends LabelScanStressTestBase(FUSING, PARALLEL) with ParallelRuntimeSpecSuite
class ParallelRuntimeLabelScanNoFusingStressTest extends LabelScanStressTestBase(NO_FUSING, PARALLEL) with ParallelRuntimeSpecSuite

// INDEX SEEK
class ParallelRuntimeNodeIndexSeekTest extends NodeIndexSeekTestBase(FUSING, PARALLEL, SIZE_HINT) with ParallelRuntimeSpecSuite
                                       with EnterpriseNodeIndexSeekTestBase[EnterpriseRuntimeContext]
class ParallelRuntimeNodeIndexSeekNoFusingTest extends NodeIndexSeekTestBase(NO_FUSING, PARALLEL, SIZE_HINT) with ParallelRuntimeSpecSuite
                                               with EnterpriseNodeIndexSeekTestBase[EnterpriseRuntimeContext]
class ParallelRuntimeNodeIndexPointDistanceSeekTest extends NodeIndexPointDistanceSeekTestBase(FUSING, PARALLEL, SIZE_HINT) with ParallelRuntimeSpecSuite
class ParallelRuntimeNodeIndexPointDistanceSeekNoFusingTest extends NodeIndexPointDistanceSeekTestBase(NO_FUSING, PARALLEL, SIZE_HINT) with ParallelRuntimeSpecSuite

class ParallelRuntimeNodeIndexSeekRangeStressTest extends NodeIndexSeekRangeStressTestBase(FUSING,PARALLEL) with ParallelRuntimeSpecSuite
class ParallelRuntimeNodeIndexSeekRangeNoFusingStressTest extends NodeIndexSeekRangeStressTestBase(NO_FUSING, PARALLEL) with ParallelRuntimeSpecSuite
class ParallelRuntimeNodeIndexSeekExactStressTest extends NodeIndexSeekExactStressTestBase(FUSING, PARALLEL) with ParallelRuntimeSpecSuite
class ParallelRuntimeNodeIndexSeekExactNoFusingStressTest extends NodeIndexSeekExactStressTestBase(NO_FUSING, PARALLEL) with ParallelRuntimeSpecSuite
class ParallelRuntimeNodeIndexStartsWithSeekTest extends NodeIndexStartsWithSeekTestBase(FUSING, PARALLEL, SIZE_HINT) with ParallelRuntimeSpecSuite
class ParallelRuntimeNodeIndexStartsWithSeekNoFusingTest extends NodeIndexStartsWithSeekTestBase(NO_FUSING, PARALLEL, SIZE_HINT) with ParallelRuntimeSpecSuite

class ParallelRuntimeRelationshipIndexSeekTest extends RelationshipIndexSeekTestBase(FUSING, PARALLEL, SIZE_HINT) with ParallelRuntimeSpecSuite
class ParallelRuntimeRelationshipIndexSeekNoFusingTest extends RelationshipIndexSeekTestBase(NO_FUSING, PARALLEL, SIZE_HINT) with ParallelRuntimeSpecSuite
class ParallelRuntimeDirectedRelationshipIndexSeekExactStressTest extends DirectedRelationshipIndexSeekExactStressTestBase(FUSING, PARALLEL) with ParallelRuntimeSpecSuite
class ParallelRuntimeDirectedRelationshipIndexSeekExactNoFusingStressTest extends DirectedRelationshipIndexSeekExactStressTestBase(NO_FUSING, PARALLEL) with ParallelRuntimeSpecSuite
class ParallelRuntimeUndirectedRelationshipIndexSeekExactStressTest extends UndirectedRelationshipIndexSeekExactStressTestBase(FUSING, PARALLEL) with ParallelRuntimeSpecSuite
class ParallelRuntimeUndirectedRelationshipIndexSeekExactNoFusingStressTest extends UndirectedRelationshipIndexSeekExactStressTestBase(NO_FUSING, PARALLEL) with ParallelRuntimeSpecSuite
class ParallelRuntimeDirectedRelationshipIndexSeekRangeStressTest extends DirectedRelationshipIndexSeekRangeStressTestBase(FUSING, PARALLEL) with ParallelRuntimeSpecSuite
class ParallelRuntimeDirectedRelationshipIndexSeekRangeNoFusingStressTest extends DirectedRelationshipIndexSeekRangeStressTestBase(NO_FUSING, PARALLEL) with ParallelRuntimeSpecSuite
class ParallelRuntimeUndirectedRelationshipIndexSeekRangeStressTest extends UndirectedRelationshipIndexSeekRangeStressTestBase(FUSING, PARALLEL) with ParallelRuntimeSpecSuite
class ParallelRuntimeUndirectedRelationshipIndexSeekRangeNoFusingStressTest extends UndirectedRelationshipIndexSeekRangeStressTestBase(NO_FUSING, PARALLEL) with ParallelRuntimeSpecSuite

// INDEX SCAN
class ParallelRuntimeNodeIndexScanTest extends NodeIndexScanTestBase(FUSING, PARALLEL, SIZE_HINT) with ParallelRuntimeSpecSuite
class ParallelRuntimeNodeIndexScanNoFusingTest extends NodeIndexScanTestBase(NO_FUSING, PARALLEL, SIZE_HINT) with ParallelRuntimeSpecSuite
class ParallelRuntimeNodeIndexScanNoFusingStressTest extends NodeIndexScanStressTestBase(FUSING, PARALLEL) with ParallelRuntimeSpecSuite
class ParallelRuntimeNodeIndexScanStressTest extends NodeIndexScanStressTestBase(NO_FUSING, PARALLEL) with ParallelRuntimeSpecSuite

// RELATIONSHIP INDEX SCAN
class ParallelRuntimeRelationshipIndexScanTest extends RelationshipIndexScanTestBase(FUSING, PARALLEL, SIZE_HINT) with ParallelRuntimeSpecSuite
class ParallelRuntimeRelationshipIndexScanNoFusingTest extends RelationshipIndexScanTestBase(NO_FUSING, PARALLEL, SIZE_HINT) with ParallelRuntimeSpecSuite
class ParallelRuntimeDirectedRelationshipIndexScanNoFusingStressTest extends DirectedRelationshipIndexScanStressTestBase(FUSING, PARALLEL) with ParallelRuntimeSpecSuite
class ParallelRuntimeDirectedRelationshipIndexScanStressTest extends DirectedRelationshipIndexScanStressTestBase(NO_FUSING, PARALLEL) with ParallelRuntimeSpecSuite
class ParallelRuntimeUndirectedRelationshipIndexScanNoFusingStressTest extends UndirectedRelationshipIndexScanStressTestBase(FUSING, PARALLEL) with ParallelRuntimeSpecSuite
class ParallelRuntimeUndirectedRelationshipIndexScanStressTest extends UndirectedRelationshipIndexScanStressTestBase(NO_FUSING, PARALLEL) with ParallelRuntimeSpecSuite

// INDEX CONTAINS SCAN
class ParallelRuntimeNodeIndexContainsScanTest extends NodeIndexContainsScanTestBase(FUSING, PARALLEL, SIZE_HINT) with ParallelRuntimeSpecSuite
class ParallelRuntimeNodeIndexContainsScanNoFusingTest extends NodeIndexContainsScanTestBase(NO_FUSING, PARALLEL, SIZE_HINT) with ParallelRuntimeSpecSuite
class ParallelRuntimeNodeIndexContainsScanStressTest extends NodeIndexContainsScanStressTestBase(FUSING, PARALLEL) with ParallelRuntimeSpecSuite
class ParallelRuntimeNodeIndexContainsScanNoFusingStressTest extends NodeIndexContainsScanStressTestBase(NO_FUSING, PARALLEL) with ParallelRuntimeSpecSuite
class ParallelRuntimeRelationshipIndexContainsScanTest extends RelationshipIndexContainsScanTestBase(FUSING, PARALLEL, SIZE_HINT) with ParallelRuntimeSpecSuite
class ParallelRuntimeRelationshipIndexContainsScanNoFusingTest extends RelationshipIndexContainsScanTestBase(NO_FUSING, PARALLEL, SIZE_HINT) with ParallelRuntimeSpecSuite
class ParallelRuntimeDirectedRelationshipIndexContainsScanStressTest extends DirectedRelationshipIndexContainsScanStressTestBase(FUSING, PARALLEL) with ParallelRuntimeSpecSuite
class ParallelRuntimeDirectedRelationshipIndexContainsScanNoFusingStressTest extends DirectedRelationshipIndexContainsScanStressTestBase(NO_FUSING, PARALLEL) with ParallelRuntimeSpecSuite
class ParallelRuntimeUndirectedRelationshipIndexContainsScanStressTest extends UndirectedRelationshipIndexContainsScanStressTestBase(FUSING, PARALLEL) with ParallelRuntimeSpecSuite
class ParallelRuntimeUndirectedRelationshipIndexContainsScanNoFusingStressTest extends UndirectedRelationshipIndexContainsScanStressTestBase(NO_FUSING, PARALLEL) with ParallelRuntimeSpecSuite


// INDEX ENDS WITH SCAN
class ParallelRuntimeNodeIndexEndsWithScanTest extends NodeIndexEndsWithScanTestBase(FUSING, PARALLEL, SIZE_HINT) with ParallelRuntimeSpecSuite
class ParallelRuntimeNodeIndexEndsWithScanNoFusingTest extends NodeIndexEndsWithScanTestBase(NO_FUSING, PARALLEL, SIZE_HINT) with ParallelRuntimeSpecSuite
class ParallelRuntimeNodeIndexEndsWithScanStressTest extends NodeIndexEndsWithScanStressTestBase(FUSING, PARALLEL) with ParallelRuntimeSpecSuite
class ParallelRuntimeNodeIndexEndsWithScanNoFusingStressTest extends NodeIndexEndsWithScanStressTestBase(NO_FUSING, PARALLEL) with ParallelRuntimeSpecSuite
class ParallelRuntimeRelationshipIndexEndsWithScanTest extends RelationshipIndexEndsWithScanTestBase(FUSING, PARALLEL, SIZE_HINT) with ParallelRuntimeSpecSuite
class ParallelRuntimeRelationshipIndexEndsWithScanNoFusingTest extends RelationshipIndexEndsWithScanTestBase(NO_FUSING, PARALLEL, SIZE_HINT) with ParallelRuntimeSpecSuite
class ParallelRuntimeDirectedRelationshipIndexEndsWithScanStressTest extends DirectedRelationshipIndexEndsWithScanStressTestBase(FUSING, PARALLEL) with ParallelRuntimeSpecSuite
class ParallelRuntimeDirectedRelationshipIndexEndsWithScanNoFusingStressTest extends DirectedRelationshipIndexEndsWithScanStressTestBase(NO_FUSING, PARALLEL) with ParallelRuntimeSpecSuite
class ParallelRuntimeUndirectedRelationshipIndexEndsWithScanStressTest extends UndirectedRelationshipIndexEndsWithScanStressTestBase(FUSING, PARALLEL) with ParallelRuntimeSpecSuite
class ParallelRuntimeUndirectedRelationshipIndexEndsWithScanNoFusingStressTest extends UndirectedRelationshipIndexEndsWithScanStressTestBase(NO_FUSING, PARALLEL) with ParallelRuntimeSpecSuite

// ARGUMENT
class ParallelRuntimeArgumentTest extends ArgumentTestBase(FUSING, PARALLEL, SIZE_HINT) with ParallelRuntimeSpecSuite
class ParallelRuntimeArgumentNoFusingTest extends ArgumentTestBase(NO_FUSING, PARALLEL, SIZE_HINT) with ParallelRuntimeSpecSuite
class ParallelRuntimeArgumentStressTest extends ArgumentStressTestBase(FUSING, PARALLEL) with ParallelRuntimeSpecSuite
class ParallelRuntimeArgumentNoFusingStressTest extends ArgumentStressTestBase(NO_FUSING, PARALLEL) with ParallelRuntimeSpecSuite

// APPLY
class ParallelRuntimeApplyStressTest extends ApplyStressTestBase(FUSING, PARALLEL) with ParallelRuntimeSpecSuite
class ParallelRuntimeApplyNoFusingStressTest extends ApplyStressTestBase(NO_FUSING, PARALLEL) with ParallelRuntimeSpecSuite

// EXPAND ALL
class ParallelRuntimeExpandAllTest extends ExpandAllTestBase(FUSING, PARALLEL, SIZE_HINT) with ParallelRuntimeSpecSuite
                                   with ExpandAllWithOtherOperatorsTestBase[EnterpriseRuntimeContext]
class ParallelRuntimeExpandAllNoFusingTest extends ExpandAllTestBase(NO_FUSING, PARALLEL, SIZE_HINT) with ParallelRuntimeSpecSuite
                                           with ExpandAllWithOtherOperatorsTestBase[EnterpriseRuntimeContext]
class ParallelRuntimeExpandAllStressTest extends ExpandAllStressTestBase(FUSING, PARALLEL) with TimeLimitedCypherTest
class ParallelRuntimeExpandAllNoFusingStressTest extends ExpandAllStressTestBase(NO_FUSING, PARALLEL) with TimeLimitedCypherTest

// EXPAND INTO
class ParallelRuntimeExpandIntoTest extends ExpandIntoTestBase(FUSING, PARALLEL, SIZE_HINT)
                                    with ExpandIntoWithOtherOperatorsTestBase[EnterpriseRuntimeContext] with ParallelRuntimeSpecSuite
class ParallelRuntimeExpandIntoNoFusingTest extends ExpandIntoTestBase(NO_FUSING, PARALLEL, SIZE_HINT)
                                            with ExpandIntoWithOtherOperatorsTestBase[EnterpriseRuntimeContext] with ParallelRuntimeSpecSuite

// OPTIONAL EXPAND ALL
class ParallelRuntimeOptionalExpandAllTest extends OptionalExpandAllTestBase(FUSING, PARALLEL, SIZE_HINT)  with ParallelRuntimeSpecSuite {
  test("should handle many optional expand with random predicates II") {
    // given
    given {
      val allLabels = Array("A", "B", "C", "D", "E")
      def randomLabel = label(allLabels(Random.nextInt(allLabels.length)))
      val nodes = nodeGraph(sizeHint, "N")
      nodes.foreach(n => {
        (1 to Random.nextInt(10)).foreach(_ => n.createRelationshipTo(tx.createNode(randomLabel), RelationshipType.withName("R")))
        (1 to Random.nextInt(10)).foreach(_ => n.createRelationshipTo(tx.createNode(randomLabel), RelationshipType.withName("S")))
        (1 to Random.nextInt(10)).foreach(_ => n.createRelationshipTo(tx.createNode(randomLabel), RelationshipType.withName("T")))
        (1 to Random.nextInt(10)).foreach(_ => n.createRelationshipTo(tx.createNode(randomLabel), RelationshipType.withName("U")))
        (1 to Random.nextInt(10)).foreach(_ => n.createRelationshipTo(tx.createNode(randomLabel), RelationshipType.withName("V")))
      })
      nodes
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nonFuseable()
      .optionalExpandAll("(x)-[r1:V]->(e)", Some("e:E"))
      .optionalExpandAll("(x)-[r1:U]->(d)", Some("d:D"))
      .optionalExpandAll("(x)-[r1:T]->(c)", Some("c:C"))
      .optionalExpandAll("(x)-[r1:S]->(b)", Some("b:B"))
      .optionalExpandAll("(x)-[r1:R]->(a)", Some("a:A"))
      .nodeByLabelScan("x", "N")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then, just make sure the query finishes
    consume(runtimeResult) should not be empty
  }

}
class ParallelRuntimeOptionalExpandAllNoFusingTest extends OptionalExpandAllTestBase(NO_FUSING, PARALLEL, SIZE_HINT)  with ParallelRuntimeSpecSuite

// OPTIONAL EXPAND INTO
class ParallelRuntimeOptionalExpandIntoTest extends OptionalExpandIntoTestBase(FUSING, PARALLEL, SIZE_HINT)  with ParallelRuntimeSpecSuite
class ParallelRuntimeOptionalExpandIntoNoFusingTest extends OptionalExpandIntoTestBase(NO_FUSING, PARALLEL, SIZE_HINT)  with ParallelRuntimeSpecSuite

// VAR EXPAND
class ParallelRuntimeVarLengthExpandTest extends VarLengthExpandTestBase(FUSING, PARALLEL, SIZE_HINT) with ParallelRuntimeSpecSuite
class ParallelRuntimeNoFusingVarLengthExpandTest extends VarLengthExpandTestBase(NO_FUSING, PARALLEL, SIZE_HINT) with ParallelRuntimeSpecSuite
class ParallelRuntimeVarExpandStressTest extends VarExpandStressTestBase(FUSING, PARALLEL) with ParallelRuntimeSpecSuite
class ParallelRuntimeVarExpandNoFusingStressTest extends VarExpandStressTestBase(NO_FUSING, PARALLEL) with ParallelRuntimeSpecSuite

// PRUNING VAR EXPAND
class ParallelRuntimePruningVarLengthExpandTest extends PruningVarLengthExpandTestBase(FUSING, PARALLEL, SIZE_HINT) with ParallelRuntimeSpecSuite
class ParallelRuntimePruningNoFusingVarLengthExpandTest extends PruningVarLengthExpandTestBase(NO_FUSING, PARALLEL, SIZE_HINT) with ParallelRuntimeSpecSuite

// PROJECTION
class ParallelRuntimeProjectionTest extends ProjectionTestBase(FUSING, PARALLEL, SIZE_HINT) with ParallelRuntimeSpecSuite
class ParallelRuntimeProjectionNoFusingTest extends ProjectionTestBase(NO_FUSING, PARALLEL, SIZE_HINT) with ParallelRuntimeSpecSuite
class ParallelRuntimeProjectionStressTest extends ProjectionStressTestBase(FUSING, PARALLEL) with ParallelRuntimeSpecSuite
class ParallelRuntimeProjectionNoFusingStressTest extends ProjectionStressTestBase(NO_FUSING, PARALLEL) with ParallelRuntimeSpecSuite
class ParallelRuntimeCachePropertiesTest extends CachePropertiesTestBase(FUSING, PARALLEL, SIZE_HINT) with ParallelRuntimeSpecSuite
class ParallelRuntimeCachePropertiesNoFusingTest extends CachePropertiesTestBase(NO_FUSING, PARALLEL, SIZE_HINT) with ParallelRuntimeSpecSuite

// FILTER
class ParallelRuntimeFilterTest extends FilterTestBase(FUSING, PARALLEL, SIZE_HINT) with ParallelRuntimeSpecSuite
class ParallelRuntimeFilterNoFusingTest extends FilterTestBase(NO_FUSING, PARALLEL, SIZE_HINT) with ParallelRuntimeSpecSuite
class ParallelRuntimeFilterStressTest extends FilterStressTestBase(FUSING, PARALLEL) with ParallelRuntimeSpecSuite
class ParallelRuntimeFilterNoFusingStressTest extends FilterStressTestBase(NO_FUSING, PARALLEL) with ParallelRuntimeSpecSuite

//Misc Expressions
class ParallelRuntimeExpressionStressTest extends ExpressionStressTestBase(FUSING, PARALLEL) with ParallelRuntimeSpecSuite
class ParallelRuntimeExpressionNoFusingStressTest extends ExpressionStressTestBase(NO_FUSING, PARALLEL) with ParallelRuntimeSpecSuite

// LIMIT
class ParallelRuntimeLimitTest extends LimitTestBase(FUSING, PARALLEL, SIZE_HINT) with ParallelRuntimeSpecSuite
class ParallelRuntimeLimitNoFusingTest extends LimitTestBase(NO_FUSING, PARALLEL, SIZE_HINT) with ParallelRuntimeSpecSuite

// SKIP
class ParallelRuntimeSkipTest extends SkipTestBase(FUSING, PARALLEL, SIZE_HINT) with ParallelRuntimeSpecSuite
class ParallelRuntimeSkipNoFusingTest extends SkipTestBase(NO_FUSING, PARALLEL, SIZE_HINT) with ParallelRuntimeSpecSuite

// DISTINCT
class ParallelRuntimeDistinctTest extends DistinctTestBase(FUSING, PARALLEL, SIZE_HINT) with ParallelRuntimeSpecSuite
class ParallelRuntimeDistinctStressTest extends DistinctStressTestBase(FUSING, PARALLEL) with ParallelRuntimeSpecSuite
class ParallelRuntimeDistinctNoFusingStressTest extends DistinctStressTestBase(NO_FUSING, PARALLEL) with ParallelRuntimeSpecSuite

// UNWIND
class ParallelRuntimeUnwindTest extends UnwindTestBase(FUSING, PARALLEL, SIZE_HINT) with ParallelRuntimeSpecSuite
class ParallelRuntimeUnwindNoFusingTest extends UnwindTestBase(NO_FUSING, PARALLEL, SIZE_HINT) with ParallelRuntimeSpecSuite
class ParallelRuntimeUnwindStressTest extends UnwindStressTestBase(FUSING, PARALLEL) with ParallelRuntimeSpecSuite
class ParallelRuntimeUnwindNoFusingStressTest extends UnwindStressTestBase(NO_FUSING, PARALLEL) with ParallelRuntimeSpecSuite

// SORT
class ParallelRuntimeSortTest extends SortTestBase(FUSING, PARALLEL, SIZE_HINT) with ParallelRuntimeSpecSuite
class ParallelRuntimeSortNoFusingTest extends SortTestBase(NO_FUSING, PARALLEL, SIZE_HINT) with ParallelRuntimeSpecSuite

// TOP
class ParallelRuntimeTopTest extends TopTestBase(FUSING, PARALLEL, SIZE_HINT) with ParallelRuntimeSpecSuite
class ParallelRuntimeTopNoFusingTest extends TopTestBase(NO_FUSING, PARALLEL, SIZE_HINT) with ParallelRuntimeSpecSuite

// TOP1 WITH TIES
class ParallelRuntimeTop1WithTiesTest extends Top1WithTiesTestBase(FUSING, PARALLEL, SIZE_HINT) with ParallelRuntimeSpecSuite
class ParallelRuntimeTop1WithTiesNoFusingTest extends Top1WithTiesTestBase(NO_FUSING, PARALLEL, SIZE_HINT) with ParallelRuntimeSpecSuite


// AGGREGATION
class ParallelRuntimeAggregationTest extends AggregationTestBase(FUSING, PARALLEL, SIZE_HINT) with ParallelRuntimeSpecSuite
class ParallelRuntimeAggregationNoFusingTest extends AggregationTestBase(NO_FUSING, PARALLEL, SIZE_HINT)
class ParallelRuntimeAggregationStressTest extends AggregationStressTestBase(FUSING, PARALLEL) with ParallelRuntimeSpecSuite

// NODE HASH JOIN
class ParallelRuntimeNodeHashJoinTest extends NodeHashJoinTestBase(FUSING, PARALLEL, SIZE_HINT) with ParallelRuntimeSpecSuite
class ParallelRuntimeNodeHashJoinNoFusingTest extends NodeHashJoinTestBase(NO_FUSING, PARALLEL, SIZE_HINT) with ParallelRuntimeSpecSuite

// NODE RIGHT OUTER HASH JOIN
class ParallelRuntimeNodeRightOuterHashJoinTest extends RightOuterHashJoinTestBase(FUSING, PARALLEL, SIZE_HINT) with ParallelRuntimeSpecSuite
class ParallelRuntimeNodeRightOuterHashJoinNoFusingTest extends RightOuterHashJoinTestBase(NO_FUSING, PARALLEL, SIZE_HINT) with ParallelRuntimeSpecSuite

// NODE LEFT OUTER HASH JOIN
class ParallelRuntimeNodeLeftOuterHashJoinTest extends LeftOuterHashJoinTestBase(FUSING, PARALLEL, SIZE_HINT) with ParallelRuntimeSpecSuite
class ParallelRuntimeNodeLeftOuterHashJoinNoFusingTest extends LeftOuterHashJoinTestBase(NO_FUSING, PARALLEL, SIZE_HINT) with ParallelRuntimeSpecSuite

// VALUE HASH JOIN
class ParallelRuntimeValueHashJoinTest extends ValueHashJoinTestBase(FUSING, PARALLEL, SIZE_HINT) with ParallelRuntimeSpecSuite
class ParallelRuntimeValueHashJoinNoFusingTest extends ValueHashJoinTestBase(NO_FUSING, PARALLEL, SIZE_HINT) with ParallelRuntimeSpecSuite

// PROVIDED ORDER
class ParallelRuntimeProvidedOrderTest extends ProvidedOrderTestBase(FUSING, PARALLEL, SIZE_HINT) with ParallelRuntimeSpecSuite
  with CartesianProductProvidedOrderTestBase[EnterpriseRuntimeContext]
class ParallelRuntimeNoFusingProvidedOrderTest extends ProvidedOrderTestBase(NO_FUSING, PARALLEL, SIZE_HINT) with ParallelRuntimeSpecSuite
  with CartesianProductProvidedOrderTestBase[EnterpriseRuntimeContext]

// REACTIVE
class ParallelRuntimeReactiveResultsTest extends ReactiveResultTestBase(FUSING, PARALLEL) with ParallelRuntimeSpecSuite
class ParallelRuntimeReactiveResultsNoFusingTest extends ReactiveResultTestBase(NO_FUSING, PARALLEL) with ParallelRuntimeSpecSuite
class ParallelRuntimeReactiveResultsStressTest
  extends ReactiveResultStressTestBase(FUSING, PARALLEL,
    ReactiveResultStressTestBase.MORSEL_SIZE + 1) with ParallelRuntimeSpecSuite// this test is slow, hence the reduced size
class ParallelRuntimeReactiveNoFusingStressTest
  extends ReactiveResultStressTestBase(NO_FUSING, PARALLEL,
    ReactiveResultStressTestBase.MORSEL_SIZE + 1) with ParallelRuntimeSpecSuite// this test is slow, hence the reduced size

// OPTIONAL
class ParallelRuntimeOptionalTest extends OptionalTestBase(FUSING, PARALLEL, SIZE_HINT) with ParallelRuntimeSpecSuite
class ParallelRuntimeOptionalNoFusingTest extends OptionalTestBase(NO_FUSING, PARALLEL, SIZE_HINT) with ParallelRuntimeSpecSuite


// CARTESIAN PRODUCT
class ParallelRuntimeCartesianProductTest extends CartesianProductTestBase(FUSING, PARALLEL, SIZE_HINT) with ParallelRuntimeSpecSuite
class ParallelRuntimeCartesianProductStressTest extends CartesianProductStressTestBase(FUSING, PARALLEL) with ParallelRuntimeSpecSuite
class ParallelRuntimeCartesianProductNoFusingTest extends CartesianProductTestBase(NO_FUSING, PARALLEL, SIZE_HINT) with ParallelRuntimeSpecSuite

// APPLY
class ParallelRuntimeApplyTest extends ApplyTestBase(FUSING, PARALLEL, SIZE_HINT) with ParallelRuntimeSpecSuite
class ParallelRuntimeApplyNoFusingTest extends ApplyTestBase(NO_FUSING, PARALLEL, SIZE_HINT) with ParallelRuntimeSpecSuite

// SHORTEST PATH
class ParallelRuntimeShortestPathTest extends ShortestPathTestBase(FUSING, PARALLEL, SIZE_HINT) with ParallelRuntimeSpecSuite
class ParallelRuntimeShortestPathNoFusingTest extends ShortestPathTestBase(NO_FUSING, PARALLEL, SIZE_HINT) with ParallelRuntimeSpecSuite

// UNION
class ParallelRuntimeUnionTest extends UnionTestBase(FUSING, PARALLEL, SIZE_HINT) with ParallelRuntimeSpecSuite
class ParallelRuntimeUnionNoFusingTest extends UnionTestBase(NO_FUSING, PARALLEL, SIZE_HINT) with ParallelRuntimeSpecSuite

// SEMI APPLY
class ParallelRuntimeSemiApplyTest extends SemiApplyTestBase(FUSING, PARALLEL, SIZE_HINT) with ParallelRuntimeSpecSuite
class ParallelRuntimeSemiApplyNoFusingTest extends SemiApplyTestBase(NO_FUSING, PARALLEL, SIZE_HINT) with ParallelRuntimeSpecSuite

// ANTI SEMI APPLY
class ParallelAntiSemiApplyTest extends AntiSemiApplyTestBase(FUSING, PARALLEL, SIZE_HINT) with ParallelRuntimeSpecSuite
class ParallelAntiSemiApplyNoFusingTest extends AntiSemiApplyTestBase(NO_FUSING, PARALLEL, SIZE_HINT) with ParallelRuntimeSpecSuite

// ROLLUP APPLY
class ParallelRuntimeRollupApplyTest extends RollupApplyTestBase(FUSING, PARALLEL, SIZE_HINT)
class ParallelRuntimeRollupApplyNoFusingTest extends RollupApplyTestBase(NO_FUSING, PARALLEL, SIZE_HINT)

// EMPTY RESULT
class ParallelRuntimeEmptyResultTest extends EmptyResultTestBase(FUSING, PARALLEL, SIZE_HINT)
class ParallelRuntimeEmptyResultNoFusingTest extends EmptyResultTestBase(NO_FUSING, PARALLEL, SIZE_HINT)

// EAGER
class ParallelRuntimeEagerTest extends EagerTestBase(FUSING, PARALLEL, SIZE_HINT)
class ParallelRuntimeEagerNoFusingTest extends EagerTestBase(NO_FUSING, PARALLEL, SIZE_HINT)

// GENERAL
class ParallelRuntimeMiscTest extends MiscTestBase(FUSING, PARALLEL) with ParallelRuntimeSpecSuite
class ParallelRuntimeNoFusingMiscTest extends MiscTestBase(NO_FUSING, PARALLEL) with ParallelRuntimeSpecSuite
class ParallelRuntimeExpressionTest extends ExpressionTestBase(FUSING, PARALLEL) with ParallelRuntimeSpecSuite
class ParallelRuntimeNoFusingExpressionTest extends ExpressionTestBase(NO_FUSING, PARALLEL) with ParallelRuntimeSpecSuite
class ParallelFusingNotificationTest extends PipelinedFusingNotificationTestBase(FUSING, PARALLEL) with TimeLimitedCypherTest // not ParallelRuntimeSpecSuite, since we expect fusing to fail
class ParallelRuntimeSchedulerTracerTest extends SchedulerTracerTestBase(PARALLEL) with ParallelRuntimeSpecSuite
class ParallelRuntimeMemoryManagementDisabledTest extends MemoryManagementDisabledTestBase(FUSING, PARALLEL) with ParallelRuntimeSpecSuite
class ParallelRuntimeSubscriberErrorTest extends SubscriberErrorTestBase(NO_FUSING, PARALLEL) with ParallelRuntimeSpecSuite

// WORKLOAD
class ParallelRuntimeWorkloadTest extends WorkloadTestBase(FUSING, PARALLEL, SIZE_HINT) with ParallelRuntimeSpecSuite
class ParallelRuntimeNoFusingWorkloadTest extends WorkloadTestBase(NO_FUSING, PARALLEL, SIZE_HINT) with ParallelRuntimeSpecSuite

// ERROR HANDLING
class ParallelErrorHandlingTest extends ParallelErrorHandlingTestBase(PARALLEL) with ParallelRuntimeSpecSuite

// PROFILE
class ParallelRuntimeProfileNoFusingRowsTest extends ProfileRowsTestBase(NO_FUSING, PARALLEL, SIZE_HINT, ENTERPRISE.MORSEL_SIZE) with ParallelRuntimeSpecSuite
class ParallelRuntimeProfileRowsTest extends ProfileRowsTestBase(FUSING, PARALLEL, SIZE_HINT, ENTERPRISE.MORSEL_SIZE) with ParallelRuntimeSpecSuite
class ParallelRuntimeProfileNoFusingTimeTest extends ProfileTimeTestBase(NO_FUSING, PARALLEL, SIZE_HINT) with ParallelRuntimeSpecSuite
class ParallelRuntimeProfileNoTimeTest extends ProfileNoTimeTestBase(FUSING, PARALLEL, SIZE_HINT) with ParallelRuntimeSpecSuite {
  //this test differs in Morsel and Parallel since we fuse differently
  test("should profile time in both fused pipelines and non-fused pipelines when they co-exist") {
    given { circleGraph(SIZE_HINT, "X") }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("c")
      .sort(Seq(Ascending("c")))
      .aggregation(Seq("x AS x"), Seq("count(*) AS c"))
      .filter("x.prop = null")
      .expand("(x)-->(y)")
      .nodeByLabelScan("x", "X", IndexOrderNone)
      .build()

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(0).time() should not be OperatorProfile.NO_DATA // produce results - not fused
    queryProfile.operatorProfile(1).time() should not be OperatorProfile.NO_DATA // sort - not fused
    queryProfile.operatorProfile(2).time() should not be OperatorProfile.NO_DATA // aggregation - not fused
    queryProfile.operatorProfile(3).time() should be(OperatorProfile.NO_DATA) // filter - fused
    queryProfile.operatorProfile(4).time() should not be OperatorProfile.NO_DATA // expand - fused
    queryProfile.operatorProfile(5).time() should not be OperatorProfile.NO_DATA // node by label scan - not fused
    // Should not attribute anything to the invalid id
    queryProfile.operatorProfile(Id.INVALID_ID.x) should be(NO_PROFILE)
  }
}
class ParallelRuntimeProfileNoFusingDbHitsTest extends PipelinedDbHitsTestBase(NO_FUSING, PARALLEL, SIZE_HINT, canFuseOverPipelines = false) with ParallelRuntimeSpecSuite
