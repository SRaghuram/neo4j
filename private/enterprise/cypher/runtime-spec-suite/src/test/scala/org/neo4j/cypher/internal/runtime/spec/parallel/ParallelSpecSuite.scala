/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.spec.parallel

import org.neo4j.cypher.internal.EnterpriseRuntimeContext
import org.neo4j.cypher.internal.MorselRuntime.{MORSEL, PARALLEL}
import org.neo4j.cypher.internal.runtime.spec._
import org.neo4j.cypher.internal.runtime.spec.morsel.{MorselDbHitsTestBase, ProfileNoTimeTestBase, SchedulerTracerTestBase}
import org.neo4j.cypher.internal.runtime.spec.parallel.ParallelRuntimeSpecSuite.SIZE_HINT
import org.neo4j.cypher.internal.runtime.spec.stress._
import org.neo4j.cypher.internal.runtime.spec.tests._

object ParallelRuntimeSpecSuite {
  val SIZE_HINT = 1000
}

// INPUT
class ParallelRuntimeInputTest extends ParallelInputTestBase(ENTERPRISE.FUSING, PARALLEL) with TimeLimitedCypherTest
class ParallelRuntimeInputTestNoFusing extends ParallelInputTestBase(ENTERPRISE.NO_FUSING, PARALLEL) with TimeLimitedCypherTest

// ALL NODE SCAN
class ParallelRuntimeAllNodeScanTest extends AllNodeScanTestBase(ENTERPRISE.FUSING, PARALLEL, SIZE_HINT) with TimeLimitedCypherTest
class ParallelRuntimeAllNodeScanNoFusingTest extends AllNodeScanTestBase(ENTERPRISE.NO_FUSING, PARALLEL, SIZE_HINT) with TimeLimitedCypherTest
class ParallelRuntimeAllNodeScanStressTest extends AllNodeScanStressTestBase(ENTERPRISE.FUSING, PARALLEL) with TimeLimitedCypherTest
class ParallelRuntimeAllNodeScanNoFusingStressTest extends AllNodeScanStressTestBase(ENTERPRISE.NO_FUSING, PARALLEL) with TimeLimitedCypherTest

// NODE BY ID SEEK
class ParallelRuntimeNodeByIdSeekTest extends NodeByIdSeekTestBase(ENTERPRISE.FUSING, PARALLEL, SIZE_HINT) with TimeLimitedCypherTest
class ParallelRuntimeNodeByIdSeekNoFusingTest extends NodeByIdSeekTestBase(ENTERPRISE.NO_FUSING, PARALLEL, SIZE_HINT) with TimeLimitedCypherTest
class ParallelRuntimeNodeByIdSeekStressTest extends NodeByIdSeekStressTestBase(ENTERPRISE.FUSING, PARALLEL) with TimeLimitedCypherTest
class ParallelRuntimeNodeByIdSeekNoFusingStressTest extends NodeByIdSeekStressTestBase(ENTERPRISE.NO_FUSING, PARALLEL) with TimeLimitedCypherTest

// RELATIONSHIP BY ID SEEK
class MorselDirectedRelationshipByIdSeekTest extends DirectedRelationshipByIdSeekTestBase(ENTERPRISE.FUSING, MORSEL, SIZE_HINT) with TimeLimitedCypherTest
class MorselDirectedRelationshipByIdSeekNoFusingTest extends DirectedRelationshipByIdSeekTestBase(ENTERPRISE.NO_FUSING, MORSEL, SIZE_HINT) with TimeLimitedCypherTest

//UNDIRECTED RELATIONSHIP BY ID SEEK
class MorselUndirectedRelationshipByIdSeekTest extends UndirectedRelationshipByIdSeekTestBase(ENTERPRISE.FUSING, MORSEL, SIZE_HINT) with TimeLimitedCypherTest
class MorselUndirectedRelationshipByIdSeekNoFusingTest extends UndirectedRelationshipByIdSeekTestBase(ENTERPRISE.NO_FUSING, MORSEL, SIZE_HINT) with TimeLimitedCypherTest

// NODE COUNT FROM COUNT STORE
class ParallelNodeCountFromCountStoreTest extends NodeCountFromCountStoreTestBase(ENTERPRISE.FUSING, PARALLEL) with TimeLimitedCypherTest
class ParallelNodeCountFromCountStoreNoFusingTest extends NodeCountFromCountStoreTestBase(ENTERPRISE.NO_FUSING, PARALLEL) with TimeLimitedCypherTest

// RELATIONSHIP COUNT FROM COUNT STORE
class ParallelRelationshipCountFromCountStoreTest extends RelationshipCountFromCountStoreTestBase(ENTERPRISE.FUSING, PARALLEL) with TimeLimitedCypherTest
class ParallelRelationshipCountFromCountStoreNoFusingTest extends RelationshipCountFromCountStoreTestBase(ENTERPRISE.NO_FUSING, PARALLEL) with TimeLimitedCypherTest

// LABEL SCAN
class ParallelRuntimeLabelScanTest extends LabelScanTestBase(ENTERPRISE.FUSING, PARALLEL, SIZE_HINT) with TimeLimitedCypherTest
class ParallelRuntimeLabelScanNoFusingTest extends LabelScanTestBase(ENTERPRISE.NO_FUSING, PARALLEL, SIZE_HINT) with TimeLimitedCypherTest
class ParallelRuntimeLabelScanStressTest extends LabelScanStressTestBase(ENTERPRISE.FUSING, PARALLEL) with TimeLimitedCypherTest
class ParallelRuntimeLabelScanNoFusingStressTest extends LabelScanStressTestBase(ENTERPRISE.NO_FUSING, PARALLEL) with TimeLimitedCypherTest

// INDEX SEEK
class ParallelRuntimeNodeIndexSeekTest extends NodeIndexSeekTestBase(ENTERPRISE.FUSING, PARALLEL, SIZE_HINT) with TimeLimitedCypherTest
                                       with NodeIndexSeekRangeAndCompositeTestBase[EnterpriseRuntimeContext]
                                       with ArrayIndexSupport[EnterpriseRuntimeContext]
class ParallelRuntimeNodeIndexSeekNoFusingTest extends NodeIndexSeekTestBase(ENTERPRISE.NO_FUSING, PARALLEL, SIZE_HINT) with TimeLimitedCypherTest
                                               with NodeIndexSeekRangeAndCompositeTestBase[EnterpriseRuntimeContext]
                                               with ArrayIndexSupport[EnterpriseRuntimeContext]

class ParallelRuntimeIndexSeekRangeStressTest extends IndexSeekRangeStressTestBase(ENTERPRISE.FUSING,PARALLEL) with TimeLimitedCypherTest
class ParallelRuntimeIndexSeekRangeNoFusingStressTest extends IndexSeekRangeStressTestBase(ENTERPRISE.NO_FUSING, PARALLEL) with TimeLimitedCypherTest
class ParallelRuntimeIndexSeekExactStressTest extends IndexSeekExactStressTest(ENTERPRISE.FUSING, PARALLEL) with TimeLimitedCypherTest
class ParallelRuntimeIndexSeekExactNoFusingStressTest extends IndexSeekExactStressTest(ENTERPRISE.NO_FUSING, PARALLEL) with TimeLimitedCypherTest

// INDEX SCAN
class ParallelRuntimeNodeIndexScanTest extends NodeIndexScanTestBase(ENTERPRISE.FUSING, PARALLEL, SIZE_HINT) with TimeLimitedCypherTest
class ParallelRuntimeNodeIndexScanNoFusingTest extends NodeIndexScanTestBase(ENTERPRISE.NO_FUSING, PARALLEL, SIZE_HINT) with TimeLimitedCypherTest
class ParallelRuntimeIndexScanNoFusingStressTest extends IndexScanStressTestBase(ENTERPRISE.FUSING, PARALLEL) with TimeLimitedCypherTest
class ParallelRuntimeIndexScanStressTest extends IndexScanStressTestBase(ENTERPRISE.NO_FUSING, PARALLEL) with TimeLimitedCypherTest

// INDEX CONTAINS SCAN
class ParallelRuntimeNodeIndexContainsScanTest extends NodeIndexContainsScanTestBase(ENTERPRISE.FUSING, PARALLEL, SIZE_HINT) with TimeLimitedCypherTest
class ParallelRuntimeNodeIndexContainsScanNoFusingTest extends NodeIndexContainsScanTestBase(ENTERPRISE.NO_FUSING, PARALLEL, SIZE_HINT) with TimeLimitedCypherTest
class ParallelRuntimeIndexContainsScanStressTest extends IndexContainsScanStressTestBase(ENTERPRISE.FUSING, PARALLEL) with TimeLimitedCypherTest
class ParallelRuntimeIndexContainsScanNoFusingStressTest extends IndexContainsScanStressTestBase(ENTERPRISE.NO_FUSING, PARALLEL) with TimeLimitedCypherTest

// INDEX ENDS WITH SCAN
class ParallelRuntimeNodeIndexEndsWithScanTest extends NodeIndexEndsWithScanTestBase(ENTERPRISE.FUSING, PARALLEL, SIZE_HINT) with TimeLimitedCypherTest
class ParallelRuntimeNodeIndexEndsWithScanNoFusingTest extends NodeIndexEndsWithScanTestBase(ENTERPRISE.NO_FUSING, PARALLEL, SIZE_HINT) with TimeLimitedCypherTest
class ParallelRuntimeIndexEndsWithScanStressTest extends IndexEndsWithScanStressTestBase(ENTERPRISE.FUSING, PARALLEL) with TimeLimitedCypherTest
class ParallelRuntimeIndexEndsWithScanNoFusingStressTest extends IndexEndsWithScanStressTestBase(ENTERPRISE.NO_FUSING, PARALLEL) with TimeLimitedCypherTest

// ARGUMENT
class ParallelRuntimeArgumentTest extends ArgumentTestBase(ENTERPRISE.FUSING, PARALLEL, SIZE_HINT) with TimeLimitedCypherTest
class ParallelRuntimeArgumentNoFusingTest extends ArgumentTestBase(ENTERPRISE.NO_FUSING, PARALLEL, SIZE_HINT) with TimeLimitedCypherTest
class ParallelRuntimeArgumentStressTest extends ArgumentStressTestBase(ENTERPRISE.FUSING, PARALLEL) with TimeLimitedCypherTest
class ParallelRuntimeArgumentNoFusingStressTest extends ArgumentStressTestBase(ENTERPRISE.NO_FUSING, PARALLEL) with TimeLimitedCypherTest

// APPLY
class ParallelRuntimeApplyStressTest extends ApplyStressTestBase(ENTERPRISE.FUSING, PARALLEL) with TimeLimitedCypherTest
class ParallelRuntimeApplyNoFusingStressTest extends ApplyStressTestBase(ENTERPRISE.NO_FUSING, PARALLEL) with TimeLimitedCypherTest

// EXPAND
class ParallelRuntimeExpandAllTest extends ExpandAllTestBase(ENTERPRISE.FUSING, PARALLEL, SIZE_HINT) with TimeLimitedCypherTest
                                   with ExpandAllWithOtherOperatorsTestBase[EnterpriseRuntimeContext]
class ParallelRuntimeExpandAllTestNoFusing extends ExpandAllTestBase(ENTERPRISE.NO_FUSING, PARALLEL, SIZE_HINT) with TimeLimitedCypherTest
                                           with ExpandAllWithOtherOperatorsTestBase[EnterpriseRuntimeContext]
class ParallelRuntimeExpandStressTest extends ExpandStressTestBase(ENTERPRISE.FUSING, PARALLEL) with TimeLimitedCypherTest
class ParallelRuntimeExpandNoFusingStressTest extends ExpandStressTestBase(ENTERPRISE.NO_FUSING, PARALLEL) with TimeLimitedCypherTest

// VAR EXPAND
class ParallelRuntimeVarLengthExpandTest extends VarLengthExpandTestBase(ENTERPRISE.FUSING, PARALLEL, SIZE_HINT) with TimeLimitedCypherTest
class ParallelRuntimeNoFusingVarLengthExpandTest extends VarLengthExpandTestBase(ENTERPRISE.NO_FUSING, PARALLEL, SIZE_HINT) with TimeLimitedCypherTest
class ParallelRuntimeVarExpandStressTest extends VarExpandStressTestBase(ENTERPRISE.FUSING, PARALLEL) with TimeLimitedCypherTest
class ParallelRuntimeVarExpandNoFusingStressTest extends VarExpandStressTestBase(ENTERPRISE.NO_FUSING, PARALLEL) with TimeLimitedCypherTest

// PROJECTION
class ParallelRuntimeProjectionTest extends ProjectionTestBase(ENTERPRISE.FUSING, PARALLEL, SIZE_HINT) with TimeLimitedCypherTest
class ParallelRuntimeProjectionNoFusingTest extends ProjectionTestBase(ENTERPRISE.NO_FUSING, PARALLEL, SIZE_HINT) with TimeLimitedCypherTest
class ParallelRuntimeProjectionStressTest extends ProjectionStressTestBase(ENTERPRISE.FUSING, PARALLEL) with TimeLimitedCypherTest
class ParallelRuntimeProjectionNoFusingStressTest extends ProjectionStressTestBase(ENTERPRISE.NO_FUSING, PARALLEL) with TimeLimitedCypherTest
class ParallelRuntimeCachePropertiesTest extends CachePropertiesTestBase(ENTERPRISE.FUSING, PARALLEL, SIZE_HINT) with TimeLimitedCypherTest
class ParallelRuntimeCachePropertiesNoFusingTest extends CachePropertiesTestBase(ENTERPRISE.NO_FUSING, PARALLEL, SIZE_HINT) with TimeLimitedCypherTest

// FILTER
class ParallelRuntimeFilterTest extends FilterTestBase(ENTERPRISE.FUSING, PARALLEL, SIZE_HINT) with TimeLimitedCypherTest
class ParallelRuntimeFilterNoFusingTest extends FilterTestBase(ENTERPRISE.NO_FUSING, PARALLEL, SIZE_HINT) with TimeLimitedCypherTest
class ParallelRuntimeFilterStressTest extends FilterStressTestBase(ENTERPRISE.FUSING, PARALLEL) with TimeLimitedCypherTest
class ParallelRuntimeFilterNoFusingStressTest extends FilterStressTestBase(ENTERPRISE.NO_FUSING, PARALLEL) with TimeLimitedCypherTest

//Misc Expressions
class ParallelRuntimeExpressionStressTest extends ExpressionStressTestBase(ENTERPRISE.FUSING, PARALLEL) with TimeLimitedCypherTest
class ParallelRuntimeExpressionNoFusingStressTest extends ExpressionStressTestBase(ENTERPRISE.NO_FUSING, PARALLEL) with TimeLimitedCypherTest

// LIMIT
class ParallelRuntimeLimitTest extends LimitTestBase(ENTERPRISE.FUSING, PARALLEL, SIZE_HINT) with TimeLimitedCypherTest
class ParallelRuntimeLimitNoFusingTest extends LimitTestBase(ENTERPRISE.NO_FUSING, PARALLEL, SIZE_HINT) with TimeLimitedCypherTest

// DISTINCT
class ParallelRuntimeDistinctTest extends DistinctTestBase(ENTERPRISE.FUSING, PARALLEL, SIZE_HINT) with TimeLimitedCypherTest
class ParallelRuntimeDistinctStressTest extends DistinctStressTestBase(ENTERPRISE.FUSING, PARALLEL) with TimeLimitedCypherTest
class ParallelRuntimeDistinctNoFusingStressTest extends DistinctStressTestBase(ENTERPRISE.NO_FUSING, PARALLEL) with TimeLimitedCypherTest

// UNWIND
class ParallelRuntimeUnwindTest extends UnwindTestBase(ENTERPRISE.FUSING, PARALLEL, SIZE_HINT) with TimeLimitedCypherTest
class ParallelRuntimeUnwindNoFusingTest extends UnwindTestBase(ENTERPRISE.NO_FUSING, PARALLEL, SIZE_HINT) with TimeLimitedCypherTest
class ParallelRuntimeUnwindStressTest extends UnwindStressTestBase(ENTERPRISE.FUSING, PARALLEL) with TimeLimitedCypherTest
class ParallelRuntimeUnwindNoFusingStressTest extends UnwindStressTestBase(ENTERPRISE.NO_FUSING, PARALLEL) with TimeLimitedCypherTest

// SORT
class ParallelRuntimeSortTest extends SortTestBase(ENTERPRISE.FUSING, PARALLEL, SIZE_HINT) with TimeLimitedCypherTest

// TOP
class ParallelRuntimeTopTest extends TopTestBase(ENTERPRISE.FUSING, PARALLEL, SIZE_HINT) with TimeLimitedCypherTest
class ParallelRuntimeTopNoFusingTest extends TopTestBase(ENTERPRISE.NO_FUSING, PARALLEL, SIZE_HINT) with TimeLimitedCypherTest

// AGGREGATION
class ParallelRuntimeAggregationTest extends AggregationTestBase(ENTERPRISE.FUSING, PARALLEL, SIZE_HINT) with TimeLimitedCypherTest
class ParallelRuntimeAggregationStressTest extends AggregationStressTestBase(ENTERPRISE.FUSING, PARALLEL) with TimeLimitedCypherTest

// NODE HASH JOIN
class ParallelRuntimeNodeHashJoinTest extends NodeHashJoinTestBase(ENTERPRISE.FUSING, PARALLEL, SIZE_HINT) with TimeLimitedCypherTest

// REACTIVE
class ParallelRuntimeReactiveResultsTest extends ReactiveResultTestBase(ENTERPRISE.FUSING, PARALLEL) with TimeLimitedCypherTest
class ParallelRuntimeReactiveResultsNoFusingTest extends ReactiveResultTestBase(ENTERPRISE.NO_FUSING, PARALLEL) with TimeLimitedCypherTest
class ParallelRuntimeReactiveResultsStressTest
  extends ReactiveResultStressTestBase(ENTERPRISE.FUSING, PARALLEL,
                                       ReactiveResultStressTestBase.MORSEL_SIZE + 1) with TimeLimitedCypherTest// this test is slow, hence the reduced size
class ParallelRuntimeReactiveNoFusingStressTest
  extends ReactiveResultStressTestBase(ENTERPRISE.NO_FUSING, PARALLEL,
                                       ReactiveResultStressTestBase.MORSEL_SIZE + 1) with TimeLimitedCypherTest// this test is slow, hence the reduced size

// OPTIONAL
class ParallelRuntimeOptionalTest extends OptionalTestBase(ENTERPRISE.FUSING, PARALLEL, SIZE_HINT) with TimeLimitedCypherTest
class ParallelRuntimeOptionalNoFusingTest extends OptionalTestBase(ENTERPRISE.NO_FUSING, PARALLEL, SIZE_HINT) with TimeLimitedCypherTest

// GENERAL
class ParallelRuntimeMiscTest extends MiscTestBase(ENTERPRISE.FUSING, PARALLEL) with TimeLimitedCypherTest
class ParallelRuntimeNoFusingMiscTest extends MiscTestBase(ENTERPRISE.NO_FUSING, PARALLEL) with TimeLimitedCypherTest
class ParallelRuntimeSchedulerTracerTest extends SchedulerTracerTestBase(PARALLEL) with TimeLimitedCypherTest
class ParallelRuntimeMemoryManagementDisabledTest extends MemoryManagementDisabledTestBase(ENTERPRISE.FUSING, PARALLEL) with TimeLimitedCypherTest
class ParallelRuntimeSubscriberErrorTest extends SubscriberErrorTestBase(ENTERPRISE.NO_FUSING, PARALLEL) with TimeLimitedCypherTest

// WORKLOAD
class ParallelRuntimeWorkloadTest extends WorkloadTestBase(ENTERPRISE.FUSING, PARALLEL, SIZE_HINT) with TimeLimitedCypherTest
class ParallelRuntimeNoFusingWorkloadTest extends WorkloadTestBase(ENTERPRISE.NO_FUSING, PARALLEL, SIZE_HINT) with TimeLimitedCypherTest

// ERROR HANDLING
class ParallelErrorHandlingTest extends ParallelErrorHandlingTestBase(PARALLEL) with TimeLimitedCypherTest

// PROFILE
class ParallelRuntimeProfileNoFusingRowsTest extends ProfileRowsTestBase(ENTERPRISE.NO_FUSING, PARALLEL, SIZE_HINT) with TimeLimitedCypherTest
class ParallelRuntimeProfileRowsTest extends ProfileRowsTestBase(ENTERPRISE.FUSING, PARALLEL, SIZE_HINT) with TimeLimitedCypherTest
class ParallelRuntimeProfileNoFusingTimeTest extends ProfileTimeTestBase(ENTERPRISE.NO_FUSING, PARALLEL, SIZE_HINT) with TimeLimitedCypherTest
class ParallelRuntimeProfileNoTimeTest extends ProfileNoTimeTestBase(ENTERPRISE.FUSING, PARALLEL, SIZE_HINT) with TimeLimitedCypherTest
class ParallelRuntimeProfileNoFusingDbHitsTest extends MorselDbHitsTestBase(ENTERPRISE.NO_FUSING, PARALLEL, SIZE_HINT) with TimeLimitedCypherTest
