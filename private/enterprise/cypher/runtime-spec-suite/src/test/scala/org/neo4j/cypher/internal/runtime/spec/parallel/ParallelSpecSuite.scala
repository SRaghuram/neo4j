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
class ParallelRuntimeInputTest extends ParallelInputTestBase(ENTERPRISE.FUSING, PARALLEL)
class ParallelRuntimeInputTestNoFusing extends ParallelInputTestBase(ENTERPRISE.NO_FUSING, PARALLEL)

// ALL NODE SCAN
class ParallelRuntimeAllNodeScanTest extends AllNodeScanTestBase(ENTERPRISE.FUSING, PARALLEL, SIZE_HINT)
class ParallelRuntimeAllNodeScanNoFusingTest extends AllNodeScanTestBase(ENTERPRISE.NO_FUSING, PARALLEL, SIZE_HINT)
class ParallelRuntimeAllNodeScanStressTest extends AllNodeScanStressTestBase(ENTERPRISE.FUSING, PARALLEL)
class ParallelRuntimeAllNodeScanNoFusingStressTest extends AllNodeScanStressTestBase(ENTERPRISE.NO_FUSING, PARALLEL)

// NODE BY ID SEEK
class ParallelRuntimeNodeByIdSeekTest extends NodeByIdSeekTestBase(ENTERPRISE.FUSING, PARALLEL, SIZE_HINT)
class ParallelRuntimeNodeByIdSeekNoFusingTest extends NodeByIdSeekTestBase(ENTERPRISE.NO_FUSING, PARALLEL, SIZE_HINT)
class ParallelRuntimeNodeByIdSeekStressTest extends NodeByIdSeekStressTestBase(ENTERPRISE.FUSING, PARALLEL)
class ParallelRuntimeNodeByIdSeekNoFusingStressTest extends NodeByIdSeekStressTestBase(ENTERPRISE.NO_FUSING, PARALLEL)

// RELATIONSHIP BY ID SEEK
class MorselDirectedRelationshipByIdSeekTest extends DirectedRelationshipByIdSeekTestBase(ENTERPRISE.FUSING, MORSEL, SIZE_HINT)
class MorselDirectedRelationshipByIdSeekNoFusingTest extends DirectedRelationshipByIdSeekTestBase(ENTERPRISE.NO_FUSING, MORSEL, SIZE_HINT)

//UNDIRECTED RELATIONSHIP BY ID SEEK
class MorselUndirectedRelationshipByIdSeekTest extends UndirectedRelationshipByIdSeekTestBase(ENTERPRISE.FUSING, MORSEL, SIZE_HINT)
class MorselUndirectedRelationshipByIdSeekNoFusingTest extends UndirectedRelationshipByIdSeekTestBase(ENTERPRISE.NO_FUSING, MORSEL, SIZE_HINT)

// NODE COUNT FROM COUNT STORE
class ParallelNodeCountFromCountStoreTest extends NodeCountFromCountStoreTestBase(ENTERPRISE.FUSING, PARALLEL)
class ParallelNodeCountFromCountStoreNoFusingTest extends NodeCountFromCountStoreTestBase(ENTERPRISE.NO_FUSING, PARALLEL)

// RELATIONSHIP COUNT FROM COUNT STORE
class ParallelRelationshipCountFromCountStoreTest extends RelationshipCountFromCountStoreTestBase(ENTERPRISE.FUSING, PARALLEL)
class ParallelRelationshipCountFromCountStoreNoFusingTest extends RelationshipCountFromCountStoreTestBase(ENTERPRISE.NO_FUSING, PARALLEL)

// LABEL SCAN
class ParallelRuntimeLabelScanTest extends LabelScanTestBase(ENTERPRISE.FUSING, PARALLEL, SIZE_HINT)
class ParallelRuntimeLabelScanNoFusingTest extends LabelScanTestBase(ENTERPRISE.NO_FUSING, PARALLEL, SIZE_HINT)
class ParallelRuntimeLabelScanStressTest extends LabelScanStressTestBase(ENTERPRISE.FUSING, PARALLEL)
class ParallelRuntimeLabelScanNoFusingStressTest extends LabelScanStressTestBase(ENTERPRISE.NO_FUSING, PARALLEL)

// INDEX SEEK
class ParallelRuntimeNodeIndexSeekTest extends NodeIndexSeekTestBase(ENTERPRISE.FUSING, PARALLEL, SIZE_HINT)
  with NodeIndexSeekRangeAndCompositeTestBase[EnterpriseRuntimeContext]
  with ArrayIndexSupport[EnterpriseRuntimeContext]
class ParallelRuntimeNodeIndexSeekNoFusingTest extends NodeIndexSeekTestBase(ENTERPRISE.NO_FUSING, PARALLEL, SIZE_HINT)
  with NodeIndexSeekRangeAndCompositeTestBase[EnterpriseRuntimeContext]
  with ArrayIndexSupport[EnterpriseRuntimeContext]

class ParallelRuntimeIndexSeekRangeStressTest extends IndexSeekRangeStressTestBase(ENTERPRISE.FUSING,PARALLEL)
class ParallelRuntimeIndexSeekRangeNoFusingStressTest extends IndexSeekRangeStressTestBase(ENTERPRISE.NO_FUSING, PARALLEL)
class ParallelRuntimeIndexSeekExactStressTest extends IndexSeekExactStressTest(ENTERPRISE.FUSING, PARALLEL)
class ParallelRuntimeIndexSeekExactNoFusingStressTest extends IndexSeekExactStressTest(ENTERPRISE.NO_FUSING, PARALLEL)

// INDEX SCAN
class ParallelRuntimeNodeIndexScanTest extends NodeIndexScanTestBase(ENTERPRISE.FUSING, PARALLEL, SIZE_HINT)
class ParallelRuntimeNodeIndexScanNoFusingTest extends NodeIndexScanTestBase(ENTERPRISE.NO_FUSING, PARALLEL, SIZE_HINT)
class ParallelRuntimeIndexScanNoFusingStressTest extends IndexScanStressTestBase(ENTERPRISE.FUSING, PARALLEL)
class ParallelRuntimeIndexScanStressTest extends IndexScanStressTestBase(ENTERPRISE.NO_FUSING, PARALLEL)

// INDEX CONTAINS SCAN
class ParallelRuntimeNodeIndexContainsScanTest extends NodeIndexContainsScanTestBase(ENTERPRISE.FUSING, PARALLEL, SIZE_HINT)
class ParallelRuntimeNodeIndexContainsScanNoFusingTest extends NodeIndexContainsScanTestBase(ENTERPRISE.NO_FUSING, PARALLEL, SIZE_HINT)
class ParallelRuntimeIndexContainsScanStressTest extends IndexContainsScanStressTestBase(ENTERPRISE.FUSING, PARALLEL)
class ParallelRuntimeIndexContainsScanNoFusingStressTest extends IndexContainsScanStressTestBase(ENTERPRISE.NO_FUSING, PARALLEL)

// INDEX ENDS WITH SCAN
class ParallelRuntimeNodeIndexEndsWithScanTest extends NodeIndexEndsWithScanTestBase(ENTERPRISE.FUSING, PARALLEL, SIZE_HINT)
class ParallelRuntimeNodeIndexEndsWithScanNoFusingTest extends NodeIndexEndsWithScanTestBase(ENTERPRISE.NO_FUSING, PARALLEL, SIZE_HINT)
class ParallelRuntimeIndexEndsWithScanStressTest extends IndexEndsWithScanStressTestBase(ENTERPRISE.FUSING, PARALLEL)
class ParallelRuntimeIndexEndsWithScanNoFusingStressTest extends IndexEndsWithScanStressTestBase(ENTERPRISE.NO_FUSING, PARALLEL)

// ARGUMENT
class ParallelRuntimeArgumentTest extends ArgumentTestBase(ENTERPRISE.FUSING, PARALLEL, SIZE_HINT)
class ParallelRuntimeArgumentNoFusingTest extends ArgumentTestBase(ENTERPRISE.NO_FUSING, PARALLEL, SIZE_HINT)
class ParallelRuntimeArgumentStressTest extends ArgumentStressTestBase(ENTERPRISE.FUSING, PARALLEL)
class ParallelRuntimeArgumentNoFusingStressTest extends ArgumentStressTestBase(ENTERPRISE.NO_FUSING, PARALLEL)

// APPLY
class ParallelRuntimeApplyStressTest extends ApplyStressTestBase(ENTERPRISE.FUSING, PARALLEL)
class ParallelRuntimeApplyNoFusingStressTest extends ApplyStressTestBase(ENTERPRISE.NO_FUSING, PARALLEL)

// EXPAND
class ParallelRuntimeExpandAllTest extends ExpandAllTestBase(ENTERPRISE.FUSING, PARALLEL, SIZE_HINT)
                                   with ExpandAllWithOtherOperatorsTestBase[EnterpriseRuntimeContext]
class ParallelRuntimeExpandAllTestNoFusing extends ExpandAllTestBase(ENTERPRISE.NO_FUSING, PARALLEL, SIZE_HINT)
                                           with ExpandAllWithOtherOperatorsTestBase[EnterpriseRuntimeContext]
class ParallelRuntimeExpandStressTest extends ExpandStressTestBase(ENTERPRISE.FUSING, PARALLEL)
class ParallelRuntimeExpandNoFusingStressTest extends ExpandStressTestBase(ENTERPRISE.NO_FUSING, PARALLEL)

// VAR EXPAND
class ParallelRuntimeVarLengthExpandTest extends VarLengthExpandTestBase(ENTERPRISE.FUSING, PARALLEL, SIZE_HINT)
class ParallelRuntimeNoFusingVarLengthExpandTest extends VarLengthExpandTestBase(ENTERPRISE.NO_FUSING, PARALLEL, SIZE_HINT)
class ParallelRuntimeVarExpandStressTest extends VarExpandStressTestBase(ENTERPRISE.FUSING, PARALLEL)
class ParallelRuntimeVarExpandNoFusingStressTest extends VarExpandStressTestBase(ENTERPRISE.NO_FUSING, PARALLEL)

// PROJECTION
class ParallelRuntimeProjectionTest extends ProjectionTestBase(ENTERPRISE.FUSING, PARALLEL, SIZE_HINT)
class ParallelRuntimeProjectionNoFusingTest extends ProjectionTestBase(ENTERPRISE.NO_FUSING, PARALLEL, SIZE_HINT)
class ParallelRuntimeProjectionStressTest extends ProjectionStressTestBase(ENTERPRISE.FUSING, PARALLEL)
class ParallelRuntimeProjectionNoFusingStressTest extends ProjectionStressTestBase(ENTERPRISE.NO_FUSING, PARALLEL)
class ParallelRuntimeCachePropertiesTest extends CachePropertiesTestBase(ENTERPRISE.FUSING, PARALLEL, SIZE_HINT)
class ParallelRuntimeCachePropertiesNoFusingTest extends CachePropertiesTestBase(ENTERPRISE.NO_FUSING, PARALLEL, SIZE_HINT)

// FILTER
class ParallelRuntimeFilterTest extends FilterTestBase(ENTERPRISE.FUSING, PARALLEL, SIZE_HINT)
class ParallelRuntimeFilterNoFusingTest extends FilterTestBase(ENTERPRISE.NO_FUSING, PARALLEL, SIZE_HINT)
class ParallelRuntimeFilterStressTest extends FilterStressTestBase(ENTERPRISE.FUSING, PARALLEL)
class ParallelRuntimeFilterNoFusingStressTest extends FilterStressTestBase(ENTERPRISE.NO_FUSING, PARALLEL)

//Misc Expressions
class ParallelRuntimeExpressionStressTest extends ExpressionStressTestBase(ENTERPRISE.FUSING, PARALLEL)
class ParallelRuntimeExpressionNoFusingStressTest extends ExpressionStressTestBase(ENTERPRISE.NO_FUSING, PARALLEL)

// LIMIT
class ParallelRuntimeLimitTest extends LimitTestBase(ENTERPRISE.FUSING, PARALLEL, SIZE_HINT)
class ParallelRuntimeLimitNoFusingTest extends LimitTestBase(ENTERPRISE.NO_FUSING, PARALLEL, SIZE_HINT)

// DISTINCT
class ParallelRuntimeDistinctTest extends DistinctTestBase(ENTERPRISE.FUSING, PARALLEL, SIZE_HINT)
class ParallelRuntimeDistinctStressTest extends DistinctStressTestBase(ENTERPRISE.FUSING, PARALLEL)
class ParallelRuntimeDistinctNoFusingStressTest extends DistinctStressTestBase(ENTERPRISE.NO_FUSING, PARALLEL)

// UNWIND
class ParallelRuntimeUnwindTest extends UnwindTestBase(ENTERPRISE.FUSING, PARALLEL, SIZE_HINT)
class ParallelRuntimeUnwindNoFusingTest extends UnwindTestBase(ENTERPRISE.NO_FUSING, PARALLEL, SIZE_HINT)
class ParallelRuntimeUnwindStressTest extends UnwindStressTestBase(ENTERPRISE.FUSING, PARALLEL)
class ParallelRuntimeUnwindNoFusingStressTest extends UnwindStressTestBase(ENTERPRISE.NO_FUSING, PARALLEL)

// SORT
class ParallelRuntimeSortTest extends SortTestBase(ENTERPRISE.FUSING, PARALLEL, SIZE_HINT)

// TOP
class ParallelRuntimeTopTest extends TopTestBase(ENTERPRISE.FUSING, PARALLEL, SIZE_HINT)
class ParallelRuntimeTopNoFusingTest extends TopTestBase(ENTERPRISE.NO_FUSING, PARALLEL, SIZE_HINT)

// AGGREGATION
class ParallelRuntimeParallelAggregationTest extends AggregationTestBase(ENTERPRISE.FUSING, PARALLEL, SIZE_HINT)
class ParallelRuntimeAggregationStressTest extends AggregationStressTestBase(ENTERPRISE.FUSING, PARALLEL)

// NODE HASH JOIN
class ParallelRuntimeNodeHashJoinTest extends NodeHashJoinTestBase(ENTERPRISE.FUSING, PARALLEL, SIZE_HINT)

// REACTIVE
class ParallelRuntimeReactiveResultsTest extends ReactiveResultTestBase(ENTERPRISE.FUSING, PARALLEL)
class ParallelRuntimeReactiveResultsNoFusingTest extends ReactiveResultTestBase(ENTERPRISE.NO_FUSING, PARALLEL)
class ParallelRuntimeReactiveResultsStressTest
  extends ReactiveResultStressTestBase(ENTERPRISE.FUSING, PARALLEL,
                                       ReactiveResultStressTestBase.MORSEL_SIZE + 1)// this test is slow, hence the reduced size
class ParallelRuntimeReactiveNoFusingStressTest
  extends ReactiveResultStressTestBase(ENTERPRISE.NO_FUSING, PARALLEL,
                                       ReactiveResultStressTestBase.MORSEL_SIZE + 1)// this test is slow, hence the reduced size

// OPTIONAL
class ParallelRuntimeOptionalTest extends OptionalTestBase(ENTERPRISE.FUSING, PARALLEL, SIZE_HINT)
class ParallelRuntimeOptionalNoFusingTest extends OptionalTestBase(ENTERPRISE.NO_FUSING, PARALLEL, SIZE_HINT)

// GENERAL
class ParallelRuntimeMiscTest extends MiscTestBase(ENTERPRISE.FUSING, PARALLEL)
class ParallelRuntimeNoFusingMiscTest extends MiscTestBase(ENTERPRISE.NO_FUSING, PARALLEL)
class ParallelRuntimeSchedulerTracerTest extends SchedulerTracerTestBase(PARALLEL)
class ParallelRuntimeMemoryManagementDisabledTest extends MemoryManagementDisabledTestBase(ENTERPRISE.FUSING, PARALLEL)
class ParallelRuntimeSubscriberErrorTest extends SubscriberErrorTestBase(ENTERPRISE.NO_FUSING, PARALLEL)

// WORKLOAD
class ParallelRuntimeWorkloadTest extends WorkloadTestBase(ENTERPRISE.FUSING, PARALLEL, SIZE_HINT)
class ParallelRuntimeNoFusingWorkloadTest extends WorkloadTestBase(ENTERPRISE.NO_FUSING, PARALLEL, SIZE_HINT)

// ERROR HANDLING
class ParallelErrorHandlingTest extends ParallelErrorHandlingTestBase(PARALLEL)

// PROFILE
class ParallelRuntimeProfileNoFusingRowsTest extends ProfileRowsTestBase(ENTERPRISE.NO_FUSING, PARALLEL, SIZE_HINT)
class ParallelRuntimeProfileRowsTest extends ProfileRowsTestBase(ENTERPRISE.FUSING, PARALLEL, SIZE_HINT)
class ParallelRuntimeProfileNoFusingTimeTest extends ProfileTimeTestBase(ENTERPRISE.NO_FUSING, PARALLEL, SIZE_HINT)
class ParallelRuntimeProfileNoTimeTest extends ProfileNoTimeTestBase(ENTERPRISE.FUSING, PARALLEL, SIZE_HINT)
class ParallelRuntimeProfileNoFusingDbHitsTest extends MorselDbHitsTestBase(ENTERPRISE.NO_FUSING, PARALLEL, SIZE_HINT)
