/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.spec.parallel

import org.neo4j.cypher.internal.EnterpriseRuntimeContext
import org.neo4j.cypher.internal.MorselRuntime.{MORSEL, PARALLEL}
import org.neo4j.cypher.internal.runtime.spec._
import org.neo4j.cypher.internal.runtime.spec.morsel.{MorselDbHitsTestBase, SchedulerTracerTestBase}
import org.neo4j.cypher.internal.runtime.spec.parallel.ParallelRuntimeSpecSuite.SIZE_HINT
import org.neo4j.cypher.internal.runtime.spec.stress._
import org.neo4j.cypher.internal.runtime.spec.tests._

object ParallelRuntimeSpecSuite {
  val SIZE_HINT = 1000
}

// INPUT
class ParallelRuntimeInputTest extends ParallelInputTestBase(ENTERPRISE.PARALLEL, PARALLEL)
class ParallelRuntimeInputTestNoFusing extends ParallelInputTestBase(ENTERPRISE.PARALLEL_NO_FUSING, PARALLEL)

// ALL NODE SCAN
class ParallelRuntimeAllNodeScanTest extends AllNodeScanTestBase(ENTERPRISE.PARALLEL, PARALLEL, SIZE_HINT)
class ParallelRuntimeAllNodeScanNoFusingTest extends AllNodeScanTestBase(ENTERPRISE.PARALLEL_NO_FUSING, PARALLEL, SIZE_HINT)
class ParallelRuntimeAllNodeScanStressTest extends AllNodeScanStressTestBase(ENTERPRISE.PARALLEL, PARALLEL)
class ParallelRuntimeAllNodeScanNoFusingStressTest extends AllNodeScanStressTestBase(ENTERPRISE.PARALLEL_NO_FUSING, PARALLEL)

// NODE BY ID SEEK
class ParallelRuntimeNodeByIdSeekTest extends NodeByIdSeekTestBase(ENTERPRISE.PARALLEL, PARALLEL, SIZE_HINT)
class ParallelRuntimeNodeByIdSeekNoFusingTest extends NodeByIdSeekTestBase(ENTERPRISE.PARALLEL_NO_FUSING, PARALLEL, SIZE_HINT)
class ParallelRuntimeNodeByIdSeekStressTest extends NodeByIdSeekStressTestBase(ENTERPRISE.PARALLEL, PARALLEL)
class ParallelRuntimeNodeByIdSeekNoFusingStressTest extends NodeByIdSeekStressTestBase(ENTERPRISE.PARALLEL_NO_FUSING, PARALLEL)

// RELATIONSHIP BY ID SEEK
class MorselDirectedRelationshipByIdSeekTest extends DirectedRelationshipByIdSeekTestBase(ENTERPRISE.PARALLEL, MORSEL, SIZE_HINT)
class MorselDirectedRelationshipByIdSeekNoFusingTest extends DirectedRelationshipByIdSeekTestBase(ENTERPRISE.PARALLEL_NO_FUSING, MORSEL, SIZE_HINT)

//UNDIRECTED RELATIONSHIP BY ID SEEK
class MorselUndirectedRelationshipByIdSeekTest extends UndirectedRelationshipByIdSeekTestBase(ENTERPRISE.PARALLEL, MORSEL, SIZE_HINT)
class MorselUndirectedRelationshipByIdSeekNoFusingTest extends UndirectedRelationshipByIdSeekTestBase(ENTERPRISE.PARALLEL_NO_FUSING, MORSEL, SIZE_HINT)

// NODE COUNT FROM COUNT STORE
class ParallelNodeCountFromCountStoreTest extends NodeCountFromCountStoreTestBase(ENTERPRISE.PARALLEL, PARALLEL)
class ParallelNodeCountFromCountStoreNoFusingTest extends NodeCountFromCountStoreTestBase(ENTERPRISE.PARALLEL_NO_FUSING, PARALLEL)

// RELATIONSHIP COUNT FROM COUNT STORE
class ParallelRelationshipCountFromCountStoreTest extends RelationshipCountFromCountStoreTestBase(ENTERPRISE.PARALLEL, PARALLEL)
class ParallelRelationshipCountFromCountStoreNoFusingTest extends RelationshipCountFromCountStoreTestBase(ENTERPRISE.PARALLEL_NO_FUSING, PARALLEL)

// LABEL SCAN
class ParallelRuntimeLabelScanTest extends LabelScanTestBase(ENTERPRISE.PARALLEL, PARALLEL, SIZE_HINT)
class ParallelRuntimeLabelScanNoFusingTest extends LabelScanTestBase(ENTERPRISE.PARALLEL_NO_FUSING, PARALLEL, SIZE_HINT)
class ParallelRuntimeLabelScanStressTest extends LabelScanStressTestBase(ENTERPRISE.PARALLEL, PARALLEL)
class ParallelRuntimeLabelScanNoFusingStressTest extends LabelScanStressTestBase(ENTERPRISE.PARALLEL_NO_FUSING, PARALLEL)

// INDEX SEEK
class ParallelRuntimeNodeIndexSeekTest extends NodeIndexSeekTestBase(ENTERPRISE.PARALLEL, PARALLEL, SIZE_HINT)
  with NodeIndexSeekRangeAndCompositeTestBase[EnterpriseRuntimeContext]
  with ArrayIndexSupport[EnterpriseRuntimeContext]
class ParallelRuntimeNodeIndexSeekNoFusingTest extends NodeIndexSeekTestBase(ENTERPRISE.PARALLEL_NO_FUSING, PARALLEL, SIZE_HINT)
  with NodeIndexSeekRangeAndCompositeTestBase[EnterpriseRuntimeContext]
  with ArrayIndexSupport[EnterpriseRuntimeContext]

class ParallelRuntimeIndexSeekRangeStressTest extends IndexSeekRangeStressTestBase(ENTERPRISE.PARALLEL,PARALLEL)
class ParallelRuntimeIndexSeekRangeNoFusingStressTest extends IndexSeekRangeStressTestBase(ENTERPRISE.PARALLEL_NO_FUSING, PARALLEL)
class ParallelRuntimeIndexSeekExactStressTest extends IndexSeekExactStressTest(ENTERPRISE.PARALLEL, PARALLEL)
class ParallelRuntimeIndexSeekExactNoFusingStressTest extends IndexSeekExactStressTest(ENTERPRISE.PARALLEL_NO_FUSING, PARALLEL)

// INDEX SCAN
class ParallelRuntimeNodeIndexScanTest extends NodeIndexScanTestBase(ENTERPRISE.PARALLEL, PARALLEL, SIZE_HINT)
class ParallelRuntimeNodeIndexScanNoFusingTest extends NodeIndexScanTestBase(ENTERPRISE.PARALLEL_NO_FUSING, PARALLEL, SIZE_HINT)
class ParallelRuntimeIndexScanNoFusingStressTest extends IndexScanStressTestBase(ENTERPRISE.PARALLEL, PARALLEL)
class ParallelRuntimeIndexScanStressTest extends IndexScanStressTestBase(ENTERPRISE.PARALLEL_NO_FUSING, PARALLEL)

// INDEX CONTAINS SCAN
class ParallelRuntimeNodeIndexContainsScanTest extends NodeIndexContainsScanTestBase(ENTERPRISE.PARALLEL, PARALLEL, SIZE_HINT)
class ParallelRuntimeNodeIndexContainsScanNoFusingTest extends NodeIndexContainsScanTestBase(ENTERPRISE.PARALLEL_NO_FUSING, PARALLEL, SIZE_HINT)
class ParallelRuntimeIndexContainsScanStressTest extends IndexContainsScanStressTestBase(ENTERPRISE.PARALLEL, PARALLEL)
class ParallelRuntimeIndexContainsScanNoFusingStressTest extends IndexContainsScanStressTestBase(ENTERPRISE.PARALLEL_NO_FUSING, PARALLEL)

// INDEX ENDS WITH SCAN
class ParallelRuntimeNodeIndexEndsWithScanTest extends NodeIndexEndsWithScanTestBase(ENTERPRISE.PARALLEL, PARALLEL, SIZE_HINT)
class ParallelRuntimeNodeIndexEndsWithScanNoFusingTest extends NodeIndexEndsWithScanTestBase(ENTERPRISE.PARALLEL_NO_FUSING, PARALLEL, SIZE_HINT)
class ParallelRuntimeIndexEndsWithScanStressTest extends IndexEndsWithScanStressTestBase(ENTERPRISE.PARALLEL, PARALLEL)
class ParallelRuntimeIndexEndsWithScanNoFusingStressTest extends IndexEndsWithScanStressTestBase(ENTERPRISE.PARALLEL_NO_FUSING, PARALLEL)

// ARGUMENT
class ParallelRuntimeArgumentTest extends ArgumentTestBase(ENTERPRISE.PARALLEL, PARALLEL, SIZE_HINT)
class ParallelRuntimeArgumentNoFusingTest extends ArgumentTestBase(ENTERPRISE.PARALLEL_NO_FUSING, PARALLEL, SIZE_HINT)
class ParallelRuntimeArgumentStressTest extends ArgumentStressTestBase(ENTERPRISE.PARALLEL, PARALLEL)
class ParallelRuntimeArgumentNoFusingStressTest extends ArgumentStressTestBase(ENTERPRISE.PARALLEL_NO_FUSING, PARALLEL)

// APPLY
class ParallelRuntimeApplyStressTest extends ApplyStressTestBase(ENTERPRISE.PARALLEL, PARALLEL)
class ParallelRuntimeApplyNoFusingStressTest extends ApplyStressTestBase(ENTERPRISE.PARALLEL_NO_FUSING, PARALLEL)

// EXPAND
class ParallelRuntimeExpandAllTest extends ExpandAllTestBase(ENTERPRISE.PARALLEL, PARALLEL, SIZE_HINT)
                                   with ExpandAllWithOtherOperatorsTestBase[EnterpriseRuntimeContext]
class ParallelRuntimeExpandAllTestNoFusing extends ExpandAllTestBase(ENTERPRISE.PARALLEL_NO_FUSING, PARALLEL, SIZE_HINT)
                                           with ExpandAllWithOtherOperatorsTestBase[EnterpriseRuntimeContext]
class ParallelRuntimeExpandStressTest extends ExpandStressTestBase(ENTERPRISE.PARALLEL, PARALLEL)
class ParallelRuntimeExpandNoFusingStressTest extends ExpandStressTestBase(ENTERPRISE.PARALLEL_NO_FUSING, PARALLEL)

// VAR EXPAND
class ParallelRuntimeVarLengthExpandTest extends VarLengthExpandTestBase(ENTERPRISE.PARALLEL, PARALLEL, SIZE_HINT)
class ParallelRuntimeNoFusingVarLengthExpandTest extends VarLengthExpandTestBase(ENTERPRISE.PARALLEL_NO_FUSING, PARALLEL, SIZE_HINT)
class ParallelRuntimeVarExpandStressTest extends VarExpandStressTestBase(ENTERPRISE.PARALLEL, PARALLEL)
class ParallelRuntimeVarExpandNoFusingStressTest extends VarExpandStressTestBase(ENTERPRISE.PARALLEL_NO_FUSING, PARALLEL)

// PROJECTION
class ParallelRuntimeProjectionTest extends ProjectionTestBase(ENTERPRISE.PARALLEL, PARALLEL, SIZE_HINT)
class ParallelRuntimeProjectionNoFusingTest extends ProjectionTestBase(ENTERPRISE.PARALLEL_NO_FUSING, PARALLEL, SIZE_HINT)
class ParallelRuntimeProjectionStressTest extends ProjectionStressTestBase(ENTERPRISE.PARALLEL, PARALLEL)
class ParallelRuntimeProjectionNoFusingStressTest extends ProjectionStressTestBase(ENTERPRISE.PARALLEL_NO_FUSING, PARALLEL)

// FILTER
class ParallelRuntimeFilterTest extends FilterTestBase(ENTERPRISE.PARALLEL, PARALLEL, SIZE_HINT)
class ParallelRuntimeFilterNoFusingTest extends FilterTestBase(ENTERPRISE.PARALLEL_NO_FUSING, PARALLEL, SIZE_HINT)
class ParallelRuntimeFilterStressTest extends FilterStressTestBase(ENTERPRISE.PARALLEL, PARALLEL)
class ParallelRuntimeFilterNoFusingStressTest extends FilterStressTestBase(ENTERPRISE.PARALLEL_NO_FUSING, PARALLEL)

//Misc Expressions
class ParallelRuntimeExpressionStressTest extends ExpressionStressTestBase(ENTERPRISE.PARALLEL, PARALLEL)
class ParallelRuntimeExpressionNoFusingStressTest extends ExpressionStressTestBase(ENTERPRISE.PARALLEL_NO_FUSING, PARALLEL)

// LIMIT
class ParallelRuntimeLimitTest extends LimitTestBase(ENTERPRISE.PARALLEL, PARALLEL, SIZE_HINT)
class ParallelRuntimeLimitNoFusingTest extends LimitTestBase(ENTERPRISE.PARALLEL_NO_FUSING, PARALLEL, SIZE_HINT)

// DISTINCT
class ParallelRuntimeDistinctTest extends DistinctTestBase(ENTERPRISE.PARALLEL, PARALLEL, SIZE_HINT)
class ParallelRuntimeDistinctStressTest extends DistinctStressTestBase(ENTERPRISE.PARALLEL, PARALLEL)
class ParallelRuntimeDistinctNoFusingStressTest extends DistinctStressTestBase(ENTERPRISE.PARALLEL_NO_FUSING, PARALLEL)

// UNWIND
class ParallelRuntimeUnwindTest extends UnwindTestBase(ENTERPRISE.PARALLEL, PARALLEL, SIZE_HINT)
class ParallelRuntimeUnwindNoFusingTest extends UnwindTestBase(ENTERPRISE.PARALLEL_NO_FUSING, PARALLEL, SIZE_HINT)
class ParallelRuntimeUnwindStressTest extends UnwindStressTestBase(ENTERPRISE.PARALLEL, PARALLEL)
class ParallelRuntimeUnwindNoFusingStressTest extends UnwindStressTestBase(ENTERPRISE.PARALLEL_NO_FUSING, PARALLEL)

// SORT
class ParallelRuntimeSortTest extends SortTestBase(ENTERPRISE.PARALLEL, PARALLEL, SIZE_HINT)

// AGGREGATION
class ParallelRuntimeParallelAggregationTest extends AggregationTestBase(ENTERPRISE.PARALLEL, PARALLEL, SIZE_HINT)
class ParallelRuntimeAggregationStressTest extends AggregationStressTestBase(ENTERPRISE.PARALLEL, PARALLEL)

// NODE HASH JOIN
//class ParallelRuntimeNodeHashJoinTest extends NodeHashJoinTestBase(ENTERPRISE.PARALLEL, PARALLEL, SIZE_HINT)

// REACTIVE
class ParallelRuntimeReactiveResultsTest extends ReactiveResultTestBase(ENTERPRISE.PARALLEL, PARALLEL)
class ParallelRuntimeReactiveResultsNoFusingTest extends ReactiveResultTestBase(ENTERPRISE.PARALLEL_NO_FUSING, PARALLEL)
class ParallelRuntimeReactiveResultsStressTest
  extends ReactiveResultStressTestBase(ENTERPRISE.PARALLEL, PARALLEL,
                                       ReactiveResultStressTestBase.MORSEL_SIZE + 1)// this test is slow, hence the reduced size
class ParallelRuntimeReactiveNoFusingStressTest
  extends ReactiveResultStressTestBase(ENTERPRISE.PARALLEL_NO_FUSING, PARALLEL,
                                       ReactiveResultStressTestBase.MORSEL_SIZE + 1)// this test is slow, hence the reduced size

// OPTIONAL
class ParallelRuntimeOptionalTest extends OptionalTestBase(ENTERPRISE.PARALLEL, PARALLEL, SIZE_HINT)
class ParallelRuntimeOptionalNoFusingTest extends OptionalTestBase(ENTERPRISE.PARALLEL_NO_FUSING, PARALLEL, SIZE_HINT)

// GENERAL
class ParallelRuntimeMiscTest extends MiscTestBase(ENTERPRISE.PARALLEL, PARALLEL)
class ParallelRuntimeNoFusingMiscTest extends MiscTestBase(ENTERPRISE.PARALLEL_NO_FUSING, PARALLEL)
class ParallelRuntimeSchedulerTracerTest extends SchedulerTracerTestBase(PARALLEL)
class ParallelRuntimeMemoryManagementDisabledTest extends MemoryManagementDisabledTestBase(ENTERPRISE.PARALLEL, MORSEL)

// WORKLOAD
class ParallelRuntimeWorkloadTest extends WorkloadTestBase(ENTERPRISE.PARALLEL, PARALLEL, SIZE_HINT)
class ParallelRuntimeNoFusingWorkloadTest extends WorkloadTestBase(ENTERPRISE.PARALLEL_NO_FUSING, PARALLEL, SIZE_HINT)

// ERROR HANDLING
class ParallelErrorHandlingTest extends ParallelErrorHandlingTestBase(PARALLEL)

// PROFILE
class ParallelRuntimeProfileNoFusingRowsTest extends ProfileRowsTestBase(ENTERPRISE.PARALLEL_NO_FUSING, PARALLEL, SIZE_HINT)
class ParallelRuntimeProfileRowsTest extends ProfileRowsTestBase(ENTERPRISE.PARALLEL, PARALLEL, SIZE_HINT)
class ParallelRuntimeProfileNoFusingTimeTest extends ProfileTimeTestBase(ENTERPRISE.PARALLEL_NO_FUSING, PARALLEL, SIZE_HINT)
class ParallelRuntimeProfileNoFusingDbHitsTest extends MorselDbHitsTestBase(ENTERPRISE.PARALLEL_NO_FUSING, PARALLEL, SIZE_HINT)
