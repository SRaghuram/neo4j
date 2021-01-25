/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.spec.profiling

import org.neo4j.configuration.GraphDatabaseInternalSettings
import org.neo4j.cypher.internal.CommunityRuntimeContext
import org.neo4j.cypher.internal.EnterpriseRuntimeContext
import org.neo4j.cypher.internal.InterpretedRuntime
import org.neo4j.cypher.internal.PipelinedRuntime.PIPELINED
import org.neo4j.cypher.internal.SlottedRuntime
import org.neo4j.cypher.internal.runtime.spec.pipelined.AssertFusingSucceeded
import org.neo4j.cypher.internal.runtime.spec.profiling.MemoryMeasurementTestBase.COMMUNITY_PROFILING
import org.neo4j.cypher.internal.runtime.spec.profiling.MemoryMeasurementTestBase.DEFAULT_MORSEL_SIZE_BIG
import org.neo4j.cypher.internal.runtime.spec.profiling.MemoryMeasurementTestBase.DEFAULT_MORSEL_SIZE_SMALL
import org.neo4j.cypher.internal.runtime.spec.profiling.MemoryMeasurementTestBase.ENTERPRISE_PROFILING

class InterpretedMemoryMeasurementTest
  extends MemoryMeasurementTestBase(COMMUNITY_PROFILING, InterpretedRuntime)
    with FullSupportMemoryMeasurementTestBase[CommunityRuntimeContext]

class SlottedMemoryMeasurementTest
  extends MemoryMeasurementTestBase(ENTERPRISE_PROFILING, SlottedRuntime)
    with FullSupportMemoryMeasurementTestBase[EnterpriseRuntimeContext]

class PipelinedBigMorselMemoryMeasurementTest
  extends MemoryMeasurementTestBase(ENTERPRISE_PROFILING, PIPELINED, DEFAULT_MORSEL_SIZE_BIG)
    with AssertFusingSucceeded

class PipelinedSmallMorselMemoryMeasurementTest
  extends MemoryMeasurementTestBase(ENTERPRISE_PROFILING, PIPELINED, DEFAULT_MORSEL_SIZE_SMALL)
  with AssertFusingSucceeded

class PipelinedBigMorselNoFusingMemoryMeasurementTest
  extends MemoryMeasurementTestBase(
    ENTERPRISE_PROFILING.copyWith(GraphDatabaseInternalSettings.cypher_operator_engine -> GraphDatabaseInternalSettings.CypherOperatorEngine.INTERPRETED),
    PIPELINED, DEFAULT_MORSEL_SIZE_BIG)
    with AssertFusingSucceeded
