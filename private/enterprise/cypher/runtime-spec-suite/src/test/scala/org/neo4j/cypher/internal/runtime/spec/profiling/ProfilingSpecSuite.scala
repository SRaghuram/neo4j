/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.spec.profiling

import org.neo4j.cypher.internal.CommunityRuntimeContext
import org.neo4j.cypher.internal.EnterpriseRuntimeContext
import org.neo4j.cypher.internal.InterpretedRuntime
import org.neo4j.cypher.internal.PipelinedRuntime.PIPELINED
import org.neo4j.cypher.internal.SlottedRuntime
import org.neo4j.cypher.internal.runtime.spec.pipelined.PipelinedSpecSuite
import org.neo4j.cypher.internal.runtime.spec.profiling.MemoryManagementProfilingBase.DEFAULT_MORSEL_SIZE_BIG
import org.neo4j.cypher.internal.runtime.spec.profiling.MemoryManagementProfilingBase.DEFAULT_MORSEL_SIZE_SMALL
import org.neo4j.cypher.internal.runtime.spec.profiling.MemoryManagementProfilingBase.ENTERPRISE_PROFILING

// EXPERIMENTAL PROFILING
// These classes are using the runtime spec suite for convenience, but are currently intended mainly for manual profiling,
// so their names do not end with Test or IT on purpose so that they do not get included in automated test runs.

// Run these to get heap dumps and memory usage estimates

class InterpretedMemoryManagementProfiling extends MemoryManagementProfilingBase(MemoryManagementProfilingBase.COMMUNITY_PROFILING, InterpretedRuntime)
                                              with FullSupportMemoryManagementProfilingBase[CommunityRuntimeContext]

class SlottedMemoryManagementProfiling extends MemoryManagementProfilingBase(MemoryManagementProfilingBase.ENTERPRISE_PROFILING, SlottedRuntime)
                                          with FullSupportMemoryManagementProfilingBase[EnterpriseRuntimeContext]

class PipelinedMemoryManagementBigMorselProfiling extends MemoryManagementProfilingBase(ENTERPRISE_PROFILING,
                                                                                        PIPELINED,
                                                                                        DEFAULT_MORSEL_SIZE_BIG) with PipelinedSpecSuite
class PipelinedMemoryManagementSmallMorselProfiling extends MemoryManagementProfilingBase(ENTERPRISE_PROFILING, PIPELINED,
                                                                                          DEFAULT_MORSEL_SIZE_SMALL) with PipelinedSpecSuite
class PipelinedMemoryManagementCustomProfiling extends MemoryManagementProfilingBase(ENTERPRISE_PROFILING, PIPELINED,
                                                                                     DEFAULT_MORSEL_SIZE_BIG,
                                                                                     runtimeSuffix="after") with PipelinedSpecSuite
