/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.spec.pipelined

import org.neo4j.cypher.internal.CypherRuntime
import org.neo4j.cypher.internal.RuntimeContext
import org.neo4j.cypher.internal.runtime.spec.ENTERPRISE
import org.neo4j.cypher.internal.runtime.spec.Edition
import org.neo4j.cypher.internal.runtime.spec.tests.ProfileDbHitsTestBase

abstract class PipelinedDbHitsTestBase[CONTEXT <: RuntimeContext](edition: Edition[CONTEXT],
                                                                  runtime: CypherRuntime[CONTEXT],
                                                                  sizeHint: Int,
                                                                  canFuseOverPipelines: Boolean)
  extends ProfileDbHitsTestBase(edition,
                                runtime,
                                sizeHint,
                                costOfLabelLookup = 0,
                                costOfSetProperty = 0,
                                costOfPropertyToken = 0,
                                costOfGetPropertyChain = 1,
                                costOfPropertyJumpedOverInChain = 1,
                                costOfProperty = 1,
                                costOfExpandGetRelCursor = 1,
                                costOfExpandOneRel = 1,
                                costOfRelationshipTypeLookup = 0,
                                cartesianProductChunkSize = ENTERPRISE.MORSEL_SIZE,
                                canFuseOverPipelines = canFuseOverPipelines,
                                createsRelValueInExpand = false)
