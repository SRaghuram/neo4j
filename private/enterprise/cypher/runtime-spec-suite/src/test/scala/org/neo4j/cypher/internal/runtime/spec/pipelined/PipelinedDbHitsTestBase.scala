/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.spec.pipelined

import org.neo4j.cypher.internal.runtime.spec.tests.ProfileDbHitsTestBase
import org.neo4j.cypher.internal.runtime.spec.{ENTERPRISE, Edition}
import org.neo4j.cypher.internal.{CypherRuntime, RuntimeContext}

abstract class PipelinedDbHitsTestBase[CONTEXT <: RuntimeContext](edition: Edition[CONTEXT],
                                                                  runtime: CypherRuntime[CONTEXT],
                                                                  sizeHint: Int)
  extends ProfileDbHitsTestBase(edition,
                                runtime,
                                sizeHint,
                                costOfLabelLookup = 0,
                                costOfGetPropertyChain = 1,
                                costOfPropertyJumpedOverInChain = 1,
                                costOfProperty = 1,
                                costOfExpandGetRelCursor = 1,
                                costOfExpandOneRel = 1,
                                costOfRelationshipTypeLookup = 0,
                                cartesianProductChunkSize = ENTERPRISE.MORSEL_SIZE)
