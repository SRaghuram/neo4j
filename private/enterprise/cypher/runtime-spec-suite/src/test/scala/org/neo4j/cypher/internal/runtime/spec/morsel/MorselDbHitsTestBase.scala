/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.spec.morsel

import org.neo4j.cypher.internal.runtime.spec.Edition
import org.neo4j.cypher.internal.runtime.spec.tests.ProfileDbHitsTestBase
import org.neo4j.cypher.internal.{CypherRuntime, RuntimeContext}

abstract class MorselDbHitsTestBase[CONTEXT <: RuntimeContext](edition: Edition[CONTEXT],
                                                               runtime: CypherRuntime[CONTEXT],
                                                               sizeHint: Int)
  extends ProfileDbHitsTestBase(edition,
                                runtime,
                                sizeHint,
                                costOfLabelLookup = 0,
                                costOfProperty = 2,
                                costOfExpand = 2,
                                costOfRelationshipTypeLookup = 0)
