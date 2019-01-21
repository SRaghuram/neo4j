/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.spec.compiled

import org.neo4j.cypher.internal.CompiledRuntime
import org.neo4j.cypher.internal.runtime.spec.ENTERPRISE_EDITION
import org.neo4j.cypher.internal.runtime.spec.interpreted.AllNodeScanTestBase

class CompiledAllNodeScanTest extends AllNodeScanTestBase(ENTERPRISE_EDITION, CompiledRuntime)
