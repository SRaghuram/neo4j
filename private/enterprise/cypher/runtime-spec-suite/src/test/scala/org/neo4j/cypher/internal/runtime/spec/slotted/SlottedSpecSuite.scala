/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.spec.slotted

import org.neo4j.cypher.internal.SlottedRuntime
import org.neo4j.cypher.internal.runtime.spec.ENTERPRISE_EDITION
import org.neo4j.cypher.internal.runtime.spec.tests.{AggregationTestBase, AllNodeScanTestBase, InputTestBase}

class SlottedAllNodeScanTest extends AllNodeScanTestBase(ENTERPRISE_EDITION, SlottedRuntime)
class SlottedAggregationTest extends AggregationTestBase(ENTERPRISE_EDITION, SlottedRuntime)
class SlottedInputTest extends InputTestBase(ENTERPRISE_EDITION, SlottedRuntime)
