/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.impl.store.format.highlimit;

import com.neo4j.kernel.impl.store.format.highlimit.v340.HighLimitV3_4_0;
import org.junit.jupiter.api.Test;

import org.neo4j.kernel.impl.store.format.CapabilityType;

import static org.junit.jupiter.api.Assertions.assertTrue;

class HighLimitStoreMigrationTest
{
    @Test
    void haveSameFormatCapabilitiesAsHighLimit3_4()
    {
        assertTrue( HighLimit.RECORD_FORMATS.hasCompatibleCapabilities( HighLimitV3_4_0.RECORD_FORMATS, CapabilityType.FORMAT ) );
    }
}
