/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.infra.resources;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class HardwareTest
{

    @Test
    public void totalMemoryMatchesRequired()
    {
        // given
        Hardware.TotalMemory requiredTotalMemory = Hardware.totalMemory( 8192 );
        Hardware.TotalMemory totalMemory = Hardware.totalMemory( 8192 );
        // when
        boolean test = totalMemory.predicate().test( requiredTotalMemory );
        // then
        assertTrue( test );
    }

    @Test
    public void totalMemoryLessRequired()
    {
        // given
        Hardware.TotalMemory requiredTotalMemory = Hardware.totalMemory( 8192 );
        Hardware.TotalMemory totalMemory = Hardware.totalMemory( 4096 );
        // when
        boolean test = totalMemory.predicate().test( requiredTotalMemory );
        // then
        assertFalse( test );
    }

    @Test
    public void totalMemoryFitsRequired()
    {
        // given
        Hardware.TotalMemory requiredTotalMemory = Hardware.totalMemory( 8192 );
        Hardware.TotalMemory totalMemory = Hardware.totalMemory( 12000 );
        // when
        boolean test = totalMemory.predicate().test( requiredTotalMemory );
        // then
        assertTrue( test );
    }

    @Test
    public void totalMemoryDoublesRequired()
    {
        // given
        Hardware.TotalMemory requiredTotalMemory = Hardware.totalMemory( 8192 );
        Hardware.TotalMemory totalMemory = Hardware.totalMemory( 16384 );
        // when
        boolean test = totalMemory.predicate().test( requiredTotalMemory );
        // then
        assertFalse( test );
    }
}
