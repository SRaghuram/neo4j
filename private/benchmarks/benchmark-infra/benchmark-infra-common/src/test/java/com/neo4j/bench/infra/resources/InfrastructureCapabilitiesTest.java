/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.infra.resources;

import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class InfrastructureCapabilitiesTest
{

    @Test
    public void parseInfrastructureCapabilitiesFromArgs()
    {
        // given
        String args = "hardware:totalMemory=16384,hardware:availableCores=16,jdk=oracle-8";
        // when
        InfrastructureCapabilities infrastructureCapabilities = InfrastructureCapabilities.fromArgs( args );
        // then
        assertEquals(
                ImmutableSet.of(
                        Hardware.totalMemory( 16384 ),
                        Hardware.availableCores( 16 ),
                        Jdk.of( "oracle", "8" )
                ),
                infrastructureCapabilities.capabilities()
        );
    }

    @Test
    public void failOnUnknownCapabilitiesFromArgs()
    {
        // given
        String args = "hardware:os=Linux,hardware:availableCores=16,jdk=oracle-8";
        // when
        assertThrows( IllegalArgumentException.class, () -> InfrastructureCapabilities.fromArgs( args ), "Unknown \"hardware:os\" capability" );
    }

    @Test
    public void failOnEmptyFromArgs()
    {
        // given
        String args = "";
        // when
        assertThrows( IllegalArgumentException.class, () -> InfrastructureCapabilities.fromArgs( args ), "invalid capabilities args ''" );
    }
}
