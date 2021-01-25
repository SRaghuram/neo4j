/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.protocol.modifier;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ModifierProtocolsTest
{
    @Test
    void shouldExposeAllAllowedValues()
    {
        var expected = Arrays.stream( ModifierProtocols.values() )
                .map( ModifierProtocols::implementation )
                .collect( Collectors.joining( ", ", "[", "]" ) );

        assertEquals( expected, ModifierProtocols.ALLOWED_VALUES_STRING );
    }
}
