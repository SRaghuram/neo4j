/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.system;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import org.neo4j.logging.Level;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class AkkaLoggingLevelTest
{
    @ParameterizedTest
    @EnumSource( Level.class )
    void shouldLookupByNeo4jLoggingLevel( Level neo4jLevel )
    {
        var akkaLevel = AkkaLoggingLevel.fromNeo4jLevel( neo4jLevel );

        assertNotNull( akkaLevel );
        assertEquals( neo4jLevel, akkaLevel.neo4jLevel() );
    }

    @Test
    void shouldFailLookupByNeo4jLoggingLevelWhenNull()
    {
        assertThrows( IllegalArgumentException.class, () -> AkkaLoggingLevel.fromNeo4jLevel( null ) );
    }
}
