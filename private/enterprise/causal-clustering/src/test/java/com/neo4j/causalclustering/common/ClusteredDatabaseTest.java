/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.common;

import org.junit.jupiter.api.Test;

import java.util.List;

import org.neo4j.kernel.database.Database;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

class ClusteredDatabaseTest
{
    @Test
    void shouldNotAllowRestart()
    {
        var kernel = mock( Database.class );
        var database = new ClusteredDatabase( List.of(), kernel, List.of() );

        database.start();
        assertTrue( database.hasBeenStarted() );

        database.stop();
        assertTrue( database.hasBeenStarted() );

        assertThrows( IllegalStateException.class, database::start );
    }
}
