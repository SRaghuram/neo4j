/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms.procedures.wait;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class WindowedTimeoutTest
{
    @Test
    void shouldGiveExpectedTimeouts()
    {
        var windowedTimeout = WindowedTimeout.builder().nextWindow( 1, 2 ).nextWindow( 100, 5 ).build( 400 );

        assertEquals( windowedTimeout.nextTimeout(), 1 );
        assertEquals( windowedTimeout.nextTimeout(), 1 );
        assertEquals( windowedTimeout.nextTimeout(), 100 );
        assertEquals( windowedTimeout.nextTimeout(), 100 );
        assertEquals( windowedTimeout.nextTimeout(), 100 );
        assertEquals( windowedTimeout.nextTimeout(), 100 );
        assertEquals( windowedTimeout.nextTimeout(), 100 );
        assertEquals( windowedTimeout.nextTimeout(), 400 );
        assertEquals( windowedTimeout.nextTimeout(), 400 );
        assertEquals( windowedTimeout.nextTimeout(), 400 );
        assertEquals( windowedTimeout.nextTimeout(), 400 );
        assertEquals( windowedTimeout.nextTimeout(), 400 );
    }
}
