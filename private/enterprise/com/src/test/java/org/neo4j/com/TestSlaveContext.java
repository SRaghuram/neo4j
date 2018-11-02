/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.com;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class TestSlaveContext
{
    @Test
    public void assertSimilarity()
    {
        // Different machine ids
        assertNotEquals( new RequestContext( 1234, 1, 2, 0, 0 ), new RequestContext( 1234, 2, 2, 0, 0 ) );

        // Different event identifiers
        assertNotEquals( new RequestContext( 1234, 1, 10, 0, 0 ), new RequestContext( 1234, 1, 20, 0, 0 ) );

        // Different session ids
        assertNotEquals( new RequestContext( 1001, 1, 5, 0, 0 ), new RequestContext( 1101, 1, 5, 0, 0 ) );

        // Same everything
        assertEquals( new RequestContext( 12345, 4, 9, 0, 0 ), new RequestContext( 12345, 4, 9, 0, 0 ) );
    }
}
