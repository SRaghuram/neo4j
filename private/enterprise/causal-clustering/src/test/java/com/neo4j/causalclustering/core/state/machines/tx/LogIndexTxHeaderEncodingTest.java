/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state.machines.tx;

import org.junit.Test;

import static com.neo4j.causalclustering.core.state.machines.tx.LogIndexTxHeaderEncoding.decodeLogIndexFromTxHeader;
import static com.neo4j.causalclustering.core.state.machines.tx.LogIndexTxHeaderEncoding.encodeLogIndexAsTxHeader;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class LogIndexTxHeaderEncodingTest
{
    @Test
    public void shouldEncodeIndexAsBytes()
    {
        long index = 123_456_789_012_567L;
        byte[] bytes = encodeLogIndexAsTxHeader( index );
        assertEquals( index, decodeLogIndexFromTxHeader( bytes ) );
    }

    @Test
    public void shouldThrowExceptionForAnEmptyByteArray()
    {
        // given
        try
        {
            // when
            decodeLogIndexFromTxHeader( new byte[0] );
            fail( "Should have thrown an exception because there's no way to decode this " );
        }
        catch ( IllegalArgumentException e )
        {
            // expected
            assertEquals( "Unable to decode RAFT log index from transaction header", e.getMessage() );
        }
    }
}
