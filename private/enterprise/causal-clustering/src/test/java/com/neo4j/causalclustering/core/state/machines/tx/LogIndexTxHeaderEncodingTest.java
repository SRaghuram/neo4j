/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state.machines.tx;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static com.neo4j.causalclustering.core.state.machines.tx.LogIndexTxHeaderEncoding.decodeLogIndexFromTxHeader;
import static com.neo4j.causalclustering.core.state.machines.tx.LogIndexTxHeaderEncoding.encodeLogIndexAsTxHeader;

class LogIndexTxHeaderEncodingTest
{
    @Test
    void shouldEncodeIndexAsBytes()
    {
        long index = 123_456_789_012_567L;
        byte[] bytes = encodeLogIndexAsTxHeader( index );
        Assertions.assertEquals( index, decodeLogIndexFromTxHeader( bytes ) );
    }

    @Test
    void shouldThrowExceptionForAnEmptyByteArray()
    {
        // given
        try
        {
            // when
            decodeLogIndexFromTxHeader( new byte[0] );
            Assertions.fail( "Should have thrown an exception because there's no way to decode this " );
        }
        catch ( IllegalArgumentException e )
        {
            // expected
            Assertions.assertEquals( "Unable to decode RAFT log index from transaction header", e.getMessage() );
        }
    }
}
