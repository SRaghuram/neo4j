/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state.machines.tx;

/**
 * Log index is encoded in the header of transactions in the transaction log.
 */
public class LogIndexTxHeaderEncoding
{
    private LogIndexTxHeaderEncoding()
    {
    }

    public static byte[] encodeLogIndexAsTxHeader( long logIndex )
    {
        byte[] b = new byte[Long.BYTES];
        for ( int i = Long.BYTES - 1; i > 0; i-- )
        {
            b[i] = (byte) logIndex;
            logIndex >>>= Byte.SIZE;
        }
        b[0] = (byte) logIndex;
        return b;
    }

    public static long decodeLogIndexFromTxHeader( byte[] bytes )
    {
        if ( bytes.length < Long.BYTES )
        {
            throw new IllegalArgumentException( "Unable to decode RAFT log index from transaction header" );
        }

        long logIndex = 0;
        for ( int i = 0; i < Long.BYTES; i++ )
        {
            logIndex <<= Byte.SIZE;
            logIndex ^= bytes[i] & 0xFF;
        }
        return logIndex;
    }
}
