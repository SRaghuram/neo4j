/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.storecopy;

import static java.lang.Long.max;
import static java.lang.String.format;
import static org.neo4j.kernel.impl.transaction.log.TransactionIdStore.BASE_TX_ID;

public class RequiredTransactionRange
{
    private final long from;
    private final long to;

    public static RequiredTransactionRange range( long fromId, long requiredId )
    {
        if ( requiredId < fromId )
        {
            throw new IllegalArgumentException( format( "Not a valid range. RequiredId[%d] must be higher or equal to fromId[%d].", requiredId, fromId ) );
        }
        return new RequiredTransactionRange( fromId, requiredId );
    }

    public static RequiredTransactionRange single( long fromId )
    {
        return new RequiredTransactionRange( fromId, -1 );
    }

    private RequiredTransactionRange( long from, long to )
    {
        if ( from < 0 )
        {
            throw new IllegalArgumentException( "Range cannot start from negative value. Got: " + from );
        }
        this.from = from;
        this.to = to;
    }

    public long startTxId()
    {
        return from;
    }

    public long requiredTxId()
    {
        return to;
    }

    public boolean withinRange( long txId )
    {
        return to == -1L ? txId == from : txId >= from && txId <= to;
    }

    @Override
    public String toString()
    {
        return "RequiredTransactionRange{" + "from=" + from + ", to=" + to + '}';
    }
}
