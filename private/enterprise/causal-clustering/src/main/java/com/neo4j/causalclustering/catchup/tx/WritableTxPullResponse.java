/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.tx;

import java.util.Objects;

import org.neo4j.kernel.database.LogEntryWriterFactory;

public class WritableTxPullResponse
{
    private final TxPullResponse txPullResponse;
    private final LogEntryWriterFactory logEntryWriterFactory;

    public WritableTxPullResponse( TxPullResponse txPullResponse, LogEntryWriterFactory logEntryWriterFactory )
    {
        this.txPullResponse = txPullResponse;
        this.logEntryWriterFactory = logEntryWriterFactory;
    }

    public LogEntryWriterFactory logEntryWriterFactory()
    {
        return logEntryWriterFactory;
    }

    public TxPullResponse txPullResponse()
    {
        return txPullResponse;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }
        WritableTxPullResponse that = (WritableTxPullResponse) o;
        return Objects.equals( txPullResponse, that.txPullResponse ) &&
               Objects.equals( logEntryWriterFactory, that.logEntryWriterFactory );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( txPullResponse, logEntryWriterFactory );
    }

    @Override
    public String toString()
    {
        return "WritableTxPullResponse{" +
               "txPullResponse=" + txPullResponse +
               ", logEntryWriterFactory=" + logEntryWriterFactory +
               '}';
    }
}
