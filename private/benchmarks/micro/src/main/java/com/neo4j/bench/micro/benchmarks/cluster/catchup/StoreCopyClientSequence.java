/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.cluster.catchup;

import org.neo4j.logging.LogProvider;

class StoreCopyClientSequence extends ClientSequence
{

    StoreCopyClientSequence( LogProvider logProvider, BareClient clientHandler )
    {
        super( logProvider, clientHandler );
    }

    @Override
    protected void firstStep( String databaseName )
    {
        clientHandler.getDatabaseId( this::stepGetStoreId, databaseName );
    }

    private void stepGetStoreId()
    {
        clientHandler.getStoreId( this::stepPrepareStoreCopy );
    }

    private void stepPrepareStoreCopy()
    {
        clientHandler.prepareStoreCopy( this::stepGetFile );
    }

    private void stepGetFile()
    {
        if ( clientHandler.hasNextFile() )
        {
            clientHandler.getNextFile( this::stepGetFile );
        }
        else
        {
            clientHandler.pullTransactions( this::finish );
        }
    }
}
