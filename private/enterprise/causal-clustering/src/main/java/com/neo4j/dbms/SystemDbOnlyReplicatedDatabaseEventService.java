/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.neo4j.function.ThrowingConsumer;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

public class SystemDbOnlyReplicatedDatabaseEventService implements ReplicatedDatabaseEventService
{
    private final Log log;
    private final List<ReplicatedDatabaseEventListener> listeners = new CopyOnWriteArrayList<>();

    public SystemDbOnlyReplicatedDatabaseEventService( LogProvider logProvider )
    {
        this.log = logProvider.getLog( getClass() );
    }

    public void registerListener( DatabaseId databaseId, ReplicatedDatabaseEventListener listener )
    {
        assertIsSystemDatabase( databaseId );
        listeners.add( listener );
    }

    public void unregisterListener( DatabaseId databaseId, ReplicatedDatabaseEventListener listener )
    {
        assertIsSystemDatabase( databaseId );
        listeners.remove( listener );
    }

    public ReplicatedDatabaseEventDispatch getDatabaseEventDispatch( DatabaseId databaseId )
    {
        if ( !databaseId.isSystemDatabase() )
        {
            return NO_EVENT_DISPATCH;
        }

        return new ReplicatedDatabaseEventDispatch()
        {
            @Override
            public void fireTransactionCommitted( long txId )
            {
                dispatchEvent( listener -> listener.transactionCommitted( txId ) );
            }

            @Override
            public void fireStoreReplaced( long txId )
            {
                dispatchEvent( listener -> listener.storeReplaced( txId ) );
            }
        };
    }

    private void dispatchEvent( ThrowingConsumer<ReplicatedDatabaseEventListener,Exception> action )
    {
        listeners.forEach( listener ->
        {
            try
            {
                action.accept( listener );
            }
            catch ( Exception e )
            {
                log.error( "Error handling database event", e );
            }
        } );
    }

    private void assertIsSystemDatabase( DatabaseId databaseId )
    {
        if ( !databaseId.isSystemDatabase() )
        {
            throw new IllegalArgumentException( "Not supported " + databaseId );
        }
    }
}
