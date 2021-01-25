/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.neo4j.function.ThrowingConsumer;
import org.neo4j.kernel.database.NamedDatabaseId;
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

    @Override
    public void registerListener( NamedDatabaseId namedDatabaseId, ReplicatedDatabaseEventListener listener )
    {
        assertIsSystemDatabase( namedDatabaseId );
        listeners.add( listener );
    }

    @Override
    public void unregisterListener( NamedDatabaseId namedDatabaseId, ReplicatedDatabaseEventListener listener )
    {
        assertIsSystemDatabase( namedDatabaseId );
        listeners.remove( listener );
    }

    @Override
    public ReplicatedDatabaseEventDispatch getDatabaseEventDispatch( NamedDatabaseId namedDatabaseId )
    {
        if ( !namedDatabaseId.isSystemDatabase() )
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

    private void assertIsSystemDatabase( NamedDatabaseId namedDatabaseId )
    {
        if ( !namedDatabaseId.isSystemDatabase() )
        {
            throw new IllegalArgumentException( "Not supported " + namedDatabaseId );
        }
    }
}
