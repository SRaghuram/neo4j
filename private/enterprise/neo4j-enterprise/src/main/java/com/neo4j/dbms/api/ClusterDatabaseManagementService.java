/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms.api;

import com.neo4j.causalclustering.core.consensus.RaftMachine;

import java.util.List;

import org.neo4j.annotations.api.PublicApi;
import org.neo4j.dbms.api.DatabaseExistsException;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseNotFoundException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.event.DatabaseEventListener;
import org.neo4j.graphdb.event.TransactionEventListener;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

@PublicApi
public class ClusterDatabaseManagementService implements DatabaseManagementService
{
    private final DatabaseManagementService delegate;

    public ClusterDatabaseManagementService( DatabaseManagementService delegate )
    {
        this.delegate = delegate;
    }

    /**
     * Returns the availability of the database for write operations.
     *
     * @param databaseName name of the database.
     * @return true if the database accepts writes, false otherwise.
     * @throws DatabaseNotFoundException if no database service with the given name is found.
     */
    public boolean isWritable( String databaseName )
    {
        GraphDatabaseAPI dbAPI = (GraphDatabaseAPI) database( databaseName );
        if ( !dbAPI.isAvailable( 0 ) )
        {
            return false;
        }
        var resolver = dbAPI.getDependencyResolver();
        return resolver.containsDependency( RaftMachine.class ) && resolver.resolveDependency( RaftMachine.class ).isLeader();
    }

    @Override
    public GraphDatabaseService database( String databaseName ) throws DatabaseNotFoundException
    {
        return delegate.database( databaseName );
    }

    @Override
    public void createDatabase( String databaseName ) throws DatabaseExistsException
    {
        delegate.createDatabase( databaseName );
    }

    @Override
    public void dropDatabase( String databaseName ) throws DatabaseNotFoundException
    {
        delegate.dropDatabase( databaseName );
    }

    @Override
    public void startDatabase( String databaseName ) throws DatabaseNotFoundException
    {
        delegate.startDatabase( databaseName );
    }

    @Override
    public void shutdownDatabase( String databaseName ) throws DatabaseNotFoundException
    {
        delegate.shutdownDatabase( databaseName );
    }

    @Override
    public List<String> listDatabases()
    {
        return delegate.listDatabases();
    }

    @Override
    public void registerDatabaseEventListener( DatabaseEventListener listener )
    {
        delegate.registerDatabaseEventListener( listener );
    }

    @Override
    public void unregisterDatabaseEventListener( DatabaseEventListener listener )
    {
        delegate.unregisterDatabaseEventListener( listener );
    }

    @Override
    public void registerTransactionEventListener( String databaseName, TransactionEventListener<?> listener )
    {
        delegate.registerTransactionEventListener( databaseName, listener );
    }

    @Override
    public void unregisterTransactionEventListener( String databaseName, TransactionEventListener<?> listener )
    {
        delegate.unregisterTransactionEventListener( databaseName, listener );
    }

    @Override
    public void shutdown()
    {
        delegate.shutdown();
    }
}
