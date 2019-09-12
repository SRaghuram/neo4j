/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.localdb;

import com.neo4j.fabric.config.FabricConfig;

import java.util.function.Function;

import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.Config;
import org.neo4j.dbms.api.DatabaseNotFoundException;
import org.neo4j.dbms.database.DatabaseContext;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.graphdb.ConstraintViolationException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.availability.UnavailableException;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.database.DatabaseIdRepository;
import org.neo4j.kernel.database.PlaceholderDatabaseIdRepository;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

public class FabricDatabaseManager extends LifecycleAdapter
{
    private final Label databaseLabel = Label.label( "Database" );
    private final DatabaseIdRepository databaseIdRepository;
    private final FabricConfig fabricConfig;
    private final DependencyResolver dependencyResolver;
    private DatabaseManager<DatabaseContext> databaseManager;

    public FabricDatabaseManager( FabricConfig fabricConfig, Config config, DependencyResolver dependencyResolver )
    {
        databaseIdRepository = new PlaceholderDatabaseIdRepository( config );
        this.dependencyResolver = dependencyResolver;
        this.fabricConfig = fabricConfig;
    }

    @Override
    public void start()
    {
        databaseManager = (DatabaseManager<DatabaseContext>) dependencyResolver.resolveDependency( DatabaseManager.class );
    }

    public void manageFabricDatabases( GraphDatabaseService system, boolean update )
    {

        try ( Transaction tx = system.beginTx() )
        {
            boolean exists = false;
            if ( update )
            {
                exists = checkExisting( system );
            }

            if ( !exists && fabricConfig.isEnabled() )
            {
                newFabricDb( system, fabricConfig.getDatabase().getName() );
            }
            tx.success();
        }
    }

    public boolean isFabricDatabase( String databaseName )
    {
        return fabricConfig.isEnabled() && fabricConfig.getDatabase().getName().equals( databaseName );
    }

    public GraphDatabaseFacade getDatabase( String databaseName ) throws UnavailableException
    {
        DatabaseId databaseId = databaseIdRepository.get( databaseName );
        GraphDatabaseFacade graphDatabaseFacade = databaseManager.getDatabaseContext( databaseId ).orElseThrow(
                () -> new DatabaseNotFoundException( "Database " + databaseName + " not found" ) ).databaseFacade();
        if ( !graphDatabaseFacade.isAvailable( 0 ) )
        {
            throw new UnavailableException( "Database %s not available " + databaseName );
        }

        return graphDatabaseFacade;
    }

    private boolean checkExisting( GraphDatabaseService system )
    {
        Function<ResourceIterator<Node>,Boolean> iterator = nodes ->
        {
            boolean found = false;
            while ( nodes.hasNext() )
            {
                Node fabricDb = nodes.next();
                var dbName = fabricDb.getProperty( "name" );

                if ( !fabricConfig.isEnabled() || !fabricConfig.getDatabase().getName().equals( dbName ) )
                {
                    fabricDb.setProperty( "status", "offline" );
                }
                else
                {
                    found = true;
                }
            }
            nodes.close();
            return found;
        };

        return iterator.apply( system.findNodes( databaseLabel, "fabric", true ) );
    }

    private void newFabricDb( GraphDatabaseService system, String dbName )
    {
        try
        {
            Node node = system.createNode( databaseLabel );
            node.setProperty( "name", dbName );
            node.setProperty( "status", "online" );
            node.setProperty( "default", false );
            node.setProperty( "fabric", true );
        }
        catch ( ConstraintViolationException e )
        {
            throw new IllegalStateException( "The specified database '" + dbName + "' already exists." );
        }
    }
}
