/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.localdb;

import com.neo4j.fabric.config.FabricConfig;

import java.util.UUID;
import java.util.function.Function;

import org.neo4j.common.DependencyResolver;
import org.neo4j.dbms.api.DatabaseManagementService;
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
import org.neo4j.kernel.database.DatabaseIdRepository;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

import static org.neo4j.dbms.database.SystemGraphDbmsModel.DATABASE_DEFAULT_PROPERTY;
import static org.neo4j.dbms.database.SystemGraphDbmsModel.DATABASE_LABEL;
import static org.neo4j.dbms.database.SystemGraphDbmsModel.DATABASE_NAME_PROPERTY;
import static org.neo4j.dbms.database.SystemGraphDbmsModel.DATABASE_STATUS_PROPERTY;
import static org.neo4j.dbms.database.SystemGraphDbmsModel.DATABASE_UUID_PROPERTY;

public class FabricDatabaseManager extends LifecycleAdapter
{
    private final Label databaseLabel = Label.label( "Database" );
    private final FabricConfig fabricConfig;
    private final DependencyResolver dependencyResolver;
    private DatabaseManager<DatabaseContext> databaseManager;
    private DatabaseIdRepository databaseIdRepository;
    private DatabaseManagementService managementService;

    public FabricDatabaseManager( FabricConfig fabricConfig, DependencyResolver dependencyResolver )
    {
        this.dependencyResolver = dependencyResolver;
        this.fabricConfig = fabricConfig;
    }

    @Override
    public void start()
    {
        databaseManager = (DatabaseManager<DatabaseContext>) dependencyResolver.resolveDependency( DatabaseManager.class );
        databaseIdRepository = databaseManager.databaseIdRepository();
    }

    public void manageFabricDatabases( GraphDatabaseService system, boolean update )
    {

        try ( Transaction tx = system.beginTx() )
        {
            boolean exists = false;
            if ( update )
            {
                exists = checkExisting( tx );
            }

            if ( !exists && fabricConfig.isEnabled() )
            {
                newFabricDb( tx, fabricConfig.getDatabase().getName() );
            }
            tx.commit();
        }
    }

    public boolean isFabricDatabase( String databaseName )
    {
        return fabricConfig.isEnabled() && fabricConfig.getDatabase().getName().equals( databaseName );
    }

    public GraphDatabaseFacade getDatabase( String databaseName ) throws UnavailableException
    {
        var graphDatabaseFacade = databaseIdRepository.getByName( databaseName )
                .flatMap( databaseId -> databaseManager.getDatabaseContext( databaseId ) )
                .orElseThrow( () -> new DatabaseNotFoundException( "Database " + databaseName + " not found" ) )
                .databaseFacade();
        if ( !graphDatabaseFacade.isAvailable( 0 ) )
        {
            throw new UnavailableException( "Database %s not available " + databaseName );
        }

        return graphDatabaseFacade;
    }

    private boolean checkExisting( Transaction tx )
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

        return iterator.apply( tx.findNodes( databaseLabel, "fabric", true ) );
    }

    private void newFabricDb( Transaction tx, String dbName )
    {
        try
        {
            Node node = tx.createNode( DATABASE_LABEL );
            node.setProperty( DATABASE_NAME_PROPERTY, dbName );
            node.setProperty( DATABASE_UUID_PROPERTY, UUID.randomUUID().toString() );
            node.setProperty( DATABASE_STATUS_PROPERTY, "online" );
            node.setProperty( DATABASE_DEFAULT_PROPERTY, false );
            node.setProperty( "fabric", true );
        }
        catch ( ConstraintViolationException e )
        {
            throw new IllegalStateException( "The specified database '" + dbName + "' already exists." );
        }
    }
}
