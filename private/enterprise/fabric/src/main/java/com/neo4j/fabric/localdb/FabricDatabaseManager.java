/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.localdb;

import com.neo4j.fabric.config.FabricConfig;

import java.util.UUID;
import java.util.function.Function;

import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.helpers.NormalizedDatabaseName;
import org.neo4j.dbms.api.DatabaseNotFoundException;
import org.neo4j.dbms.database.DatabaseContext;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.graphdb.ConstraintViolationException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.availability.UnavailableException;
import org.neo4j.kernel.database.DatabaseIdRepository;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.util.FeatureToggles;

import static org.neo4j.dbms.database.SystemGraphDbmsModel.DATABASE_DEFAULT_PROPERTY;
import static org.neo4j.dbms.database.SystemGraphDbmsModel.DATABASE_LABEL;
import static org.neo4j.dbms.database.SystemGraphDbmsModel.DATABASE_NAME_PROPERTY;
import static org.neo4j.dbms.database.SystemGraphDbmsModel.DATABASE_STATUS_PROPERTY;
import static org.neo4j.dbms.database.SystemGraphDbmsModel.DATABASE_UUID_PROPERTY;

public class FabricDatabaseManager extends LifecycleAdapter
{
    public static final String FABRIC_BY_DEFAULT_FLAG_NAME = "fabric_by_default";

    private final FabricConfig fabricConfig;
    private final DependencyResolver dependencyResolver;
    private final Log log;
    private DatabaseManager<DatabaseContext> databaseManager;
    private DatabaseIdRepository databaseIdRepository;

    public FabricDatabaseManager( FabricConfig fabricConfig, DependencyResolver dependencyResolver, LogProvider logProvider )
    {
        this.dependencyResolver = dependencyResolver;
        this.fabricConfig = fabricConfig;
        this.log = logProvider.getLog( getClass() );
    }

    @Override
    public void start()
    {
        databaseManager = (DatabaseManager<DatabaseContext>) dependencyResolver.resolveDependency( DatabaseManager.class );
        databaseIdRepository = databaseManager.databaseIdRepository();
    }

    public DatabaseIdRepository databaseIdRepository()
    {
        return databaseIdRepository;
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

            if ( fabricConfig.isEnabled() )
            {
                NormalizedDatabaseName dbName = fabricConfig.getDatabase().getName();
                if ( exists )
                {
                    log.info( "Using existing Fabric virtual database '%s'", dbName.name() );
                }
                else
                {
                    log.info( "Creating Fabric virtual database '%s'", dbName.name() );
                    newFabricDb( tx, dbName );
                }
            }
            tx.commit();
        }
    }

    public boolean isFabricDatabase( String databaseNameRaw )
    {
        var fabricByDefault = fabricEnabledForAllDatabases();
        var databaseName = new NormalizedDatabaseName( databaseNameRaw ).name();
        var isSystemDatabase = databaseName.equals( GraphDatabaseSettings.SYSTEM_DATABASE_NAME );
        return !isSystemDatabase && (fabricByDefault || isConfiguredFabricDatabase( databaseNameRaw ));
    }

    public boolean hasFabricRoutingTable( String databaseNameRaw )
    {
        return isConfiguredFabricDatabase( databaseNameRaw );
    }

    public boolean fabricEnabledForAllDatabases()
    {
        return FeatureToggles.flag( FabricDatabaseManager.class, FABRIC_BY_DEFAULT_FLAG_NAME, false );
    }

    public boolean isConfiguredFabricDatabase( String databaseNameRaw )
    {
        var databaseName = new NormalizedDatabaseName( databaseNameRaw );
        return fabricConfig.isEnabled() && fabricConfig.getDatabase().getName().equals( databaseName );
    }

    public GraphDatabaseFacade getDatabase( String databaseNameRaw ) throws UnavailableException
    {
        var graphDatabaseFacade = databaseIdRepository.getByName( databaseNameRaw )
                                                      .flatMap( databaseId -> databaseManager.getDatabaseContext( databaseId ) )
                                                      .orElseThrow( () -> new DatabaseNotFoundException( "Database " + databaseNameRaw + " not found" ) )
                                                      .databaseFacade();
        if ( !graphDatabaseFacade.isAvailable( 0 ) )
        {
            throw new UnavailableException( "Database %s not available " + databaseNameRaw );
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

                if ( !fabricConfig.isEnabled() || !fabricConfig.getDatabase().getName().name().equals( dbName ) )
                {
                    log.info( "Setting Fabric virtual database '%s' status to offline", dbName );
                    fabricDb.setProperty( "status", "offline" );
                }
                else
                {
                    log.info( "Setting Fabric virtual database '%s' status to online", dbName );
                    fabricDb.setProperty( "status", "online" );
                    found = true;
                }
            }
            nodes.close();
            return found;
        };

        return iterator.apply( tx.findNodes( DATABASE_LABEL, "fabric", true ) );
    }

    private void newFabricDb( Transaction tx, NormalizedDatabaseName dbName )
    {
        try
        {
            Node node = tx.createNode( DATABASE_LABEL );
            node.setProperty( DATABASE_NAME_PROPERTY, dbName.name() );
            node.setProperty( DATABASE_UUID_PROPERTY, UUID.randomUUID().toString() );
            node.setProperty( DATABASE_STATUS_PROPERTY, "online" );
            node.setProperty( DATABASE_DEFAULT_PROPERTY, false );
            node.setProperty( "fabric", true );
        }
        catch ( ConstraintViolationException e )
        {
            throw new IllegalStateException( "The specified database '" + dbName.name() + "' already exists." );
        }
    }
}
