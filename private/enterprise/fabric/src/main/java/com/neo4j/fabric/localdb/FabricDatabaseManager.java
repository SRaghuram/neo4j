/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.localdb;

import com.neo4j.fabric.config.FabricConfig;

import java.util.UUID;
import java.util.function.Function;

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
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.util.FeatureToggles;

import static org.neo4j.dbms.database.SystemGraphDbmsModel.DATABASE_DEFAULT_PROPERTY;
import static org.neo4j.dbms.database.SystemGraphDbmsModel.DATABASE_LABEL;
import static org.neo4j.dbms.database.SystemGraphDbmsModel.DATABASE_NAME_PROPERTY;
import static org.neo4j.dbms.database.SystemGraphDbmsModel.DATABASE_STATUS_PROPERTY;
import static org.neo4j.dbms.database.SystemGraphDbmsModel.DATABASE_UUID_PROPERTY;

public abstract class FabricDatabaseManager
{
    public static final String FABRIC_BY_DEFAULT_FLAG_NAME = "fabric_by_default";

    private final DatabaseManager<DatabaseContext> databaseManager;
    private final DatabaseIdRepository databaseIdRepository;

    public FabricDatabaseManager( DatabaseManager<DatabaseContext> databaseManager )
    {
        this.databaseManager = databaseManager;
        this.databaseIdRepository = databaseManager.databaseIdRepository();
    }

    public DatabaseIdRepository databaseIdRepository()
    {
        return databaseIdRepository;
    }

    public boolean hasMultiGraphCapabilities( String databaseNameRaw )
    {
        var multiGraphByDefault = multiGraphCapabilitiesEnabledForAllDatabases();
        var databaseName = new NormalizedDatabaseName( databaseNameRaw ).name();
        var isSystemDatabase = databaseName.equals( GraphDatabaseSettings.SYSTEM_DATABASE_NAME );
        return !isSystemDatabase && (multiGraphByDefault || isFabricDatabase( databaseNameRaw ));
    }

    public boolean multiGraphCapabilitiesEnabledForAllDatabases()
    {
        return FeatureToggles.flag( FabricDatabaseManager.class, FABRIC_BY_DEFAULT_FLAG_NAME, false );
    }

    public GraphDatabaseFacade getDatabase( String databaseNameRaw ) throws UnavailableException
    {
        var graphDatabaseFacade = databaseIdRepository.getByName( databaseNameRaw )
                .flatMap( databaseManager::getDatabaseContext )
                .orElseThrow( () -> new DatabaseNotFoundException( "Database " + databaseNameRaw + " not found" ) )
                .databaseFacade();
        if ( !graphDatabaseFacade.isAvailable( 0 ) )
        {
            throw new UnavailableException( "Database %s not available " + databaseNameRaw );
        }

        return graphDatabaseFacade;
    }

    public abstract boolean isFabricDatabasePresent();

    public abstract void manageFabricDatabases( GraphDatabaseService system, boolean update );

    public abstract boolean isFabricDatabase( String databaseNameRaw );

    /**
     * Fabric database manager on non-cluster instances.
     */
    public static class Single extends FabricDatabaseManager
    {
        private final FabricConfig fabricConfig;
        private final Log log;

        public Single( FabricConfig fabricConfig, DatabaseManager<DatabaseContext> databaseManager, LogProvider logProvider )
        {
            super( databaseManager );
            this.fabricConfig = fabricConfig;
            this.log = logProvider.getLog( getClass() );
        }

        @Override
        public boolean isFabricDatabasePresent()
        {
            return fabricConfig.getDatabase() != null;
        }

        @Override
        public void manageFabricDatabases( GraphDatabaseService system, boolean update )
        {

            try ( Transaction tx = system.beginTx() )
            {
                boolean exists = false;
                if ( update )
                {
                    exists = checkExisting( tx );
                }

                if ( fabricConfig.getDatabase() != null )
                {
                    NormalizedDatabaseName dbName = fabricConfig.getDatabase().getName();
                    if ( exists )
                    {
                        log.info( "Using existing Fabric virtual database '%s'", dbName.name());
                    }
                    else
                    {
                        log.info( "Creating Fabric virtual database '%s'", dbName.name());
                        newFabricDb( tx, dbName );
                    }
                }
                tx.commit();
            }
        }

        public boolean isFabricDatabase( String databaseNameRaw )
        {
            var databaseName = new NormalizedDatabaseName( databaseNameRaw );
            return isFabricDatabasePresent() && fabricConfig.getDatabase().getName().equals( databaseName );
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

                    if ( fabricConfig == null
                            || fabricConfig.getDatabase() == null
                            || fabricConfig.getDatabase().getName() == null
                            || !fabricConfig.getDatabase().getName().name().equals( dbName ) )
                    {
                        log.info( "Setting Fabric virtual database '%s' status to offline", dbName);
                        fabricDb.setProperty( "status", "offline" );
                    }
                    else
                    {
                        log.info( "Setting Fabric virtual database '%s' status to online", dbName);
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

    /**
     * Fabric database manager on cluster instances.
     */
    public static class Cluster extends FabricDatabaseManager
    {
        public Cluster( DatabaseManager<DatabaseContext> databaseManager )
        {
            super( databaseManager );
        }

        @Override
        public boolean isFabricDatabasePresent()
        {
            return false;
        }

        @Override
        public void manageFabricDatabases( GraphDatabaseService system, boolean update )
        {
            // a "Fabric" database with special capabilities cannot exist on a cluster member
        }

        @Override
        public boolean isFabricDatabase( String databaseNameRaw )
        {
            // a "Fabric" database with special capabilities cannot exist on a cluster member
            return false;
        }
    }
}
