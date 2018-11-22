/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms.database;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.graphdb.facade.spi.ClassicCoreSPI;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.factory.module.DatabaseModule;
import org.neo4j.graphdb.factory.module.PlatformModule;
import org.neo4j.graphdb.factory.module.edition.AbstractEditionModule;
import org.neo4j.helpers.Exceptions;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.impl.util.CopyOnWriteHashMap;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Logger;

import static java.util.Comparator.naturalOrder;
import static java.util.Objects.requireNonNull;

public class MultiDatabaseManager extends LifecycleAdapter implements DatabaseManager
{
    private final Map<String, GraphDatabaseFacade> databaseMap = new CopyOnWriteHashMap<>();
    private final PlatformModule platform;
    private final AbstractEditionModule edition;
    private final Procedures procedures;
    private final Logger log;
    private final GraphDatabaseFacade graphDatabaseFacade;

    public MultiDatabaseManager( PlatformModule platform, AbstractEditionModule edition, Procedures procedures,
            Logger log, GraphDatabaseFacade graphDatabaseFacade )
    {
        this.platform = platform;
        this.edition = edition;
        this.procedures = procedures;
        this.log = log;
        this.graphDatabaseFacade = graphDatabaseFacade;
    }

    @Override
    public GraphDatabaseFacade createDatabase( String databaseName )
    {
        requireNonNull( databaseName, "Database name should be not null" );
        log.log( "Creating '%s' database.", databaseName );

        GraphDatabaseFacade facade =
                platform.config.get( GraphDatabaseSettings.active_database ).equals( databaseName ) ? graphDatabaseFacade : new GraphDatabaseFacade();
        DatabaseModule dataSource = new DatabaseModule( databaseName, platform, edition, procedures, facade );
        ClassicCoreSPI spi = new ClassicCoreSPI( platform, dataSource, log, dataSource.coreAPIAvailabilityGuard, edition.getThreadToTransactionBridge() );
        facade.init( spi, edition.getThreadToTransactionBridge(), platform.config, dataSource.database.getTokenHolders() );
        databaseMap.put( databaseName, facade );
        return facade;
    }

    @Override
    public Optional<GraphDatabaseFacade> getDatabaseFacade( String name )
    {
        return Optional.ofNullable( databaseMap.get( name ) );
    }

    @Override
    public void shutdownDatabase( String databaseName )
    {
        GraphDatabaseFacade databaseFacade = databaseMap.remove( databaseName );
        if ( databaseFacade != null )
        {
            shutdownDatabase( databaseName, databaseFacade );
        }
    }

    @Override
    public void stop() throws Throwable
    {
        Throwable stopException = null;
        for ( Map.Entry<String, GraphDatabaseFacade> databaseFacade : databaseMap.entrySet() )
        {
            try
            {
                shutdownDatabase( databaseFacade.getKey(), databaseFacade.getValue() );
            }
            catch ( Throwable t )
            {
                stopException = Exceptions.chain( stopException, t );
            }
        }
        databaseMap.clear();
        if ( stopException != null )
        {
            throw stopException;
        }
    }

    @Override
    public List<String> listDatabases()
    {
        ArrayList<String> databaseNames = new ArrayList<>( databaseMap.keySet() );
        databaseNames.sort( naturalOrder() );
        return databaseNames;
    }

    private void shutdownDatabase( String databaseName, GraphDatabaseFacade databaseFacade )
    {
        log.log( "Shutting down '%s' database.", databaseName );
        databaseFacade.shutdown();
    }
}
