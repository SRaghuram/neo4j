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

import org.neo4j.dbms.database.DatabaseContext;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.graphdb.facade.spi.ClassicCoreSPI;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.factory.module.DatabaseModule;
import org.neo4j.graphdb.factory.module.PlatformModule;
import org.neo4j.graphdb.factory.module.edition.AbstractEditionModule;
import org.neo4j.helpers.Exceptions;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.impl.util.CopyOnWriteHashMap;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Logger;

import static java.util.Comparator.naturalOrder;
import static java.util.Objects.requireNonNull;

public class MultiDatabaseManager extends LifecycleAdapter implements DatabaseManager
{
    private final Map<String, DatabaseContext> databaseMap = new CopyOnWriteHashMap<>();
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
    public DatabaseContext createDatabase( String databaseName )
    {
        requireNonNull( databaseName, "Database name should be not null" );
        log.log( "Creating '%s' database.", databaseName );

        GraphDatabaseFacade facade =
                platform.config.get( GraphDatabaseSettings.active_database ).equals( databaseName ) ? graphDatabaseFacade : new GraphDatabaseFacade();
        DatabaseModule dataSource = new DatabaseModule( databaseName, platform, edition, procedures, facade );
        ClassicCoreSPI spi = new ClassicCoreSPI( platform, dataSource, log, dataSource.coreAPIAvailabilityGuard, edition.getThreadToTransactionBridge() );
        Database database = dataSource.database;
        facade.init( spi, edition.getThreadToTransactionBridge(), platform.config, database.getTokenHolders() );
        DatabaseContext databaseContext = new DatabaseContext( database, database.getDependencyResolver(), graphDatabaseFacade );
        databaseMap.put( databaseName, databaseContext );
        return databaseContext;
    }

    @Override
    public Optional<DatabaseContext> getDatabaseContext( String name )
    {
        return Optional.ofNullable( databaseMap.get( name ) );
    }

    @Override
    public void shutdownDatabase( String databaseName )
    {
        DatabaseContext databaseContext = databaseMap.remove( databaseName );
        if ( databaseContext != null )
        {
            shutdownDatabase( databaseName, databaseContext );
        }
    }

    @Override
    public void stop() throws Throwable
    {
        Throwable stopException = null;
        for ( Map.Entry<String, DatabaseContext> databaseContextEntry : databaseMap.entrySet() )
        {
            try
            {
                shutdownDatabase( databaseContextEntry.getKey(), databaseContextEntry.getValue() );
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

    private void shutdownDatabase( String databaseName, DatabaseContext context )
    {
        log.log( "Shutting down '%s' database.", databaseName );
        Database database = context.getDatabase();
        database.stop();
        database.shutdown();
    }
}
