/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms.database;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.database.DatabaseContext;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.graphdb.facade.spi.ClassicCoreSPI;
import org.neo4j.graphdb.factory.module.DatabaseModule;
import org.neo4j.graphdb.factory.module.GlobalModule;
import org.neo4j.graphdb.factory.module.edition.AbstractEditionModule;
import org.neo4j.helpers.Exceptions;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.proc.GlobalProcedures;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Logger;

import static java.lang.String.format;
import static java.util.Comparator.naturalOrder;
import static java.util.Objects.requireNonNull;

public class MultiDatabaseManager extends LifecycleAdapter implements DatabaseManager
{
    private final ConcurrentHashMap<String, DatabaseContext> databaseMap = new ConcurrentHashMap<>();
    private final GlobalModule globalModule;
    private final AbstractEditionModule edition;
    private final GlobalProcedures globalProcedures;
    private final Logger log;
    private final GraphDatabaseFacade graphDatabaseFacade;
    private volatile boolean started;

    public MultiDatabaseManager( GlobalModule globalModule, AbstractEditionModule edition, GlobalProcedures globalProcedures,
            Logger log, GraphDatabaseFacade graphDatabaseFacade )
    {
        this.globalModule = globalModule;
        this.edition = edition;
        this.globalProcedures = globalProcedures;
        this.log = log;
        this.graphDatabaseFacade = graphDatabaseFacade;
    }

    @Override
    public DatabaseContext createDatabase( String databaseName )
    {
        requireNonNull( databaseName, "Database name should be not null." );
        return databaseMap.compute( databaseName, ( key, currentContext ) ->
        {
            if ( currentContext != null )
            {
                throw new IllegalStateException( format( "Database with name `%s` already exists.", databaseName ) );
            }
            return createNewDatabaseContext( databaseName );
        } );
    }

    @Override
    public void dropDatabase( String databaseName )
    {
        databaseMap.compute( databaseName, ( key, currentContext ) ->
        {
            if ( currentContext == null )
            {
                throw new IllegalStateException( format( "Database with name `%s` not found.", databaseName ) );
            }
            dropDatabase( databaseName, currentContext );
            return null;
        } );
    }

    @Override
    public Optional<DatabaseContext> getDatabaseContext( String name )
    {
        return Optional.ofNullable( databaseMap.get( name ) );
    }

    @Override
    public void stopDatabase( String databaseName )
    {
        databaseMap.compute( databaseName, ( key, currentContext ) ->
        {
            if ( currentContext == null )
            {
                throw new IllegalStateException( format( "Database with name `%s` not found.", databaseName ) );
            }
            stopDatabase( databaseName, currentContext );
            return currentContext;
        } );
    }

    @Override
    public void startDatabase( String databaseName )
    {
        databaseMap.compute( databaseName, ( key, currentContext ) ->
        {
            if ( currentContext == null )
            {
                throw new IllegalStateException( format( "Database with name `%s` not found.", databaseName ) );
            }
            startDatabase( databaseName, currentContext );
            return currentContext;
        } );
    }

    @Override
    public void start() throws Throwable
    {
        started = true;
        Throwable startException = doWithAllDatabases( this::startDatabase );
        if ( startException != null )
        {
            throw startException;
        }
    }

    @Override
    public void stop() throws Throwable
    {
        started = false;
        Throwable stopException = doWithAllDatabases( this::stopDatabase );
        if ( stopException != null )
        {
            throw stopException;
        }

    }

    @Override
    public void shutdown()
    {
        databaseMap.clear();
    }

    @Override
    public List<String> listDatabases()
    {
        ArrayList<String> databaseNames = new ArrayList<>( databaseMap.keySet() );
        databaseNames.sort( naturalOrder() );
        return databaseNames;
    }

    private DatabaseContext createNewDatabaseContext( String databaseName )
    {
        log.log( "Creating '%s' database.", databaseName );
        Config globalConfig = globalModule.getGlobalConfig();
        GraphDatabaseFacade facade =
                globalConfig.get( GraphDatabaseSettings.active_database ).equals( databaseName ) ? graphDatabaseFacade : new GraphDatabaseFacade();
        DatabaseModule dataSource = new DatabaseModule( databaseName, globalModule, edition, globalProcedures, facade );
        ClassicCoreSPI spi = new ClassicCoreSPI( globalModule, dataSource, log, dataSource.coreAPIAvailabilityGuard, edition.getThreadToTransactionBridge() );
        Database database = dataSource.database;
        facade.init( spi, edition.getThreadToTransactionBridge(), globalConfig, database.getTokenHolders() );
        if ( started )
        {
            database.start();
        }
        return new DatabaseContext( database, facade );
    }

    private Throwable doWithAllDatabases( BiConsumer<String, DatabaseContext> consumer )
    {
        Throwable combinedException = null;
        for ( Map.Entry<String,DatabaseContext> databaseContextEntry : databaseMap.entrySet() )
        {
            try
            {
                consumer.accept( databaseContextEntry.getKey(), databaseContextEntry.getValue() );
            }
            catch ( Throwable t )
            {
                combinedException = Exceptions.chain( combinedException, t );
            }
        }
        return combinedException;
    }

    private void startDatabase( String databaseName, DatabaseContext context )
    {
        log.log( "Starting '%s' database.", databaseName );
        Database database = context.getDatabase();
        database.start();
    }

    private void stopDatabase( String databaseName, DatabaseContext context )
    {
        log.log( "Stop '%s' database.", databaseName );
        Database database = context.getDatabase();
        database.stop();
    }

    private void dropDatabase( String databaseName, DatabaseContext context )
    {
        log.log( "Drop '%s' database.", databaseName );
        Database database = context.getDatabase();
        database.drop();
    }
}
