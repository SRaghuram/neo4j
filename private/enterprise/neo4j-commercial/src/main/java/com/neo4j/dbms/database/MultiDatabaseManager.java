/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms.database;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import org.neo4j.dbms.database.DatabaseContext;
import org.neo4j.dmbs.database.AbstractDatabaseManager;
import org.neo4j.graphdb.factory.module.GlobalModule;
import org.neo4j.graphdb.factory.module.edition.AbstractEditionModule;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.logging.Logger;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class MultiDatabaseManager extends AbstractDatabaseManager
{
    private final ConcurrentHashMap<String, DatabaseContext> databaseMap = new ConcurrentHashMap<>();
    private volatile boolean started;

    public MultiDatabaseManager( GlobalModule globalModule, AbstractEditionModule edition, GlobalProcedures globalProcedures,
            Logger log, GraphDatabaseFacade graphDatabaseFacade )
    {
        super( globalModule, edition, globalProcedures, log, graphDatabaseFacade );
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
            DatabaseContext databaseContext = createNewDatabaseContext( databaseName );
            if ( started )
            {
                databaseContext.getDatabase().start();
            }
            return databaseContext;
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
        super.start();
    }

    @Override
    public void stop() throws Throwable
    {
        started = false;
        super.stop();
    }

    @Override
    protected Map<String,DatabaseContext> getDatabaseMap()
    {
        return databaseMap;
    }

    @Override
    public void shutdown()
    {
        databaseMap.clear();
    }

    private void dropDatabase( String databaseName, DatabaseContext context )
    {
        log.log( "Drop '%s' database.", databaseName );
        Database database = context.getDatabase();
        database.drop();
    }
}
