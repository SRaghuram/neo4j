/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms.database;

import java.util.Collections;
import java.util.Optional;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;

import org.neo4j.dbms.database.DatabaseContext;
import org.neo4j.dbms.database.DatabaseExistsException;
import org.neo4j.dbms.database.DatabaseNotFoundException;
import org.neo4j.dmbs.database.AbstractDatabaseManager;
import org.neo4j.graphdb.factory.module.GlobalModule;
import org.neo4j.graphdb.factory.module.edition.AbstractEditionModule;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.logging.Logger;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public abstract class MultiDatabaseManager<DB extends DatabaseContext> extends AbstractDatabaseManager<DB>
{
    protected final ConcurrentSkipListMap<String,DB> databaseMap = new ConcurrentSkipListMap<>();
    protected volatile boolean started;

    public MultiDatabaseManager( GlobalModule globalModule, AbstractEditionModule edition, Logger log, GraphDatabaseFacade graphDatabaseFacade )
    {
        super( globalModule, edition, log, graphDatabaseFacade );
    }

    @Override
    public DB createDatabase( String databaseName ) throws DatabaseExistsException
    {
        requireNonNull( databaseName, "Database name should be not null." );
        return databaseMap.compute( databaseName, ( key, currentContext ) ->
        {
            if ( currentContext != null )
            {
                throw new DatabaseExistsException( format( "Database with name `%s` already exists.", databaseName ) );
            }
            DB databaseContext = createNewDatabaseContext( databaseName );
            if ( started )
            {
                databaseContext.database().start();
            }
            return databaseContext;
        } );
    }

    @Override
    public void dropDatabase( String databaseName ) throws DatabaseNotFoundException
    {
        databaseMap.compute( databaseName, ( key, currentContext ) ->
        {
            if ( currentContext == null )
            {
                throw new DatabaseNotFoundException( format( "Database with name `%s` not found.", databaseName ) );
            }
            dropDatabase( databaseName, currentContext );
            return null;
        } );
    }

    @Override
    public Optional<DB> getDatabaseContext( String name )
    {
        return Optional.ofNullable( databaseMap.get( name ) );
    }

    @Override
    public void stopDatabase( String databaseName ) throws DatabaseNotFoundException
    {
        databaseMap.compute( databaseName, ( key, currentContext ) ->
        {
            if ( currentContext == null )
            {
                throw new DatabaseNotFoundException( format( "Database with name `%s` not found.", databaseName ) );
            }
            stopDatabase( databaseName, currentContext );
            return currentContext;
        } );
    }

    @Override
    public void startDatabase( String databaseName ) throws DatabaseNotFoundException
    {
        databaseMap.compute( databaseName, ( key, currentContext ) ->
        {
            if ( currentContext == null )
            {
                throw new DatabaseNotFoundException( format( "Database with name `%s` not found.", databaseName ) );
            }
            startDatabase( databaseName, currentContext );
            return currentContext;
        } );
    }

    @Override
    public void start() throws Exception
    {
        started = true;
        super.start();
    }

    @Override
    public void stop() throws Exception
    {
        started = false;
        super.stop();
    }

    @Override
    public void shutdown()
    {
        databaseMap.clear();
    }

    @Override
    public SortedMap<String,DB> registeredDatabases()
    {
        return Collections.unmodifiableSortedMap( databaseMap );
    }

    protected void dropDatabase( String databaseName, DB context )
    {
        log.log( "Drop '%s' database.", databaseName );
        Database database = context.database();
        database.drop();
    }
}
