/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms.database;

import java.util.Comparator;
import java.util.Optional;

import org.neo4j.dbms.database.DatabaseContext;
import org.neo4j.dbms.database.DatabaseExistsException;
import org.neo4j.dbms.database.DatabaseNotFoundException;
import org.neo4j.dmbs.database.AbstractDatabaseManager;
import org.neo4j.graphdb.factory.module.GlobalModule;
import org.neo4j.graphdb.factory.module.edition.AbstractEditionModule;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.logging.Logger;
import org.neo4j.util.VisibleForTesting;

import static java.lang.String.format;

public abstract class MultiDatabaseManager<DB extends DatabaseContext> extends AbstractDatabaseManager<DB>
{
    protected volatile boolean started;

    public MultiDatabaseManager( GlobalModule globalModule, AbstractEditionModule edition, Logger log, GraphDatabaseFacade graphDatabaseFacade )
    {
        super( globalModule, edition, log, graphDatabaseFacade );
    }

    @VisibleForTesting
    MultiDatabaseManager( GlobalModule globalModule, AbstractEditionModule edition, Logger log, GraphDatabaseFacade graphDatabaseFacade,
            Comparator<DatabaseId> databasesOrdering )
    {
        super( globalModule, edition, log, graphDatabaseFacade, databasesOrdering );
    }

    @Override
    public DB createDatabase( DatabaseId databaseId ) throws DatabaseExistsException
    {
        return databaseMap.compute( databaseId, ( key, currentContext ) ->
        {
            if ( currentContext != null )
            {
                throw new DatabaseExistsException( format( "Database with name `%s` already exists.", databaseId.name() ) );
            }
            DB databaseContext = createNewDatabaseContext( databaseId );
            if ( started )
            {
                databaseContext.database().start();
            }
            return databaseContext;
        } );
    }

    @Override
    public void dropDatabase( DatabaseId databaseId ) throws DatabaseNotFoundException
    {
        databaseMap.compute( databaseId, ( key, currentContext ) ->
        {
            if ( currentContext == null )
            {
                throw new DatabaseNotFoundException( format( "Database with name `%s` not found.", databaseId.name() ) );
            }
            dropDatabase( databaseId, currentContext );
            return null;
        } );
    }

    @Override
    public Optional<DB> getDatabaseContext( DatabaseId databaseId )
    {
        return Optional.ofNullable( databaseMap.get( databaseId ) );
    }

    @Override
    public void stopDatabase( DatabaseId databaseId ) throws DatabaseNotFoundException
    {
        databaseMap.compute( databaseId, ( key, currentContext ) ->
        {
            if ( currentContext == null )
            {
                throw new DatabaseNotFoundException( format( "Database with name `%s` not found.", databaseId.name() ) );
            }
            stopDatabase( databaseId, currentContext );
            return currentContext;
        } );
    }

    @Override
    public void startDatabase( DatabaseId databaseId ) throws DatabaseNotFoundException
    {
        databaseMap.compute( databaseId, ( key, currentContext ) ->
        {
            if ( currentContext == null )
            {
                throw new DatabaseNotFoundException( format( "Database with name `%s` not found.", databaseId.name() ) );
            }
            startDatabase( databaseId, currentContext );
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

    protected void dropDatabase( DatabaseId databaseId, DB context )
    {
        log.log( "Drop '%s' database.", databaseId.name() );
        Database database = context.database();
        database.drop();
    }
}
