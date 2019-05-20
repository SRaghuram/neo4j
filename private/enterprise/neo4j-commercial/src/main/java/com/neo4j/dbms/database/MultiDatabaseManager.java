/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms.database;

import java.util.Optional;

import org.neo4j.dbms.database.AbstractDatabaseManager;
import org.neo4j.dbms.database.DatabaseContext;
import org.neo4j.dbms.database.DatabaseExistsException;
import org.neo4j.dbms.database.DatabaseManagementException;
import org.neo4j.dbms.database.DatabaseNotFoundException;
import org.neo4j.graphdb.factory.module.GlobalModule;
import org.neo4j.graphdb.factory.module.edition.AbstractEditionModule;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.logging.Log;

import static com.neo4j.kernel.impl.enterprise.configuration.CommercialEditionSettings.maxNumberOfDatabases;
import static java.lang.String.format;

public abstract class MultiDatabaseManager<DB extends DatabaseContext> extends AbstractDatabaseManager<DB>
{
    protected volatile boolean started;
    private final long maximumNumerOfDatabases;

    public MultiDatabaseManager( GlobalModule globalModule, AbstractEditionModule edition, Log log )
    {
        super( globalModule, edition, log );
        maximumNumerOfDatabases = globalModule.getGlobalConfig().get( maxNumberOfDatabases );
    }

    public DB createStoppedDatabase( DatabaseId databaseId ) throws DatabaseExistsException
    {
        return createDatabase( databaseId, false );
    }

    @Override
    public DB createDatabase( DatabaseId databaseId ) throws DatabaseExistsException
    {
        return createDatabase( databaseId, true );
    }

    private DB createDatabase( DatabaseId databaseId, boolean autostart )
    {
        return databaseMap.compute( databaseId, ( key, currentContext ) ->
        {
            if ( currentContext != null )
            {
                throw new DatabaseExistsException( format( "Database with name `%s` already exists.", databaseId.name() ) );
            }
            if ( databaseMap.size() >= maximumNumerOfDatabases )
            {
                throw new DatabaseManagementException(
                        format( "Reached maximum number of active databases. Fail to create new database `%s`.", databaseId.name() ) );
            }
            DB databaseContext = createNewDatabaseContext( databaseId );
            if ( autostart && started )
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
        log.info( "Drop '%s' database.", databaseId.name() );
        Database database = context.database();
        database.drop();
    }
}
