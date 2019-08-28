/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms.database;

import java.util.Optional;
import java.util.function.BiFunction;

import org.neo4j.dbms.api.DatabaseExistsException;
import org.neo4j.dbms.api.DatabaseManagementException;
import org.neo4j.dbms.api.DatabaseNotFoundException;
import org.neo4j.dbms.database.AbstractDatabaseManager;
import org.neo4j.dbms.database.DatabaseContext;
import org.neo4j.graphdb.factory.module.GlobalModule;
import org.neo4j.graphdb.factory.module.edition.AbstractEditionModule;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.database.DatabaseId;

import static com.neo4j.kernel.impl.enterprise.configuration.CommercialEditionSettings.maxNumberOfDatabases;
import static java.lang.String.format;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;

public abstract class MultiDatabaseManager<DB extends DatabaseContext> extends AbstractDatabaseManager<DB>
{
    private final long maximumNumberOfDatabases;
    private volatile boolean databaseManagerStarted;

    public MultiDatabaseManager( GlobalModule globalModule, AbstractEditionModule edition )
    {
        this( globalModule, edition, false );
    }

    MultiDatabaseManager( GlobalModule globalModule, AbstractEditionModule edition, boolean manageDatabasesOnStartAndStop )
    {
        super( globalModule, edition, manageDatabasesOnStartAndStop );
        maximumNumberOfDatabases = globalModule.getGlobalConfig().get( maxNumberOfDatabases );
    }

    @Override
    public DB createDatabase( DatabaseId databaseId ) throws DatabaseExistsException
    {
        return createDatabase( databaseId, true );
    }

    public DB createDatabase( DatabaseId databaseId, boolean autoStart )
    {
        return databaseMap.compute( databaseId, ( key, currentContext ) ->
        {
            if ( currentContext != null )
            {
                throw new DatabaseExistsException( format( "Database with name `%s` already exists.", databaseId.name() ) );
            }
            if ( databaseMap.size() >= maximumNumberOfDatabases )
            {
                throw new DatabaseManagementException(
                        format( "Reached maximum number of active databases. Fail to create new database `%s`.", databaseId.name() ) );
            }
            DB databaseContext;
            try
            {
                log.info( "Creating '%s' database.", databaseId.name() );
                databaseContext = createDatabaseContext( databaseId );
            }
            catch ( Exception e )
            {
                throw new DatabaseManagementException(
                        format( "An error occured! Fail to create new database `%s`.", databaseId.name() ), e );
            }
            // TODO: Autostart only used in tests, update tests to create -> start
            if ( autoStart && databaseManagerStarted )
            {
                try
                {
                    startDatabase( databaseId, databaseContext );
                }
                catch ( Throwable t )
                {
                    databaseContext.fail( t );
                }
            }

            databaseIdRepository().cache( databaseId );

            return databaseContext;
        } );
    }

    @Override
    public Optional<DB> getDatabaseContext( DatabaseId databaseId )
    {
        return Optional.ofNullable( databaseMap.get( databaseId ) );
    }

    @Override
    public void dropDatabase( DatabaseId databaseId ) throws DatabaseNotFoundException
    {
        if ( SYSTEM_DATABASE_NAME.equals( databaseId.name() ) )
        {
            throw new DatabaseManagementException( "System database can't be dropped." );
        }

        forSingleDatabase( databaseId, this::dropDatabase );
    }

    @Override
    public void stopDatabase( DatabaseId databaseId ) throws DatabaseNotFoundException
    {
        forSingleDatabase( databaseId, this::stopDatabase );
    }

    @Override
    public void startDatabase( DatabaseId databaseId ) throws DatabaseNotFoundException
    {
        forSingleDatabase( databaseId, this::startDatabase );
    }

    protected void forSingleDatabase( DatabaseId databaseId, BiFunction<DatabaseId,DB,DB> operation )
    {
        requireStarted( databaseId );
        databaseMap.compute( databaseId, ( key, currentContext ) ->
        {
            if ( currentContext == null )
            {
                throw new DatabaseNotFoundException( format( "Database with name `%s` not found.", databaseId.name() ) );
            }
            try
            {
                return operation.apply( databaseId, currentContext );
            }
            catch ( Throwable t )
            {
                log.error( "Failed to perform operation with database " + databaseId, t );
                currentContext.fail( t );
                return currentContext;
            }
        } );
    }

    @Override
    public void start() throws Exception
    {
        if ( !databaseManagerStarted )
        {
            databaseManagerStarted = true;
            super.start();
        }
    }

    @Override
    public void stop() throws Exception
    {
        if ( databaseManagerStarted )
        {
            super.stop();
            databaseManagerStarted = false;
        }
    }

    @Override
    public final void shutdown()
    {
        databaseMap.clear();
    }

    protected DB dropDatabase( DatabaseId databaseId, DB context )
    {
        log.info( "Drop '%s' database.", databaseId.name() );
        Database database = context.database();
        database.drop();
        databaseIdRepository().invalidate( databaseId );
        return null;
    }

    private void requireStarted( DatabaseId databaseId )
    {
        if ( !databaseManagerStarted )
        {
            throw new IllegalStateException( String.format( "The database manager must be started in order to operate on database `%s`", databaseId.name() ) );
        }
    }
}
