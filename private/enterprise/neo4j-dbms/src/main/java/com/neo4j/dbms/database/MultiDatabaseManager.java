/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms.database;

import java.util.Optional;
import java.util.function.BiConsumer;

import org.neo4j.dbms.api.DatabaseExistsException;
import org.neo4j.dbms.api.DatabaseLimitReachedException;
import org.neo4j.dbms.api.DatabaseManagementException;
import org.neo4j.dbms.api.DatabaseNotFoundException;
import org.neo4j.dbms.database.AbstractDatabaseManager;
import org.neo4j.dbms.database.DatabaseContext;
import org.neo4j.dbms.database.DatabaseOperationCounts;
import org.neo4j.graphdb.factory.module.GlobalModule;
import org.neo4j.graphdb.factory.module.edition.AbstractEditionModule;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.database.NamedDatabaseId;

import static com.neo4j.kernel.impl.enterprise.configuration.EnterpriseEditionSettings.maxNumberOfDatabases;
import static java.lang.String.format;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;

public abstract class MultiDatabaseManager<DB extends DatabaseContext> extends AbstractDatabaseManager<DB>
{
    private final long maximumNumberOfDatabases;
    private volatile boolean databaseManagerStarted;
    private DatabaseOperationCounts.Counter operationCounts;
    private final RuntimeDatabaseDumper runtimeDatabaseDumper;

    public MultiDatabaseManager( GlobalModule globalModule, AbstractEditionModule edition )
    {
        this( globalModule, edition, false );
    }

    MultiDatabaseManager( GlobalModule globalModule, AbstractEditionModule edition, boolean manageDatabasesOnStartAndStop )
    {
        super( globalModule, edition, manageDatabasesOnStartAndStop );
        operationCounts = globalModule.getGlobalDependencies().resolveDependency( DatabaseOperationCounts.Counter.class );
        maximumNumberOfDatabases = globalModule.getGlobalConfig().get( maxNumberOfDatabases );
        runtimeDatabaseDumper = new RuntimeDatabaseDumper( globalModule.getGlobalClock(), globalModule.getGlobalConfig(), globalModule.getFileSystem() );
    }

    public void validateDatabaseCreation( NamedDatabaseId namedDatabaseId ) throws DatabaseManagementException
    {
        if ( databaseMap.get( namedDatabaseId ) != null )
        {
            throw new DatabaseExistsException( format( "Database with name `%s` already exists.", namedDatabaseId.name() ) );
        }
        if ( databaseMap.size() >= maximumNumberOfDatabases )
        {
            throw new DatabaseLimitReachedException(
                    format( "Reached maximum number of active databases. Unable to create new database `%s`.", namedDatabaseId.name() ) );
        }
    }

    @Override
    public DB createDatabase( NamedDatabaseId namedDatabaseId ) throws DatabaseManagementException
    {
        DB databaseContext;
        try
        {
            log.info( "Creating '%s' database.", namedDatabaseId.name() );
            databaseContext = createDatabaseContext( namedDatabaseId );
        }
        catch ( Exception e )
        {
            throw new DatabaseManagementException(
                    format( "An error occurred! Unable to create new database `%s`.", namedDatabaseId.name() ), e );
        }

        databaseMap.put( namedDatabaseId, databaseContext );
        databaseIdRepository().cache( namedDatabaseId );
        operationCounts.increaseCreateCount();

        return databaseContext;
    }

    @Override
    public Optional<DB> getDatabaseContext( NamedDatabaseId namedDatabaseId )
    {
        return Optional.ofNullable( databaseMap.get( namedDatabaseId ) );
    }

    @Override
    public void dropDatabase( NamedDatabaseId namedDatabaseId ) throws DatabaseNotFoundException
    {
        dropDatabase( namedDatabaseId, false );
    }

    public void dropDatabaseDumpData( NamedDatabaseId namedDatabaseId ) throws DatabaseNotFoundException
    {
        dropDatabase( namedDatabaseId, true );
    }

    private void dropDatabase( NamedDatabaseId namedDatabaseId, boolean dumpData ) throws DatabaseNotFoundException
    {
        if ( SYSTEM_DATABASE_NAME.equals( namedDatabaseId.name() ) )
        {
            throw new DatabaseManagementException( "System database can't be dropped." );
        }
        forSingleDatabase( namedDatabaseId, ( id, context ) -> dropDatabase( id, context, dumpData ) );
        operationCounts.increaseDropCount();
    }

    @Override
    public void stopDatabase( NamedDatabaseId namedDatabaseId ) throws DatabaseNotFoundException
    {
        forSingleDatabase( namedDatabaseId, this::stopDatabase );
        operationCounts.increaseStopCount();
    }

    @Override
    public void startDatabase( NamedDatabaseId namedDatabaseId ) throws DatabaseNotFoundException
    {
        forSingleDatabase( namedDatabaseId, this::startDatabase );
        operationCounts.increaseStartCount();
    }

    protected final void forSingleDatabase( NamedDatabaseId namedDatabaseId, BiConsumer<NamedDatabaseId,DB> consumer )
    {
        requireStarted( namedDatabaseId );

        DB ctx = databaseMap.get( namedDatabaseId );

        if ( ctx == null )
        {
            throw new DatabaseNotFoundException( format( "Database with name `%s` not found.", namedDatabaseId.name() ) );
        }

        consumer.accept( namedDatabaseId, ctx );
    }

    @Override
    public void start() throws Exception
    {
        if ( !databaseManagerStarted )
        {
            databaseManagerStarted = true;
            super.start();
            runtimeDatabaseDumper.start();
        }
    }

    @Override
    public void stop() throws Exception
    {
        if ( databaseManagerStarted )
        {
            super.stop();
            databaseManagerStarted = false;
            runtimeDatabaseDumper.stop();
        }
    }

    @Override
    public final void shutdown()
    {
        databaseMap.clear();
    }

    protected RuntimeDatabaseDumper dropDumpJob()
    {
        return runtimeDatabaseDumper;
    }

    protected void dropDatabase( NamedDatabaseId namedDatabaseId, DB context, boolean dumpData )
    {
        try
        {
            log.info( "Drop '%s' database.", namedDatabaseId.name() );
            if ( dumpData )
            {
                dropDumpJob().dump( context );
            }
            Database database = context.database();
            database.drop();
            databaseIdRepository().invalidate( namedDatabaseId );
            databaseMap.remove( namedDatabaseId );
        }
        catch ( Throwable t )
        {
            throw new DatabaseManagementException( format( "An error occurred! Unable to drop database with name `%s`.", namedDatabaseId.name() ), t );
        }
    }

    private void requireStarted( NamedDatabaseId namedDatabaseId )
    {
        if ( !databaseManagerStarted )
        {
            throw new IllegalStateException( format( "The database manager must be started in order to operate on database `%s`", namedDatabaseId.name() ) );
        }
    }
}
