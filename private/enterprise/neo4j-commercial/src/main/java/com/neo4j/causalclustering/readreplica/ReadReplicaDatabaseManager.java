/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.readreplica;

import com.neo4j.causalclustering.catchup.CatchupComponentsFactory;
import com.neo4j.causalclustering.common.ClusteredDatabaseContext;
import com.neo4j.causalclustering.common.ClusteredMultiDatabaseManager;

import org.neo4j.collection.Dependencies;
import org.neo4j.configuration.Config;
import org.neo4j.dbms.database.DatabaseExistsException;
import org.neo4j.dbms.database.DatabaseManagementException;
import org.neo4j.dbms.database.DatabaseNotFoundException;
import org.neo4j.graphdb.factory.module.GlobalModule;
import org.neo4j.graphdb.factory.module.ModularDatabaseCreationContext;
import org.neo4j.graphdb.factory.module.edition.context.EditionDatabaseComponents;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.database.DatabaseCreationContext;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.monitoring.Health;
import org.neo4j.monitoring.Monitors;

import static java.lang.String.format;

public class ReadReplicaDatabaseManager extends ClusteredMultiDatabaseManager
{
    protected final ReadReplicaEditionModule edition;

    ReadReplicaDatabaseManager( GlobalModule globalModule, ReadReplicaEditionModule edition, Log log,
            CatchupComponentsFactory catchupComponentsFactory, FileSystemAbstraction fs, PageCache pageCache, LogProvider logProvider, Config config,
            Health globalHealths )
    {
        super( globalModule, edition, log, catchupComponentsFactory, fs, pageCache, logProvider, config, globalHealths );
        this.edition = edition;
    }

    @Override
    protected ClusteredDatabaseContext createDatabaseContext( DatabaseId databaseId )
    {
        Monitors readReplicaMonitors = new Monitors( globalModule.getGlobalMonitors() );
        Dependencies readReplicaDependencies = new Dependencies( globalModule.getGlobalDependencies() );

        log.info( "Creating '%s' database.", databaseId.name() );
        DatabaseCreationContext databaseCreationContext = newDatabaseCreationContext( databaseId, readReplicaDependencies, readReplicaMonitors );
        Database kernelDatabase = new Database( databaseCreationContext );

        LogFiles transactionLogs = buildTransactionLogs( kernelDatabase.getDatabaseLayout() );
        LogProvider debugLog = globalModule.getLogService().getInternalLogProvider();
        ReadReplicaDatabaseContext databaseContext = new ReadReplicaDatabaseContext( kernelDatabase, readReplicaMonitors, readReplicaDependencies, storeFiles,
                transactionLogs, debugLog );

        LifeSupport readReplicaLife = edition.readReplicaDatabaseFactory().createDatabase( databaseContext );

        return contextFactory.create( kernelDatabase, kernelDatabase.getDatabaseFacade(), transactionLogs, storeFiles, logProvider, catchupComponentsFactory,
                readReplicaLife, readReplicaMonitors );
    }

    private DatabaseCreationContext newDatabaseCreationContext( DatabaseId databaseId, Dependencies parentDependencies, Monitors parentMonitors )
    {
        EditionDatabaseComponents editionDatabaseComponents = edition.createDatabaseComponents( databaseId );
        GlobalProcedures globalProcedures = edition.getGlobalProcedures();
        return new ModularDatabaseCreationContext( databaseId, globalModule, parentDependencies, parentMonitors, editionDatabaseComponents, globalProcedures );
    }

    @Override
    public ClusteredDatabaseContext createDatabase( DatabaseId databaseId ) throws DatabaseExistsException
    {
        return databaseMap.compute( databaseId, ( key, currentContext ) ->
        {
            if ( currentContext != null )
            {
                throw new DatabaseExistsException( format( "Database with name `%s` already exists.", databaseId.name() ) );
            }
            ClusteredDatabaseContext databaseContext = createDatabaseContext( databaseId );
            if ( started )
            {
                databaseContext.clusterDatabaseLife().start();
            }
            return databaseContext;
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
            currentContext.clusterDatabaseLife().start();
            return currentContext;
        } );
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
            currentContext.clusterDatabaseLife().stop();
            stopDatabase( databaseId, currentContext );
            return currentContext;
        } );
    }

    @Override
    public void dropDatabase( DatabaseId databaseId ) throws DatabaseNotFoundException
    {
        throw new UnsupportedOperationException( "Not implemented" );
//        databaseMap.compute( databaseId, ( key, currentContext ) ->
//        {
//            if ( currentContext == null )
//            {
//                throw new DatabaseNotFoundException( format( "Database with name `%s` not found.", databaseId.name() ) );
//            }
//            dropDatabase( databaseId, currentContext );
//            return null;
//        } );
    }

    @Override
    protected void startDatabase( DatabaseId databaseId, ClusteredDatabaseContext context )
    {
        try
        {
            log.info( "Starting '%s' database.", databaseId.name() );
            context.clusterDatabaseLife().start();
        }
        catch ( Throwable t )
        {
            throw new DatabaseManagementException( format( "Unable to start database %s", databaseId ), t );
        }
    }

    @Override
    protected void stopDatabase( DatabaseId databaseId, ClusteredDatabaseContext context )
    {
        try
        {
            log.info( "Stopping '%s' database.", databaseId.name() );
            context.clusterDatabaseLife().stop();
        }
        catch ( Throwable t )
        {
            throw new DatabaseManagementException( format( "Unable to stop database %s", databaseId ), t );
        }
    }

    @Override
    protected void dropDatabase( DatabaseId databaseId, ClusteredDatabaseContext context )
    {
        throw new UnsupportedOperationException();
    }
}
