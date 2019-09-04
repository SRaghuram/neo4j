/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.common;

import com.neo4j.causalclustering.catchup.CatchupComponentsFactory;
import com.neo4j.causalclustering.catchup.CatchupComponentsRepository.CatchupComponents;
import com.neo4j.causalclustering.catchup.storecopy.StoreFiles;

import java.io.File;
import java.io.IOException;

import org.neo4j.collection.Dependencies;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.monitoring.Monitors;
import org.neo4j.storageengine.api.StoreId;

/**
 * Instances extending this class represent individual clustered databases in Neo4j.
 *
 * Instances are responsible for exposing per database dependency management, monitoring and io operations.
 *
 * Collections of these instances should be managed by a {@link DatabaseManager<ClusteredDatabaseContext>}
 */
public class DefaultClusteredDatabaseContext implements ClusteredDatabaseContext
{
    private final DatabaseLayout databaseLayout;
    private final StoreFiles storeFiles;
    private final Log log;
    private final DatabaseId databaseId;
    private final LogFiles txLogs;
    private final Database database;
    private final GraphDatabaseFacade facade;
    private volatile Throwable failureCause;
    private final CatchupComponents catchupComponents;
    private final ClusteredDatabaseLife clusterDatabaseLife;
    private final Monitors clusterDatabaseMonitors;

    private volatile StoreId storeId;

    DefaultClusteredDatabaseContext( Database database, GraphDatabaseFacade facade, LogFiles txLogs, StoreFiles storeFiles, LogProvider logProvider,
            CatchupComponentsFactory catchupComponentsFactory, ClusteredDatabaseLife clusterDatabaseLife, Monitors clusterDatabaseMonitors )
    {
        this.database = database;
        this.facade = facade;
        this.databaseLayout = database.getDatabaseLayout();
        this.storeFiles = storeFiles;
        this.txLogs = txLogs;
        this.databaseId = database.getDatabaseId();
        this.log = logProvider.getLog( getClass() );
        this.clusterDatabaseLife = clusterDatabaseLife;
        this.clusterDatabaseMonitors = clusterDatabaseMonitors;
        this.catchupComponents = catchupComponentsFactory.createDatabaseComponents( this );
    }

    /**
     * Reads metadata about this database from disk and calculates a uniquely {@link StoreId}.
     * The store id should be cached so that future calls do not require IO.
     * @return store id for this database
     */
    @Override
    public StoreId storeId()
    {
        if ( storeId == null )
        {
            storeId = readStoreIdFromDisk();
        }
        return storeId;
    }

    private StoreId readStoreIdFromDisk()
    {
        try
        {
            return storeFiles.readStoreId( databaseLayout );
        }
        catch ( IOException e )
        {
            log.error( "Failure reading store id", e );
            return null;
        }
    }

    /**
     * Returns per-database {@link Monitors}
     * @return monitors for this database
     */
    @Override
    public Monitors monitors()
    {
        return clusterDatabaseMonitors;
    }

    @Override
    public Dependencies dependencies()
    {
        return database().getDependencyResolver();
    }

    /**
     * Delete the store files for this database
     */
    @Override
    public void delete() throws IOException
    {
        storeFiles.delete( databaseLayout, txLogs );
    }

    @Override
    public void fail( Throwable failureCause )
    {
        this.failureCause = failureCause;
    }

    @Override
    public Throwable failureCause()
    {
        return failureCause;
    }

    @Override
    public boolean isFailed()
    {
        return failureCause != null;
    }

    /**
     * @return Whether or not the store files for this database are empty/non-existent.
     */
    @Override
    public boolean isEmpty()
    {
        return storeFiles.isEmpty( databaseLayout );
    }

    /**
     * @return A listing of all store files which comprise this database
     */
    @Override
    public DatabaseLayout databaseLayout()
    {
        return databaseLayout;
    }

    /**
     * Replace the store files for this database
     * @param sourceDir the store files to replace this databases's current files with
     */
    @Override
    public void replaceWith( File sourceDir ) throws IOException
    {
        storeFiles.delete( databaseLayout, txLogs );
        storeFiles.moveTo( sourceDir, databaseLayout, txLogs );
    }

    @Override
    public Database database()
    {
        return database;
    }

    @Override
    public GraphDatabaseFacade databaseFacade()
    {
        return facade;
    }

    /**
     * @return the name of this database
     */
    @Override
    public DatabaseId databaseId()
    {
        return databaseId;
    }

    @Override
    public CatchupComponents catchupComponents()
    {
        return catchupComponents;
    }

    @Override
    public ClusteredDatabaseLife clusteredDatabaseLife()
    {
        return clusterDatabaseLife;
    }
}
