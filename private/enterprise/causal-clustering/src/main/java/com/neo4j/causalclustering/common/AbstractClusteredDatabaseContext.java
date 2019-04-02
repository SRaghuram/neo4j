/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.common;

import com.neo4j.causalclustering.catchup.CatchupComponentsFactory;
import com.neo4j.causalclustering.catchup.CatchupComponentsRepository;
import com.neo4j.causalclustering.catchup.storecopy.StoreFiles;

import java.io.File;
import java.io.IOException;
import java.util.function.BooleanSupplier;

import org.neo4j.collection.Dependencies;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.monitoring.Monitors;
import org.neo4j.storageengine.api.StoreId;

/**
 * Instances extending this class represent individual clustered databases in Neo4j.
 *
 * Instances are responsible for exposing per database dependency management, monitoring and io operations.
 *
 * Collections of these instances should be managed by a {@link ClusteredDatabaseManager}
 */
public abstract class AbstractClusteredDatabaseContext extends LifecycleAdapter implements ClusteredDatabaseContext
{
    private final DatabaseLayout databaseLayout;
    private final StoreFiles storeFiles;
    private final Log log;
    private final String databaseName;
    private final BooleanSupplier isAvailable;
    private final LogFiles txLogs;
    private final Database database;
    private final GraphDatabaseFacade facade;
    private final CatchupComponentsRepository.DatabaseCatchupComponents catchupComponents;

    private volatile StoreId storeId;

    public AbstractClusteredDatabaseContext( Database database, GraphDatabaseFacade facade, LogFiles txLogs, StoreFiles storeFiles, LogProvider logProvider,
            BooleanSupplier isAvailable, CatchupComponentsFactory catchupComponentsFactory )
    {
        this.database = database;
        this.facade = facade;
        this.databaseLayout = database.getDatabaseLayout();
        this.storeFiles = storeFiles;
        this.txLogs = txLogs;
        this.databaseName = database.getDatabaseName();
        this.isAvailable = isAvailable;
        this.log = logProvider.getLog( getClass() );
        this.catchupComponents = catchupComponentsFactory.createDatabaseComponents( this );
    }

    @Override
    public final void start() throws Exception
    {
        if ( isAvailable.getAsBoolean() )
        {
            return;
        }
        storeId = storeId();
        log.info( "Initialising with storeId: " + storeId );
        start0();
    }

    protected abstract void start0() throws Exception;

    @Override
    public final void stop() throws Exception
    {
        stop0();
    }

    protected abstract void stop0() throws Exception;

    /**
     * Reads metadata about this database from disk and calculates a uniquely {@link StoreId}.
     * The store id should be cached so that future calls do not require IO.
     * @return store id for this database
     */
    @Override
    public synchronized StoreId storeId()
    {
        if ( isAvailable.getAsBoolean() )
        {
            return storeId;
        }
        else
        {
            return readStoreIdFromDisk();
        }
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
        return database().getMonitors();
    }

    @Override
    public Dependencies dependencies()
    {
        return database().getDependencyResolver();
    }

    /**
     * Delete the store files for this database
     * @throws IOException
     */
    @Override
    public void delete() throws IOException
    {
        storeFiles.delete( databaseLayout, txLogs );
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
     * @throws IOException
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
    public String databaseName()
    {
        return databaseName;
    }

    @Override
    public CatchupComponentsRepository.DatabaseCatchupComponents catchupComponents()
    {
        return catchupComponents;
    }
}
