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
import java.util.concurrent.ThreadLocalRandom;

import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.LogProvider;
import org.neo4j.monitoring.Monitors;
import org.neo4j.storageengine.api.StoreId;

/**
 * StubClusteredDatabaseContext for testing.
 */
public class StubClusteredDatabaseContext extends LifecycleAdapter implements ClusteredDatabaseContext
{
    private final Database database;
    private final GraphDatabaseFacade facade;
    private final LogProvider logProvider;
    private final CatchupComponentsRepository.CatchupComponents catchupComponents;
    private boolean isEmpty;
    private StoreId storeId;
    private final Monitors monitors;
    private final StoreFiles storeFiles;
    private final LogFiles logFiles;
    private final Throwable failure;

    StubClusteredDatabaseContext( Database database, GraphDatabaseFacade facade, LogFiles logFiles,
            StoreFiles storeFiles, LogProvider logProvider, CatchupComponentsFactory catchupComponentsFactory, Throwable failure )
    {
        this.database = database;
        this.facade = facade;
        this.logProvider = logProvider;
        this.storeFiles = storeFiles;
        this.logFiles = logFiles;
        ThreadLocalRandom rng = ThreadLocalRandom.current();
        storeId = new StoreId( rng.nextLong(), rng.nextLong(), rng.nextLong(), rng.nextLong(), rng.nextLong() );
        this.monitors = new Monitors();
        this.catchupComponents = catchupComponentsFactory.createDatabaseComponents( this );
        this.failure = failure;
    }

    @Override
    public void replaceWith( File sourceDir ) throws IOException
    {
        storeFiles.delete( database.getDatabaseLayout(), logFiles );
        storeFiles.moveTo( sourceDir, database.getDatabaseLayout(), logFiles );
    }

    @Override
    public void delete() throws IOException
    {
        storeFiles.delete( database.getDatabaseLayout(), logFiles );
    }

    @Override
    public Monitors monitors()
    {
        return monitors;
    }

    @Override
    public boolean isEmpty()
    {
        return isEmpty;
    }

    public void setEmpty( boolean isEmpty )
    {
        this.isEmpty = isEmpty;
    }

    @Override
    public StoreId storeId()
    {
        return storeId;
    }

    public void setStoreId( StoreId storeId )
    {
        this.storeId = storeId;
    }

    @Override
    public DatabaseLayout databaseLayout()
    {
        return database.getDatabaseLayout();
    }

    @Override
    public DatabaseId databaseId()
    {
        return database.getDatabaseId();
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

    @Override
    public void fail( Throwable t )
    {
    }

    @Override
    public boolean isFailed()
    {
        return failure != null;
    }

    @Override
    public Throwable failureCause()
    {
        return failure;
    }

    @Override
    public CatchupComponentsRepository.CatchupComponents catchupComponents()
    {
        return catchupComponents;
    }

    @Override
    public ClusteredDatabaseLife clusteredDatabaseLife()
    {
        return null;
    }
}
