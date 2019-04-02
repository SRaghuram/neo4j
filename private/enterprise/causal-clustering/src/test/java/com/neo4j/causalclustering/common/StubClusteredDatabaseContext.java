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
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.BooleanSupplier;

import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
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
    private final BooleanSupplier isAvailable;
    private final CatchupComponentsRepository.DatabaseCatchupComponents catchupComponents;
    private boolean isEmpty;
    private StoreId storeId;
    private final Monitors monitors;

    /* This constructor is provided so that StubClusteredDatabaseContext can still conform to the ClusteredDatabaseContextFactory interface */
    StubClusteredDatabaseContext( Database database, GraphDatabaseFacade facade, LogFiles ignoredTxLogs,
            StoreFiles ignoredStoreFiles, LogProvider logProvider, BooleanSupplier isAvailable, CatchupComponentsFactory catchupComponentsFactory )
    {
        this( database, facade, logProvider, isAvailable, null, catchupComponentsFactory );
    }

    StubClusteredDatabaseContext( Database database, GraphDatabaseFacade facade, LogProvider logProvider, BooleanSupplier isAvailable,
            Monitors monitors, CatchupComponentsFactory catchupComponentsFactory )
    {
        this.database = database;
        this.facade = facade;
        this.logProvider = logProvider;
        this.isAvailable = isAvailable;
        ThreadLocalRandom rng = ThreadLocalRandom.current();
        storeId = new StoreId( rng.nextLong(), rng.nextLong(), rng.nextLong(), rng.nextLong(), rng.nextLong() );
        this.monitors = monitors;
        this.catchupComponents = catchupComponentsFactory.createDatabaseComponents( this );
    }

    @Override
    public void replaceWith( File sourceDir )
    { //no-op
    }

    @Override
    public void delete()
    { //no-op
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
    public String databaseName()
    {
        return database.getDatabaseName();
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
    public CatchupComponentsRepository.DatabaseCatchupComponents catchupComponents()
    {
        return catchupComponents;
    }
}
