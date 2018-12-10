/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.common;

import com.neo4j.causalclustering.catchup.storecopy.StoreFiles;
import com.neo4j.causalclustering.identity.StoreId;

import java.io.File;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.LogProvider;

/**
 * StubLocalDatabase for testing.
 */
public class StubLocalDatabase extends AbstractLocalDatabase
{
    private boolean isEmpty;
    private StoreId storeId;
    private final Monitors monitors;

    /* This constructor is provided so that StubLocalDatabase can still conform to the LocalDatabaseFactory interface */
    StubLocalDatabase( String databaseName, Supplier<DatabaseManager> databaseManagerSupplier, DatabaseLayout databaseLayout, LogFiles ignoredTxLogs,
            StoreFiles ignoredStoreFiles, LogProvider logProvider, BooleanSupplier isAvailable )
    {
        this( databaseName, databaseManagerSupplier, databaseLayout, logProvider, isAvailable, null );
    }

    StubLocalDatabase( String databaseName, Supplier<DatabaseManager> databaseManagerSupplier, DatabaseLayout databaseLayout, LogProvider logProvider,
            BooleanSupplier isAvailable, Monitors monitors )
    {
        super( databaseName, databaseManagerSupplier, databaseLayout, null, null, logProvider, isAvailable );

        ThreadLocalRandom rng = ThreadLocalRandom.current();
        storeId = new StoreId( rng.nextInt(), rng.nextInt(), rng.nextInt(), rng.nextInt() );
        this.monitors = monitors;
    }

    @Override
    public void start0()
    { //no-op
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
        return monitors == null ? super.monitors() : monitors;
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

}
