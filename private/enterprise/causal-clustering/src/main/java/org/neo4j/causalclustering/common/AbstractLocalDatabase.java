/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.common;

import java.io.File;
import java.io.IOException;
import java.util.Set;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

import org.neo4j.causalclustering.catchup.storecopy.StoreFiles;
import org.neo4j.causalclustering.identity.StoreId;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.util.Dependencies;
import org.neo4j.kernel.lifecycle.SafeLifecycle;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static java.lang.String.format;

public abstract class AbstractLocalDatabase extends SafeLifecycle implements LocalDatabase
{
    private final DatabaseLayout databaseLayout;
    private final StoreFiles storeFiles;
    private final Log log;
    private final Supplier<DatabaseManager> databaseManagerSupplier;
    private final String databaseName;
    private final BooleanSupplier isAvailable;
    private final LogFiles txLogs;

    private volatile StoreId storeId;

    public AbstractLocalDatabase( String databaseName, Supplier<DatabaseManager> databaseManagerSupplier, DatabaseLayout databaseLayout, LogFiles txLogs,
            StoreFiles storeFiles, LogProvider logProvider, BooleanSupplier isAvailable )
    {
        this.databaseLayout = databaseLayout;
        this.storeFiles = storeFiles;
        this.txLogs = txLogs;
        this.databaseManagerSupplier = databaseManagerSupplier;
        this.databaseName = databaseName;
        this.isAvailable = isAvailable;
        this.log = logProvider.getLog( getClass() );
    }

    public void init0()
    {
        if ( isAvailable.getAsBoolean() )
        {
            return;
        }
        storeId = storeId();
        log.info( "Initialising LocalDatabase with storeId: " + storeId );
    }

    @Override
    public abstract void start0();

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

    @Override
    public void delete() throws IOException
    {
        storeFiles.delete( databaseLayout.databaseDirectory(), txLogs );
    }

    @Override
    public boolean isEmpty() throws IOException
    {
        Set<File> filesToLookFor = databaseLayout.storeFiles();
        return storeFiles.isEmpty( databaseLayout.databaseDirectory(), filesToLookFor );
    }

    @Override
    public DatabaseLayout databaseLayout()
    {
        return databaseLayout;
    }

    @Override
    public void replaceWith( File sourceDir ) throws IOException
    {
        storeFiles.delete( databaseLayout.databaseDirectory(), txLogs );
        storeFiles.moveTo( sourceDir, databaseLayout.databaseDirectory(), txLogs );
    }

    @Override
    public Database database()
    {
        return databaseManagerSupplier
                .get()
                .getDatabaseContext( databaseName )
                .orElseThrow( () -> new IllegalStateException( format( "No database with name '%s' registered", databaseName ) ) )
                .getDatabase();
    }

    @Override
    public String databaseName()
    {
        return databaseName;
    }
}
