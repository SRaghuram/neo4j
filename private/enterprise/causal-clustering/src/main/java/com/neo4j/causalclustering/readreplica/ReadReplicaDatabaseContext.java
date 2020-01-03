/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.readreplica;

import com.neo4j.causalclustering.catchup.storecopy.StoreFiles;
import com.neo4j.dbms.ClusterInternalDbmsOperator;

import java.io.IOException;

import org.neo4j.collection.Dependencies;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.logging.Log;
import org.neo4j.monitoring.Monitors;
import org.neo4j.storageengine.api.StoreId;

public class ReadReplicaDatabaseContext
{
    private final Database kernelDatabase;
    private final Monitors monitors;
    private final Dependencies dependencies;
    private final StoreFiles storeFiles;
    private final LogFiles transactionLogs;
    private final Log log;
    private final ClusterInternalDbmsOperator internalOperator;

    ReadReplicaDatabaseContext( Database kernelDatabase, Monitors monitors, Dependencies dependencies, StoreFiles storeFiles, LogFiles transactionLogs,
            ClusterInternalDbmsOperator internalOperator )
    {
        this.kernelDatabase = kernelDatabase;
        this.monitors = monitors;
        this.dependencies = dependencies;
        this.storeFiles = storeFiles;
        this.transactionLogs = transactionLogs;
        this.log = kernelDatabase.getInternalLogProvider().getLog( getClass() );
        this.internalOperator = internalOperator;
    }

    public NamedDatabaseId databaseId()
    {
        return kernelDatabase.getNamedDatabaseId();
    }

    public StoreId storeId()
    {
        return readStoreIdFromDisk();
    }

    private StoreId readStoreIdFromDisk()
    {
        try
        {
            return storeFiles.readStoreId( kernelDatabase.getDatabaseLayout() );
        }
        catch ( IOException e )
        {
            log.warn( "Failure reading store id", e );
            return null;
        }
    }

    ClusterInternalDbmsOperator.StoreCopyHandle stopForStoreCopy()
    {
        return internalOperator.stopForStoreCopy( kernelDatabase.getNamedDatabaseId() );
    }

    public Database database()
    {
        return kernelDatabase;
    }

    public Monitors monitors()
    {
        return monitors;
    }

    public Dependencies dependencies()
    {
        return dependencies;
    }

    public boolean isEmpty()
    {
        return storeFiles.isEmpty( kernelDatabase.getDatabaseLayout() );
    }

    public void delete() throws IOException
    {
        storeFiles.delete( kernelDatabase.getDatabaseLayout(), transactionLogs );
    }
}
