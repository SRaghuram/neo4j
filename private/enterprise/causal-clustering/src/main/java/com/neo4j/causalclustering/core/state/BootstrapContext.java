/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state;

import com.neo4j.causalclustering.catchup.storecopy.StoreFiles;

import java.io.File;
import java.io.IOException;

import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;

public class BootstrapContext
{
    private final DatabaseId databaseId;
    private final DatabaseLayout databaseLayout;
    private final StoreFiles storeFiles;
    private final LogFiles transactionLogs;

    public BootstrapContext( DatabaseId databaseId, DatabaseLayout databaseLayout, StoreFiles storeFiles, LogFiles transactionLogs )
    {
        this.databaseId = databaseId;
        this.databaseLayout = databaseLayout;
        this.storeFiles = storeFiles;
        this.transactionLogs = transactionLogs;
    }

    DatabaseId databaseId()
    {
        return databaseId;
    }

    DatabaseLayout databaseLayout()
    {
        return databaseLayout;
    }

    void replaceWith( File sourceDir ) throws IOException
    {
        storeFiles.delete( databaseLayout, transactionLogs );
        storeFiles.moveTo( sourceDir, databaseLayout, transactionLogs );
    }

    void removeTransactionLogs()
    {
        storeFiles.delete( transactionLogs );
    }
}
