/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state;

import com.neo4j.causalclustering.catchup.storecopy.StoreFiles;

import java.io.IOException;
import java.nio.file.Path;

import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;

public class BootstrapContext
{
    private final NamedDatabaseId namedDatabaseId;
    private final DatabaseLayout databaseLayout;
    private final StoreFiles storeFiles;
    private final LogFiles transactionLogs;

    public BootstrapContext( NamedDatabaseId namedDatabaseId, DatabaseLayout databaseLayout, StoreFiles storeFiles, LogFiles transactionLogs )
    {
        this.namedDatabaseId = namedDatabaseId;
        this.databaseLayout = databaseLayout;
        this.storeFiles = storeFiles;
        this.transactionLogs = transactionLogs;
    }

    NamedDatabaseId databaseId()
    {
        return namedDatabaseId;
    }

    DatabaseLayout databaseLayout()
    {
        return databaseLayout;
    }

    void replaceWith( Path sourceDir ) throws IOException
    {
        storeFiles.delete( databaseLayout, transactionLogs );
        storeFiles.moveTo( sourceDir, databaseLayout, transactionLogs );
    }

    void removeTransactionLogs() throws IOException
    {
        storeFiles.delete( transactionLogs );
    }
}
