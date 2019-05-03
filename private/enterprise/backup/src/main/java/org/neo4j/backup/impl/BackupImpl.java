/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.backup.impl;

import java.util.function.Supplier;

import org.neo4j.backup.TheBackupInterface;
import org.neo4j.com.RequestContext;
import org.neo4j.com.Response;
import org.neo4j.com.storecopy.ResponsePacker;
import org.neo4j.com.storecopy.StoreCopyServer;
import org.neo4j.com.storecopy.StoreWriter;
import org.neo4j.kernel.impl.transaction.log.LogFileInformation;
import org.neo4j.kernel.impl.transaction.log.LogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.Logger;
import org.neo4j.storageengine.api.StoreId;

import static org.neo4j.com.RequestContext.anonymous;

public class BackupImpl implements TheBackupInterface
{
    static final String FULL_BACKUP_CHECKPOINT_TRIGGER = "Full backup";

    private final StoreCopyServer storeCopyServer;
    private final ResponsePacker incrementalResponsePacker;
    private final LogicalTransactionStore logicalTransactionStore;
    private final Supplier<StoreId> storeId;
    private final TransactionIdStore transactionIdStore;
    private final LogFileInformation logFileInformation;
    private final Logger logger;

    public BackupImpl( StoreCopyServer storeCopyServer,
                       LogicalTransactionStore logicalTransactionStore, TransactionIdStore transactionIdStore,
                       LogFileInformation logFileInformation, Supplier<StoreId> storeId, LogProvider logProvider )
    {
        this.storeCopyServer = storeCopyServer;
        this.logicalTransactionStore = logicalTransactionStore;
        this.transactionIdStore = transactionIdStore;
        this.logFileInformation = logFileInformation;
        this.storeId = storeId;
        this.logger = logProvider.getLog( getClass() ).infoLogger();
        this.incrementalResponsePacker = new ResponsePacker( logicalTransactionStore, transactionIdStore, storeId );
    }

    @Override
    public Response<Void> fullBackup( StoreWriter writer, boolean forensics )
    {
        String backupIdentifier = getBackupIdentifier();
        try ( StoreWriter storeWriter = writer )
        {
            logger.log( "%s: Full backup started...", backupIdentifier );
            RequestContext copyStartContext = storeCopyServer.flushStoresAndStreamStoreFiles(
                    FULL_BACKUP_CHECKPOINT_TRIGGER, storeWriter, forensics );
            ResponsePacker responsePacker = new StoreCopyResponsePacker( logicalTransactionStore, transactionIdStore,
                    logFileInformation, storeId, copyStartContext.lastAppliedTransaction() + 1,
                    storeCopyServer.monitor() );
            long optionalTransactionId = copyStartContext.lastAppliedTransaction();
            return responsePacker.packTransactionStreamResponse( anonymous( optionalTransactionId ), null/*no response object*/ );
        }
        finally
        {
            logger.log( "%s: Full backup finished.", backupIdentifier );
        }
    }

    @Override
    public Response<Void> incrementalBackup( RequestContext context )
    {
        String backupIdentifier = getBackupIdentifier();
        try
        {
            logger.log( "%s: Incremental backup started...", backupIdentifier );
            return incrementalResponsePacker.packTransactionStreamResponse( context, null );
        }
        finally
        {
            logger.log( "%s: Incremental backup finished.", backupIdentifier );
        }
    }

    private static String getBackupIdentifier()
    {
        return Thread.currentThread().getName();
    }
}
