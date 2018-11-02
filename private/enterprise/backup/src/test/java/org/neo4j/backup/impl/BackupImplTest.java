/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.backup.impl;

import org.junit.Test;

import java.util.function.Supplier;

import org.neo4j.com.RequestContext;
import org.neo4j.com.storecopy.StoreCopyServer;
import org.neo4j.com.storecopy.StoreWriter;
import org.neo4j.kernel.impl.transaction.log.LogFileInformation;
import org.neo4j.kernel.impl.transaction.log.LogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.storageengine.api.StoreId;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class BackupImplTest
{
    @Test
    public void flushStoreFilesWithCorrectCheckpointTriggerName()
    {
        StoreCopyServer storeCopyServer = mock( StoreCopyServer.class );
        when( storeCopyServer.flushStoresAndStreamStoreFiles( anyString(), any( StoreWriter.class ), anyBoolean() ) )
                .thenReturn( RequestContext.EMPTY );

        BackupImpl backup = new BackupImpl( storeCopyServer, mock( LogicalTransactionStore.class ),
                mock( TransactionIdStore.class ), mock( LogFileInformation.class ), defaultStoreIdSupplier(),
                NullLogProvider.getInstance() );

        backup.fullBackup( mock( StoreWriter.class ), false ).close();

        verify( storeCopyServer ).flushStoresAndStreamStoreFiles(
                eq( BackupImpl.FULL_BACKUP_CHECKPOINT_TRIGGER ), any( StoreWriter.class ), eq( false ) );
    }

    private static Supplier<StoreId> defaultStoreIdSupplier()
    {
        return () -> StoreId.DEFAULT;
    }
}
