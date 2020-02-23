/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.internal.freki;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.configuration.Config;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.internal.id.IdController;
import org.neo4j.internal.id.IdGeneratorFactory;
import org.neo4j.internal.metadatastore.GBPTreeMetaDataStore;
import org.neo4j.internal.schema.IndexConfigCompleter;
import org.neo4j.internal.schema.SchemaState;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.DefaultPageCursorTracerSupplier;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.lock.LockService;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.internal.LogService;
import org.neo4j.monitoring.DatabaseHealth;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.CommandReaderFactory;
import org.neo4j.storageengine.api.ConstraintRuleAccessor;
import org.neo4j.storageengine.api.LogVersionRepository;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.storageengine.api.StorageFilesState;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.storageengine.api.StoreVersion;
import org.neo4j.storageengine.api.StoreVersionCheck;
import org.neo4j.storageengine.api.TransactionIdStore;
import org.neo4j.storageengine.api.TransactionMetaDataStore;
import org.neo4j.storageengine.migration.SchemaRuleMigrationAccess;
import org.neo4j.storageengine.migration.StoreMigrationParticipant;
import org.neo4j.token.TokenHolders;

/**
 * The little notebook of goals:
 * - Main store, one-size single records
 * - Add multi-size records
 * - Add record chains
 */
@ServiceProvider
public class FrekiStorageEngineFactory implements StorageEngineFactory
{
    @Override
    public StoreVersionCheck versionCheck( FileSystemAbstraction fs, DatabaseLayout databaseLayout, Config config, PageCache pageCache, LogService logService )
    {
        return new FrekiStoreVersionCheck();
    }

    @Override
    public StoreVersion versionInformation( String storeVersion )
    {
        return new FrekiStoreVersion();
    }

    @Override
    public List<StoreMigrationParticipant> migrationParticipants( FileSystemAbstraction fs, Config config, PageCache pageCache, JobScheduler jobScheduler,
            LogService logService, PageCacheTracer pageCacheTracer )
    {
        return Collections.emptyList();
    }

    @Override
    public StorageEngine instantiate( FileSystemAbstraction fs, DatabaseLayout databaseLayout, Config config, PageCache pageCache, TokenHolders tokenHolders,
            SchemaState schemaState, ConstraintRuleAccessor constraintSemantics, IndexConfigCompleter indexConfigCompleter, LockService lockService,
            IdGeneratorFactory idGeneratorFactory, IdController idController, DatabaseHealth databaseHealth, LogProvider logProvider,
            RecoveryCleanupWorkCollector recoveryCleanupWorkCollector, PageCacheTracer pageCacheTracer, boolean createStoreIfNotExists ) throws IOException
    {
        return new FrekiStorageEngine( fs, databaseLayout, config, pageCache, tokenHolders, schemaState, constraintSemantics, indexConfigCompleter, lockService,
                idGeneratorFactory, idController, databaseHealth, logProvider, recoveryCleanupWorkCollector, createStoreIfNotExists, pageCacheTracer,
                DefaultPageCursorTracerSupplier.TRACER_SUPPLIER );
    }

    @Override
    public List<File> listStorageFiles( FileSystemAbstraction fileSystem, DatabaseLayout databaseLayout ) throws IOException
    {
        return null;
    }

    @Override
    public boolean storageExists( FileSystemAbstraction fileSystem, DatabaseLayout databaseLayout, PageCache pageCache )
    {
        return fileSystem.fileExists( databaseLayout.file( Stores.META_DATA_STORE_FILENAME ) );
    }

    @Override
    public TransactionIdStore readOnlyTransactionIdStore( FileSystemAbstraction filySystem, DatabaseLayout databaseLayout, PageCache pageCache )
            throws IOException
    {
        return null;
    }

    @Override
    public LogVersionRepository readOnlyLogVersionRepository( DatabaseLayout databaseLayout, PageCache pageCache ) throws IOException
    {
        return null;
    }

    @Override
    public TransactionMetaDataStore transactionMetaDataStore( FileSystemAbstraction fs, DatabaseLayout databaseLayout, Config config, PageCache pageCache,
            PageCacheTracer pageCacheTracer )
            throws IOException
    {
        return null;
    }

    @Override
    public StoreId storeId( DatabaseLayout databaseLayout, PageCache pageCache ) throws IOException
    {
        try ( GBPTreeMetaDataStore metaDataStore = openMetaDataStore( databaseLayout, pageCache, PageCacheTracer.NULL, PageCursorTracer.NULL ) )
        {
            return metaDataStore.getStoreId();
        }
    }

    @Override
    public SchemaRuleMigrationAccess schemaRuleMigrationAccess( FileSystemAbstraction fs, PageCache pageCache, Config config, DatabaseLayout databaseLayout,
            LogService logService, String recordFormats, PageCacheTracer cacheTracer )
    {
        return null;
    }

    @Override
    public StorageFilesState checkRecoveryRequired( FileSystemAbstraction fs, DatabaseLayout databaseLayout, PageCache pageCache )
    {
        return StorageFilesState.recoveredState();
    }

    @Override
    public CommandReaderFactory commandReaderFactory()
    {
        return FrekiCommandReaderFactory.INSTANCE;
    }

    static GBPTreeMetaDataStore openMetaDataStore( DatabaseLayout databaseLayout, PageCache pageCache, PageCacheTracer pageCacheTracer,
            PageCursorTracer cursorTracer )
    {
        return new GBPTreeMetaDataStore( pageCache, databaseLayout.file( Stores.META_DATA_STORE_FILENAME ), 123456789, false, pageCacheTracer,
                cursorTracer );
    }
}
