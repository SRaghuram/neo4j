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
<<<<<<< HEAD
=======
import java.util.concurrent.atomic.AtomicReference;
>>>>>>> f26a3005d9b9a7f42b480941eb059582c7469aaa

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.configuration.Config;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
<<<<<<< HEAD
=======
import org.neo4j.internal.id.DefaultIdController;
import org.neo4j.internal.id.DefaultIdGeneratorFactory;
>>>>>>> f26a3005d9b9a7f42b480941eb059582c7469aaa
import org.neo4j.internal.id.IdController;
import org.neo4j.internal.id.IdGeneratorFactory;
import org.neo4j.internal.metadatastore.GBPTreeMetaDataStore;
import org.neo4j.internal.schema.IndexConfigCompleter;
import org.neo4j.internal.schema.SchemaState;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
<<<<<<< HEAD
import org.neo4j.lock.LockService;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.internal.LogService;
=======
import org.neo4j.kernel.impl.api.DatabaseSchemaState;
import org.neo4j.lock.LockService;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.internal.LogService;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.memory.MemoryTracker;
>>>>>>> f26a3005d9b9a7f42b480941eb059582c7469aaa
import org.neo4j.monitoring.DatabaseHealth;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.CommandReaderFactory;
import org.neo4j.storageengine.api.ConstraintRuleAccessor;
import org.neo4j.storageengine.api.LogVersionRepository;
<<<<<<< HEAD
=======
import org.neo4j.storageengine.api.StandardConstraintRuleAccessor;
>>>>>>> f26a3005d9b9a7f42b480941eb059582c7469aaa
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.storageengine.api.StorageFilesState;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.storageengine.api.StoreVersion;
import org.neo4j.storageengine.api.StoreVersionCheck;
import org.neo4j.storageengine.api.TransactionIdStore;
import org.neo4j.storageengine.api.TransactionMetaDataStore;
import org.neo4j.storageengine.migration.SchemaRuleMigrationAccess;
import org.neo4j.storageengine.migration.StoreMigrationParticipant;
<<<<<<< HEAD
import org.neo4j.token.TokenHolders;

=======
import org.neo4j.token.DelegatingTokenHolder;
import org.neo4j.token.TokenHolders;

import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.immediate;
import static org.neo4j.internal.freki.CursorAccessPatternTracer.NO_TRACING;
import static org.neo4j.io.pagecache.tracing.PageCacheTracer.NULL;
import static org.neo4j.lock.LockService.NO_LOCK_SERVICE;
import static org.neo4j.token.api.TokenHolder.TYPE_LABEL;
import static org.neo4j.token.api.TokenHolder.TYPE_PROPERTY_KEY;
import static org.neo4j.token.api.TokenHolder.TYPE_RELATIONSHIP_TYPE;

>>>>>>> f26a3005d9b9a7f42b480941eb059582c7469aaa
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
    public StoreVersionCheck versionCheck( FileSystemAbstraction fs, DatabaseLayout databaseLayout, Config config, PageCache pageCache, LogService logService,
            PageCacheTracer pageCacheTracer )
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
<<<<<<< HEAD
            LogService logService, PageCacheTracer pageCacheTracer )
=======
            LogService logService, PageCacheTracer pageCacheTracer, MemoryTracker memoryTracker )
>>>>>>> f26a3005d9b9a7f42b480941eb059582c7469aaa
    {
        return Collections.emptyList();
    }

    @Override
    public FrekiStorageEngine instantiate( FileSystemAbstraction fs, DatabaseLayout databaseLayout, Config config, PageCache pageCache,
            TokenHolders tokenHolders, SchemaState schemaState, ConstraintRuleAccessor constraintSemantics, IndexConfigCompleter indexConfigCompleter,
            LockService lockService, IdGeneratorFactory idGeneratorFactory, IdController idController, DatabaseHealth databaseHealth, LogProvider logProvider,
<<<<<<< HEAD
            RecoveryCleanupWorkCollector recoveryCleanupWorkCollector, PageCacheTracer pageCacheTracer, boolean createStoreIfNotExists ) throws IOException
    {
        return new FrekiStorageEngine( fs, databaseLayout, config, pageCache, tokenHolders, schemaState, constraintSemantics, indexConfigCompleter, lockService,
                idGeneratorFactory, idController, databaseHealth, logProvider, recoveryCleanupWorkCollector, createStoreIfNotExists, pageCacheTracer,
                CursorAccessPatternTracer.decidedByFeatureToggle() );
=======
            RecoveryCleanupWorkCollector recoveryCleanupWorkCollector, PageCacheTracer pageCacheTracer, boolean createStoreIfNotExists,
            MemoryTracker memoryTracker ) throws IOException
    {
        return new FrekiStorageEngine( fs, databaseLayout, config, pageCache, tokenHolders, schemaState, constraintSemantics, indexConfigCompleter, lockService,
                idGeneratorFactory, idController, databaseHealth, logProvider, recoveryCleanupWorkCollector, createStoreIfNotExists, pageCacheTracer,
                CursorAccessPatternTracer.decidedByFeatureToggle(), memoryTracker );
>>>>>>> f26a3005d9b9a7f42b480941eb059582c7469aaa
    }

    @Override
    public List<File> listStorageFiles( FileSystemAbstraction fileSystem, DatabaseLayout databaseLayout ) throws IOException
    {
        return List.of( fileSystem.listFiles( databaseLayout.databaseDirectory() ) );
    }

    @Override
    public boolean storageExists( FileSystemAbstraction fileSystem, DatabaseLayout databaseLayout, PageCache pageCache )
    {
        return fileSystem.fileExists( databaseLayout.file( Stores.META_DATA_STORE_FILENAME ) );
    }

    @Override
    public TransactionIdStore readOnlyTransactionIdStore( FileSystemAbstraction filySystem, DatabaseLayout databaseLayout, PageCache pageCache,
            PageCursorTracer cursorTracer ) throws IOException
    {
        return null;
    }

    @Override
    public LogVersionRepository readOnlyLogVersionRepository( DatabaseLayout databaseLayout, PageCache pageCache, PageCursorTracer cursorTracer )
            throws IOException
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
    public StoreId storeId( DatabaseLayout databaseLayout, PageCache pageCache, PageCursorTracer cursorTracer ) throws IOException
    {
        // TODO PageCacheTracer should be passed in to this method
        try ( GBPTreeMetaDataStore metaDataStore = openMetaDataStore( databaseLayout, pageCache, PageCacheTracer.NULL ) )
        {
            return metaDataStore.getStoreId();
        }
    }

    @Override
    public SchemaRuleMigrationAccess schemaRuleMigrationAccess( FileSystemAbstraction fs, PageCache pageCache, Config config, DatabaseLayout databaseLayout,
<<<<<<< HEAD
            LogService logService, String recordFormats, PageCacheTracer cacheTracer, PageCursorTracer cursorTracer )
=======
            LogService logService, String recordFormats, PageCacheTracer cacheTracer, PageCursorTracer cursorTracer, MemoryTracker memoryTracker )
>>>>>>> f26a3005d9b9a7f42b480941eb059582c7469aaa
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

    static GBPTreeMetaDataStore openMetaDataStore( DatabaseLayout databaseLayout, PageCache pageCache, PageCacheTracer pageCacheTracer )
    {
        return new GBPTreeMetaDataStore( pageCache, databaseLayout.file( Stores.META_DATA_STORE_FILENAME ), 123456789, false, pageCacheTracer );
    }
<<<<<<< HEAD
=======

    static FrekiStorageEngine instantiateStandalone( FileSystemAbstraction fs, DatabaseLayout databaseLayout, PageCache pageCache, LogProvider logProvider )
            throws IOException
    {
        AtomicReference<FrekiStorageEngine> ref = new AtomicReference<>();
        TokenHolders tokenHolders = new TokenHolders(
                new DelegatingTokenHolder( new ProxyImmediateTokenCreator( () -> ref.get().stores().propertyKeyTokenStore ), TYPE_PROPERTY_KEY ),
                new DelegatingTokenHolder( new ProxyImmediateTokenCreator( () -> ref.get().stores().labelTokenStore ), TYPE_LABEL ),
                new DelegatingTokenHolder( new ProxyImmediateTokenCreator( () -> ref.get().stores().relationshipTypeTokenStore ), TYPE_RELATIONSHIP_TYPE ) );
        DatabaseHealth databaseHealth = new DatabaseHealth( causeOfPanic -> {}, logProvider.getLog( DatabaseHealth.class ) );
        return new FrekiStorageEngine( fs, databaseLayout, Config.defaults(), pageCache, tokenHolders,
                new DatabaseSchemaState( logProvider ), new StandardConstraintRuleAccessor(), i -> i, NO_LOCK_SERVICE,
                new DefaultIdGeneratorFactory( fs, immediate() ), new DefaultIdController(), databaseHealth, logProvider, immediate(), true, NULL,
                NO_TRACING, EmptyMemoryTracker.INSTANCE );
    }
>>>>>>> f26a3005d9b9a7f42b480941eb059582c7469aaa
}
