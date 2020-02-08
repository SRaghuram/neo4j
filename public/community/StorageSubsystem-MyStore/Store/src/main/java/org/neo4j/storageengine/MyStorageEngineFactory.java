package org.neo4j.storageengine;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.configuration.Config;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.internal.id.IdController;
import org.neo4j.internal.id.IdGeneratorFactory;
import org.neo4j.internal.schema.IndexConfigCompleter;
import org.neo4j.internal.schema.SchemaState;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.impl.store.MyMetadataStore;
import org.neo4j.lock.LockService;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.internal.LogService;
import org.neo4j.monitoring.DatabaseHealth;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.*;
import org.neo4j.storageengine.migration.SchemaRuleMigrationAccess;
import org.neo4j.storageengine.migration.StoreMigrationParticipant;
import org.neo4j.token.TokenHolders;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@ServiceProvider
public class MyStorageEngineFactory implements StorageEngineFactory {

    MyStorageEngine myStorageEngine = null;
    @Override
    public StoreVersionCheck versionCheck(FileSystemAbstraction fs, DatabaseLayout databaseLayout, Config config, PageCache pageCache, LogService logService) {
        System.out.println("MyStorageEngineFactory::" + Thread.currentThread().getStackTrace()[1].getMethodName());
        return new MyStoreVersionCheck();
    }

    @Override
    public StoreVersion versionInformation(String storeVersion) {
        System.out.println("MyStorageEngineFactory::" + Thread.currentThread().getStackTrace()[1].getMethodName());
        return null;
    }

    @Override
    public List<StoreMigrationParticipant> migrationParticipants(FileSystemAbstraction fs, Config config, PageCache pageCache, JobScheduler jobScheduler,
                                                                 LogService logService, PageCacheTracer cacheTracer ) {
        System.out.println("MyStorageEngineFactory::" + Thread.currentThread().getStackTrace()[1].getMethodName());
        return null;
    }

    @Override
    public StorageEngine instantiate(FileSystemAbstraction fs, DatabaseLayout databaseLayout, Config config, PageCache pageCache, TokenHolders tokenHolders,
                                     SchemaState schemaState, ConstraintRuleAccessor constraintSemantics, IndexConfigCompleter indexConfigCompleter,
                                     LockService lockService, IdGeneratorFactory idGeneratorFactory, IdController idController, DatabaseHealth databaseHealth,
                                     LogProvider logProvider, RecoveryCleanupWorkCollector recoveryCleanupWorkCollector, PageCacheTracer cacheTracer,
                                     boolean createStoreIfNotExists) {
        System.out.println("MyStorageEngineFactory::" + Thread.currentThread().getStackTrace()[1].getMethodName());
        myStorageEngine = new MyStorageEngine( databaseLayout, config, pageCache, fs, logProvider, tokenHolders, schemaState, constraintSemantics,
                indexConfigCompleter, lockService, databaseHealth, idGeneratorFactory, idController, recoveryCleanupWorkCollector, createStoreIfNotExists );
        return myStorageEngine;
    }

    @Override
    public List<File> listStorageFiles(FileSystemAbstraction fileSystem, DatabaseLayout databaseLayout) throws IOException {
        List<File> files = new ArrayList<File>();
        //files.add(databaseLayout.storeFiles().toArray(new File[0]));
        System.out.println("MyStorageEngineFactory::" + Thread.currentThread().getStackTrace()[1].getMethodName());
        return files;
    }

    @Override
    public boolean storageExists(FileSystemAbstraction fileSystem, DatabaseLayout databaseLayout, PageCache pageCache) {
        System.out.println("MyStorageEngineFactory::" + Thread.currentThread().getStackTrace()[1].getMethodName());
        return databaseLayout.metadataStore().exists();
    }

    @Override
    public TransactionIdStore readOnlyTransactionIdStore(FileSystemAbstraction fileSystem, DatabaseLayout databaseLayout, PageCache pageCache) throws IOException {
        System.out.println("MyStorageEngineFactory::" + Thread.currentThread().getStackTrace()[1].getMethodName());
        return new MyReadOnlyTransactionIdStore( fileSystem, pageCache, databaseLayout );
    }

    @Override
    public LogVersionRepository readOnlyLogVersionRepository(DatabaseLayout databaseLayout, PageCache pageCache) throws IOException {
        System.out.println("MyStorageEngineFactory::" + Thread.currentThread().getStackTrace()[1].getMethodName());
        return new MyLogVersionRepository( pageCache, databaseLayout );
    }

    @Override
    public TransactionMetaDataStore transactionMetaDataStore(FileSystemAbstraction fs, DatabaseLayout databaseLayout, Config config,
                                                             PageCache pageCache, PageCacheTracer cacheTracer) throws IOException {
        System.out.println("MyStorageEngineFactory::" + Thread.currentThread().getStackTrace()[1].getMethodName());
        return myStorageEngine.myStore.getTransactionMetaDataStore();
    }

    @Override
    public StoreId storeId(DatabaseLayout databaseLayout, PageCache pageCache) throws IOException {
        System.out.println("MyStorageEngineFactory::" + Thread.currentThread().getStackTrace()[1].getMethodName());
        File neoStoreFile = databaseLayout.metadataStore();
        StoreId storeId = null;
        if (neoStoreFile.exists())
             storeId = MyMetadataStore.getStoreId( pageCache, neoStoreFile );
        if (storeId != null)
            return storeId;
        return new StoreId(1572647304196l, 2487352140268501236l, 7074645039649280775l, 1572647304196l, 1 );
    }

    @Override
    public SchemaRuleMigrationAccess schemaRuleMigrationAccess(FileSystemAbstraction fs, PageCache pageCache, Config config, DatabaseLayout databaseLayout,
                                                               LogService logService, String recordFormats, PageCacheTracer cacheTracer) {
        System.out.println("MyStorageEngineFactory::" + Thread.currentThread().getStackTrace()[1].getMethodName());
        return null;
    }


    public void checkAllFilesPresence( DatabaseLayout databaseLayout, FileSystemAbstraction fs )
    {
        /*RecoveryStoreFileHelper.StoreFilesInfo storeFilesInfo = checkStoreFiles( databaseLayout, fs );
        if ( storeFilesInfo.allFilesPresent() )
        {
            return;
        }
        throw new RuntimeException( format( "Store files %s is(are) missing and recovery is not possible. Please restore from a consistent backup.",
                getMissingStoreFiles( storeFilesInfo ) ) );*/
    }
}

