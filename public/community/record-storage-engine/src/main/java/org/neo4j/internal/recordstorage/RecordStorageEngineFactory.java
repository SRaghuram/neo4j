/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.internal.recordstorage;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.common.DependencyResolver;
import org.neo4j.common.DependencySatisfier;
import org.neo4j.configuration.Config;
import org.neo4j.exceptions.UnsatisfiedDependencyException;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.cursor.context.VersionContextSupplier;
import org.neo4j.kernel.impl.api.SchemaState;
import org.neo4j.kernel.impl.constraints.ConstraintSemantics;
import org.neo4j.kernel.impl.core.TokenHolders;
import org.neo4j.kernel.impl.locking.LockService;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.StoreType;
import org.neo4j.kernel.impl.store.format.RecordFormatSelector;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.store.id.DefaultIdGeneratorFactory;
import org.neo4j.kernel.impl.store.id.IdController;
import org.neo4j.kernel.impl.store.id.IdGeneratorFactory;
import org.neo4j.kernel.impl.storemigration.RecordStorageMigrator;
import org.neo4j.kernel.impl.storemigration.RecordStoreVersion;
import org.neo4j.kernel.impl.storemigration.RecordStoreVersionCheck;
import org.neo4j.kernel.internal.DatabaseHealth;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.logging.internal.LogService;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.LogVersionRepository;
import org.neo4j.storageengine.api.ReadableStorageEngine;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.storageengine.api.StoreVersion;
import org.neo4j.storageengine.api.StoreVersionCheck;
import org.neo4j.storageengine.api.TransactionIdStore;
import org.neo4j.storageengine.api.TransactionMetaDataStore;
import org.neo4j.storageengine.migration.StoreMigrationParticipant;

import static org.neo4j.kernel.impl.store.StoreType.META_DATA;
import static org.neo4j.kernel.impl.store.format.RecordFormatSelector.selectForStoreOrConfig;

@ServiceProvider
public class RecordStorageEngineFactory implements StorageEngineFactory
{
    @Override
    public StoreVersionCheck versionCheck( DependencyResolver dependencyResolver )
    {
        return new RecordStoreVersionCheck(
                dependencyResolver.resolveDependency( FileSystemAbstraction.class ),
                dependencyResolver.resolveDependency( PageCache.class ),
                dependencyResolver.resolveDependency( DatabaseLayout.class ),
                resolveLogProvider( dependencyResolver ),
                dependencyResolver.resolveDependency( Config.class ) );
    }

    @Override
    public StoreVersion versionInformation( String storeVersion )
    {
        return new RecordStoreVersion( RecordFormatSelector.selectForVersion( storeVersion ) );
    }

    @Override
    public StoreMigrationParticipant migrationParticipant( DependencyResolver dependencyResolver )
    {
        return new RecordStorageMigrator(
                dependencyResolver.resolveDependency( FileSystemAbstraction.class ),
                dependencyResolver.resolveDependency( PageCache.class ),
                dependencyResolver.resolveDependency( Config.class ),
                dependencyResolver.resolveDependency( LogService.class ),
                dependencyResolver.resolveDependency( JobScheduler.class ) );
    }

    @Override
    public StorageEngine instantiate( DependencyResolver dependencyResolver, DependencySatisfier dependencySatisfier )
    {
        RecordStorageEngine storageEngine = new RecordStorageEngine(
                dependencyResolver.resolveDependency( DatabaseLayout.class ),
                dependencyResolver.resolveDependency( Config.class ),
                dependencyResolver.resolveDependency( PageCache.class ),
                dependencyResolver.resolveDependency( FileSystemAbstraction.class ),
                resolveLogProvider( dependencyResolver ),
                dependencyResolver.resolveDependency( TokenHolders.class ),
                dependencyResolver.resolveDependency( SchemaState.class ),
                dependencyResolver.resolveDependency( ConstraintSemantics.class ),
                dependencyResolver.resolveDependency( LockService.class ),
                dependencyResolver.resolveDependency( DatabaseHealth.class ),
                dependencyResolver.resolveDependency( IdGeneratorFactory.class ),
                dependencyResolver.resolveDependency( IdController.class ),
                dependencyResolver.resolveDependency( VersionContextSupplier.class ) );

        // We pretend that the storage engine abstract hides all details within it. Whereas that's mostly
        // true it's not entirely true for the time being. As long as we need this call below, which
        // makes available one or more internal things to the outside world, there are leaks to plug.
        storageEngine.satisfyDependencies( dependencySatisfier );

        return storageEngine;
    }

    @Override
    public ReadableStorageEngine instantiateReadable( DependencyResolver dependencyResolver )
    {
        return new ReadableRecordStorageEngine(
                dependencyResolver.resolveDependency( DatabaseLayout.class ),
                dependencyResolver.resolveDependency( Config.class ),
                dependencyResolver.resolveDependency( PageCache.class ),
                dependencyResolver.resolveDependency( FileSystemAbstraction.class ),
                dependencyResolver.resolveDependency( LogService.class ).getInternalLogProvider() );
    }

    public List<File> listStorageFiles( FileSystemAbstraction fileSystem, DatabaseLayout databaseLayout ) throws IOException
    {
        if ( !fileSystem.fileExists( databaseLayout.file( META_DATA.getDatabaseFile() ).findFirst().get() ) )
        {
            throw new IOException( "No storage present at " + databaseLayout + " on " + fileSystem );
        }

        List<File> files = new ArrayList<>();
        for ( StoreType type : StoreType.values() )
        {
            databaseLayout.file( type.getDatabaseFile() ).filter( fileSystem::fileExists ).forEach( files::add );
        }
        return files;
    }

    @Override
    public boolean storageExists( FileSystemAbstraction fs, PageCache pageCache, DatabaseLayout databaseLayout )
    {
        return NeoStores.isStorePresent( pageCache, databaseLayout );
    }

    @Override
    public TransactionIdStore readOnlyTransactionIdStore( DependencyResolver dependencyResolver ) throws IOException
    {
        return new ReadOnlyTransactionIdStore(
                dependencyResolver.resolveDependency( PageCache.class ),
                dependencyResolver.resolveDependency( DatabaseLayout.class ) );
    }

    @Override
    public LogVersionRepository readOnlyLogVersionRepository( DependencyResolver dependencyResolver ) throws IOException
    {
        return new ReadOnlyLogVersionRepository(
                dependencyResolver.resolveDependency( PageCache.class ),
                dependencyResolver.resolveDependency( DatabaseLayout.class ) );
    }

    @Override
    public TransactionMetaDataStore transactionMetaDataStore( DependencyResolver dependencyResolver )
    {
        DatabaseLayout databaseLayout = dependencyResolver.resolveDependency( DatabaseLayout.class );
        FileSystemAbstraction fs = dependencyResolver.resolveDependency( FileSystemAbstraction.class );
        PageCache pageCache = dependencyResolver.resolveDependency( PageCache.class );
        Config config = dependencyResolver.resolveDependency( Config.class );
        NullLogProvider logProvider = NullLogProvider.getInstance();

        RecordFormats recordFormats = selectForStoreOrConfig( Config.defaults(), databaseLayout, fs, pageCache, logProvider );
        return new StoreFactory( databaseLayout, config, new DefaultIdGeneratorFactory( fs ), pageCache, fs, recordFormats, logProvider )
                .openNeoStores( META_DATA ).getMetaDataStore();
    }

    @Override
    public StoreId storeId( DependencyResolver dependencyResolver ) throws IOException
    {
        DatabaseLayout databaseLayout = dependencyResolver.resolveDependency( DatabaseLayout.class );
        PageCache pageCache = dependencyResolver.resolveDependency( PageCache.class );
        FileSystemAbstraction fs = dependencyResolver.resolveDependency( FileSystemAbstraction.class );

        File neoStoreFile = databaseLayout.metadataStore();
        return MetaDataStore.getStoreId( pageCache, neoStoreFile );
    }

    private LogProvider resolveLogProvider( DependencyResolver dependencyResolver )
    {
        try
        {
            return dependencyResolver.resolveDependency( LogService.class ).getInternalLogProvider();
        }
        catch ( UnsatisfiedDependencyException e )
        {
            return dependencyResolver.resolveDependency( LogProvider.class );
        }
    }
}
