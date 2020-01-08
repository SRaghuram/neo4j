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
package org.neo4j.kernel.impl.store;

import java.io.IOException;
import java.nio.file.OpenOption;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.exceptions.UnderlyingStorageException;
import org.neo4j.internal.id.IdGeneratorFactory;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.logging.LogProvider;

import static org.neo4j.internal.helpers.ArrayUtil.concat;
import static org.neo4j.internal.helpers.ArrayUtil.contains;
import static org.neo4j.io.pagecache.PageCacheOpenOptions.DIRECT;
import static org.neo4j.kernel.impl.store.format.RecordFormatPropertyConfigurator.configureRecordFormat;
import static org.neo4j.kernel.impl.store.format.RecordFormatSelector.selectForStoreOrConfig;

/**
 * Factory for Store implementations. Can also be used to create empty stores.
 */
public class StoreFactory
{
    private final DatabaseLayout databaseLayout;
    private final Config config;
    private final IdGeneratorFactory idGeneratorFactory;
    private final FileSystemAbstraction fileSystemAbstraction;
    private final LogProvider logProvider;
    private final PageCache pageCache;
    private final RecordFormats recordFormats;
    private final PageCacheTracer cacheTracer;
    private final OpenOption[] openOptions;

    public StoreFactory( DatabaseLayout directoryStructure, Config config, IdGeneratorFactory idGeneratorFactory, PageCache pageCache,
            FileSystemAbstraction fileSystemAbstraction, LogProvider logProvider, PageCacheTracer cacheTracer )
    {
        this( directoryStructure, config, idGeneratorFactory, pageCache, fileSystemAbstraction,
                selectForStoreOrConfig( config, directoryStructure, fileSystemAbstraction, pageCache, logProvider ),
                logProvider, cacheTracer );
    }

    public StoreFactory( DatabaseLayout databaseLayout, Config config, IdGeneratorFactory idGeneratorFactory, PageCache pageCache,
            FileSystemAbstraction fileSystemAbstraction, RecordFormats recordFormats, LogProvider logProvider, PageCacheTracer cacheTracer,
            OpenOption... openOptions )
    {
        this.databaseLayout = databaseLayout;
        this.config = config;
        this.idGeneratorFactory = idGeneratorFactory;
        this.fileSystemAbstraction = fileSystemAbstraction;
        this.recordFormats = recordFormats;
        this.cacheTracer = cacheTracer;
        this.openOptions = buildOpenOptions( config, openOptions );
        this.logProvider = logProvider;
        this.pageCache = pageCache;
        configureRecordFormat( recordFormats, config );
    }

    /**
     * Open {@link NeoStores} with all possible stores. If some store does not exist it will <b>not</b> be created.
     * @return container with all opened stores
     */
    public NeoStores openAllNeoStores()
    {
        return openNeoStores( false, StoreType.values() );
    }

    /**
     * Open {@link NeoStores} with all possible stores with a possibility to create store if it not exist.
     * @param createStoreIfNotExists - should store be created if it's not exist
     * @return container with all opened stores
     */
    public NeoStores openAllNeoStores( boolean createStoreIfNotExists )
    {
        return openNeoStores( createStoreIfNotExists, StoreType.values() );
    }

    /**
     * Open {@link NeoStores} for requested and store types. If requested store depend from non request store,
     * it will be automatically opened as well.
     * If some store does not exist it will <b>not</b> be created.
     * @param storeTypes - types of stores to be opened.
     * @return container with opened stores
     */
    public NeoStores openNeoStores( StoreType... storeTypes )
    {
        return openNeoStores( false, storeTypes );
    }

    /**
     * Open {@link NeoStores} for requested and store types. If requested store depend from non request store,
     * it will be automatically opened as well.
     * @param createStoreIfNotExists - should store be created if it's not exist
     * @param storeTypes - types of stores to be opened.
     * @return container with opened stores
     */
    public NeoStores openNeoStores( boolean createStoreIfNotExists, StoreType... storeTypes )
    {
        if ( createStoreIfNotExists )
        {
            try
            {
                fileSystemAbstraction.mkdirs( databaseLayout.databaseDirectory() );
            }
            catch ( IOException e )
            {
                throw new UnderlyingStorageException(
                        "Could not create database directory: " + databaseLayout.databaseDirectory(), e );
            }
        }
        return new NeoStores( fileSystemAbstraction, databaseLayout, config, idGeneratorFactory, pageCache, logProvider, recordFormats, createStoreIfNotExists,
                cacheTracer, storeTypes, openOptions );
    }

    private static OpenOption[] buildOpenOptions( Config config, OpenOption[] openOptions )
    {
        if ( !config.get( GraphDatabaseSettings.pagecache_direct_io ) )
        {
            return openOptions;
        }

        if ( contains( openOptions, DIRECT ) )
        {
            return openOptions;
        }
        return concat( openOptions, DIRECT );
    }
}
