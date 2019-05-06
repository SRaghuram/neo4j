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
package org.neo4j.kernel.impl.index.schema;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.neo4j.gis.spatial.index.curves.SpaceFillingCurveConfiguration;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.helpers.collection.BoundedIterable;
import org.neo4j.helpers.collection.CombiningIterable;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexConfigProvider;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.api.index.IndexReader;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.impl.api.index.IndexUpdateMode;
import org.neo4j.kernel.impl.index.schema.config.SpaceFillingCurveSettings;
import org.neo4j.storageengine.api.NodePropertyAccessor;
import org.neo4j.storageengine.api.StorageIndexReference;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.Value;

import static org.neo4j.helpers.collection.Iterators.concatResourceIterators;
import static org.neo4j.index.internal.gbptree.GBPTree.NO_HEADER_WRITER;
import static org.neo4j.kernel.impl.index.schema.fusion.FusionIndexBase.forAll;

class SpatialIndexAccessor extends SpatialIndexCache<SpatialIndexAccessor.PartAccessor> implements IndexAccessor
{
    private final StorageIndexReference descriptor;

    SpatialIndexAccessor( StorageIndexReference descriptor,
                          PageCache pageCache,
                          FileSystemAbstraction fs,
                          RecoveryCleanupWorkCollector recoveryCleanupWorkCollector,
                          IndexProvider.Monitor monitor,
                          SpatialIndexFiles spatialIndexFiles,
                          SpaceFillingCurveConfiguration searchConfiguration )
    {
        super( new PartFactory( pageCache,
                                fs,
                                recoveryCleanupWorkCollector,
                                monitor,
                                descriptor,
                                spatialIndexFiles,
                                searchConfiguration ) );
        this.descriptor = descriptor;
        spatialIndexFiles.loadExistingIndexes( this );
    }

    @Override
    public void drop()
    {
        forAll( NativeIndexAccessor::drop, this );
    }

    @Override
    public IndexUpdater newUpdater( IndexUpdateMode mode )
    {
        return new SpatialIndexUpdater( this, mode );
    }

    @Override
    public void force( IOLimiter ioLimiter )
    {
        for ( NativeIndexAccessor part : this )
        {
            part.force( ioLimiter );
        }
    }

    @Override
    public void refresh()
    {
        // not required in this implementation
    }

    @Override
    public void close()
    {
        closeInstantiateCloseLock();
        forAll( NativeIndexAccessor::close, this );
    }

    @Override
    public IndexReader newReader()
    {
        return new SpatialIndexReader( descriptor, this );
    }

    @Override
    public BoundedIterable<Long> newAllEntriesReader()
    {
        ArrayList<BoundedIterable<Long>> allEntriesReader = new ArrayList<>();
        for ( NativeIndexAccessor<?,?> part : this )
        {
            allEntriesReader.add( part.newAllEntriesReader() );
        }

        return new BoundedIterable<Long>()
        {
            @Override
            public long maxCount()
            {
                long sum = 0L;
                for ( BoundedIterable<Long> part : allEntriesReader )
                {
                    long partMaxCount = part.maxCount();
                    if ( partMaxCount == UNKNOWN_MAX_COUNT )
                    {
                        return UNKNOWN_MAX_COUNT;
                    }
                    sum += partMaxCount;
                }
                return sum;
            }

            @Override
            public void close() throws Exception
            {
                forAll( BoundedIterable::close, allEntriesReader );
            }

            @Override
            public Iterator<Long> iterator()
            {
                return new CombiningIterable<>( allEntriesReader ).iterator();
            }
        };
    }

    @Override
    public ResourceIterator<File> snapshotFiles()
    {
        List<ResourceIterator<File>> snapshotFiles = new ArrayList<>();
        for ( NativeIndexAccessor<?,?> part : this )
        {
            snapshotFiles.add( part.snapshotFiles() );
        }
        return concatResourceIterators( snapshotFiles.iterator() );
    }

    @Override
    public Map<String,Value> indexConfig()
    {
        Map<String,Value> indexConfig = new HashMap<>();
        for ( NativeIndexAccessor<?,?> part : this )
        {
            IndexConfigProvider.putAllNoOverwrite( indexConfig, part.indexConfig() );
        }
        return indexConfig;
    }

    @Override
    public void verifyDeferredConstraints( NodePropertyAccessor nodePropertyAccessor ) throws IndexEntryConflictException
    {
        for ( NativeIndexAccessor<?,?> part : this )
        {
            part.verifyDeferredConstraints( nodePropertyAccessor );
        }
    }

    @Override
    public boolean isDirty()
    {
        return Iterators.stream( iterator() ).anyMatch( NativeIndexAccessor::isDirty );
    }

    static class PartAccessor extends NativeIndexAccessor<SpatialIndexKey,NativeIndexValue>
    {
        private final StorageIndexReference descriptor;
        private final SpaceFillingCurveConfiguration searchConfiguration;
        private final CoordinateReferenceSystem crs;
        private final SpaceFillingCurveSettings settings;

        PartAccessor( PageCache pageCache, FileSystemAbstraction fs, IndexFiles indexFiles, IndexLayout<SpatialIndexKey,NativeIndexValue> layout,
                SpaceFillingCurveSettings settings, CoordinateReferenceSystem crs,
                RecoveryCleanupWorkCollector recoveryCleanupWorkCollector,
                IndexProvider.Monitor monitor, StorageIndexReference descriptor,
                SpaceFillingCurveConfiguration searchConfiguration )
        {
            super( pageCache, fs, indexFiles, layout, monitor, descriptor, NO_HEADER_WRITER );
            this.descriptor = descriptor;
            this.searchConfiguration = searchConfiguration;
            this.crs = crs;
            this.settings = settings;
            instantiateTree( recoveryCleanupWorkCollector, headerWriter );
        }

        @Override
        public SpatialIndexPartReader<NativeIndexValue> newReader()
        {
            assertOpen();
            return new SpatialIndexPartReader<>( tree, layout, descriptor, searchConfiguration );
        }

        @Override
        public void verifyDeferredConstraints( NodePropertyAccessor nodePropertyAccessor ) throws IndexEntryConflictException
        {
            SpatialVerifyDeferredConstraint.verify( nodePropertyAccessor, layout, tree, descriptor );
            super.verifyDeferredConstraints( nodePropertyAccessor );
        }

        @Override
        public Map<String,Value> indexConfig()
        {
            Map<String,Value> map = new HashMap<>();
            SpatialIndexConfig.addSpatialConfig( map, crs, settings );
            return map;
        }
    }

    static class PartFactory implements Factory<PartAccessor>
    {
        private final PageCache pageCache;
        private final FileSystemAbstraction fs;
        private final RecoveryCleanupWorkCollector recoveryCleanupWorkCollector;
        private final IndexProvider.Monitor monitor;
        private final StorageIndexReference descriptor;
        private final SpatialIndexFiles spatialIndexFiles;
        private final SpaceFillingCurveConfiguration searchConfiguration;

        PartFactory( PageCache pageCache,
                     FileSystemAbstraction fs,
                     RecoveryCleanupWorkCollector recoveryCleanupWorkCollector,
                     IndexProvider.Monitor monitor,
                     StorageIndexReference descriptor,
                     SpatialIndexFiles spatialIndexFiles,
                     SpaceFillingCurveConfiguration searchConfiguration )
        {
            this.pageCache = pageCache;
            this.fs = fs;
            this.recoveryCleanupWorkCollector = recoveryCleanupWorkCollector;
            this.monitor = monitor;
            this.descriptor = descriptor;
            this.spatialIndexFiles = spatialIndexFiles;
            this.searchConfiguration = searchConfiguration;
        }

        @Override
        public PartAccessor newSpatial( CoordinateReferenceSystem crs ) throws IOException
        {
            SpatialIndexFiles.SpatialFile spatialFile = spatialIndexFiles.forCrs( crs );
            if ( !fs.fileExists( spatialFile.indexFiles.getStoreFile() ) )
            {
                SpatialIndexFiles.SpatialFileLayout fileLayout = spatialFile.getLayoutForNewIndex();
                createEmptyIndex( fileLayout );
                return createPartAccessor( fileLayout );
            }
            else
            {
                return createPartAccessor( spatialFile.getLayoutForExistingIndex( pageCache ) );
            }
        }

        private PartAccessor createPartAccessor( SpatialIndexFiles.SpatialFileLayout fileLayout )
        {
            return new PartAccessor( pageCache,
                                     fs,
                                     fileLayout.indexFiles,
                                     fileLayout.layout,
                                     fileLayout.settings,
                                     fileLayout.crs,
                                     recoveryCleanupWorkCollector,
                                     monitor,
                                     descriptor,
                                     searchConfiguration );
        }

        private void createEmptyIndex( SpatialIndexFiles.SpatialFileLayout fileLayout )
        {
            IndexPopulator populator = new SpatialIndexPopulator.PartPopulator( pageCache,
                                                                                fs,
                                                                                fileLayout.indexFiles,
                                                                                fileLayout.layout,
                                                                                monitor,
                                                                                descriptor,
                                                                                searchConfiguration,
                                                                                fileLayout.settings,
                                                                                fileLayout.crs );
            populator.create();
            populator.close( true );
        }
    }
}
