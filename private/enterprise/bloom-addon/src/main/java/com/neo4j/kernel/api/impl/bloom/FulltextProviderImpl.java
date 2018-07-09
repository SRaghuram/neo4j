/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.api.impl.bloom;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.concurrent.BinaryLatch;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Resource;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.io.IOUtils;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.AvailabilityGuard;
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.kernel.impl.transaction.state.NeoStoreFileListing;
import org.neo4j.kernel.impl.util.MultiResource;
import org.neo4j.logging.Log;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.StoreFileMetadata;

import static org.neo4j.kernel.impl.transaction.state.NeoStoreFileListing.getSnapshotFilesMetadata;

/**
 * Provider class that manages and provides fulltext indices. This is the main entry point for the fulltext addon.
 */
public class FulltextProviderImpl implements FulltextProvider
{
    private static final BinaryLatch STOPPED_FLIPPER_SIGNAL = new FlipperStopSignal();
    private static final JobScheduler.Group FLIPPER = new JobScheduler.Group( "FulltextIndexSideFlipper" );

    private final GraphDatabaseService db;
    private final Log log;
    private final EnumMap<FulltextIndexType,EnumMap<IndexSide,Map<String,WritableFulltext>>> writableIndices;
    private final FulltextUpdateApplier applier;
    private final FulltextFactory factory;
    private final ReadWriteLock configurationLock;
    private final JobScheduler.JobHandle flipperJob;
    private final AtomicReference<BinaryLatch> flipCompletionLatch = new AtomicReference<>();
    private volatile boolean closed;
    private volatile IndexSide side;

    /**
     * Creates a provider of fulltext indices for the given database. This is the entry point for all fulltext index
     * operations.
     * @param db Database that this provider should work with.
     * @param log For logging errors.
     * @param availabilityGuard Used for waiting with populating the index until the database is available.
     * @param scheduler For background work.
     * @param fileSystem The filesystem to use.
     * @param storeDir Store directory of the database.
     * @param analyzerClassName The Lucene analyzer to use for the {@link LuceneFulltext} created by this factory.
     */
    public FulltextProviderImpl( GraphDatabaseService db, Log log, AvailabilityGuard availabilityGuard, JobScheduler scheduler,
            FileSystemAbstraction fileSystem, File storeDir, String analyzerClassName, Duration refreshDelay )
    {
        this.db = db;
        this.log = log;
        side = IndexSide.SIDE_A;
        applier = new FulltextUpdateApplier( log, availabilityGuard, scheduler );
        applier.start();
        factory = new FulltextFactory( fileSystem, storeDir, analyzerClassName );
        writableIndices = new EnumMap<>( FulltextIndexType.class );
        for ( FulltextIndexType indexType : FulltextIndexType.values() )
        {
            EnumMap<IndexSide,Map<String,WritableFulltext>> writableIndexSideMap = new EnumMap<>( IndexSide.class );
            for ( IndexSide side : IndexSide.values() )
            {
                writableIndexSideMap.put( side, new ConcurrentHashMap<>() );
            }
            writableIndices.put( indexType, writableIndexSideMap );
        }
        configurationLock = new ReentrantReadWriteLock( true );
        flipperJob = scheduler.schedule( FLIPPER, () ->
        {
            boolean isAvailable;
            do
            {
                isAvailable = availabilityGuard.isAvailable( 100 );
            }
            while ( !isAvailable && !availabilityGuard.isShutdown() );
            while ( !closed )
            {
                try
                {
                    flip();
                }
                catch ( IOException e )
                {
                    log.error( "Unable to flip fulltext index", e );
                }
            }
        }, refreshDelay.getSeconds(), TimeUnit.SECONDS );
    }

    private boolean matchesConfiguration( WritableFulltext index ) throws IOException
    {
        index.maybeRefreshBlocking();
        FulltextIndexConfiguration currentConfig = new FulltextIndexConfiguration( index.getAnalyzerName(), index.getProperties() );

        FulltextIndexConfiguration storedConfig;
        try ( ReadOnlyFulltext indexReader = index.getIndexReader() )
        {
            storedConfig = indexReader.getConfigurationDocument();
        }
        return storedConfig == null && index.getProperties().isEmpty() || storedConfig != null && storedConfig.equals( currentConfig );
    }

    @Override
    public void awaitPopulation()
    {
        try
        {
            applier.writeBarrier().awaitCompletion();
        }
        catch ( ExecutionException e )
        {
            throw new AssertionError( "The writeBarrier operation should never throw an exception", e );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    @Override
    public void openIndex( String identifier, FulltextIndexType type ) throws IOException
    {
        LuceneFulltext a = factory.openFulltextIndex( identifier, IndexSide.SIDE_A, type );
        LuceneFulltext b = factory.openFulltextIndex( identifier, IndexSide.SIDE_B, type );
        configurationLock.writeLock().lock();
        try
        {
            register( a, IndexSide.SIDE_A );
            register( b, IndexSide.SIDE_B );
        }
        finally
        {
            configurationLock.writeLock().unlock();
        }
    }

    @Override
    public void createIndex( String identifier, FulltextIndexType type, Set<String> properties ) throws IOException
    {
        LuceneFulltext a = factory.createFulltextIndex( identifier, IndexSide.SIDE_A, type, properties );
        LuceneFulltext b = factory.createFulltextIndex( identifier, IndexSide.SIDE_B, type, properties );
        configurationLock.writeLock().lock();
        try
        {
            register( a, IndexSide.SIDE_A );
            register( b, IndexSide.SIDE_B );
        }
        finally
        {
            configurationLock.writeLock().unlock();
        }
    }

    private void register( LuceneFulltext fulltextIndex, IndexSide side ) throws IOException
    {
        FulltextIndexType indexType = fulltextIndex.getType();
        WritableFulltext writableFulltext = new WritableFulltext( fulltextIndex );
        writableFulltext.open();
        if ( !matchesConfiguration( writableFulltext ) )
        {
            recreateIndex( writableFulltext );
            if ( !writableFulltext.getProperties().isEmpty() )
            {
                applier.populate( indexType, db, writableFulltext );
            }
        }
        writableIndices.get( indexType ).get( side ).put( fulltextIndex.getIdentifier(), writableFulltext );
    }

    private void recreateIndex( WritableFulltext writableFulltext ) throws IOException
    {
        writableFulltext.drop();
        writableFulltext.open();
        writableFulltext.saveConfiguration();
    }

    @Override
    public ReadOnlyFulltext getReader( String identifier, FulltextIndexType type ) throws IOException
    {
        //Lock to protect from flips before we get the reader.
        Lock readLock = configurationLock.readLock();
        readLock.lock();
        try
        {
            WritableFulltext writableFulltext = getIndexMap( type ).get( identifier );
            if ( writableFulltext == null )
            {
                throw new IllegalArgumentException( "No such " + type + " index '" + identifier + "'." );
            }
            ReadOnlyFulltext indexReader = writableFulltext.getIndexReader();
            return lockedIndexReader( readLock, indexReader );
        }
        finally
        {
            readLock.unlock();
        }
    }

    private Map<String,WritableFulltext> getIndexMap( FulltextIndexType type )
    {
        return writableIndices.get( type ).get( side );
    }

    @Override
    public Set<String> getProperties( String identifier, FulltextIndexType type )
    {
        return applyToMatchingIndex( identifier, type, WritableFulltext::getProperties );
    }

    private <E> E applyToMatchingIndex( String identifier, FulltextIndexType type, Function<WritableFulltext,E> function )
    {
        return function.apply( writableIndices.get( type ).get( side ).get( identifier ) );
    }

    @Override
    public InternalIndexState getState( String identifier, FulltextIndexType type )
    {
        return applyToMatchingIndex( identifier, type, WritableFulltext::getState );
    }

    void drop( String identifier, FulltextIndexType type ) throws IOException
    {
        configurationLock.writeLock().lock();
        try
        {
            // Wait for the queue of updates to drain, before deleting an index.
            awaitPopulation();
            for ( Map<String,WritableFulltext> indexMap : writableIndices.get( type ).values() )
            {
                indexMap.remove( identifier ).drop();
            }
        }
        finally
        {
            configurationLock.writeLock().unlock();
        }
    }

    @Override
    public void changeIndexedProperties( String identifier, FulltextIndexType type, Set<String> propertyKeys ) throws IOException, InvalidArgumentsException
    {
        configurationLock.writeLock().lock();
        try
        {
            if ( propertyKeys.stream().anyMatch( s -> s.startsWith( LUCENE_FULLTEXT_ADDON_PREFIX ) ) )
            {
                throw new InvalidArgumentsException(
                        "It is not possible to index property keys starting with " + LUCENE_FULLTEXT_ADDON_PREFIX );
            }
            Set<String> currentProperties = getProperties( identifier, type );
            if ( !currentProperties.equals( propertyKeys ) )
            {
                drop( identifier, type );
                createIndex( identifier, type, propertyKeys );
            }
        }
        finally
        {
            configurationLock.writeLock().unlock();
        }
    }

    @Override
    public void registerFileListing( NeoStoreFileListing fileListing )
    {
        fileListing.registerStoreFileProvider( this::snapshotStoreFiles );
    }

    @Override
    public void awaitFlip()
    {
        getOrInstallFlipCompletionLatch().await();
    }

    private void flip() throws IOException
    {
        configurationLock.writeLock().lock();

        BinaryLatch completionLatch = getOrInstallFlipCompletionLatch();
        if ( completionLatch == STOPPED_FLIPPER_SIGNAL )
        {
            configurationLock.writeLock().unlock();
            return;
        }

        try
        {
            List<AsyncFulltextIndexOperation> populations;
            IndexSide tentativeSide;
            try
            {
                populations = new ArrayList<>();
                tentativeSide = side.otherSide();
                applier.writeBarrier().awaitCompletion();
                for ( FulltextIndexType indexType : FulltextIndexType.values() )
                {
                    for ( WritableFulltext index : writableIndices.get( indexType ).get( tentativeSide ).values() )
                    {
                        recreateIndex( index );
                        populations.add( applier.populate( indexType, db, index ) );
                    }
                }
            }
            finally
            {
                configurationLock.writeLock().unlock();
            }
            for ( AsyncFulltextIndexOperation population : populations )
            {
                population.awaitCompletion();
            }
            side = tentativeSide;
        }
        catch ( ExecutionException e )
        {
            throw new IOException( "Unable to flip fulltext indexes", e );
        }
        finally
        {
            completionLatch.release();
            flipCompletionLatch.compareAndSet( completionLatch, null );
        }
    }

    private BinaryLatch getOrInstallFlipCompletionLatch()
    {
        BinaryLatch completionLatch;
        do
        {
            completionLatch = flipCompletionLatch.get();
            if ( completionLatch != null )
            {
                break;
            }
            else
            {
                completionLatch = new BinaryLatch();
            }
        }
        while ( flipCompletionLatch.compareAndSet( null, completionLatch ) );
        return completionLatch;
    }

    private Resource snapshotStoreFiles( Collection<StoreFileMetadata> files ) throws IOException
    {
        // Save the last committed transaction, then drain the update queue to make sure that we have applied _at least_ the commits the config claims.
        Lock listingLock = configurationLock.readLock();
        listingLock.lock();
        try
        {
            final Collection<ResourceIterator<File>> snapshots = new ArrayList<>();
            List<WritableFulltext> indexes = allIndexes().collect( Collectors.toList() );
            for ( WritableFulltext index : indexes )
            {
                index.saveConfiguration();
                try
                {
                    applier.writeBarrier().awaitCompletion();
                }
                catch ( ExecutionException e )
                {
                    throw new IOException( "Unable to prepare index for snapshot.", e );
                }
            }
            for ( WritableFulltext index : indexes )
            {
                ResourceIterator<File> snapshot;
                try
                {
                    index.flush();
                    snapshot = index.snapshot();
                }
                catch ( IOException e )
                {
                    IOUtils.closeAllSilently( snapshots );
                    throw e;
                }
                ResourceIterator<File> lockedSnapshot = lockedSnapshot( listingLock, snapshot );
                snapshots.add( lockedSnapshot );
                files.addAll( getSnapshotFilesMetadata( lockedSnapshot ) );
            }
            // Intentionally don't close the snapshots here, return them for closing by the consumer of the targetFiles list.
            return new MultiResource( snapshots );
        }
        finally
        {
            listingLock.unlock();
        }
    }

    private ResourceIterator<File> lockedSnapshot( Lock listingLock, ResourceIterator<File> snapshot )
    {
        listingLock.lock(); // Released in ResourceIterator#close.
        return new ResourceIterator<File>()
        {
            @Override
            public void close()
            {
                try
                {
                    snapshot.close();
                }
                finally
                {
                    listingLock.unlock();
                }
            }

            @Override
            public boolean hasNext()
            {
                return snapshot.hasNext();
            }

            @Override
            public File next()
            {
                return snapshot.next();
            }
        };
    }

    private ReadOnlyFulltext lockedIndexReader( Lock readLock, ReadOnlyFulltext indexReader )
    {
        readLock.lock(); // Released in ReadOnlyFulltext#close.
        return new ReadOnlyFulltext()
        {
            @Override
            public ScoreEntityIterator query( Collection<String> terms, boolean matchAll )
            {
                return indexReader.query( terms, matchAll );
            }

            @Override
            public ScoreEntityIterator fuzzyQuery( Collection<String> terms, boolean matchAll )
            {
                return indexReader.fuzzyQuery( terms, matchAll );
            }

            @Override
            public void close()
            {
                try
                {
                    indexReader.close();
                }
                finally
                {
                    readLock.unlock();
                }
            }

            @Override
            public FulltextIndexConfiguration getConfigurationDocument() throws IOException
            {
                return indexReader.getConfigurationDocument();
            }
        };
    }

    /**
     * Closes the provider and all associated resources.
     */
    @Override
    public void close()
    {
        closed = true;
        configurationLock.writeLock().lock();
        try
        {
            flipperJob.cancel( false );
            flipCompletionLatch.getAndSet( STOPPED_FLIPPER_SIGNAL );
            try
            {
                awaitPopulation();
            }
            catch ( Exception e )
            {
                log.warn( "Failed to wait for fulltext index applier to finish, before closing.", e );
            }
            applier.stop();
            Consumer<WritableFulltext> fulltextCloser = luceneFulltextIndex ->
            {
                try
                {
                    luceneFulltextIndex.saveConfiguration();
                    luceneFulltextIndex.close();
                }
                catch ( IOException e )
                {
                    log.error( "Unable to close fulltext index.", e );
                }
            };
            allIndexes().forEach( fulltextCloser );
        }
        finally
        {
            configurationLock.writeLock().unlock();
        }
    }

    private Stream<WritableFulltext> allIndexes()
    {
        return writableIndices.values().stream()
                       .flatMap( sideMap -> sideMap.values().stream() )
                       .flatMap( indexMap -> indexMap.values().stream() );
    }
}
