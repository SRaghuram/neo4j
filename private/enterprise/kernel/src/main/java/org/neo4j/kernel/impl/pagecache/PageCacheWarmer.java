/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.kernel.impl.pagecache;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.neo4j.graphdb.Resource;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.impl.FileIsNotMappedException;
import org.neo4j.kernel.impl.transaction.state.DatabaseFileListing;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.StoreFileMetadata;

import static java.util.Comparator.naturalOrder;
import static org.neo4j.io.pagecache.PagedFile.PF_NO_FAULT;
import static org.neo4j.io.pagecache.PagedFile.PF_SHARED_READ_LOCK;

/**
 * The page cache warmer profiles the page cache to figure out what data is in memory and what is not, and uses those
 * profiles to load probably-desirable data into the page cache during startup.
 * <p>
 * The profiling data is stored in a "profiles" directory in the same directory the mapped files.
 * The profile files have the same name as their corresponding mapped file, except they end with a dot-hexadecimal
 * sequence number, and ".cacheprof".
 * <p>
 * The profiles are collected in the "profiles" directory, so it is easy to get rid of all of them, on the off chance
 * that something is wrong with them.
 * <p>
 * These cacheprof files are compressed bitmaps where each raised bit indicates that the page identified by the
 * bit-index was in memory.
 */
public class PageCacheWarmer implements DatabaseFileListing.StoreFileProvider
{
    public static final String SUFFIX_CACHEPROF = ".cacheprof";

    private static final int IO_PARALLELISM = Runtime.getRuntime().availableProcessors();

    private final FileSystemAbstraction fs;
    private final PageCache pageCache;
    private final JobScheduler scheduler;
    private final File databaseDirectory;
    private final ProfileRefCounts refCounts;
    private volatile boolean stopped;
    private ExecutorService executor;
    private PageLoaderFactory pageLoaderFactory;

    PageCacheWarmer( FileSystemAbstraction fs, PageCache pageCache, JobScheduler scheduler, File databaseDirectory )
    {
        this.fs = fs;
        this.pageCache = pageCache;
        this.scheduler = scheduler;
        this.databaseDirectory = databaseDirectory;
        this.refCounts = new ProfileRefCounts();
    }

    @Override
    public synchronized Resource addFilesTo( Collection<StoreFileMetadata> coll ) throws IOException
    {
        if ( stopped )
        {
            return Resource.EMPTY;
        }
        List<PagedFile> files = pageCache.listExistingMappings();
        Profile[] existingProfiles = findExistingProfiles( files );
        for ( Profile profile : existingProfiles )
        {
            coll.add( new StoreFileMetadata( profile.file(), 1, false ) );
        }
        refCounts.incrementRefCounts( existingProfiles );
        return () -> refCounts.decrementRefCounts( existingProfiles );
    }

    public synchronized void start()
    {
        stopped = false;
        executor = buildExecutorService( scheduler );
        pageLoaderFactory = new PageLoaderFactory( executor, pageCache );
    }

    public void stop()
    {
        stopped = true;
        stopWarmer();
    }

    /**
     * Stopping warmer process, needs to be synchronised to prevent racing with profiling and heating
     */
    private synchronized void stopWarmer()
    {
        if ( executor != null )
        {
            executor.shutdown();
        }
    }

    /**
     * Reheat the page cache based on existing profiling data, or do nothing if no profiling data is available.
     *
     * @return An {@link OptionalLong} of the number of pages loaded in, or {@link OptionalLong#empty()} if the
     * reheating was stopped early via {@link #stop()}.
     * @throws IOException if anything goes wrong while reading the profiled data back in.
     */
    synchronized OptionalLong reheat() throws IOException
    {
        if ( stopped )
        {
            return OptionalLong.empty();
        }
        long pagesLoaded = 0;
        List<PagedFile> files = pageCache.listExistingMappings();
        Profile[] existingProfiles = findExistingProfiles( files );
        for ( PagedFile file : files )
        {
            try
            {
                pagesLoaded += reheat( file, existingProfiles );
            }
            catch ( FileIsNotMappedException ignore )
            {
                // The database is allowed to map and unmap files while we are trying to heat it up.
            }
        }
        return OptionalLong.of( pagesLoaded );
    }

    /**
     * Profile the in-memory data in the page cache, and write it to "cacheprof" file siblings of the mapped files.
     *
     * @return An {@link OptionalLong} of the number of pages that were found to be in memory, or
     * {@link OptionalLong#empty()} if the profiling was stopped early via {@link #stop()}.
     * @throws IOException If anything goes wrong while accessing the page cache or writing out the profile data.
     */
    public synchronized OptionalLong profile() throws IOException
    {
        if ( stopped )
        {
            return OptionalLong.empty();
        }
        // Note that we could in principle profile the files in parallel. However, producing a profile is usually so
        // fast, that it rivals the overhead of starting and stopping threads. Because of this, the complexity of
        // profiling in parallel is just not worth it.
        long pagesInMemory = 0;
        List<PagedFile> files = pageCache.listExistingMappings();
        Profile[] existingProfiles = findExistingProfiles( files );
        for ( PagedFile file : files )
        {
            try
            {
                pagesInMemory += profile( file, existingProfiles );
            }
            catch ( FileIsNotMappedException ignore )
            {
                // The database is allowed to map and unmap files while we are profiling the page cache.
            }
            if ( stopped )
            {
                pageCache.reportEvents();
                return OptionalLong.empty();
            }
        }
        pageCache.reportEvents();
        return OptionalLong.of( pagesInMemory );
    }

    private long reheat( PagedFile file, Profile[] existingProfiles ) throws IOException
    {
        Optional<Profile> savedProfile = filterRelevant( existingProfiles, file )
                .sorted( Comparator.reverseOrder() ) // Try most recent profile first.
                .filter( this::verifyChecksum )
                .findFirst();

        if ( !savedProfile.isPresent() )
        {
            return 0;
        }

        // The file contents checks out. Let's load it in.
        long pagesLoaded = 0;
        try ( InputStream input = savedProfile.get().read( fs );
              PageLoader loader = pageLoaderFactory.getLoader( file ) )
        {
            long pageId = 0;
            int b;
            while ( (b = input.read()) != -1 )
            {
                for ( int i = 0; i < 8; i++ )
                {
                    if ( stopped )
                    {
                        pageCache.reportEvents();
                        return pagesLoaded;
                    }
                    if ( (b & 1) == 1 )
                    {
                        loader.load( pageId );
                        pagesLoaded++;
                    }
                    b >>= 1;
                    pageId++;
                }
            }
        }
        pageCache.reportEvents();
        return pagesLoaded;
    }

    private boolean verifyChecksum( Profile profile )
    {
        // Successfully reading through and closing the compressed file implies verifying the gzip checksum.
        try ( InputStream input = profile.read( fs ) )
        {
            int b;
            do
            {
                b = input.read();
            }
            while ( b != -1 );
        }
        catch ( IOException ignore )
        {
            return false;
        }
        return true;
    }

    private long profile( PagedFile file, Profile[] existingProfiles ) throws IOException
    {
        long pagesInMemory = 0;
        Profile nextProfile = filterRelevant( existingProfiles, file )
                .max( naturalOrder() )
                .map( Profile::next )
                .orElse( Profile.first( file.file() ) );

        try ( OutputStream output = nextProfile.write( fs );
              PageCursor cursor = file.io( 0, PF_SHARED_READ_LOCK | PF_NO_FAULT ) )
        {
            int stepper = 0;
            int b = 0;
            while ( cursor.next() )
            {
                if ( cursor.getCurrentPageId() != PageCursor.UNBOUND_PAGE_ID )
                {
                    pagesInMemory++;
                    b |= 1 << stepper;
                }
                stepper++;
                if ( stepper == 8 )
                {
                    output.write( b );
                    b = 0;
                    stepper = 0;
                }
            }
            output.write( b );
            output.flush();
        }

        // Delete previous profile files.
        filterRelevant( existingProfiles, file )
                .filter( profile -> !refCounts.contains( profile ) )
                .forEach( profile -> profile.delete( fs ) );

        return pagesInMemory;
    }

    private static ExecutorService buildExecutorService( JobScheduler scheduler )
    {
        BlockingQueue<Runnable> workQueue = new LinkedBlockingQueue<>( IO_PARALLELISM * 4 );
        RejectedExecutionHandler rejectionPolicy = new ThreadPoolExecutor.CallerRunsPolicy();
        ThreadFactory threadFactory = scheduler.threadFactory( Group.FILE_IO_HELPER );
        return new ThreadPoolExecutor(
                0, IO_PARALLELISM, 10, TimeUnit.SECONDS, workQueue,
                threadFactory, rejectionPolicy );
    }

    private static Stream<Profile> filterRelevant( Profile[] profiles, PagedFile pagedFile )
    {
        return Stream.of( profiles ).filter( Profile.relevantTo( pagedFile ) );
    }

    private Profile[] findExistingProfiles( List<PagedFile> pagedFiles )
    {
        Path databasePath = databaseDirectory.toPath();
        return pagedFiles.stream()
                         .filter( pf -> pf.file().toPath().startsWith( databasePath ) )
                         .map( pf -> pf.file().getParentFile() )
                         .distinct()
                         .flatMap( dir -> Profile.findProfilesInDirectory( fs, dir ) )
                         .toArray( Profile[]::new );
    }
}
