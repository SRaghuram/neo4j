/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.impl.pagecache;

import org.eclipse.collections.api.iterator.IntIterator;
import org.eclipse.collections.api.set.primitive.MutableIntSet;
import org.eclipse.collections.impl.set.mutable.primitive.IntHashSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.OptionalLong;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import org.neo4j.configuration.Config;
import org.neo4j.graphdb.Resource;
import org.neo4j.io.ByteUnit;
import org.neo4j.io.fs.EphemeralFileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.tracing.DefaultPageCacheTracer;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.monitoring.tracing.Tracers;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.Log;
import org.neo4j.logging.NullLog;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobHandle;
import org.neo4j.scheduler.JobMonitoringParams;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.StoreFileMetadata;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.pagecache.PageCacheSupportExtension;
import org.neo4j.test.extension.testdirectory.EphemeralTestDirectoryExtension;
import org.neo4j.test.rule.PageCacheConfig;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.time.Clocks;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.READ;
import static org.eclipse.collections.api.factory.Sets.immutable;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.neo4j.configuration.GraphDatabaseSettings.pagecache_warmup_prefetch;
import static org.neo4j.configuration.GraphDatabaseSettings.pagecache_warmup_prefetch_allowlist;
import static org.neo4j.io.pagecache.PagedFile.PF_READ_AHEAD;
import static org.neo4j.io.pagecache.PagedFile.PF_SHARED_READ_LOCK;
import static org.neo4j.kernel.impl.scheduler.JobSchedulerFactory.createScheduler;
import static org.neo4j.logging.AssertableLogProvider.Level.DEBUG;
import static org.neo4j.logging.AssertableLogProvider.Level.INFO;
import static org.neo4j.logging.LogAssertions.assertThat;

@EphemeralTestDirectoryExtension
class PageCacheWarmerTest
{
    @RegisterExtension
    static PageCacheSupportExtension pageCacheExtension = new PageCacheSupportExtension();

    @Inject
    private EphemeralFileSystemAbstraction fs;
    @Inject
    private TestDirectory testDirectory;

    private LifeSupport life;
    private JobScheduler scheduler;
    private PageCacheTracer cacheTracer;
    private PageCacheConfig cfg;
    private Path file;
    private Tracers tracers;
    private Log log = NullLog.getInstance();

    @BeforeEach
    void setUp() throws IOException
    {
        life = new LifeSupport();
        scheduler = life.add( createScheduler() );
        life.start();
        tracers = new Tracers( "", log, null, null, Clocks.nanoClock(), Config.defaults() );
        cacheTracer = tracers.getPageCacheTracer();
        cfg = PageCacheConfig.config().withTracer( cacheTracer );
        file = testDirectory.homePath().resolve( "a" );
        fs.write( file );
    }

    @AfterEach
    void tearDown()
    {
        life.shutdown();
    }

    @Test
    void doNotReheatAfterStop() throws IOException
    {
        try ( PageCache pageCache = pageCacheExtension.getPageCache( fs, cfg );
              PagedFile ignore = pageCache.map( file, pageCache.pageSize(), immutable.of( CREATE ) ) )
        {
            PageCacheWarmer warmer = createPageCacheWarmer( pageCache, Config.defaults(), log );
            warmer.start();
            warmer.stop();
            assertSame( OptionalLong.empty(), warmer.reheat() );
        }
    }

    @Test
    void doNoProfileAfterStop() throws IOException
    {
        try ( PageCache pageCache = pageCacheExtension.getPageCache( fs, cfg );
              PagedFile ignore = pageCache.map( file, pageCache.pageSize(), immutable.of( CREATE ) ) )
        {
            PageCacheWarmer warmer = createPageCacheWarmer( pageCache, Config.defaults(), log );
            warmer.start();
            warmer.stop();
            assertSame( OptionalLong.empty(), warmer.profile() );
        }
    }

    @Test
    void profileAndReheatAfterRestart() throws IOException
    {
        try ( PageCache pageCache = pageCacheExtension.getPageCache( fs, cfg );
              PagedFile pf = pageCache.map( file, pageCache.pageSize(), immutable.of( CREATE ) ) )
        {
            PageCacheWarmer warmer = createPageCacheWarmer( pageCache, Config.defaults(), log );
            warmer.start();
            warmer.stop();
            warmer.start();
            try ( var tracer = cacheTracer.createPageCursorTracer( "profileAndReheatAfterRestart" );
                  PageCursor writer = pf.io( 0, PagedFile.PF_SHARED_WRITE_LOCK, tracer ) )
            {
                assertTrue( writer.next( 1 ) );
                assertTrue( writer.next( 3 ) );
            }
            warmer.profile();
            assertNotSame( OptionalLong.empty(), warmer.reheat() );
        }
    }

    @Test
    void mustDoNothingWhenReheatingUnprofiledPageCache() throws Exception
    {
        try ( PageCache pageCache = pageCacheExtension.getPageCache( fs, cfg );
              PagedFile ignore = pageCache.map( file, pageCache.pageSize(), immutable.of( CREATE ) ) )
        {
            PageCacheWarmer warmer = createPageCacheWarmer( pageCache, Config.defaults(), log );
            warmer.reheat();
        }
        assertThat( cacheTracer.faults() ).isEqualTo( 0L );
    }

    @Test
    void mustReheatProfiledPageCache() throws Exception
    {
        try ( PageCache pageCache = pageCacheExtension.getPageCache( fs, cfg );
              PagedFile pf = pageCache.map( file, pageCache.pageSize(), immutable.of( CREATE ) ) )
        {
            try ( var tracer = cacheTracer.createPageCursorTracer( "mustReheatProfiledPageCache" );
                  PageCursor writer = pf.io( 0, PagedFile.PF_SHARED_WRITE_LOCK, tracer ) )
            {
                assertTrue( writer.next( 1 ) );
                assertTrue( writer.next( 3 ) );
            }
            pf.flushAndForce();
            PageCacheWarmer warmer = createPageCacheWarmer( pageCache, Config.defaults(), log );
            warmer.start();
            warmer.profile();
        }

        long initialFaults = cacheTracer.faults();
        try ( PageCache pageCache = pageCacheExtension.getPageCache( fs, cfg );
              PagedFile pf = pageCache.map( file, pageCache.pageSize() ) )
        {
            PageCacheWarmer warmer = createPageCacheWarmer( pageCache, Config.defaults(), log );
            warmer.start();
            warmer.reheat();

            assertThat( cacheTracer.faults() ).isEqualTo( initialFaults + 2L );

            try ( var tracer = cacheTracer.createPageCursorTracer( "mustReheatProfiledPageCache" );
                  PageCursor reader = pf.io( 0, PF_SHARED_READ_LOCK, tracer ) )
            {
                assertTrue( reader.next( 1 ) );
                assertTrue( reader.next( 3 ) );
            }

            // No additional faults must have been reported.
            assertThat( cacheTracer.faults() ).isEqualTo( initialFaults + 2L );
        }
    }

    @Test
    void reheatingMustWorkOnLargeNumberOfPages() throws Exception
    {
        int maxPagesInMemory = 1_000;
        int[] pageIds = randomSortedPageIds( maxPagesInMemory );

        String pageCacheMemory = String.valueOf( maxPagesInMemory * ByteUnit.kibiBytes( 9 ) );
        try ( PageCache pageCache = pageCacheExtension.getPageCache( fs, cfg.withMemory( pageCacheMemory ) );
              PagedFile pf = pageCache.map( file, pageCache.pageSize(), immutable.of( CREATE ) ) )
        {
            try ( var tracer = cacheTracer.createPageCursorTracer( "reheatingMustWorkOnLargeNumberOfPages" );
                  PageCursor writer = pf.io( 0, PagedFile.PF_SHARED_WRITE_LOCK, tracer ) )
            {
                for ( int pageId : pageIds )
                {
                    assertTrue( writer.next( pageId ) );
                }
            }
            pf.flushAndForce();
            PageCacheWarmer warmer = createPageCacheWarmer( pageCache, Config.defaults(), log );
            warmer.profile();
        }

        long initialFaults = cacheTracer.faults();
        try ( PageCache pageCache = pageCacheExtension.getPageCache( fs, cfg );
              PagedFile pf = pageCache.map( file, pageCache.pageSize() ) )
        {
            PageCacheWarmer warmer = createPageCacheWarmer( pageCache, Config.defaults(), log );
            warmer.start();
            warmer.reheat();

            assertThat( cacheTracer.faults() ).isEqualTo( initialFaults + pageIds.length );

            try ( var tracer = cacheTracer.createPageCursorTracer( "reheatingMustWorkOnLargeNumberOfPages" );
                  PageCursor reader = pf.io( 0, PF_SHARED_READ_LOCK, tracer ) )
            {
                for ( int pageId : pageIds )
                {
                    assertTrue( reader.next( pageId ) );
                }
            }

            // No additional faults must have been reported.
            assertThat( cacheTracer.faults() ).isEqualTo( initialFaults + pageIds.length );
        }
    }

    @SuppressWarnings( "unused" )
    @Test
    void profileMustNotDeleteFilesCurrentlyExposedViaFileListing() throws Exception
    {
        try ( PageCache pageCache = pageCacheExtension.getPageCache( fs, cfg );
              PagedFile pf = pageCache.map( file, pageCache.pageSize(), immutable.of( CREATE ) ) )
        {
            try ( var tracer = cacheTracer.createPageCursorTracer( "profileMustNotDeleteFilesCurrentlyExposedViaFileListing" );
                  PageCursor writer = pf.io( 0, PagedFile.PF_SHARED_WRITE_LOCK, tracer ) )
            {
                assertTrue( writer.next( 1 ) );
                assertTrue( writer.next( 3 ) );
            }
            pf.flushAndForce();
            PageCacheWarmer warmer = createPageCacheWarmer( pageCache, Config.defaults(), log );
            warmer.start();
            warmer.profile();
            warmer.profile();
            warmer.profile();

            List<StoreFileMetadata> fileListing = new ArrayList<>();
            try ( Resource firstListing = warmer.addFilesTo( fileListing ) )
            {
                warmer.profile();
                warmer.profile();

                // The files in the file listing cannot be deleted while the listing is in use.
                assertThat( fileListing.size() ).isGreaterThan( 0 );
                assertFilesExists( fileListing );
                warmer.profile();
                try ( Resource secondListing = warmer.addFilesTo( new ArrayList<>() ) )
                {
                    warmer.profile();
                    // This must hold even when there are file listings overlapping in time.
                    assertFilesExists( fileListing );
                }
                warmer.profile();
                // And continue to hold after other overlapping listing finishes.
                assertFilesExists( fileListing );
            }
            // Once we are done with the file listing, profile should remove those files.
            warmer.profile();
            warmer.stop();
            assertFilesNotExists( fileListing );
        }
    }

    @Test
    @Disabled
    void profilerMustProfileMultipleFilesWithSameName() throws IOException
    {
        Path aaFile = testDirectory.homePath().resolve( "a" ).resolve( "a" );
        Path baFile = testDirectory.homePath().resolve( "b" ).resolve( "a" );
        fs.mkdirs( aaFile.getParent() );
        fs.mkdirs( baFile.getParent() );
        try ( PageCache pageCache = pageCacheExtension.getPageCache( fs, cfg );
              PagedFile aa = pageCache.map( aaFile, pageCache.pageSize(), immutable.of( CREATE ) );
              PagedFile ba = pageCache.map( baFile, pageCache.pageSize(), immutable.of( CREATE ) ) )
        {
            PageCacheWarmer warmer = createPageCacheWarmer( pageCache, Config.defaults(), log );
            warmer.start();
            warmer.profile();
            List<StoreFileMetadata> fileListing = new ArrayList<>();
            try ( Resource resource = warmer.addFilesTo( fileListing ) )
            {
                List<Path> uniqueProfileFiles = fileListing.stream()
                        .map( StoreFileMetadata::path )
                        .distinct()
                        .collect( Collectors.toList() );
                assertEquals( 2, uniqueProfileFiles.size(),
                        "expected mapped files in different directories but with same name to both have a unique profile. Profile files where " +
                                uniqueProfileFiles );
            }
            warmer.stop();
        }
    }

    @Test
    void profilesMustKeepTheirSubDirectoryStructureInsideProfileDirectory()
    {
        Path baseDir = Paths.get( "baseDir" );
        Path profileDir = baseDir.resolve( Profile.PROFILE_DIR );
        Path dirA = baseDir.resolve( "dirA" );
        Path dirAA = dirA.resolve( "dirA" );
        Path dirB = baseDir.resolve( "dirB" );
        Path fileAAA = dirAA.resolve( "a" );
        Path fileAA = dirA.resolve( "a" );
        Path fileBA = dirB.resolve( "a" );
        Path fileA = baseDir.resolve( "a" );
        Profile aaa = Profile.first( baseDir, fileAAA );
        Profile aa = Profile.first( baseDir, fileAA );
        Profile ba = Profile.first( baseDir, fileBA );
        Profile a = Profile.first( baseDir, fileA );
        Profile aaaNext = aaa.next();
        Profile aaNext = aa.next();
        Profile baNext = ba.next();
        Profile aNext = a.next();

        assertSameRelativePath( baseDir, profileDir, fileAAA, aaa );
        assertSameRelativePath( baseDir, profileDir, fileAA, aa );
        assertSameRelativePath( baseDir, profileDir, fileBA, ba );
        assertSameRelativePath( baseDir, profileDir, fileA, a );
        assertSameRelativePath( baseDir, profileDir, fileAAA, aaaNext );
        assertSameRelativePath( baseDir, profileDir, fileAA, aaNext );
        assertSameRelativePath( baseDir, profileDir, fileBA, baNext );
        assertSameRelativePath( baseDir, profileDir, fileA, aNext );
    }

    private void assertSameRelativePath( Path baseDir, Path profileDir, Path mappedFile, Profile profile )
    {
        Path fileRelativePath = baseDir.relativize( mappedFile ).getParent();
        Path profileRelativePath = profileDir.relativize( profile.file() ).getParent();
        assertEquals( fileRelativePath, profileRelativePath,
                "expected relative path to be equal but was " + fileRelativePath + " and " + profileRelativePath );
    }

    @Test
    void profilesMustSortByPagedFileAndProfileSequenceId()
    {
        Path baseDir = Path.of( "baseDir" );
        Path fileAA = baseDir.resolve( "aa" );
        Path fileAB = baseDir.resolve( "ab" );
        Path fileBA = baseDir.resolve( "ba" );
        Profile aa;
        Profile ab;
        Profile ba;
        List<Profile> sortedProfiles = Arrays.asList(
                aa = Profile.first( baseDir, fileAA ),
                aa = aa.next(), aa = aa.next(), aa = aa.next(), aa = aa.next(), aa = aa.next(),
                aa = aa.next(), aa = aa.next(), aa = aa.next(), aa = aa.next(), aa = aa.next(),
                aa = aa.next(), aa = aa.next(), aa = aa.next(), aa = aa.next(), aa = aa.next(),
                aa = aa.next(), aa = aa.next(), aa = aa.next(), aa = aa.next(), aa.next(),
                ab = Profile.first( baseDir, fileAB ),
                ab = ab.next(), ab = ab.next(), ab = ab.next(), ab = ab.next(), ab = ab.next(),
                ab = ab.next(), ab = ab.next(), ab = ab.next(), ab = ab.next(), ab = ab.next(),
                ab = ab.next(), ab = ab.next(), ab = ab.next(), ab = ab.next(), ab = ab.next(),
                ab = ab.next(), ab = ab.next(), ab = ab.next(), ab = ab.next(), ab.next(),
                ba = Profile.first( baseDir, fileBA ),
                ba = ba.next(), ba = ba.next(), ba = ba.next(), ba = ba.next(), ba = ba.next(),
                ba = ba.next(), ba = ba.next(), ba = ba.next(), ba = ba.next(), ba = ba.next(),
                ba = ba.next(), ba = ba.next(), ba = ba.next(), ba = ba.next(), ba = ba.next(),
                ba = ba.next(), ba = ba.next(), ba = ba.next(), ba = ba.next(), ba.next()
        );
        List<Profile> resortedProfiles = new ArrayList<>( sortedProfiles );
        Collections.shuffle( resortedProfiles );
        Collections.sort( resortedProfiles );
        assertThat( resortedProfiles ).isEqualTo( sortedProfiles );
    }

    @Test
    void canPrefetchOneFile() throws IOException
    {
        int numPages = 7;

        try ( PageCache pageCache = pageCacheExtension.getPageCache( fs, cfg ) )
        {
            int pageSize = pageCache.pageSize();
            Path testFile = testDirectory.createFile( "testfile" );
            fs.write( testFile ).writeAll( ByteBuffer.wrap( new byte[ numPages * pageSize] ) );

            try ( PagedFile ignore = pageCache.map( testFile, pageCache.pageSize(), immutable.of( CREATE ) ) )
            {

                Config config = Config.newBuilder()
                        .set( pagecache_warmup_prefetch, true )
                        .build();
                PageCacheWarmer warmer = createPageCacheWarmer( pageCache, config, log );
                warmer.start();
                long pagesLoadedReportedByWarmer = warmer.reheat().orElse( -1 );
                warmer.stop();

                assertEquals( numPages, cacheTracer.faults() );
                assertEquals( numPages, pagesLoadedReportedByWarmer );
            }
        }
    }

    @Test
    void canPrefetchMultipleFiles() throws IOException
    {
        int numPagesFile1 = 7;
        int numPagesFile2 = 3;

        try ( PageCache pageCache = pageCacheExtension.getPageCache( fs, cfg ) )
        {
            int pageSize = pageCache.pageSize();
            Path testFile1 = testDirectory.createFile( "testfile1" );
            Path testFile2 = testDirectory.createFile( "testfile2" );
            fs.write( testFile1 ).writeAll( ByteBuffer.wrap( new byte[ numPagesFile1 * pageSize] ) );
            fs.write( testFile2 ).writeAll( ByteBuffer.wrap( new byte[ numPagesFile2 * pageSize] ) );

            try ( PagedFile pf1 = pageCache.map( testFile1, pageCache.pageSize(), immutable.of( READ ) );
                  PagedFile pf2 = pageCache.map( testFile2, pageCache.pageSize(), immutable.of( READ ) ) )
            {
                Config config = Config.defaults( pagecache_warmup_prefetch, true );
                PageCacheWarmer warmer = createPageCacheWarmer( pageCache, config, log );
                warmer.start();
                long pagesLoadedReportedByWarmer = warmer.reheat().orElse( -1 );
                warmer.stop();

                assertEquals( numPagesFile1 + numPagesFile2, cacheTracer.faults() );
                assertEquals( numPagesFile1 + numPagesFile2, pagesLoadedReportedByWarmer );
            }
        }
    }

    @Test
    void canFilterPrefetchFiles() throws IOException
    {
        int numPagesFile1 = 7;
        int numPagesFile2 = 3;
        int numPagesFile3 = 5;

        try ( PageCache pageCache = pageCacheExtension.getPageCache( fs, cfg ) )
        {
            int pageSize = pageCache.pageSize();
            Path testFile1 = testDirectory.createFile( "testfile1.taken" );
            Path testFile2 = testDirectory.createFile( "testfile.ignored" );
            Path testFile3 = testDirectory.createFile( "testfile2.taken" );
            fs.write( testFile1 ).writeAll( ByteBuffer.wrap( new byte[ numPagesFile1 * pageSize] ) );
            fs.write( testFile2 ).writeAll( ByteBuffer.wrap( new byte[ numPagesFile2 * pageSize] ) );
            fs.write( testFile3 ).writeAll( ByteBuffer.wrap( new byte[ numPagesFile3 * pageSize] ) );

            try ( PagedFile pf1 = pageCache.map( testFile1, pageCache.pageSize() );
                  PagedFile pf2 = pageCache.map( testFile2, pageCache.pageSize() );
                  PagedFile pf3 = pageCache.map( testFile3, pageCache.pageSize() ) )
            {

                Config config = Config.newBuilder()
                        .set( pagecache_warmup_prefetch, true )
                        .set( pagecache_warmup_prefetch_allowlist, "\\.taken" )
                        .build();
                PageCacheWarmer warmer = createPageCacheWarmer( pageCache, config, log );
                warmer.start();
                long pagesLoadedReportedByWarmer = warmer.reheat().orElse( -1 );
                warmer.stop();

                assertEquals( numPagesFile1 + numPagesFile3, cacheTracer.faults() );
                assertEquals( numPagesFile1 + numPagesFile3, pagesLoadedReportedByWarmer );

            }
        }
    }

    @Test
    void willLogPrefetchInfo() throws IOException
    {
        try ( PageCache pageCache = pageCacheExtension.getPageCache( fs, cfg ) )
        {
            var logProvider = new AssertableLogProvider();
            var log = logProvider.getLog( PageCacheWarmer.class );

            Path testfile = testDirectory.createFile( "testfile" );
            try ( PagedFile ignore = pageCache.map( testfile, pageCache.pageSize(), immutable.of( CREATE ) ) )
            {

                Config config = Config.newBuilder()
                        .set( pagecache_warmup_prefetch, true )
                        .build();
                PageCacheWarmer warmer = createPageCacheWarmer( pageCache, config, log );
                warmer.start();
                warmer.reheat();
                warmer.stop();
            }
            assertThat( logProvider ).forClass( PageCacheWarmer.class ).forLevel( INFO ).containsMessageWithArguments(
                    "Warming up page cache by pre-fetching files matching regex: %s", pagecache_warmup_prefetch_allowlist.defaultValue() );
            assertThat( logProvider ).forClass( PageCacheWarmer.class ).forLevel( DEBUG )
                    .containsMessageWithArguments(  "Pre-fetching %s", testfile.getFileName() );
            assertThat( logProvider ).forClass( PageCacheWarmer.class ).forLevel( INFO ).containsMessages( "Warming of page cache completed" );
        }
    }

    @Test
    void isStoppableWhileReheating() throws IOException, ExecutionException, InterruptedException
    {
        try ( PageCache pageCacheOrig = pageCacheExtension.getPageCache( fs, cfg ) )
        {
            //Setup
            PageCache pageCache = spy( pageCacheOrig );
            PagedFile pagedFile = mock( PagedFile.class );
            PageCursor cursor = mock( PageCursor.class );
            doReturn( List.of( pagedFile ) ).when( pageCache ).listExistingMappings();
            when( pagedFile.io( eq( 0L ), eq( PF_READ_AHEAD | PF_SHARED_READ_LOCK ), any( PageCursorTracer.class ) ) ).thenReturn( cursor );
            when( pagedFile.path() ).thenReturn( Path.of( "testfile" ) );

            AtomicBoolean startedFetching = new AtomicBoolean( false );
            Semaphore lock = new Semaphore( 0 );
            when( cursor.next() ).then( invocationOnMock ->
            {
                if ( !startedFetching.getAndSet( true ) )
                {
                    lock.release(); //we are fetching stuff inside reheat(), ready to be stopped
                }
                Thread.sleep( 100 );
                return true;
            } );

            Config config = Config.defaults( pagecache_warmup_prefetch, true );
            PageCacheWarmer warmer = createPageCacheWarmer( pageCache, config, log );

            warmer.start();
            JobHandle handle = scheduler.schedule( Group.FILE_IO_HELPER, JobMonitoringParams.NOT_MONITORED, () ->
            {
                try
                {
                    long pagesLoadedReportedByWarmer = warmer.reheat().orElse( -1 );
                    assertThat( pagesLoadedReportedByWarmer ).isGreaterThan( 0L );
                }
                catch ( IOException e )
                {
                    lock.release(); //Release here to avoid deadlock if something goes wrong.
                    throw new RuntimeException( "Failed during reheat", e );
                }
            } );
            lock.acquire(); //wait until we are warming
            warmer.stop();
            handle.waitTermination();
        }
    }

    @Test
    void shouldOnlyWarmupWhatFitsInPageCache() throws IOException
    {
        //Given
        PageCacheTracer largeCacheTracer = new DefaultPageCacheTracer();
        PageCacheConfig largePageCache = PageCacheConfig.config().withMemory( "3M" ).withTracer( largeCacheTracer );
        PageCacheConfig smallPageCache = PageCacheConfig.config().withMemory( "1M" ).withTracer( this.cacheTracer );

        //When
        long largePageCacheMaxPages;
        try ( PageCache pageCache = pageCacheExtension.getPageCache( fs, largePageCache );
                PagedFile pf = pageCache.map( file, pageCache.pageSize(), immutable.of( CREATE ) ) )
        {
            try ( var tracer = largeCacheTracer.createPageCursorTracer( "mustReheatProfiledPageCache" );
                    PageCursor writer = pf.io( 0, PagedFile.PF_SHARED_WRITE_LOCK, tracer ) )
            {
                for ( int i = 0; i < pageCache.maxCachedPages(); i++ )
                {
                    writer.next( i );
                    writer.putLong( -1 );
                }
            }
            pf.flushAndForce();
            PageCacheWarmer warmer = createPageCacheWarmer( pageCache, Config.defaults(), log );
            warmer.start();
            warmer.profile();
            warmer.stop();

            largePageCacheMaxPages = pageCache.maxCachedPages();
            assertThat( largePageCacheMaxPages ).isEqualTo( largeCacheTracer.faults() );
        }

        //Then (profile will contain all pages, load in a smaller cache)
        long smallPageCacheMaxPages;
        try ( PageCache pageCache = pageCacheExtension.getPageCache( fs, smallPageCache );
                PagedFile pf = pageCache.map( file, pageCache.pageSize(), immutable.of( CREATE ) ) )
        {
            PageCacheWarmer warmer = createPageCacheWarmer( pageCache, Config.defaults(), log );
            warmer.start();
            long pagesReportedLoaded = warmer.reheat().orElse( 0 );
            warmer.stop();

            smallPageCacheMaxPages = pageCache.maxCachedPages();
            assertThat( pagesReportedLoaded ).isEqualTo( smallPageCacheMaxPages );
            assertThat( cacheTracer.faults() ).isEqualTo( smallPageCacheMaxPages );
        }

        assertThat( smallPageCacheMaxPages ).isEqualTo( largePageCacheMaxPages / 3 );
    }

    private PageCacheWarmer createPageCacheWarmer( PageCache pageCache, Config defaults, Log log )
    {
        return new PageCacheWarmer( fs, pageCache, scheduler, testDirectory.homePath(), "test database", defaults, log, tracers );
    }

    private void assertFilesExists( List<StoreFileMetadata> fileListing )
    {
        for ( StoreFileMetadata fileMetadata : fileListing )
        {
            assertTrue( fs.fileExists( fileMetadata.path() ) );
        }
    }

    private void assertFilesNotExists( List<StoreFileMetadata> fileListing )
    {
        for ( StoreFileMetadata fileMetadata : fileListing )
        {
            assertFalse( fs.fileExists( fileMetadata.path() ) );
        }
    }

    private static int[] randomSortedPageIds( int maxPagesInMemory )
    {
        MutableIntSet setIds = new IntHashSet();
        ThreadLocalRandom rng = ThreadLocalRandom.current();
        for ( int i = 0; i < maxPagesInMemory; i++ )
        {
            setIds.add( rng.nextInt( maxPagesInMemory * 7 ) );
        }
        int[] pageIds = new int[setIds.size()];
        IntIterator itr = setIds.intIterator();
        int i = 0;
        while ( itr.hasNext() )
        {
            pageIds[i] = itr.next();
            i++;
        }
        Arrays.sort( pageIds );
        return pageIds;
    }
}
