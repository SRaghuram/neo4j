/*
 * Copyright (c) 2002-2019 "Neo4j,"
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

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
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
import org.neo4j.io.pagecache.tracing.cursor.DefaultPageCursorTracerSupplier;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.Log;
import org.neo4j.logging.NullLog;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobHandle;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.StoreFileMetadata;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.pagecache.PageCacheSupportExtension;
import org.neo4j.test.extension.testdirectory.EphemeralTestDirectoryExtension;
import org.neo4j.test.rule.PageCacheConfig;
import org.neo4j.test.rule.TestDirectory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.neo4j.configuration.GraphDatabaseSettings.pagecache_warmup_prefetch;
import static org.neo4j.configuration.GraphDatabaseSettings.pagecache_warmup_prefetch_whitelist;
import static org.neo4j.io.pagecache.PagedFile.PF_READ_AHEAD;
import static org.neo4j.io.pagecache.PagedFile.PF_SHARED_READ_LOCK;
import static org.neo4j.kernel.impl.scheduler.JobSchedulerFactory.createScheduler;
import static org.neo4j.logging.AssertableLogProvider.inLog;

@EphemeralTestDirectoryExtension
@Disabled
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
    private DefaultPageCacheTracer cacheTracer;
    private DefaultPageCursorTracerSupplier cursorTracer;
    private PageCacheConfig cfg;
    private File file;
    private Log log = NullLog.getInstance();

    @BeforeEach
    void setUp() throws IOException
    {
        life = new LifeSupport();
        scheduler = life.add( createScheduler() );
        life.start();
        cacheTracer = DefaultPageCacheTracer.TRACER;
        cursorTracer = DefaultPageCursorTracerSupplier.TRACER_SUPPLIER;
        clearTracerCounts();
        cfg = PageCacheConfig.config().withTracer( cacheTracer );
        file = new File( testDirectory.homeDir(), "a" );
        fs.write( file );
    }

    @AfterEach
    void tearDown()
    {
        life.shutdown();
    }

    private void clearTracerCounts()
    {
        cursorTracer.get().reportEvents();
    }

    @Test
    void doNotReheatAfterStop() throws IOException
    {
        try ( PageCache pageCache = pageCacheExtension.getPageCache( fs, cfg );
              PagedFile ignore = pageCache.map( file, pageCache.pageSize(), StandardOpenOption.CREATE ) )
        {
            PageCacheWarmer warmer = new PageCacheWarmer( fs, pageCache, scheduler, testDirectory.homeDir(), Config.defaults(), log );
            warmer.start();
            warmer.stop();
            assertSame( OptionalLong.empty(), warmer.reheat() );
        }
    }

    @Test
    void doNoProfileAfterStop() throws IOException
    {
        try ( PageCache pageCache = pageCacheExtension.getPageCache( fs, cfg );
              PagedFile ignore = pageCache.map( file, pageCache.pageSize(), StandardOpenOption.CREATE ) )
        {
            PageCacheWarmer warmer = new PageCacheWarmer( fs, pageCache, scheduler, testDirectory.homeDir(), Config.defaults(), log );
            warmer.start();
            warmer.stop();
            assertSame( OptionalLong.empty(), warmer.profile() );
        }
    }

    @Test
    void profileAndReheatAfterRestart() throws IOException
    {
        try ( PageCache pageCache = pageCacheExtension.getPageCache( fs, cfg );
              PagedFile pf = pageCache.map( file, pageCache.pageSize(), StandardOpenOption.CREATE ) )
        {
            PageCacheWarmer warmer = new PageCacheWarmer( fs, pageCache, scheduler, testDirectory.homeDir(), Config.defaults(), log );
            warmer.start();
            warmer.stop();
            warmer.start();
            try ( PageCursor writer = pf.io( 0, PagedFile.PF_SHARED_WRITE_LOCK, cursorTracer.get() ) )
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
              PagedFile ignore = pageCache.map( file, pageCache.pageSize(), StandardOpenOption.CREATE ) )
        {
            PageCacheWarmer warmer = new PageCacheWarmer( fs, pageCache, scheduler, testDirectory.homeDir(), Config.defaults(), log );
            warmer.reheat();
        }
        cursorTracer.get().reportEvents();
        assertThat( cacheTracer.faults() ).isEqualTo( 0L );
    }

    @Test
    void mustReheatProfiledPageCache() throws Exception
    {
        try ( PageCache pageCache = pageCacheExtension.getPageCache( fs, cfg );
              PagedFile pf = pageCache.map( file, pageCache.pageSize(), StandardOpenOption.CREATE ) )
        {
            try ( PageCursor writer = pf.io( 0, PagedFile.PF_SHARED_WRITE_LOCK, cursorTracer.get() ) )
            {
                assertTrue( writer.next( 1 ) );
                assertTrue( writer.next( 3 ) );
            }
            pf.flushAndForce();
            PageCacheWarmer warmer = new PageCacheWarmer( fs, pageCache, scheduler, testDirectory.homeDir(), Config.defaults(), log );
            warmer.start();
            warmer.profile();
        }

        clearTracerCounts();
        long initialFaults = cacheTracer.faults();
        try ( PageCache pageCache = pageCacheExtension.getPageCache( fs, cfg );
              PagedFile pf = pageCache.map( file, pageCache.pageSize() ) )
        {
            PageCacheWarmer warmer = new PageCacheWarmer( fs, pageCache, scheduler, testDirectory.homeDir(), Config.defaults(), log );
            warmer.start();
            warmer.reheat();

            assertThat( cacheTracer.faults() ).isEqualTo( initialFaults + 2L );

            try ( PageCursor reader = pf.io( 0, PagedFile.PF_SHARED_READ_LOCK, cursorTracer.get() ) )
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
              PagedFile pf = pageCache.map( file, pageCache.pageSize(), StandardOpenOption.CREATE ) )
        {
            try ( PageCursor writer = pf.io( 0, PagedFile.PF_SHARED_WRITE_LOCK, cursorTracer.get() ) )
            {
                for ( int pageId : pageIds )
                {
                    assertTrue( writer.next( pageId ) );
                }
            }
            pf.flushAndForce();
            PageCacheWarmer warmer = new PageCacheWarmer( fs, pageCache, scheduler, testDirectory.homeDir(), Config.defaults(), log );
            warmer.profile();
        }

        long initialFaults = cacheTracer.faults();
        clearTracerCounts();
        try ( PageCache pageCache = pageCacheExtension.getPageCache( fs, cfg );
              PagedFile pf = pageCache.map( file, pageCache.pageSize() ) )
        {
            PageCacheWarmer warmer = new PageCacheWarmer( fs, pageCache, scheduler, testDirectory.homeDir(), Config.defaults(), log );
            warmer.start();
            warmer.reheat();

            assertThat( cacheTracer.faults() ).isEqualTo( initialFaults + pageIds.length );

            try ( PageCursor reader = pf.io( 0, PagedFile.PF_SHARED_READ_LOCK, cursorTracer.get() ) )
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
              PagedFile pf = pageCache.map( file, pageCache.pageSize(), StandardOpenOption.CREATE ) )
        {
            try ( PageCursor writer = pf.io( 0, PagedFile.PF_SHARED_WRITE_LOCK, cursorTracer.get() ) )
            {
                assertTrue( writer.next( 1 ) );
                assertTrue( writer.next( 3 ) );
            }
            pf.flushAndForce();
            PageCacheWarmer warmer = new PageCacheWarmer( fs, pageCache, scheduler, testDirectory.homeDir(), Config.defaults(), log );
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
    void profilerMustProfileMultipleFilesWithSameName() throws IOException
    {
        File aaFile = new File( new File( testDirectory.homeDir(), "a" ), "a" );
        File baFile = new File( new File( testDirectory.homeDir(), "b" ), "a" );
        fs.mkdirs( aaFile.getParentFile() );
        fs.mkdirs( baFile.getParentFile() );
        try ( PageCache pageCache = pageCacheExtension.getPageCache( fs, cfg );
              PagedFile aa = pageCache.map( aaFile, pageCache.pageSize(), StandardOpenOption.CREATE );
              PagedFile ba = pageCache.map( baFile, pageCache.pageSize(), StandardOpenOption.CREATE ) )
        {
            PageCacheWarmer warmer = new PageCacheWarmer( fs, pageCache, scheduler, testDirectory.homeDir(), Config.defaults(), log );
            warmer.start();
            warmer.profile();
            List<StoreFileMetadata> fileListing = new ArrayList<>();
            try ( Resource resource = warmer.addFilesTo( fileListing ) )
            {
                List<File> uniqueProfileFiles = fileListing.stream()
                        .map( StoreFileMetadata::file )
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
        File baseDirFile = baseDir.toFile();
        Profile aaa = Profile.first( baseDirFile, fileAAA.toFile() );
        Profile aa = Profile.first( baseDirFile, fileAA.toFile() );
        Profile ba = Profile.first( baseDirFile, fileBA.toFile() );
        Profile a = Profile.first( baseDirFile, fileA.toFile() );
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
        Path profileRelativePath = profileDir.relativize( profile.file().toPath() ).getParent();
        assertEquals( fileRelativePath, profileRelativePath,
                "expected relative path to be equal but was " + fileRelativePath + " and " + profileRelativePath );
    }

    @Test
    void profilesMustSortByPagedFileAndProfileSequenceId()
    {
        File baseDir = new File( "baseDir" );
        File fileAA = new File( baseDir, "aa" );
        File fileAB = new File( baseDir, "ab" );
        File fileBA = new File( baseDir, "ba" );
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
            File testFile = testDirectory.createFile( "testfile" );
            fs.write( testFile ).writeAll( ByteBuffer.wrap( new byte[ numPages * pageSize] ) );

            try ( PagedFile ignore = pageCache.map( testFile, pageCache.pageSize(), StandardOpenOption.CREATE ) )
            {

                Config config = Config.newBuilder()
                        .set( pagecache_warmup_prefetch, true )
                        .build();
                PageCacheWarmer warmer = new PageCacheWarmer( fs, pageCache, scheduler, testDirectory.homeDir(), config, log );
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
            File testFile1 = testDirectory.createFile( "testfile1" );
            File testFile2 = testDirectory.createFile( "testfile2" );
            fs.write( testFile1 ).writeAll( ByteBuffer.wrap( new byte[ numPagesFile1 * pageSize] ) );
            fs.write( testFile2 ).writeAll( ByteBuffer.wrap( new byte[ numPagesFile2 * pageSize] ) );

            try ( PagedFile pf1 = pageCache.map( testFile1, pageCache.pageSize(), StandardOpenOption.READ );
                  PagedFile pf2 = pageCache.map( testFile2, pageCache.pageSize(), StandardOpenOption.READ ) )
            {

                Config config = Config.defaults( pagecache_warmup_prefetch, true );
                PageCacheWarmer warmer = new PageCacheWarmer( fs, pageCache, scheduler, testDirectory.homeDir(), config, log );
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
            File testFile1 = testDirectory.createFile( "testfile1.taken" );
            File testFile2 = testDirectory.createFile( "testfile.ignored" );
            File testFile3 = testDirectory.createFile( "testfile2.taken" );
            fs.write( testFile1 ).writeAll( ByteBuffer.wrap( new byte[ numPagesFile1 * pageSize] ) );
            fs.write( testFile2 ).writeAll( ByteBuffer.wrap( new byte[ numPagesFile2 * pageSize] ) );
            fs.write( testFile3 ).writeAll( ByteBuffer.wrap( new byte[ numPagesFile3 * pageSize] ) );

            try ( PagedFile pf1 = pageCache.map( testFile1, pageCache.pageSize() );
                  PagedFile pf2 = pageCache.map( testFile2, pageCache.pageSize() );
                  PagedFile pf3 = pageCache.map( testFile3, pageCache.pageSize() ) )
            {

                Config config = Config.newBuilder()
                        .set( pagecache_warmup_prefetch, true )
                        .set( pagecache_warmup_prefetch_whitelist, "\\.taken" )
                        .build();
                PageCacheWarmer warmer = new PageCacheWarmer( fs, pageCache, scheduler, testDirectory.homeDir(), config, log );
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

            File testfile = testDirectory.createFile( "testfile" );
            try ( PagedFile ignore = pageCache.map( testfile, pageCache.pageSize(), StandardOpenOption.CREATE ) )
            {

                Config config = Config.newBuilder()
                        .set( pagecache_warmup_prefetch, true )
                        .build();
                PageCacheWarmer warmer = new PageCacheWarmer( fs, pageCache, scheduler, testDirectory.homeDir(), config, log );
                warmer.start();
                warmer.reheat();
                warmer.stop();
            }
            var matcher = inLog( PageCacheWarmer.class );
            logProvider.assertExactly(
                    matcher.info( "Warming up page cache by pre-fetching files matching regex: %s", pagecache_warmup_prefetch_whitelist.defaultValue() ),
                    matcher.debug( "Pre-fetching %s", testfile.getName() ),
                    matcher.info( "Warming of page cache completed" )
            );
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
            when( pagedFile.io( 0, PF_READ_AHEAD | PF_SHARED_READ_LOCK, cursorTracer.get() ) ).thenReturn( cursor );
            when( pagedFile.file() ).thenReturn( new File( "testfile" ) );

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
            PageCacheWarmer warmer = new PageCacheWarmer( fs, pageCache, scheduler, testDirectory.homeDir(), config, log );

            warmer.start();
            JobHandle handle = scheduler.schedule( Group.FILE_IO_HELPER, () ->
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

    private void assertFilesExists( List<StoreFileMetadata> fileListing )
    {
        for ( StoreFileMetadata fileMetadata : fileListing )
        {
            assertTrue( fs.fileExists( fileMetadata.file() ) );
        }
    }

    private void assertFilesNotExists( List<StoreFileMetadata> fileListing )
    {
        for ( StoreFileMetadata fileMetadata : fileListing )
        {
            assertFalse( fs.fileExists( fileMetadata.file() ) );
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
