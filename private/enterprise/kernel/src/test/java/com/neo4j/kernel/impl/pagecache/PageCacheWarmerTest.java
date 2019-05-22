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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.OptionalLong;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

import org.neo4j.graphdb.Resource;
import org.neo4j.io.ByteUnit;
import org.neo4j.io.fs.EphemeralFileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.tracing.DefaultPageCacheTracer;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.DefaultPageCursorTracerSupplier;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.StoreFileMetadata;
import org.neo4j.test.extension.EphemeralFileSystemExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.extension.pagecache.PageCacheSupportExtension;
import org.neo4j.test.rule.PageCacheConfig;
import org.neo4j.test.rule.TestDirectory;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.kernel.impl.scheduler.JobSchedulerFactory.createScheduler;

@ExtendWith( {EphemeralFileSystemExtension.class, TestDirectoryExtension.class} )
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

    @BeforeEach
    void setUp() throws IOException
    {
        life = new LifeSupport();
        scheduler = life.add( createScheduler() );
        life.start();
        cacheTracer = new DefaultPageCacheTracer();
        cursorTracer = DefaultPageCursorTracerSupplier.INSTANCE;
        clearTracerCounts();
        cfg = PageCacheConfig.config().withTracer( cacheTracer ).withCursorTracerSupplier( cursorTracer );
        file = new File( testDirectory.storeDir(), "a" );
        fs.write( file );
    }

    @AfterEach
    void tearDown()
    {
        life.shutdown();
    }

    private void clearTracerCounts()
    {
        cursorTracer.get().init( PageCacheTracer.NULL );
        cursorTracer.get().reportEvents();
        cursorTracer.get().init( cacheTracer );
    }

    @Test
    void doNotReheatAfterStop() throws IOException
    {
        try ( PageCache pageCache = pageCacheExtension.getPageCache( fs, cfg );
              PagedFile ignore = pageCache.map( file, pageCache.pageSize(), StandardOpenOption.CREATE ) )
        {
            PageCacheWarmer warmer = new PageCacheWarmer( fs, pageCache, scheduler, testDirectory.storeDir() );
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
            PageCacheWarmer warmer = new PageCacheWarmer( fs, pageCache, scheduler, testDirectory.storeDir() );
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
            PageCacheWarmer warmer = new PageCacheWarmer( fs, pageCache, scheduler, testDirectory.storeDir() );
            warmer.start();
            warmer.stop();
            warmer.start();
            try ( PageCursor writer = pf.io( 0, PagedFile.PF_SHARED_WRITE_LOCK ) )
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
            PageCacheWarmer warmer = new PageCacheWarmer( fs, pageCache, scheduler, testDirectory.storeDir() );
            warmer.reheat();
        }
        cursorTracer.get().reportEvents();
        assertThat( cacheTracer.faults(), is( 0L ) );
    }

    @Test
    void mustReheatProfiledPageCache() throws Exception
    {
        try ( PageCache pageCache = pageCacheExtension.getPageCache( fs, cfg );
              PagedFile pf = pageCache.map( file, pageCache.pageSize(), StandardOpenOption.CREATE ) )
        {
            try ( PageCursor writer = pf.io( 0, PagedFile.PF_SHARED_WRITE_LOCK ) )
            {
                assertTrue( writer.next( 1 ) );
                assertTrue( writer.next( 3 ) );
            }
            pf.flushAndForce();
            PageCacheWarmer warmer = new PageCacheWarmer( fs, pageCache, scheduler, testDirectory.storeDir() );
            warmer.start();
            warmer.profile();
        }

        clearTracerCounts();
        long initialFaults = cacheTracer.faults();
        try ( PageCache pageCache = pageCacheExtension.getPageCache( fs, cfg );
              PagedFile pf = pageCache.map( file, pageCache.pageSize() ) )
        {
            PageCacheWarmer warmer = new PageCacheWarmer( fs, pageCache, scheduler, testDirectory.storeDir() );
            warmer.start();
            warmer.reheat();

            pageCache.reportEvents();
            assertThat( cacheTracer.faults(), is( initialFaults + 2L ) );

            try ( PageCursor reader = pf.io( 0, PagedFile.PF_SHARED_READ_LOCK ) )
            {
                assertTrue( reader.next( 1 ) );
                assertTrue( reader.next( 3 ) );
            }

            // No additional faults must have been reported.
            pageCache.reportEvents();
            assertThat( cacheTracer.faults(), is( initialFaults + 2L ) );
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
            try ( PageCursor writer = pf.io( 0, PagedFile.PF_SHARED_WRITE_LOCK ) )
            {
                for ( int pageId : pageIds )
                {
                    assertTrue( writer.next( pageId ) );
                }
            }
            pf.flushAndForce();
            PageCacheWarmer warmer = new PageCacheWarmer( fs, pageCache, scheduler, testDirectory.storeDir() );
            warmer.profile();
        }

        long initialFaults = cacheTracer.faults();
        clearTracerCounts();
        try ( PageCache pageCache = pageCacheExtension.getPageCache( fs, cfg );
              PagedFile pf = pageCache.map( file, pageCache.pageSize() ) )
        {
            PageCacheWarmer warmer = new PageCacheWarmer( fs, pageCache, scheduler, testDirectory.storeDir() );
            warmer.start();
            warmer.reheat();

            pageCache.reportEvents();
            assertThat( cacheTracer.faults(), is( initialFaults + pageIds.length ) );

            try ( PageCursor reader = pf.io( 0, PagedFile.PF_SHARED_READ_LOCK ) )
            {
                for ( int pageId : pageIds )
                {
                    assertTrue( reader.next( pageId ) );
                }
            }

            // No additional faults must have been reported.
            pageCache.reportEvents();
            assertThat( cacheTracer.faults(), is( initialFaults + pageIds.length ) );
        }
    }

    @SuppressWarnings( "unused" )
    @Test
    void profileMustNotDeleteFilesCurrentlyExposedViaFileListing() throws Exception
    {
        try ( PageCache pageCache = pageCacheExtension.getPageCache( fs, cfg );
              PagedFile pf = pageCache.map( file, pageCache.pageSize(), StandardOpenOption.CREATE ) )
        {
            try ( PageCursor writer = pf.io( 0, PagedFile.PF_SHARED_WRITE_LOCK ) )
            {
                assertTrue( writer.next( 1 ) );
                assertTrue( writer.next( 3 ) );
            }
            pf.flushAndForce();
            PageCacheWarmer warmer = new PageCacheWarmer( fs, pageCache, scheduler, testDirectory.storeDir() );
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
                assertThat( fileListing.size(), greaterThan( 0 ) );
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
        File aaFile = new File( new File( testDirectory.storeDir(), "a" ), "a" );
        File baFile = new File( new File( testDirectory.storeDir(), "b" ), "a" );
        fs.mkdirs( aaFile.getParentFile() );
        fs.mkdirs( baFile.getParentFile() );
        try ( PageCache pageCache = pageCacheExtension.getPageCache( fs, cfg );
              PagedFile aa = pageCache.map( aaFile, pageCache.pageSize(), StandardOpenOption.CREATE );
              PagedFile ba = pageCache.map( baFile, pageCache.pageSize(), StandardOpenOption.CREATE ) )
        {
            PageCacheWarmer warmer = new PageCacheWarmer( fs, pageCache, scheduler, testDirectory.storeDir() );
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
        assertThat( resortedProfiles, is( sortedProfiles ) );
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
