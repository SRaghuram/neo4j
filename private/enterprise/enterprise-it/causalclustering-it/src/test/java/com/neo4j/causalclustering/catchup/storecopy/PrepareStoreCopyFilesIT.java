/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.storecopy;

import com.neo4j.test.extension.EnterpriseDbmsExtension;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseFile;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.internal.NativeIndexFileFilter;
import org.neo4j.test.extension.Inject;

import static java.util.Collections.disjoint;
import static java.util.stream.Collectors.toList;
import static org.eclipse.collections.impl.factory.Sets.intersect;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.SchemaIndex.NATIVE30;
import static org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer.NULL;

@EnterpriseDbmsExtension
class PrepareStoreCopyFilesIT
{
    @Inject
    private GraphDatabaseAPI db;
    @Inject
    private PageCache pageCache;
    @Inject
    private Database database;
    @Inject
    private FileSystemAbstraction fileSystem;

    @Test
    void shouldReturnDifferentAtomicAndReplayableFiles() throws Exception
    {
        try ( var prepareStoreCopyFiles = newPrepareStoreCopyFiles() )
        {
            // given
            createSchemaAndData( db );

            // when
            var atomicFiles = atomicFiles( prepareStoreCopyFiles );
            var replayableFiles = replayableFiles( prepareStoreCopyFiles );

            // then
            assertTrue( disjoint( atomicFiles, replayableFiles ),
                    () -> "Atomic and replayable files contain same elements:\n" + intersect( atomicFiles, replayableFiles ) );
        }
    }

    @Test
    void shouldReturnAllGBPTreeFilesInAtomicSectionNotReplayableSection() throws IOException
    {
        // GBPTree files include:
        // - Label index
        // - Native indexes
        // - .id files (as of 4.0, the IndexedIdGenerator)

        try ( var prepareStoreCopyFiles = newPrepareStoreCopyFiles() )
        {
            // given
            createSchemaAndData( db );

            // when
            var atomicFiles = atomicFiles( prepareStoreCopyFiles );
            var replayableFiles = replayableFiles( prepareStoreCopyFiles );

            // then
            assertContainsSomeGBPTreeFiles( atomicFiles );
            assertContainsNoGBPTreeFiles( replayableFiles );
        }
    }

    private void assertContainsSomeGBPTreeFiles( Set<File> files )
    {
        NativeIndexFileFilter nativeIndexFileFilter = new NativeIndexFileFilter( db.databaseLayout().databaseDirectory() );
        long count = files.stream().filter( file ->
                isKnownGBPTreeFile( nativeIndexFileFilter, db.databaseLayout(), file ) ||
                fileContentsLooksLikeAGBPTree( file, pageCache ) ).count();
        assertThat( count, greaterThan( 0L ) );
    }

    private void assertContainsNoGBPTreeFiles( Set<File> files )
    {
        NativeIndexFileFilter nativeIndexFileFilter = new NativeIndexFileFilter( db.databaseLayout().databaseDirectory() );
        for ( File file : files )
        {
            // What we know today
            assertFalse( isKnownGBPTreeFile( nativeIndexFileFilter, db.databaseLayout(), file ) );
            // Future-proofness, sort of
            assertFalse( fileContentsLooksLikeAGBPTree( file, pageCache ) );
        }
    }

    private static boolean fileContentsLooksLikeAGBPTree( File file, PageCache pageCache )
    {
        try
        {
            MutableBoolean headerRead = new MutableBoolean();
            GBPTree.readHeader( pageCache, file, buffer -> headerRead.setTrue(), NULL );
            return headerRead.booleanValue();
        }
        catch ( Exception e )
        {
            // In addition to IOException and MetaDataMismatchException there could be stuff like IllegalArgumentException from trying to
            // map a file with page size 0 or something like that, because this method is simply trying to open any type of file as a GBPTree
            // file so the contents could be anything.
            return false;
        }
    }

    private static boolean isKnownGBPTreeFile( NativeIndexFileFilter nativeIndexFileFilter, DatabaseLayout databaseLayout, File file )
    {
        String name = file.getName();
        return name.equals( DatabaseFile.LABEL_SCAN_STORE.getName() ) ||
                nativeIndexFileFilter.accept( file ) ||
                databaseLayout.idFiles().contains( file );
    }

    private PrepareStoreCopyFiles newPrepareStoreCopyFiles()
    {
        return new PrepareStoreCopyFiles( database, fileSystem );
    }

    private static Set<File> atomicFiles( PrepareStoreCopyFiles prepareStoreCopyFiles ) throws IOException
    {
        var atomicFilesList = Arrays.stream( prepareStoreCopyFiles.getAtomicFilesSnapshot() )
                .map( StoreResource::file )
                .collect( toList() );

        var atomicFilesSet = Set.copyOf( atomicFilesList );
        assertEquals( atomicFilesList.size(), atomicFilesSet.size() );
        return atomicFilesSet;
    }

    private static Set<File> replayableFiles( PrepareStoreCopyFiles prepareStoreCopyFiles ) throws IOException
    {
        var replayableFilesArray = prepareStoreCopyFiles.listReplayableFiles();
        var replayableFilesSet = Set.of( replayableFilesArray );
        assertEquals( replayableFilesArray.length, replayableFilesSet.size() );
        return replayableFilesSet;
    }

    private static void createSchemaAndData( GraphDatabaseService db )
    {
        try ( Transaction transaction = db.beginTx() )
        {
            transaction.execute( "CREATE INDEX FOR (n:Person) ON (n.id)" ).close();
            transaction.execute( "CALL db.createIndex('person names', ['Person'], ['name'], $provider)",
                    Map.of( "provider", NATIVE30.providerName() ) ).close();
            transaction.execute( "CALL db.index.fulltext.createNodeIndex('nameAndTitle', ['Person'], ['name', 'title'])" ).close();
            transaction.execute( "CALL db.index.fulltext.createRelationshipIndex('description', ['KNOWS'], ['description'])" ).close();
            transaction.commit();
        }
        try ( Transaction transaction = db.beginTx() )
        {
            transaction.execute( "CALL db.awaitIndexes(60)" ).close();

            transaction.execute( "UNWIND range(1, 100) AS x " +
                    "WITH x AS x, x + 1 AS y " +
                    "CREATE (p1:Person {id: x, name: 'name-' + x, title: 'title-' + x})," +
                    "       (p2:Person {id: y, name: 'name-' + y, title: 'title-' + y})," +
                    "       (p1)-[:KNOWS {description: 'description-' + x + '-' + y}]->(p2)" ).close();

            transaction.execute( "CALL db.index.fulltext.awaitEventuallyConsistentIndexRefresh()" ).close();
            transaction.commit();
        }
    }
}
