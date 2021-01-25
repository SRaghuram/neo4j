/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.storecopy;

import com.neo4j.test.extension.EnterpriseDbmsExtension;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;

import org.neo4j.configuration.Config;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseFile;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.impl.index.schema.RelationshipTypeScanStoreSettings;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.internal.NativeIndexFileFilter;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.ExtensionCallback;
import org.neo4j.test.extension.Inject;

import static java.util.Collections.disjoint;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;
import static org.eclipse.collections.impl.factory.Sets.intersect;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.SchemaIndex.NATIVE30;
import static org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer.NULL;

@EnterpriseDbmsExtension( configurationCallback = "configure" )
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
    @Inject
    private Config config;

    @ExtensionCallback
    void configure( TestDatabaseManagementServiceBuilder builder )
    {
        doConfigure( builder );
    }

    void doConfigure( TestDatabaseManagementServiceBuilder builder )
    {   // no-op
    }

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
        try ( var prepareStoreCopyFiles = newPrepareStoreCopyFiles() )
        {
            // given
            createSchemaAndData( db );

            // when
            var atomicFiles = atomicFiles( prepareStoreCopyFiles );
            var replayableFiles = replayableFiles( prepareStoreCopyFiles );

            // then
            assertContainsAllExpectedGBPTreeFiles( atomicFiles );
            assertContainsNoGBPTreeFiles( replayableFiles );
        }
    }

    private void assertContainsAllExpectedGBPTreeFiles( Set<Path> atomicFiles )
    {
        DatabaseLayout layout = db.databaseLayout();
        NativeIndexFileFilter nativeIndexFileFilter = getNativeIndexFileFilter();

        // - Label index
        // - Index statistics store
        // - Counts store
        assertThat( atomicFiles ).contains( layout.labelScanStore(), layout.indexStatisticsStore(), layout.countStore() );

        // - RelationshipType index
        if ( config.get( RelationshipTypeScanStoreSettings.enable_relationship_type_scan_store ) )
        {
            assertThat( atomicFiles ).contains( layout.relationshipTypeScanStore() );
        }

        // - .id files
        assertThat( atomicFiles ).contains( layout.idFiles().toArray( new Path[0] ) );

        // - Native indexes
        assertThat( atomicFiles ).filteredOn( nativeIndexFileFilter ).isNotEmpty();
    }

    private void assertContainsNoGBPTreeFiles( Set<Path> files )
    {
        NativeIndexFileFilter nativeIndexFileFilter = getNativeIndexFileFilter();
        for ( Path file : files )
        {
            // What we know today
            assertFalse( isKnownGBPTreeFile( nativeIndexFileFilter, db.databaseLayout(), file ) );
            // Future-proofness, sort of
            assertFalse( fileContentsLooksLikeAGBPTree( file, pageCache ) );
        }
    }

    private NativeIndexFileFilter getNativeIndexFileFilter()
    {
        return new NativeIndexFileFilter( db.databaseLayout().databaseDirectory() );
    }

    private static boolean fileContentsLooksLikeAGBPTree( Path file, PageCache pageCache )
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

    private static boolean isKnownGBPTreeFile( NativeIndexFileFilter nativeIndexFileFilter, DatabaseLayout databaseLayout, Path file )
    {
        String name = file.getFileName().toString();
        return name.equals( DatabaseFile.LABEL_SCAN_STORE.getName() ) ||
                name.equals( DatabaseFile.RELATIONSHIP_TYPE_SCAN_STORE.getName() ) ||
                name.equals( DatabaseFile.COUNTS_STORE.getName() ) ||
                name.equals( DatabaseFile.INDEX_STATISTICS_STORE.getName() ) ||
                nativeIndexFileFilter.test( file ) ||
                databaseLayout.idFiles().contains( file );
    }

    private PrepareStoreCopyFiles newPrepareStoreCopyFiles()
    {
        return new PrepareStoreCopyFiles( database, fileSystem );
    }

    private static Set<Path> atomicFiles( PrepareStoreCopyFiles prepareStoreCopyFiles ) throws IOException
    {
        var atomicFilesList = Arrays.stream( prepareStoreCopyFiles.getAtomicFilesSnapshot() )
                .map( StoreResource::path )
                .collect( toList() );

        var atomicFilesSet = Set.copyOf( atomicFilesList );
        assertEquals( atomicFilesList.size(), atomicFilesSet.size() );
        return atomicFilesSet;
    }

    private static Set<Path> replayableFiles( PrepareStoreCopyFiles prepareStoreCopyFiles ) throws IOException
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
