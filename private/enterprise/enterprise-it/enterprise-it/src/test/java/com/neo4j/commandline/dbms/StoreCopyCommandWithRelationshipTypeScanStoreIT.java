/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.commandline.dbms;

import com.neo4j.dbms.commandline.StoreCopyCommand;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.neo4j.collection.PrimitiveLongResourceIterator;
import org.neo4j.counts.CountsStore;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.index.schema.RelationshipTypeScanStore;
import org.neo4j.kernel.impl.index.schema.TokenScanReader;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.ExtensionCallback;
import org.neo4j.test.extension.Inject;

import static com.neo4j.commandline.dbms.StoreCopyCommandIT.getCopyName;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.internal.helpers.ArrayUtil.contains;
import static org.neo4j.io.pagecache.tracing.PageCacheTracer.NULL;
import static org.neo4j.kernel.impl.index.schema.RelationshipTypeScanStoreSettings.enable_relationship_type_scan_store;
import static org.neo4j.token.api.TokenConstants.ANY_LABEL;
import static org.neo4j.token.api.TokenConstants.ANY_RELATIONSHIP_TYPE;

public class StoreCopyCommandWithRelationshipTypeScanStoreIT extends AbstractCommandIT
{
    private static final RelationshipType KNOWS = RelationshipType.withName( "KNOWS" );
    private static final RelationshipType LOVES = RelationshipType.withName( "LOVES" );
    @Inject
    private DatabaseLayout databaseLayout;

    @Override
    @ExtensionCallback
    void configuration( TestDatabaseManagementServiceBuilder builder )
    {
        // Enable
        super.configuration( builder );
        builder.setConfig( enable_relationship_type_scan_store, true );
    }

    @Test
    void mustBuildRelationshipTypeScanStoreIfEnabled() throws Exception
    {
        // sanity check
        FileSystemAbstraction fs = testDirectory.getFileSystem();
        assertHasFile( fs, databaseLayout.databaseDirectory(), databaseLayout.relationshipTypeScanStore() );

        // given
        appendConfigSetting( enable_relationship_type_scan_store, true );
        Map<Integer,List<Long>> expectedTypeToRelationships = createRelationshipWithTypes();

        // when
        String databaseName = databaseAPI.databaseName();
        String copyName = getCopyName( databaseName, "copy" );
        managementService.shutdownDatabase( databaseName );
        copyDatabase( "--from-database=" + databaseName, "--to-database=" + copyName );

        // then
        DatabaseLayout copyLayout = neo4jLayout.databaseLayout( copyName );
        assertHasFile( fs, copyLayout.databaseDirectory(), copyLayout.relationshipTypeScanStore() );

        // and
        managementService.createDatabase( copyName );
        GraphDatabaseService copyDb = managementService.database( copyName );
        Map<Integer,List<Long>> contentOfRtss;
        try ( Transaction tx = copyDb.beginTx() )
        {
            RelationshipTypeScanStore rtss = ((GraphDatabaseAPI) copyDb).getDependencyResolver().resolveDependency( RelationshipTypeScanStore.class );
            contentOfRtss = collectRelationshipsFromRtss( expectedTypeToRelationships.keySet(), rtss );
            tx.commit();
        }
        assertEquals( expectedTypeToRelationships, contentOfRtss );
        managementService.dropDatabase( copyName );
    }

    @Test
    void mustCorrectlyBuildRelationshipCountsWithRelationshipTypeScanStoreEnabled() throws Exception
    {
        // given
        appendConfigSetting( enable_relationship_type_scan_store, true );
        Map<Integer,List<Long>> expectedTypeToRelationships = createRelationshipWithTypes();

        // when
        String databaseName = databaseAPI.databaseName();
        String copyName = getCopyName( databaseName, "copy" );
        managementService.shutdownDatabase( databaseName );
        copyDatabase( "--from-database=" + databaseName, "--to-database=" + copyName );

        // then
        DatabaseLayout copyLayout = neo4jLayout.databaseLayout( copyName );
        assertHasFile( fs, copyLayout.databaseDirectory(), copyLayout.countStore() );

        // and
        managementService.createDatabase( copyName );
        GraphDatabaseService copyDb = managementService.database( copyName );
        Map<Integer,List<Long>> contentOfRtss;
        try ( Transaction tx = copyDb.beginTx() )
        {
            CountsStore countsStore = ((GraphDatabaseAPI) copyDb).getDependencyResolver().resolveDependency( CountsStore.class );
            long actualCount = countsStore.relationshipCount( ANY_LABEL, ANY_RELATIONSHIP_TYPE, ANY_LABEL, PageCursorTracer.NULL );
            long expectedCount = expectedTypeToRelationships.values().stream().map( List::size ).reduce( Integer::sum ).orElseThrow();
            assertEquals( expectedCount, actualCount );
            tx.commit();
        }
        managementService.dropDatabase( copyName );
    }

    private Map<Integer,List<Long>> collectRelationshipsFromRtss( Set<Integer> types, RelationshipTypeScanStore rtss )
    {
        Map<Integer,List<Long>> contentOfRtss = new HashMap<>();
        TokenScanReader reader = rtss.newReader();
        for ( Integer token : types )
        {
            contentOfRtss.put( token, relationshipsWithToken( token, reader ) );
        }
        return contentOfRtss;
    }

    private Map<Integer,List<Long>> createRelationshipWithTypes()
    {
        Map<Integer,List<Long>> tokenToRelationships = new HashMap<>();
        try ( Transaction tx = databaseAPI.beginTx() )
        {
            Node a = tx.createNode();
            Node b = tx.createNode();

            List<Long> expectedKnowsList = new ArrayList<>();
            List<Long> expectedLovesList = new ArrayList<>();
            for ( int i = 0; i < 128; i++ )
            {
                expectedKnowsList.add( a.createRelationshipTo( b, KNOWS ).getId() );
                expectedLovesList.add( b.createRelationshipTo( a, LOVES ).getId() );
            }
            expectedKnowsList.sort( Long::compareTo );
            expectedLovesList.sort( Long::compareTo );

            TokenRead tokenRead = ((InternalTransaction) tx).kernelTransaction().tokenRead();
            int knowsToken = tokenRead.relationshipType( KNOWS.name() );
            int lovesToken = tokenRead.relationshipType( LOVES.name() );
            tokenToRelationships.put( knowsToken, expectedKnowsList );
            tokenToRelationships.put( lovesToken, expectedLovesList );
            tx.commit();
        }
        return tokenToRelationships;
    }

    private List<Long> relationshipsWithToken( int token, TokenScanReader reader )
    {
        List<Long> relationships = new ArrayList<>();
        try ( PrimitiveLongResourceIterator knows = reader.entitiesWithToken( token, PageCursorTracer.NULL ) )
        {
            while ( knows.hasNext() )
            {
                relationships.add( knows.next() );
            }
        }
        relationships.sort( Long::compareTo );
        return relationships;
    }

    private void assertHasFile( FileSystemAbstraction fs, Path databaseDirectory, Path file )
    {
        Path[] files = fs.listFiles( databaseDirectory );
        assertTrue( contains( files, file ) );
    }

    private void copyDatabase( String... args ) throws Exception
    {
        var command = new StoreCopyCommand( getExtensionContext() );

        CommandLine.populateCommand( command, args );
        command.setPageCacheTracer( NULL );
        command.execute();
    }
}
