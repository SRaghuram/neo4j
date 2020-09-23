/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.batchinsert;

import com.neo4j.test.TestEnterpriseDatabaseManagementServiceBuilder;
import com.neo4j.test.TestWithRecordFormats;

import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.batchinsert.BatchInserter;
import org.neo4j.batchinsert.BatchInserters;
import org.neo4j.collection.PrimitiveLongResourceIterator;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.GraphDatabaseSettings.LogQueryLevel;
import org.neo4j.counts.CountsStore;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.internal.index.label.LabelScanStore;
import org.neo4j.internal.index.label.RelationshipTypeScanStore;
import org.neo4j.internal.index.label.RelationshipTypeScanStoreSettings;
import org.neo4j.internal.index.label.TokenScanStore;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.impl.MyRelTypes;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.collection.PrimitiveLongCollections.count;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.internal.helpers.collection.Iterables.single;
import static org.neo4j.internal.helpers.collection.MapUtil.map;
import static org.neo4j.token.api.TokenConstants.ANY_LABEL;
import static org.neo4j.token.api.TokenConstants.ANY_RELATIONSHIP_TYPE;

/**
 * Just testing the {@link BatchInserter} in an enterprise setting, i.e. with all packages and extensions
 * that exist in enterprise edition.
 */
@TestDirectoryExtension
class BatchInsertEnterpriseIT
{
    @Inject
    private TestDirectory directory;
    @Inject
    private FileSystemAbstraction fs;

    private DatabaseManagementService managementService;

    @TestWithRecordFormats
    void shouldInsertDifferentTypesOfThings( String recordFormat ) throws Exception
    {
        DatabaseLayout layout = Neo4jLayout.of( directory.homePath() ).databaseLayout( DEFAULT_DATABASE_NAME );
        // GIVEN
        Config config = Config.newBuilder()
                              .set( GraphDatabaseSettings.log_queries, LogQueryLevel.INFO )
                              .set( GraphDatabaseSettings.record_format, recordFormat )
                              .set( GraphDatabaseSettings.log_queries_filename, directory.file( "query.log" ).toAbsolutePath() ).build();
        BatchInserter inserter = BatchInserters.inserter( layout, fs, config );
        long node1Id;
        long node2Id;
        long relationshipId;
        try
        {
            // WHEN
            node1Id = inserter.createNode( someProperties( 1 ), Labels.values() );
            node2Id = node1Id + 10;
            inserter.createNode( node2Id, someProperties( 2 ), Labels.values() );
            relationshipId = inserter.createRelationship( node1Id, node2Id, MyRelTypes.TEST, someProperties( 3 ) );
            inserter.createDeferredSchemaIndex( Labels.One ).on( "key" ).create();
            inserter.createDeferredConstraint( Labels.Two ).assertPropertyIsUnique( "key" ).create();
        }
        finally
        {
            inserter.shutdown();
        }

        // THEN
        GraphDatabaseService db = newDb( directory.homePath(), config );

        try ( Transaction tx = db.beginTx() )
        {
            Node node1 = tx.getNodeById( node1Id );
            Node node2 = tx.getNodeById( node2Id );
            assertEquals( someProperties( 1 ), node1.getAllProperties() );
            assertEquals( someProperties( 2 ), node2.getAllProperties() );
            assertEquals( relationshipId, single( node1.getRelationships() ).getId() );
            assertEquals( relationshipId, single( node2.getRelationships() ).getId() );
            assertEquals( someProperties( 3 ), single( node1.getRelationships() ).getAllProperties() );
            tx.commit();
        }
        finally
        {
            managementService.shutdown();
        }
    }

    @TestWithRecordFormats
    void insertIntoExistingDatabase( String recordFormat ) throws IOException
    {
        Path storeDir = directory.homePath();
        Config config = Config.defaults();
        config.set( GraphDatabaseSettings.record_format, recordFormat );
        config.set( RelationshipTypeScanStoreSettings.enable_relationship_type_scan_store, true );

        GraphDatabaseService db = newDb( storeDir, config );
        try
        {
            createTwoNodesWithRelationship( db );
        }
        finally
        {
            managementService.shutdown();
        }

        DatabaseLayout layout = Neo4jLayout.of( directory.homePath() ).databaseLayout( DEFAULT_DATABASE_NAME );

        BatchInserter inserter = BatchInserters.inserter( layout, fs, config );
        try
        {
            long start = inserter.createNode( someProperties( 5 ), Labels.One );
            long end = inserter.createNode( someProperties( 5 ), Labels.One );
            inserter.createRelationship( start, end, MyRelTypes.TEST, someProperties( 5 ) );
        }
        finally
        {
            inserter.shutdown();
        }

        db = newDb( storeDir, config );
        try
        {
            verifyNodeCount( db, 4 );
            verifyRelationshipCount( db, 2 );
        }
        finally
        {
            managementService.shutdown();
        }
    }

    @TestWithRecordFormats
    void shouldCorrectlyUpdateCountsAndScanStores( String recordFormat ) throws IOException
    {
        Path storeDir = directory.homePath();
        Config config = Config.defaults();
        config.set( GraphDatabaseSettings.record_format, recordFormat );
        config.set( RelationshipTypeScanStoreSettings.enable_relationship_type_scan_store, true );

        GraphDatabaseService db = newDb( storeDir, config );
        try
        {
            createTwoNodesWithRelationship( db );
        }
        finally
        {
            managementService.shutdown();
        }

        DatabaseLayout layout = Neo4jLayout.of( directory.homePath() ).databaseLayout( DEFAULT_DATABASE_NAME );

        BatchInserter inserter = BatchInserters.inserter( layout, fs, config );
        try
        {
            long start = inserter.createNode( someProperties( 5 ), Labels.One );
            long end = inserter.createNode( someProperties( 5 ), Labels.One );
            inserter.createRelationship( start, end, MyRelTypes.TEST, someProperties( 5 ) );
        }
        finally
        {
            inserter.shutdown();
        }

        db = newDb( storeDir, config );
        try
        {
            verifyCountsStore( db, 4, 2 );

            int labelOneId;
            int labelTwoId;
            int testTypeId;
            try ( Transaction tx = db.beginTx() )
            {
                TokenRead tokenRead = ((InternalTransaction) tx).kernelTransaction().tokenRead();
                labelOneId = tokenRead.nodeLabel( Labels.One.name() );
                labelTwoId = tokenRead.nodeLabel( Labels.Two.name() );
                testTypeId = tokenRead.relationshipType( MyRelTypes.TEST.name() );
                tx.commit();
            }
            Map<Integer,Integer> nodeCountPerLabel = new HashMap<>();
            Map<Integer,Integer> relationshipCountPerType = new HashMap<>();
            nodeCountPerLabel.put( labelOneId, 3 );
            nodeCountPerLabel.put( labelTwoId, 1 );
            relationshipCountPerType.put( testTypeId, 2 );

            verifyScanStore( db, nodeCountPerLabel, LabelScanStore.class );
            verifyScanStore( db, relationshipCountPerType, RelationshipTypeScanStore.class );
        }
        finally
        {
            managementService.shutdown();
        }
    }

    private static void verifyNodeCount( GraphDatabaseService db, int expectedNodeCount )
    {
        try ( Transaction tx = db.beginTx() )
        {
            assertEquals( expectedNodeCount, Iterables.count( tx.getAllNodes() ) );
            tx.commit();
        }
    }

    private static void verifyRelationshipCount( GraphDatabaseService db, int expectedRelationshipCount )
    {
        try ( Transaction tx = db.beginTx() )
        {
            CountsStore countsStore = ((GraphDatabaseAPI) db).getDependencyResolver().resolveDependency( CountsStore.class );
            assertEquals( expectedRelationshipCount, Iterables.count( tx.getAllRelationships() ) );
            tx.commit();
        }
    }

    private static void verifyCountsStore( GraphDatabaseService db, int expectedNodeCount, int expectedRelationshipCount )
    {
        try ( Transaction tx = db.beginTx() )
        {
            CountsStore countsStore = ((GraphDatabaseAPI) db).getDependencyResolver().resolveDependency( CountsStore.class );
            long actualNodeCount = countsStore.nodeCount( ANY_LABEL, PageCursorTracer.NULL );
            long actualRelationshipCount = countsStore.relationshipCount( ANY_LABEL, ANY_RELATIONSHIP_TYPE, ANY_LABEL, PageCursorTracer.NULL );
            assertEquals( expectedNodeCount, actualNodeCount );
            assertEquals( expectedRelationshipCount, actualRelationshipCount );
            tx.commit();
        }
    }

    private static void verifyScanStore( GraphDatabaseService db, Map<Integer,Integer> entryCountPerToken, Class<? extends TokenScanStore> clazz )
    {
        TokenScanStore labelScanStore = ((GraphDatabaseAPI) db).getDependencyResolver().resolveDependency( clazz );

        for ( Map.Entry<Integer,Integer> countForToken : entryCountPerToken.entrySet() )
        {
            try ( PrimitiveLongResourceIterator entries = labelScanStore.newReader().entitiesWithToken( countForToken.getKey(), PageCursorTracer.NULL ) )
            {
                int actualCount = count( entries );
                assertEquals( countForToken.getValue(), actualCount );
            }
        }
    }

    private static void createTwoNodesWithRelationship( GraphDatabaseService db )
    {
        try ( Transaction tx = db.beginTx() )
        {
            Node start = tx.createNode( Labels.One );
            someProperties( 5 ).forEach( start::setProperty );

            Node end = tx.createNode( Labels.Two );
            someProperties( 5 ).forEach( end::setProperty );

            Relationship rel = start.createRelationshipTo( end, MyRelTypes.TEST );
            someProperties( 5 ).forEach( rel::setProperty );

            tx.commit();
        }
    }

    private static Map<String,Object> someProperties( int id )
    {
        return map( "key", "value" + id, "number", 10 + id );
    }

    private GraphDatabaseService newDb( Path storeDir, Config config )
    {
        managementService = new TestEnterpriseDatabaseManagementServiceBuilder( storeDir )
                .setConfig( config )
                .build();
        return managementService.database( DEFAULT_DATABASE_NAME );
    }

    private enum Labels implements Label
    {
        One,
        Two
    }
}
