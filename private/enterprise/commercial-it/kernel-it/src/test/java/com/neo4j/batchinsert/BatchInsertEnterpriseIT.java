/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.batchinsert;

import com.neo4j.kernel.impl.store.format.highlimit.HighLimit;
import com.neo4j.test.TestCommercialDatabaseManagementServiceBuilder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.neo4j.batchinsert.BatchInserter;
import org.neo4j.batchinsert.BatchInserters;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.kernel.impl.MyRelTypes;
import org.neo4j.kernel.impl.store.format.standard.Standard;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;

import static org.junit.Assert.assertEquals;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.internal.helpers.collection.Iterables.single;
import static org.neo4j.internal.helpers.collection.MapUtil.map;
import static org.neo4j.internal.helpers.collection.MapUtil.stringMap;

/**
 * Just testing the {@link BatchInserter} in an enterprise setting, i.e. with all packages and extensions
 * that exist in enterprise edition.
 */
@RunWith( Parameterized.class )
public class BatchInsertEnterpriseIT
{
    @Rule
    public final TestDirectory directory = TestDirectory.testDirectory();
    @Rule
    public final DefaultFileSystemRule fileSystemRule = new DefaultFileSystemRule();

    @Parameter
    public String recordFormat;
    private DatabaseManagementService managementService;

    @Parameters( name = "{0}" )
    public static List<String> recordFormats()
    {
        return Arrays.asList( Standard.LATEST_NAME, HighLimit.NAME );
    }

    @Test
    public void shouldInsertDifferentTypesOfThings() throws Exception
    {
        // GIVEN
        BatchInserter inserter = BatchInserters.inserter( directory.databaseLayout(), fileSystemRule.get(),
                stringMap( GraphDatabaseSettings.log_queries.name(), "true",
                        GraphDatabaseSettings.record_format.name(), recordFormat,
                        GraphDatabaseSettings.log_queries_filename.name(),
                        directory.file( "query.log" ).getAbsolutePath() ) );
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
        DatabaseManagementService managementService = new TestCommercialDatabaseManagementServiceBuilder( directory.storeDir() ).build();
        GraphDatabaseService db = managementService.database( DEFAULT_DATABASE_NAME );

        try ( Transaction tx = db.beginTx() )
        {
            Node node1 = db.getNodeById( node1Id );
            Node node2 = db.getNodeById( node2Id );
            assertEquals( someProperties( 1 ), node1.getAllProperties() );
            assertEquals( someProperties( 2 ), node2.getAllProperties() );
            assertEquals( relationshipId, single( node1.getRelationships() ).getId() );
            assertEquals( relationshipId, single( node2.getRelationships() ).getId() );
            assertEquals( someProperties( 3 ), single( node1.getRelationships() ).getAllProperties() );
            tx.success();
        }
        finally
        {
            managementService.shutdown();
        }
    }

    @Test
    public void insertIntoExistingDatabase() throws IOException
    {
        File storeDir = directory.storeDir();

        GraphDatabaseService db = newDb( storeDir, recordFormat );
        try
        {
            createThreeNodes( db );
        }
        finally
        {
            managementService.shutdown();
        }

        BatchInserter inserter = BatchInserters.inserter( directory.databaseLayout(), fileSystemRule.get() );
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

        db = newDb( storeDir, recordFormat );
        try
        {
            verifyNodeCount( db, 4 );
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
            assertEquals( expectedNodeCount, Iterables.count( db.getAllNodes() ) );
            tx.success();
        }
    }

    private static void createThreeNodes( GraphDatabaseService db )
    {
        try ( Transaction tx = db.beginTx() )
        {
            Node start = db.createNode( Labels.One );
            someProperties( 5 ).forEach( start::setProperty );

            Node end = db.createNode( Labels.Two );
            someProperties( 5 ).forEach( end::setProperty );

            Relationship rel = start.createRelationshipTo( end, MyRelTypes.TEST );
            someProperties( 5 ).forEach( rel::setProperty );

            tx.success();
        }
    }

    private static Map<String,Object> someProperties( int id )
    {
        return map( "key", "value" + id, "number", 10 + id );
    }

    private GraphDatabaseService newDb( File storeDir, String recordFormat )
    {
        managementService = new TestCommercialDatabaseManagementServiceBuilder( storeDir )
                .setConfig( GraphDatabaseSettings.record_format, recordFormat )
                .build();
        return managementService.database( DEFAULT_DATABASE_NAME );
    }

    private enum Labels implements Label
    {
        One,
        Two
    }
}
