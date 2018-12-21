/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.kernel.api.impl.fulltext;

import com.neo4j.test.TestCommercialGraphDatabaseFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.neo4j.backup.impl.OnlineBackupContext;
import org.neo4j.backup.impl.OnlineBackupExecutor;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.kernel.configuration.ConnectorPortRegister;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.SuppressOutputExtension;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static java.lang.String.format;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.neo4j.causalclustering.core.TransactionBackupServiceProvider.BACKUP_SERVER_NAME;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.kernel.api.impl.fulltext.FulltextProceduresTest.NODE;
import static org.neo4j.kernel.api.impl.fulltext.FulltextProceduresTest.NODE_CREATE;
import static org.neo4j.kernel.api.impl.fulltext.FulltextProceduresTest.QUERY_NODES;
import static org.neo4j.kernel.api.impl.fulltext.FulltextProceduresTest.QUERY_RELS;
import static org.neo4j.kernel.api.impl.fulltext.FulltextProceduresTest.RELATIONSHIP;
import static org.neo4j.kernel.api.impl.fulltext.FulltextProceduresTest.RELATIONSHIP_CREATE;
import static org.neo4j.kernel.api.impl.fulltext.FulltextProceduresTest.array;

@ExtendWith( {TestDirectoryExtension.class, SuppressOutputExtension.class} )
class FulltextIndexBackupIT
{
    private static final Label LABEL = Label.label( "LABEL" );
    private static final String PROP = "prop";
    private static final RelationshipType REL = RelationshipType.withName( "REL" );
    private static final String NODE_INDEX = "nodeIndex";
    private static final String REL_INDEX = "relIndex";
    private static final String BACKUP_DIR_NAME = "backups";

    @Inject
    private TestDirectory dir;

    private long nodeId1;
    private long nodeId2;
    private long relId1;

    private GraphDatabaseAPI db;
    private GraphDatabaseAPI backupDb;

    @BeforeEach
    void setUp()
    {
        db = (GraphDatabaseAPI) new TestCommercialGraphDatabaseFactory().newEmbeddedDatabase( dir.databaseDir() );
    }

    @AfterEach
    void tearDown()
    {
        if ( db != null )
        {
            db.shutdown();
        }
        if ( backupDb != null )
        {
            backupDb.shutdown();
        }
    }

    @Test
    void fulltextIndexesMustBeTransferredInBackup() throws Exception
    {
        initializeTestData();
        verifyData( db );
        Path backupDir = executeBackup();
        db.shutdown();

        backupDb = startBackupDatabase( backupDir.toFile() );
        verifyData( backupDb );
    }

    @Test
    void fulltextIndexesMustBeUpdatedByIncrementalBackup() throws Exception
    {
        initializeTestData();
        Path backupDir = executeBackup();

        long nodeId3;
        long nodeId4;
        long relId2;
        try ( Transaction tx = db.beginTx() )
        {
            Node node3 = db.createNode( LABEL );
            node3.setProperty( PROP, "Additional data." );
            Node node4 = db.createNode( LABEL );
            node4.setProperty( PROP, "Even more additional data." );
            Relationship rel = node3.createRelationshipTo( node4, REL );
            rel.setProperty( PROP, "Knows of" );
            nodeId3 = node3.getId();
            nodeId4 = node4.getId();
            relId2 = rel.getId();
            tx.success();
        }
        verifyData( db );

        executeBackup();

        db.shutdown();

        backupDb = startBackupDatabase( backupDir.toFile() );
        verifyData( backupDb );

        try ( Transaction tx = backupDb.beginTx() )
        {
            try ( Result nodes = backupDb.execute( format( QUERY_NODES, NODE_INDEX, "additional" ) ) )
            {
                List<Long> nodeIds = nodes.stream().map( m -> ((Node) m.get( NODE )).getId() ).collect( Collectors.toList() );
                assertThat( nodeIds, containsInAnyOrder( nodeId3, nodeId4 ) );
            }
            try ( Result relationships = backupDb.execute( format( QUERY_RELS, REL_INDEX, "knows" ) ) )
            {
                List<Long> relIds = relationships.stream().map( m -> ((Relationship) m.get( RELATIONSHIP )).getId() ).collect( Collectors.toList() );
                assertThat( relIds, containsInAnyOrder( relId2 ) );
            }
            tx.success();
        }
    }

    // TODO test that creation and dropping of fulltext indexes is applied through incremental backup.
    // TODO test that custom analyzer configurations are applied through incremental backup.
    // TODO test that the eventually_consistent setting is transferred through incremental backup.

    private void initializeTestData()
    {
        try ( Transaction tx = db.beginTx() )
        {
            Node node1 = db.createNode( LABEL );
            node1.setProperty( PROP, "This is an integration test." );
            Node node2 = db.createNode( LABEL );
            node2.setProperty( PROP, "This is a related integration test." );
            Relationship relationship = node1.createRelationshipTo( node2, REL );
            relationship.setProperty( PROP, "They relate" );
            nodeId1 = node1.getId();
            nodeId2 = node2.getId();
            relId1 = relationship.getId();
            tx.success();
        }
        try ( Transaction tx = db.beginTx() )
        {
            db.execute( format( NODE_CREATE, NODE_INDEX, array( LABEL.name() ), array( PROP ) ) ).close();
            db.execute( format( RELATIONSHIP_CREATE, REL_INDEX, array( REL.name() ), array( PROP ) ) ).close();
            tx.success();
        }
        awaitPopulation( db );
    }

    private static void awaitPopulation( GraphDatabaseAPI db )
    {
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().awaitIndexesOnline( 10, TimeUnit.SECONDS );
            tx.success();
        }
    }

    private GraphDatabaseAPI startBackupDatabase( File backupDatabaseDir )
    {
        return (GraphDatabaseAPI) new TestCommercialGraphDatabaseFactory().newEmbeddedDatabaseBuilder( backupDatabaseDir ).newGraphDatabase();
    }

    private void verifyData( GraphDatabaseAPI db )
    {
        try ( Transaction tx = db.beginTx() )
        {
            awaitPopulation( db );
            tx.success();
        }
        try ( Transaction tx = db.beginTx() )
        {
            try ( Result nodes = db.execute( format( QUERY_NODES, NODE_INDEX, "integration" ) ) )
            {
                List<Long> nodeIds = nodes.stream().map( m -> ((Node) m.get( NODE )).getId() ).collect( Collectors.toList() );
                assertThat( nodeIds, containsInAnyOrder( nodeId1, nodeId2 ) );
            }
            try ( Result relationships = db.execute( format( QUERY_RELS, REL_INDEX, "relate" ) ) )
            {
                List<Long> relIds = relationships.stream().map( m -> ((Relationship) m.get( RELATIONSHIP )).getId() ).collect( Collectors.toList() );
                assertThat( relIds, containsInAnyOrder( relId1 ) );
            }
            tx.success();
        }
    }

    private Path executeBackup() throws Exception
    {
        DependencyResolver resolver = db.getDependencyResolver();
        ConnectorPortRegister portRegister = resolver.resolveDependency( ConnectorPortRegister.class );
        HostnamePort backupAddress = portRegister.getLocalAddress( BACKUP_SERVER_NAME );

        String backupName = DEFAULT_DATABASE_NAME;
        Path backupDir = dir.directory( BACKUP_DIR_NAME ).toPath();

        OnlineBackupContext context = OnlineBackupContext.builder()
                .withAddress( backupAddress.getHost(), backupAddress.getPort() )
                .withBackupName( backupName )
                .withBackupDirectory( backupDir )
                .withReportsDirectory( backupDir )
                .withConsistencyCheck( true )
                .withConsistencyCheckPropertyOwners( true )
                .build();

        OnlineBackupExecutor.buildDefault().executeBackup( context );

        return backupDir.resolve( backupName );
    }
}
