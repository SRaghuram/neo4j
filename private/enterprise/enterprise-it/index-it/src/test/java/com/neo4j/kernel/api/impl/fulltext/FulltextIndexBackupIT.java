/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.api.impl.fulltext;

import com.neo4j.backup.impl.OnlineBackupContext;
import com.neo4j.backup.impl.OnlineBackupExecutor;
import com.neo4j.configuration.OnlineBackupSettings;
import com.neo4j.test.TestEnterpriseDatabaseManagementServiceBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.connectors.ConnectorPortRegister;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.helpers.HostnamePort;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.SuppressOutputExtension;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static com.neo4j.causalclustering.common.TransactionBackupServiceProvider.BACKUP_SERVER_NAME;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.configuration.GraphDatabaseInternalSettings.databases_root_path;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.transaction_logs_root_path;
import static org.neo4j.kernel.api.impl.fulltext.FulltextIndexProceduresUtil.NODE_CREATE;
import static org.neo4j.kernel.api.impl.fulltext.FulltextIndexProceduresUtil.QUERY_NODES;
import static org.neo4j.kernel.api.impl.fulltext.FulltextIndexProceduresUtil.QUERY_RELS;
import static org.neo4j.kernel.api.impl.fulltext.FulltextIndexProceduresUtil.RELATIONSHIP_CREATE;
import static org.neo4j.kernel.api.impl.fulltext.FulltextIndexProceduresUtil.asStrList;

@TestDirectoryExtension
@ExtendWith( SuppressOutputExtension.class )
@ResourceLock( Resources.SYSTEM_OUT )
class FulltextIndexBackupIT
{
    private static final String NODE = "node";
    private static final String RELATIONSHIP = "relationship";
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
    private DatabaseManagementService dbManagementService;
    private static DatabaseManagementService backupManagementService;

    @BeforeEach
    void setUp()
    {
        dbManagementService = new TestEnterpriseDatabaseManagementServiceBuilder( dir.homePath() )
                .setConfig( OnlineBackupSettings.online_backup_enabled, true )
                .build();
        db = (GraphDatabaseAPI) dbManagementService.database( DEFAULT_DATABASE_NAME );
    }

    @AfterEach
    void tearDown()
    {
        if ( db != null )
        {
            dbManagementService.shutdown();
        }
        if ( backupDb != null )
        {
            backupManagementService.shutdown();
        }
    }

    @Test
    void fulltextIndexesMustBeTransferredInBackup() throws Exception
    {
        initializeTestData();
        verifyData( db );
        Path backupDir = executeBackup();
        dbManagementService.shutdown();

        backupDb = startBackupDatabase( backupDir );
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
            Node node3 = tx.createNode( LABEL );
            node3.setProperty( PROP, "Additional data." );
            Node node4 = tx.createNode( LABEL );
            node4.setProperty( PROP, "Even more additional data." );
            Relationship rel = node3.createRelationshipTo( node4, REL );
            rel.setProperty( PROP, "Knows of" );
            nodeId3 = node3.getId();
            nodeId4 = node4.getId();
            relId2 = rel.getId();
            tx.commit();
        }
        verifyData( db );

        executeBackup();

        dbManagementService.shutdown();

        backupDb = startBackupDatabase( backupDir );
        verifyData( backupDb );

        try ( Transaction tx = backupDb.beginTx() )
        {
            try ( Result nodes = tx.execute( format( QUERY_NODES, NODE_INDEX, "additional" ) ) )
            {
                List<Long> nodeIds = nodes.stream().map( m -> ((Node) m.get( NODE )).getId() ).collect( Collectors.toList() );
                assertThat( nodeIds ).contains( nodeId3, nodeId4 );
            }
            try ( Result relationships = tx.execute( format( QUERY_RELS, REL_INDEX, "knows" ) ) )
            {
                List<Long> relIds = relationships.stream().map( m -> ((Relationship) m.get( RELATIONSHIP )).getId() ).collect( Collectors.toList() );
                assertThat( relIds ).contains( relId2 );
            }
            tx.commit();
        }
    }

    // TODO test that creation and dropping of fulltext indexes is applied through incremental backup.
    // TODO test that custom analyzer configurations are applied through incremental backup.
    // TODO test that the eventually_consistent setting is transferred through incremental backup.

    private void initializeTestData()
    {
        try ( Transaction tx = db.beginTx() )
        {
            Node node1 = tx.createNode( LABEL );
            node1.setProperty( PROP, "This is an integration test." );
            Node node2 = tx.createNode( LABEL );
            node2.setProperty( PROP, "This is a related integration test." );
            Relationship relationship = node1.createRelationshipTo( node2, REL );
            relationship.setProperty( PROP, "They relate" );
            nodeId1 = node1.getId();
            nodeId2 = node2.getId();
            relId1 = relationship.getId();
            tx.commit();
        }
        try ( Transaction tx = db.beginTx() )
        {
            tx.execute( format( NODE_CREATE, NODE_INDEX, asStrList( LABEL.name() ), asStrList( PROP ) ) ).close();
            tx.execute( format( RELATIONSHIP_CREATE, REL_INDEX, asStrList( REL.name() ), asStrList( PROP ) ) ).close();
            tx.commit();
        }
        awaitPopulation( db );
    }

    private static void awaitPopulation( GraphDatabaseAPI db )
    {
        try ( Transaction tx = db.beginTx() )
        {
            tx.schema().awaitIndexesOnline( 30, TimeUnit.SECONDS );
            tx.commit();
        }
    }

    private static GraphDatabaseAPI startBackupDatabase( Path backupDatabaseDir )
    {
        backupManagementService = new TestEnterpriseDatabaseManagementServiceBuilder( backupDatabaseDir )
                .setConfig( transaction_logs_root_path, backupDatabaseDir.toAbsolutePath() )
                .setConfig( databases_root_path, backupDatabaseDir.toAbsolutePath() )
                .build();
        return (GraphDatabaseAPI) backupManagementService.database( DEFAULT_DATABASE_NAME );
    }

    private void verifyData( GraphDatabaseAPI db )
    {
        awaitPopulation( db );
        try ( Transaction tx = db.beginTx() )
        {
            try ( Result nodes = tx.execute( format( QUERY_NODES, NODE_INDEX, "integration" ) ) )
            {
                List<Long> nodeIds = nodes.stream().map( m -> ((Node) m.get( NODE )).getId() ).collect( Collectors.toList() );
                assertThat( nodeIds ).contains( nodeId1, nodeId2 );
            }
            try ( Result relationships = tx.execute( format( QUERY_RELS, REL_INDEX, "relate" ) ) )
            {
                List<Long> relIds = relationships.stream().map( m -> ((Relationship) m.get( RELATIONSHIP )).getId() ).collect( Collectors.toList() );
                assertThat( relIds ).contains( relId1 );
            }
            tx.commit();
        }
    }

    private Path executeBackup() throws Exception
    {
        DependencyResolver resolver = db.getDependencyResolver();
        ConnectorPortRegister portRegister = resolver.resolveDependency( ConnectorPortRegister.class );
        HostnamePort backupAddress = portRegister.getLocalAddress( BACKUP_SERVER_NAME );

        Path backupDir = dir.directory( BACKUP_DIR_NAME );

        var contextBuilder = OnlineBackupContext.builder()
                .withAddress( backupAddress.getHost(), backupAddress.getPort() )
                .withDatabaseNamePattern( DEFAULT_DATABASE_NAME )
                .withBackupDirectory( backupDir )
                .withReportsDirectory( backupDir )
                .withConsistencyCheck( true )
                .withConsistencyCheckPropertyOwners( true );

        OnlineBackupExecutor.buildDefault().executeBackups( contextBuilder );

        return backupDir;
    }
}
