/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.api.impl.bloom.surface;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;

import org.neo4j.backup.OnlineBackup;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.rule.SuppressOutput;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings.online_backup_enabled;
import static org.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings.online_backup_server;
import static org.neo4j.ports.allocation.PortAuthority.allocatePort;
import static org.neo4j.test.rule.SuppressOutput.suppressAll;

public class BloomBackupIT
{
    @Rule
    public TestDirectory testDirectory = TestDirectory.testDirectory();
    @Rule
    public SuppressOutput suppressOutput = suppressAll();
    private GraphDatabaseAPI db;
    private int backupPort;

    @Before
    public void portSetup()
    {
        backupPort = allocatePort();
        db = (GraphDatabaseAPI) new GraphDatabaseFactory().newEmbeddedDatabaseBuilder( testDirectory.graphDbDir() )
                .setConfig( online_backup_enabled,"true" )
                .setConfig( online_backup_server, "127.0.0.1:" + backupPort )
                .setConfig( BloomFulltextConfig.bloom_enabled, "true" )
                .setConfig( BloomFulltextConfig.bloom_refresh_delay, "1s" )
                .newGraphDatabase();
    }

    @Test
    public void shouldContainIndexAfterBackupAndRestore() throws Exception
    {
        registerProcedures( db );
        setupIndicesAndInitialData();
        db.execute( BloomIT.AWAIT_REFRESH );

        File backupDir = new File( db.getStoreDir().getParentFile(), "backup" );
        OnlineBackup.from( "127.0.0.1", backupPort ).backup( backupDir );
        db.shutdown();

        GraphDatabaseAPI backedUpDb = getBackupDb( backupDir );
        registerProcedures( backedUpDb );
        verifyStandardData( backedUpDb );
        backedUpDb.shutdown();
    }

    @Test
    public void shouldFindEntitiesFromIncrementalBackup() throws Exception
    {
        registerProcedures( db );
        setupIndicesAndInitialData();
        db.execute( BloomIT.AWAIT_REFRESH );

        // Full backup
        File backupDir = new File( db.getStoreDir().getParentFile(), "backup" );
        OnlineBackup.from( "127.0.0.1", backupPort ).backup( backupDir );

        // Add some more data
        long additionalId1;
        long additionalId2;
        long additionalRelId;
        try ( Transaction transaction = db.beginTx() )
        {
            Node node1 = db.createNode();
            node1.setProperty( "prop", "Addiditional data" );
            additionalId1 = node1.getId();
            Node node2 = db.createNode();
            node2.setProperty( "prop", "Even more additional data" );
            additionalId2 = node2.getId();
            Relationship relationship = node1.createRelationshipTo( node2, RelationshipType.withName( "type" ) );
            relationship.setProperty( "relprop", "Knows of" );
            additionalRelId = relationship.getId();
            transaction.success();
        }
        db.execute( BloomIT.AWAIT_REFRESH );

        //Incremental backup
        OnlineBackup.from( "127.0.0.1", backupPort ).backup( backupDir );
        db.shutdown();

        GraphDatabaseAPI backedUpDb = getBackupDb( backupDir );
        registerProcedures( backedUpDb );
        verifyStandardData( backedUpDb );

        Result result = backedUpDb.execute( String.format( BloomIT.NODES, "\"additional\"" ) );
        assertTrue( result.hasNext() );
        assertEquals( additionalId1, result.next().get( BloomIT.ENTITYID ) );
        assertTrue( result.hasNext() );
        assertEquals( additionalId2, result.next().get( BloomIT.ENTITYID ) );
        assertFalse( result.hasNext() );
        result = backedUpDb.execute( String.format( BloomIT.RELS, "\"knows\"" ) );
        assertTrue( result.hasNext() );
        assertEquals( additionalRelId, result.next().get( BloomIT.ENTITYID ) );
        assertFalse( result.hasNext() );
        backedUpDb.shutdown();
    }

    private GraphDatabaseAPI getBackupDb( File dir )
    {
        return (GraphDatabaseAPI) new GraphDatabaseFactory().newEmbeddedDatabaseBuilder( dir )
                .setConfig( BloomFulltextConfig.bloom_enabled,"true" )
                .setConfig( BloomFulltextConfig.bloom_refresh_delay, "1s" )
                .newGraphDatabase();
    }

    private void setupIndicesAndInitialData()
    {
        db.execute( String.format( BloomIT.SET_NODE_KEYS, "\"prop\", \"relprop\"" ) );
        db.execute( String.format( BloomIT.SET_REL_KEYS, "\"prop\", \"relprop\"" ) );
        try ( Transaction transaction = db.beginTx() )
        {
            Node node1 = db.createNode();
            node1.setProperty( "prop", "This is a integration test." );
            Node node2 = db.createNode();
            node2.setProperty( "prop", "This is a related integration test" );
            Relationship relationship = node1.createRelationshipTo( node2, RelationshipType.withName( "type" ) );
            relationship.setProperty( "relprop", "They relate" );
            transaction.success();
        }
    }

    private void verifyStandardData( GraphDatabaseService backedUpDb )
    {
        backedUpDb.execute( BloomIT.AWAIT_REFRESH ).close();
        Result result = backedUpDb.execute( String.format( BloomIT.NODES, "\"integration\"" ) );
        assertTrue( result.hasNext() );
        assertEquals( 0L, result.next().get( BloomIT.ENTITYID ) );
        assertTrue( result.hasNext() );
        assertEquals( 1L, result.next().get( BloomIT.ENTITYID ) );
        assertFalse( result.hasNext() );
        result = backedUpDb.execute( String.format( BloomIT.RELS, "\"relate\"" ) );
        assertTrue( result.hasNext() );
        assertEquals( 0L, result.next().get( BloomIT.ENTITYID ) );
        assertFalse( result.hasNext() );
    }

    private void registerProcedures( GraphDatabaseAPI db ) throws KernelException
    {
        db.beginTx().close();
        db.getDependencyResolver().resolveDependency( Procedures.class ).registerProcedure( BloomProcedures.class );
    }
}
