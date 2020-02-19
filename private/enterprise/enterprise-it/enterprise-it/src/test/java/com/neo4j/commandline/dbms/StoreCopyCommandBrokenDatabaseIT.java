/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.commandline.dbms;

import com.neo4j.dbms.commandline.StoreCopyCommand;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;

import org.neo4j.cli.ExecutionContext;
import org.neo4j.configuration.Config;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.id.ScanOnOpenReadOnlyIdGeneratorFactory;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.storageengine.util.IdUpdateListener;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.extension.Inject;

import static com.neo4j.commandline.dbms.StoreCopyCommandIT.getCopyName;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.neo4j.io.pagecache.tracing.PageCacheTracer.NULL;

class StoreCopyCommandBrokenDatabaseIT extends AbstractCommandIT
{
    @Inject
    private FileSystemAbstraction fs;
    @Inject
    private PageCache pageCache;

    @Test
    void ignoreRelationshipForGhostNodes() throws Exception
    {
        // Create some data
        try ( Transaction tx = databaseAPI.beginTx() )
        {
            Node a = tx.createNode( Label.label( "Number" ) );
            a.setProperty( "name", "Uno" );
            Node b = tx.createNode( Label.label( "Number" ) );
            b.setProperty( "name", "Dos" );
            Node c = tx.createNode( Label.label( "Number" ) );
            c.setProperty( "name", "Tres" );

            a.createRelationshipTo( b, RelationshipType.withName( "KNOWS" ) );
            tx.commit();
        }
        String databaseName = databaseAPI.databaseName();
        String copyName = getCopyName( databaseName, "copy" );
        managementService.shutdownDatabase( databaseName );

        try ( NeoStores neoStores = getNeoStores( databaseName ) )
        {
            // Mark node b as not in use
            NodeStore nodeStore = neoStores.getNodeStore();
            NodeRecord nodeRecord = nodeStore.newRecord();
            nodeRecord.setId( 1 );
            nodeRecord.setInUse( false );
            nodeStore.updateRecord( nodeRecord, IdUpdateListener.IGNORE, PageCursorTracer.NULL );
        }

        copyDatabase( "--from-database=" + databaseName, "--to-database=" + copyName );

        managementService.createDatabase( copyName );
        GraphDatabaseService copyDb = managementService.database( copyName );
        try ( Transaction tx = copyDb.beginTx() )
        {
            assertEquals( "Uno", tx.getNodeById( 0 ).getProperty( "name" ) );
            assertEquals( "Tres", tx.getNodeById( 1 ).getProperty( "name" ) );
            assertFalse( tx.getNodeById( 0 ).getRelationships().iterator().hasNext() );
            tx.commit();
        }
        managementService.dropDatabase( copyName );

    }

    private NeoStores getNeoStores( String databaseName )
    {
        return new StoreFactory( databaseAPI.databaseLayout().getNeo4jLayout().databaseLayout( databaseName ), Config.defaults(),
                new ScanOnOpenReadOnlyIdGeneratorFactory(),
                pageCache, fs, NullLogProvider.getInstance(), NULL ).openAllNeoStores();
    }

    private void copyDatabase( String... args ) throws Exception
    {
        var context = new ExecutionContext( neo4jHome, configDir );
        var command = new StoreCopyCommand( context );

        CommandLine.populateCommand( command, args );

        command.execute();
    }
}
