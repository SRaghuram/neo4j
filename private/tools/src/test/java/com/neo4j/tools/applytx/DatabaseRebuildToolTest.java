/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.tools.applytx;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.PrintStream;

import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.TransactionIdStore;
import org.neo4j.test.DbRepresentation;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.SuppressOutputExtension;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static com.neo4j.tools.input.ConsoleUtil.NULL_PRINT_STREAM;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.graphdb.RelationshipType.withName;
import static org.neo4j.io.pagecache.impl.muninn.StandalonePageCacheFactory.createPageCache;
import static org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer.NULL;
import static org.neo4j.kernel.impl.scheduler.JobSchedulerFactory.createInitialisedScheduler;

@TestDirectoryExtension
@ExtendWith( SuppressOutputExtension.class )
@ResourceLock( Resources.SYSTEM_OUT )
class DatabaseRebuildToolTest
{

    @Inject
    private TestDirectory testDirectory;

    private DatabaseLayout fromLayout;
    private DatabaseLayout toLayout;

    @BeforeEach
    void setup()
    {
        var neoLayout = Neo4jLayout.ofFlat( testDirectory.homePath( "flat" ) );
        fromLayout = neoLayout.databaseLayout( "old" );
        toLayout = neoLayout.databaseLayout( "new" );
    }

    @Test
    void shouldRebuildDbFromTransactions() throws Exception
    {
        // This tests the basic functionality of this tool, there are more things, but it's not as important
        // to test as the functionality of applying transactions.

        // GIVEN
        databaseWithSomeTransactions( fromLayout );
        DatabaseRebuildTool tool = new DatabaseRebuildTool( System.in, NULL_PRINT_STREAM, NULL_PRINT_STREAM );

        var toNeoLayout = toLayout.getNeo4jLayout();
        // WHEN
        tool.run( "--from", fromLayout.databaseDirectory().toAbsolutePath().toString(),
                "--fromTx", fromLayout.getTransactionLogsDirectory().toAbsolutePath().toString(),
                "--to", toLayout.databaseDirectory().toAbsolutePath().toString(),
                "-D" + GraphDatabaseSettings.transaction_logs_root_path.name(), toNeoLayout.transactionLogsRootDirectory().toAbsolutePath().toString(),
                "-D" + GraphDatabaseInternalSettings.databases_root_path.name(), toNeoLayout.databasesDirectory().toAbsolutePath().toString(),
                "apply last" );

        // THEN
        assertEquals( DbRepresentation.of( fromLayout ), DbRepresentation.of( toLayout ) );
    }

    @Test
    void shouldApplySomeTransactions() throws Exception
    {
        // This tests the basic functionality of this tool, there are more things, but it's not as important
        // to test as the functionality of applying transactions.

        // GIVEN
        databaseWithSomeTransactions( fromLayout );
        DatabaseRebuildTool tool = new DatabaseRebuildTool( input( "apply next", "apply next", "cc", "exit" ),
                NULL_PRINT_STREAM, NULL_PRINT_STREAM );

        // WHEN
        tool.run( "--from", fromLayout.databaseDirectory().toAbsolutePath().toString(),
                "--fromTx", fromLayout.getTransactionLogsDirectory().toAbsolutePath().toString(),
                "--to", toLayout.databaseDirectory().toString(), "-i" );

        // THEN
        assertEquals( TransactionIdStore.BASE_TX_ID + 2, lastAppliedTx( toLayout ) );
    }

    @Test
    void shouldDumpNodePropertyChain() throws Exception
    {
        shouldPrint( "dump node properties 0",
                "Property",
                "name0" );
    }

    @Test
    void shouldDumpRelationshipPropertyChain() throws Exception
    {
        shouldPrint( "dump relationship properties 0",
                "Property",
                "name0" );
    }

    @Test
    void shouldDumpRelationships() throws Exception
    {
        shouldPrint( "dump node relationships 0",
                "Relationship[0,",
                "Relationship[10," );
    }

    @Test
    void shouldDumpRelationshipTypeTokens() throws Exception
    {
        shouldPrint( "dump tokens relationship-type",
                "TYPE_0",
                "TYPE_1" );
    }

    @Test
    void shouldDumpLabelTokens() throws Exception
    {
        shouldPrint( "dump tokens label",
                "Label_0",
                "Label_1" );
    }

    @Test
    void shouldDumpPropertyKeyTokens() throws Exception
    {
        shouldPrint( "dump tokens property-key",
                "name" );
    }

    private void shouldPrint( String command, String... expectedResultContaining ) throws Exception
    {
        // GIVEN
        databaseWithSomeTransactions( toLayout );
        ByteArrayOutputStream byteArrayOut = new ByteArrayOutputStream();
        PrintStream out = new PrintStream( byteArrayOut );
        DatabaseRebuildTool tool = new DatabaseRebuildTool( input( command, "exit" ),
                out, NULL_PRINT_STREAM );

        // WHEN
        tool.run( "--from", fromLayout.databaseDirectory().toAbsolutePath().toString(),
                "--to", toLayout.databaseDirectory().toAbsolutePath().toString(), "-i" );

        // THEN
        out.flush();
        String dump = new String( byteArrayOut.toByteArray() );
        for ( String string : expectedResultContaining )
        {
            assertThat( dump ).as( "dump from command '" + command + "'" ).contains( string );
        }
    }

    private static long lastAppliedTx( DatabaseLayout databaseLayout )
    {
        try ( FileSystemAbstraction fileSystem = new DefaultFileSystemAbstraction();
              JobScheduler scheduler = createInitialisedScheduler();
              PageCache pageCache = createPageCache( fileSystem, scheduler, PageCacheTracer.NULL ) )
        {
            return MetaDataStore.getRecord( pageCache, databaseLayout.metadataStore(), MetaDataStore.Position.LAST_TRANSACTION_ID, NULL );
        }
        catch ( Exception e )
        {
            throw new RuntimeException( e );
        }
    }

    private static InputStream input( String... strings )
    {
        StringBuilder all = new StringBuilder();
        for ( String string : strings )
        {
            all.append( string ).append( format( "%n" ) );
        }
        return new ByteArrayInputStream( all.toString().getBytes() );
    }

    private static void databaseWithSomeTransactions( DatabaseLayout databaseLayout )
    {
        Neo4jLayout layout = databaseLayout.getNeo4jLayout();
        DatabaseManagementService managementService = new TestDatabaseManagementServiceBuilder( layout.homeDirectory() )
                .setConfig( GraphDatabaseSettings.transaction_logs_root_path, layout.transactionLogsRootDirectory().toAbsolutePath() )
                .setConfig( GraphDatabaseInternalSettings.databases_root_path, layout.databasesDirectory().toAbsolutePath() )
                .setConfig( GraphDatabaseSettings.default_database, databaseLayout.getDatabaseName() )
                .build();
        GraphDatabaseService db = managementService.database( databaseLayout.getDatabaseName() );
        Node[] nodes = new Node[10];
        for ( int i = 0; i < nodes.length; i++ )
        {
            try ( Transaction tx = db.beginTx() )
            {
                Node node = tx.createNode( label( "Label_" + (i % 2) ) );
                setProperties( node, i );
                nodes[i] = node;
                tx.commit();
            }
        }
        for ( int i = 0; i < 40; i++ )
        {
            try ( Transaction tx = db.beginTx() )
            {
                Relationship relationship = tx.getNodeById( nodes[i % nodes.length].getId() )
                        .createRelationshipTo( nodes[(i + 1) % nodes.length], withName( "TYPE_" + (i % 3) ) );
                setProperties( relationship, i );
                tx.commit();
            }
        }
        try ( Transaction tx = db.beginTx() )
        {
            Node node = tx.getNodeById( nodes[nodes.length - 1].getId() );
            for ( Relationship relationship : node.getRelationships() )
            {
                relationship.delete();
            }
            node.delete();
            tx.commit();
        }
        managementService.shutdown();
    }

    private static void setProperties( Entity entity, int i )
    {
        entity.setProperty( "key", "name" + i );
    }
}
