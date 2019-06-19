/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.tools.applytx;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.PrintStream;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.TransactionIdStore;
import org.neo4j.test.DbRepresentation;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.SuppressOutputExtension;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static com.neo4j.tools.input.ConsoleUtil.NULL_PRINT_STREAM;
import static java.lang.String.format;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.graphdb.RelationshipType.withName;
import static org.neo4j.io.pagecache.impl.muninn.StandalonePageCacheFactory.createPageCache;
import static org.neo4j.kernel.impl.scheduler.JobSchedulerFactory.createInitialisedScheduler;

@ExtendWith( {TestDirectoryExtension.class, SuppressOutputExtension.class} )
class DatabaseRebuildToolTest
{

    @Inject
    private TestDirectory directory;

    @Test
    void shouldRebuildDbFromTransactions() throws Exception
    {
        // This tests the basic functionality of this tool, there are more things, but it's not as important
        // to test as the functionality of applying transactions.

        // GIVEN
        var fromLayout = directory.databaseLayout( "old" );
        var toLayout = directory.databaseLayout( "new" );
        databaseWithSomeTransactions( fromLayout );
        DatabaseRebuildTool tool = new DatabaseRebuildTool( System.in, NULL_PRINT_STREAM, NULL_PRINT_STREAM );

        // WHEN
        tool.run( "--from", fromLayout.databaseDirectory().getAbsolutePath(),
                "--fromTx", fromLayout.getTransactionLogsDirectory().getAbsolutePath(),
                "--to", toLayout.databaseDirectory().getAbsolutePath(),
                "-D" + GraphDatabaseSettings.transaction_logs_root_path.name(), toLayout.getTransactionLogsDirectory().getParentFile().getAbsolutePath(),
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
        var fromLayout = directory.databaseLayout( "old" );
        var toLayout = directory.databaseLayout( "new" );
        databaseWithSomeTransactions( fromLayout );
        DatabaseRebuildTool tool = new DatabaseRebuildTool( input( "apply next", "apply next", "cc", "exit" ),
                NULL_PRINT_STREAM, NULL_PRINT_STREAM );

        // WHEN
        tool.run( "--from", fromLayout.databaseDirectory().getAbsolutePath(),
                "--fromTx", fromLayout.getTransactionLogsDirectory().getAbsolutePath(),
                "--to", toLayout.databaseDirectory().getPath(), "-i" );

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
        var fromLayout = directory.databaseLayout( "old" );
        var toLayout = directory.databaseLayout( "new" );
        databaseWithSomeTransactions( toLayout );
        ByteArrayOutputStream byteArrayOut = new ByteArrayOutputStream();
        PrintStream out = new PrintStream( byteArrayOut );
        DatabaseRebuildTool tool = new DatabaseRebuildTool( input( command, "exit" ),
                out, NULL_PRINT_STREAM );

        // WHEN
        tool.run( "--from", fromLayout.databaseDirectory().getAbsolutePath(), "--to", toLayout.databaseDirectory().getAbsolutePath(), "-i" );

        // THEN
        out.flush();
        String dump = new String( byteArrayOut.toByteArray() );
        for ( String string : expectedResultContaining )
        {
            assertThat( "dump from command '" + command + "'", dump, containsString( string ) );
        }
    }

    private static long lastAppliedTx( DatabaseLayout databaseLayout )
    {
        try ( FileSystemAbstraction fileSystem = new DefaultFileSystemAbstraction();
              JobScheduler scheduler = createInitialisedScheduler();
              PageCache pageCache = createPageCache( fileSystem, scheduler ) )
        {
            return MetaDataStore.getRecord( pageCache, databaseLayout.metadataStore(),
                    MetaDataStore.Position.LAST_TRANSACTION_ID );
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
        DatabaseManagementService managementService = new TestDatabaseManagementServiceBuilder( databaseLayout.getStoreLayout().storeDirectory() )
                .setConfig( GraphDatabaseSettings.default_database, databaseLayout.getDatabaseName() )
                .setConfig( GraphDatabaseSettings.record_id_batch_size, "1" )
                .build();
        GraphDatabaseService db = managementService.database( databaseLayout.getDatabaseName() );
        Node[] nodes = new Node[10];
        for ( int i = 0; i < nodes.length; i++ )
        {
            try ( Transaction tx = db.beginTx() )
            {
                Node node = db.createNode( label( "Label_" + (i % 2) ) );
                setProperties( node, i );
                nodes[i] = node;
                tx.success();
            }
        }
        for ( int i = 0; i < 40; i++ )
        {
            try ( Transaction tx = db.beginTx() )
            {
                Relationship relationship = nodes[i % nodes.length]
                        .createRelationshipTo( nodes[(i + 1) % nodes.length], withName( "TYPE_" + (i % 3) ) );
                setProperties( relationship, i );
                tx.success();
            }
        }
        try ( Transaction tx = db.beginTx() )
        {
            Node node = nodes[nodes.length - 1];
            for ( Relationship relationship : node.getRelationships() )
            {
                relationship.delete();
            }
            node.delete();
            tx.success();
        }
        managementService.shutdown();
    }

    private static void setProperties( PropertyContainer entity, int i )
    {
        entity.setProperty( "key", "name" + i );
    }
}
