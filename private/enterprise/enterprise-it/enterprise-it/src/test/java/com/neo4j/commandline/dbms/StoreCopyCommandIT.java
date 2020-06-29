/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.commandline.dbms;

import com.neo4j.dbms.commandline.StoreCopyCommand;
import com.neo4j.kernel.impl.store.format.highlimit.HighLimitFormatFamily;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import org.neo4j.cli.CommandFailedException;
import org.neo4j.cli.ExecutionContext;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.recordstorage.RecordStorageEngine;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.DefaultPageCacheTracer;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.impl.store.PropertyKeyTokenStore;
import org.neo4j.kernel.impl.store.format.FormatFamily;
import org.neo4j.kernel.impl.store.format.RecordFormatSelector;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.store.format.standard.StandardFormatFamily;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.kernel.internal.locker.FileLockException;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.rule.SuppressOutput;
import org.neo4j.token.TokenHolders;

import static java.lang.Math.min;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.internal.helpers.collection.Iterables.single;
import static org.neo4j.io.pagecache.tracing.PageCacheTracer.NULL;

class StoreCopyCommandIT extends AbstractCommandIT
{
    @Inject
    private FileSystemAbstraction fs;
    @Inject
    private PageCache pageCache;
    @Inject
    private SuppressOutput suppressOutput;
    private static final Label NUMBER_LABEL = Label.label( "Number" );
    private static final Label CHARACTER_LABEL = Label.label( "Character" );
    private static final Label ERROR_LABEL = Label.label( "Error" );
    private static final RelationshipType KNOWS = RelationshipType.withName( "KNOWS" );
    private static final RelationshipType SECRET = RelationshipType.withName( "SECRET" );

    @Test
    void cantCopyFromRunningDatabase()
    {
        CommandFailedException commandFailedException = assertThrows( CommandFailedException.class,
                () -> copyDatabase( "--from-database=" + databaseAPI.databaseName(), "--to-database=copy" ) );
        assertTrue( commandFailedException.getCause() instanceof FileLockException );
        assertThat( commandFailedException.getMessage() ).contains( "The database is in use" );
    }

    @Test
    void failOnIncorrectFromDatabaseName()
    {
        var exception = assertThrows( Exception.class,
                () -> copyDatabase( "--from-database=" + databaseAPI.databaseName() + "_", "--to-database=copy" ) );
        assertThat( exception ).hasMessageContaining( "Invalid database name '" + databaseAPI.databaseName() + "_" + "'" );
    }

    @Test
    void failOnIncorrectToDatabaseName()
    {
        var e = assertThrows( Exception.class,
                () -> copyDatabase( "--from-database=" + databaseAPI.databaseName(), "--to-database=copy_" ) );
        assertThat( e ).hasMessageContaining( "Invalid database name 'copy_'" );
    }

    @Test
    void destinationMustBeEmpty() throws IOException
    {
        managementService.shutdownDatabase( databaseAPI.databaseName() );
        Path file = Path.of( getDatabaseAbsolutePath( "copy" ), "non-empty" );
        try
        {
            Files.createDirectories( file.getParent() );
            Files.writeString( file, "DATA!" );

            CommandFailedException commandFailedException = assertThrows( CommandFailedException.class,
                    () -> copyDatabase( "--from-database=" + databaseAPI.databaseName(), "--to-database=copy" ) );
            assertThat( commandFailedException.getMessage() ).contains( "The directory is not empty" );
        }
        finally
        {
            Files.delete( file );
        }
    }

    @Test
    void tracePageCacheAccessOnStoreCopy() throws Exception
    {
        try ( Transaction tx = databaseAPI.beginTx() )
        {
            Node a = tx.createNode( NUMBER_LABEL );
            a.setProperty( "name", "Uno" );
            Node b = tx.createNode( NUMBER_LABEL );
            b.setProperty( "name", "Dos" );
            a.createRelationshipTo( b, RelationshipType.withName( "KNOWS" ) );
            tx.commit();
        }
        String databaseName = databaseAPI.databaseName();
        String copyName = getCopyName( databaseName, "copy" );
        managementService.shutdownDatabase( databaseName );

        var pageCacheTracer = new DefaultPageCacheTracer();
        copyDatabase( pageCacheTracer, "--from-database=" + databaseName, "--to-database=" + copyName );

        assertThat( pageCacheTracer.hits() ).isEqualTo( 19 );
        assertThat( pageCacheTracer.faults() ).isEqualTo( 16 );
        assertThat( pageCacheTracer.pins() ).isEqualTo( 35 );
        assertThat( pageCacheTracer.unpins() ).isEqualTo( 35 );
    }

    @Test
    void shouldCopyData() throws Exception
    {
        // Create some data
        try ( Transaction tx = databaseAPI.beginTx() )
        {
            Node a = tx.createNode( NUMBER_LABEL );
            a.setProperty( "name", "Uno" );
            Node b = tx.createNode( NUMBER_LABEL );
            b.setProperty( "name", "Dos" );
            Node c = tx.createNode( NUMBER_LABEL );
            c.setProperty( "name", "Tres" );

            a.createRelationshipTo( b, KNOWS );
            tx.commit();
        }
        String databaseName = databaseAPI.databaseName();
        String copyName = getCopyName( databaseName, "copy" );
        managementService.shutdownDatabase( databaseName );

        copyDatabase( "--from-database=" + databaseName, "--to-database=" + copyName );

        managementService.createDatabase( copyName );
        GraphDatabaseService copyDb = managementService.database( copyName );
        try ( Transaction tx = copyDb.beginTx() )
        {
            assertEquals( "Uno", tx.getNodeById( 0 ).getProperty( "name" ) );
            assertEquals( "Dos", tx.getNodeById( 1 ).getProperty( "name" ) );
            assertEquals( "Tres", tx.getNodeById( 2 ).getProperty( "name" ) );
            assertEquals( KNOWS, single( tx.getNodeById( 1 ).getRelationships() ).getType() );
            assertThrows( NotFoundException.class, () -> tx.getNodeById( 3 ) );
            tx.commit();
        }
        managementService.dropDatabase( copyName );
    }

    @Test
    void pathArgument() throws Exception
    {
        // Create some data
        try ( Transaction tx = databaseAPI.beginTx() )
        {
            Node a = tx.createNode( NUMBER_LABEL );
            a.setProperty( "name", "On" );
            Node b = tx.createNode( NUMBER_LABEL );
            b.setProperty( "name", "Those" );
            Node c = tx.createNode( NUMBER_LABEL );
            c.setProperty( "name", "Trays" );

            a.createRelationshipTo( b, KNOWS );
            tx.commit();
        }
        String databaseName = databaseAPI.databaseName();
        String copyName = getCopyName( databaseName, "copy" );
        managementService.shutdownDatabase( databaseName );

        CommandFailedException commandFailedException = assertThrows( CommandFailedException.class, () -> copyDatabase(
                "--from-path=" + getDatabaseAbsolutePath( databaseName ),
                "--to-database=" + copyName ) );
        assertTrue( commandFailedException.getMessage().contains( "--from-path-tx" ) );

        copyDatabase(
                "--from-path=" + getDatabaseAbsolutePath( databaseName ),
                "--from-path-tx=" + databaseAPI.databaseLayout().getTransactionLogsDirectory().toAbsolutePath(),
                "--to-database=" + copyName );

        managementService.createDatabase( copyName );
        GraphDatabaseService copyDb = managementService.database( copyName );
        try ( Transaction tx = copyDb.beginTx() )
        {
            assertEquals( "On", tx.getNodeById( 0 ).getProperty( "name" ) );
            assertEquals( "Those", tx.getNodeById( 1 ).getProperty( "name" ) );
            assertEquals( "Trays", tx.getNodeById( 2 ).getProperty( "name" ) );
            assertEquals( KNOWS, single( tx.getNodeById( 1 ).getRelationships() ).getType() );
            assertThrows( NotFoundException.class, () -> tx.getNodeById( 3 ) );
            tx.commit();
        }
        managementService.dropDatabase( copyName );
    }

    @Test
    void canDeleteNodesFromLabels() throws Exception
    {
        // Create some data
        try ( Transaction tx = databaseAPI.beginTx() )
        {
            Node a = tx.createNode( NUMBER_LABEL );
            a.setProperty( "name", "Uno" );
            Node b = tx.createNode( CHARACTER_LABEL );
            b.setProperty( "name", "Dos" );
            Node c = tx.createNode( NUMBER_LABEL );
            c.setProperty( "name", "Tres" );

            a.createRelationshipTo( b, KNOWS );
            tx.commit();
        }
        String databaseName = databaseAPI.databaseName();
        String copyName = getCopyName( databaseName, "copy" );
        managementService.shutdownDatabase( databaseName );

        copyDatabase( "--from-database=" + databaseName, "--to-database=" + copyName, "--delete-nodes-with-labels=Character" );

        managementService.createDatabase( copyName );
        GraphDatabaseService copyDb = managementService.database( copyName );
        try ( Transaction tx = copyDb.beginTx() )
        {
            assertEquals( "Uno", tx.getNodeById( 0 ).getProperty( "name" ) );
            assertEquals( "Tres", tx.getNodeById( 1 ).getProperty( "name" ) );
            assertFalse( tx.getNodeById( 1 ).getRelationships().iterator().hasNext() );
            assertThrows( NotFoundException.class, () -> tx.getNodeById( 2 ) );
            tx.commit();
        }
        managementService.dropDatabase( copyName );
    }

    @Test
    void cantCombineKeepOnlyAndDeleteNodesWithLabels()
    {
        String databaseName = databaseAPI.databaseName();
        String copyName = getCopyName( databaseName, "copy" );
        managementService.shutdownDatabase( databaseAPI.databaseName() );

        CommandFailedException commandFailedException = assertThrows( CommandFailedException.class,
                () -> copyDatabase( "--from-database=" + databaseName, "--to-database=" + copyName,
                        "--delete-nodes-with-labels=Error", "--keep-only-nodes-with-labels=Number" ) );
        assertThat( commandFailedException.getMessage() ).contains( "--delete-nodes-with-labels and --keep-only-nodes-with-labels can not be combined" );
    }

    @Test
    void shouldKeepOnlyNodesWithLabels() throws Exception
    {
        // Create some data
        try ( Transaction tx = databaseAPI.beginTx() )
        {
            Node a = tx.createNode( CHARACTER_LABEL );
            a.setProperty( "name", "Uno" );
            Node b = tx.createNode( NUMBER_LABEL );
            b.setProperty( "name", "Dos" );
            Node c = tx.createNode( NUMBER_LABEL, ERROR_LABEL );
            c.setProperty( "name", "Tres" );

            a.createRelationshipTo( b, KNOWS );
            a.createRelationshipTo( c, KNOWS );
            b.createRelationshipTo( c, KNOWS );
            tx.commit();
        }
        String databaseName = databaseAPI.databaseName();
        String copyName = getCopyName( databaseName, "copy" );
        managementService.shutdownDatabase( databaseName );

        copyDatabase( "--from-database=" + databaseName, "--to-database=" + copyName, "--keep-only-nodes-with-labels=Character,Error" );

        managementService.createDatabase( copyName );
        GraphDatabaseService copyDb = managementService.database( copyName );
        try ( Transaction tx = copyDb.beginTx() )
        {
            Node a = tx.getNodeById( 0 );
            Node c = tx.getNodeById( 1 );
            assertEquals( "Uno", a.getProperty( "name" ) );
            assertEquals( "Tres", c.getProperty( "name" ) );
            assertEquals( single( a.getRelationships() ).getEndNode(), c );
            assertEquals( single( c.getRelationships() ).getOtherNode( c ), a );
            assertThrows( NotFoundException.class, () -> tx.getNodeById( 2 ) );
            tx.commit();
        }
        managementService.dropDatabase( copyName );
    }

    @Test
    void canFilterOutLabelsAndPropertiesAndRelationships() throws Exception
    {
        // Create some data
        try ( Transaction tx = databaseAPI.beginTx() )
        {
            Node a = tx.createNode( NUMBER_LABEL, ERROR_LABEL );
            a.setProperty( "name", "On" );
            a.setProperty( "secretProperty", "Please delete me!" );
            Node b = tx.createNode( NUMBER_LABEL );
            b.setProperty( "name", "Those" );
            Node c = tx.createNode( NUMBER_LABEL );
            c.setProperty( "name", "Trays" );

            a.createRelationshipTo( b, KNOWS );
            b.createRelationshipTo( c, KNOWS );
            a.createRelationshipTo( c, SECRET );
            tx.commit();
        }
        String databaseName = databaseAPI.databaseName();
        String copyName = getCopyName( databaseName, "copy" );
        managementService.shutdownDatabase( databaseName );

        copyDatabase( "--from-database=" + databaseName,
                "--to-database=" + copyName,
                "--skip-labels=Error",
                "--skip-properties=secretProperty",
                "--skip-relationships=" + SECRET.name() );

        managementService.createDatabase( copyName );
        GraphDatabaseService copyDb = managementService.database( copyName );
        try ( Transaction tx = copyDb.beginTx() )
        {
            Node a = tx.getNodeById( 0 );
            Node b = tx.getNodeById( 1 );
            Node c = tx.getNodeById( 2 );
            assertThrows( NotFoundException.class, () -> tx.getNodeById( 3 ) );

            // Validate a
            assertEquals( "On",  a.getProperty( "name" ) );
            assertThrows( NotFoundException.class, () -> a.getProperty( "secretProperty" ) );
            Iterator<Relationship> aRelationships = a.getRelationships( Direction.OUTGOING ).iterator();
            Relationship aKnowsB = aRelationships.next();
            assertEquals( b.getId(), aKnowsB.getEndNodeId() );
            assertEquals( "KNOWS", aKnowsB.getType().name() );
            assertFalse( aRelationships.hasNext() );
            assertTrue( a.hasLabel( NUMBER_LABEL ) );
            assertFalse( a.hasLabel( ERROR_LABEL ) );

            // Validate b
            assertEquals( "Those",  b.getProperty( "name" ) );
            Iterator<Relationship> bRelationships = b.getRelationships( Direction.OUTGOING ).iterator();
            Relationship bKnowsC = bRelationships.next();
            assertEquals( c.getId(), bKnowsC.getEndNodeId() );
            assertEquals( "KNOWS", bKnowsC.getType().name() );
            assertFalse( bRelationships.hasNext() );
            assertTrue( b.hasLabel( NUMBER_LABEL ) );

            // Validate c
            assertEquals( "Trays",  c.getProperty( "name" ) );
            assertFalse( c.getRelationships( Direction.OUTGOING ).iterator().hasNext() );
            assertTrue( c.hasLabel( NUMBER_LABEL ) );
            tx.commit();
        }
        managementService.dropDatabase( copyName );
    }

    @Test
    void cantCombineSkipAndKeepNodeProperties()
    {
        String databaseName = databaseAPI.databaseName();
        String copyName = getCopyName( databaseName, "copy" );
        managementService.shutdownDatabase( databaseAPI.databaseName() );

        CommandFailedException commandFailedException = assertThrows( CommandFailedException.class,
                () -> copyDatabase( "--from-database=" + databaseName, "--to-database=" + copyName,
                        "--skip-properties=prop", "--skip-node-properties=Number.prop" ) );
        assertThat( commandFailedException.getMessage() )
                .contains( "--skip-properties, --skip-node-properties and --keep-only-node-properties can not be combined" );

        commandFailedException = assertThrows( CommandFailedException.class,
                () -> copyDatabase( "--from-database=" + databaseName, "--to-database=" + copyName,
                        "--skip-properties=prop", "--keep-only-node-properties=Number.prop" ) );
        assertThat( commandFailedException.getMessage() )
                .contains( "--skip-properties, --skip-node-properties and --keep-only-node-properties can not be combined" );

        commandFailedException = assertThrows( CommandFailedException.class,
                () -> copyDatabase( "--from-database=" + databaseName, "--to-database=" + copyName,
                        "--skip-node-properties=Error.prop", "--keep-only-node-properties=Number.prop" ) );
        assertThat( commandFailedException.getMessage() )
                .contains( "--skip-properties, --skip-node-properties and --keep-only-node-properties can not be combined" );
    }

    @Test
    void canSkipNodeProperties() throws Exception
    {
        // Create some data
        try ( Transaction tx = databaseAPI.beginTx() )
        {
            Node a = tx.createNode( NUMBER_LABEL, CHARACTER_LABEL );
            a.setProperty( "name", "On" );
            a.setProperty( "secretProperty", "Please delete me!" );
            Node b = tx.createNode( NUMBER_LABEL );
            b.setProperty( "name", "Those" );
            b.setProperty( "secretProperty", "keep me" );
            Node c = tx.createNode( NUMBER_LABEL );
            c.setProperty( "name", "Trays" );

            a.createRelationshipTo( b, KNOWS );
            Relationship rel = b.createRelationshipTo( c, KNOWS );
            rel.setProperty( "secretProperty", "keep me" );
            tx.commit();
        }
        String databaseName = databaseAPI.databaseName();
        String copyName = getCopyName( databaseName, "copy" );
        managementService.shutdownDatabase( databaseName );

        copyDatabase( "--from-database=" + databaseName,
                "--to-database=" + copyName,
                "--skip-node-properties=Character.secretProperty");

        managementService.createDatabase( copyName );
        GraphDatabaseService copyDb = managementService.database( copyName );
        try ( Transaction tx = copyDb.beginTx() )
        {
            Node a = tx.getNodeById( 0 );
            Node b = tx.getNodeById( 1 );
            Node c = tx.getNodeById( 2 );
            assertThrows( NotFoundException.class, () -> tx.getNodeById( 3 ) );

            // Validate a
            assertEquals( "On",  a.getProperty( "name" ) );
            assertThrows( NotFoundException.class, () -> a.getProperty( "secretProperty" ) );
            Iterator<Relationship> aRelationships = a.getRelationships( Direction.OUTGOING ).iterator();
            Relationship aKnowsB = aRelationships.next();
            assertEquals( b.getId(), aKnowsB.getEndNodeId() );
            assertEquals( "KNOWS", aKnowsB.getType().name() );
            assertFalse( aRelationships.hasNext() );

            // Validate b
            assertEquals( "Those",  b.getProperty( "name" ) );
            Iterator<Relationship> bRelationships = b.getRelationships( Direction.OUTGOING ).iterator();
            Relationship bKnowsC = bRelationships.next();
            assertEquals( c.getId(), bKnowsC.getEndNodeId() );
            assertEquals( "KNOWS", bKnowsC.getType().name() );
            assertEquals( bKnowsC.getProperty( "secretProperty" ), "keep me" );
            assertFalse( bRelationships.hasNext() );

            // Validate c
            assertEquals( "Trays",  c.getProperty( "name" ) );
            assertFalse( c.getRelationships( Direction.OUTGOING ).iterator().hasNext() );
            tx.commit();
        }
        managementService.dropDatabase( copyName );
    }

    @Test
    void canKeepNodeProperties() throws Exception
    {
        // Create some data
        try ( Transaction tx = databaseAPI.beginTx() )
        {
            Node a = tx.createNode( CHARACTER_LABEL );
            a.setProperty( "name", "On" );
            a.setProperty( "secretProperty", "Please delete me!" );
            Node b = tx.createNode( NUMBER_LABEL, CHARACTER_LABEL );
            b.setProperty( "name", "Those" );
            b.setProperty( "secretProperty", "keep me" );
            Node c = tx.createNode( NUMBER_LABEL );
            c.setProperty( "name", "Trays" );

            a.createRelationshipTo( b, KNOWS );
            Relationship rel = b.createRelationshipTo( c, KNOWS );
            rel.setProperty( "secretProperty", "keep me" );
            tx.commit();
        }
        String databaseName = databaseAPI.databaseName();
        String copyName = getCopyName( databaseName, "copy" );
        managementService.shutdownDatabase( databaseName );

        copyDatabase( "--from-database=" + databaseName,
                "--to-database=" + copyName,
                "--keep-only-node-properties=Number.secretProperty,Character.name,Number.name");

        managementService.createDatabase( copyName );
        GraphDatabaseService copyDb = managementService.database( copyName );
        try ( Transaction tx = copyDb.beginTx() )
        {
            Node a = tx.getNodeById( 0 );
            Node b = tx.getNodeById( 1 );
            Node c = tx.getNodeById( 2 );
            assertThrows( NotFoundException.class, () -> tx.getNodeById( 3 ) );

            // Validate a
            assertEquals( "On",  a.getProperty( "name" ) );
            assertThrows( NotFoundException.class, () -> a.getProperty( "secretProperty" ) );
            Iterator<Relationship> aRelationships = a.getRelationships( Direction.OUTGOING ).iterator();
            Relationship aKnowsB = aRelationships.next();
            assertEquals( b.getId(), aKnowsB.getEndNodeId() );
            assertEquals( "KNOWS", aKnowsB.getType().name() );
            assertFalse( aRelationships.hasNext() );

            // Validate b
            assertEquals( "Those",  b.getProperty( "name" ) );
            assertEquals( "keep me",  b.getProperty( "secretProperty" ) );
            Iterator<Relationship> bRelationships = b.getRelationships( Direction.OUTGOING ).iterator();
            Relationship bKnowsC = bRelationships.next();
            assertEquals( c.getId(), bKnowsC.getEndNodeId() );
            assertEquals( "KNOWS", bKnowsC.getType().name() );
            assertEquals( bKnowsC.getProperty( "secretProperty" ), "keep me" );
            assertFalse( bRelationships.hasNext() );

            // Validate c
            assertEquals( "Trays",  c.getProperty( "name" ) );
            assertFalse( c.getRelationships( Direction.OUTGOING ).iterator().hasNext() );
            tx.commit();
        }
        managementService.dropDatabase( copyName );
    }

    @Test
    void cantCombineSkipAndKeepRelationshipProperties()
    {
        String databaseName = databaseAPI.databaseName();
        String copyName = getCopyName( databaseName, "copy" );
        managementService.shutdownDatabase( databaseAPI.databaseName() );

        CommandFailedException commandFailedException = assertThrows( CommandFailedException.class,
                () -> copyDatabase( "--from-database=" + databaseName, "--to-database=" + copyName,
                        "--skip-properties=prop", "--skip-relationship-properties=KNOWS.prop" ) );
        assertThat( commandFailedException.getMessage() )
                .contains( "--skip-properties, --skip-relationship-properties and --keep-only-relationship-properties can not be combined" );

        commandFailedException = assertThrows( CommandFailedException.class,
                () -> copyDatabase( "--from-database=" + databaseName, "--to-database=" + copyName,
                        "--skip-properties=prop", "--keep-only-relationship-properties=SECRET.prop" ) );
        assertThat( commandFailedException.getMessage() )
                .contains( "--skip-properties, --skip-relationship-properties and --keep-only-relationship-properties can not be combined" );

        commandFailedException = assertThrows( CommandFailedException.class,
                () -> copyDatabase( "--from-database=" + databaseName, "--to-database=" + copyName,
                        "--skip-relationship-properties=KNOWS.prop", "--keep-only-relationship-properties=SECRET.prop" ) );
        assertThat( commandFailedException.getMessage() )
                .contains( "--skip-properties, --skip-relationship-properties and --keep-only-relationship-properties can not be combined" );
    }

    @Test
    void canSkipRelationshipProperties() throws Exception
    {
        // Create some data
        try ( Transaction tx = databaseAPI.beginTx() )
        {
            Node a = tx.createNode( NUMBER_LABEL, CHARACTER_LABEL );
            a.setProperty( "name", "On" );
            Node b = tx.createNode( NUMBER_LABEL );
            b.setProperty( "name", "Those" );
            b.setProperty( "secretProperty", "keep me" );
            Node c = tx.createNode( NUMBER_LABEL );
            c.setProperty( "name", "Trays" );

            Relationship rel = a.createRelationshipTo( b, KNOWS );
            rel.setProperty( "otherProperty", "keep me" );
            rel = b.createRelationshipTo( c, KNOWS );
            rel.setProperty( "secretProperty", "Please delete me!" );
            rel = c.createRelationshipTo( a, SECRET );
            rel.setProperty( "secretProperty", "keep me" );
            tx.commit();
        }
        String databaseName = databaseAPI.databaseName();
        String copyName = getCopyName( databaseName, "copy" );
        managementService.shutdownDatabase( databaseName );

        copyDatabase( "--from-database=" + databaseName,
                "--to-database=" + copyName,
                "--skip-relationship-properties=KNOWS.secretProperty");

        managementService.createDatabase( copyName );
        GraphDatabaseService copyDb = managementService.database( copyName );
        try ( Transaction tx = copyDb.beginTx() )
        {
            Node a = tx.getNodeById( 0 );
            Node b = tx.getNodeById( 1 );
            Node c = tx.getNodeById( 2 );
            assertThrows( NotFoundException.class, () -> tx.getNodeById( 3 ) );

            // Validate a
            assertEquals( "On",  a.getProperty( "name" ) );
            Iterator<Relationship> aRelationships = a.getRelationships( Direction.OUTGOING ).iterator();
            Relationship aKnowsB = aRelationships.next();
            assertEquals( b.getId(), aKnowsB.getEndNodeId() );
            assertEquals( "KNOWS", aKnowsB.getType().name() );
            assertEquals( aKnowsB.getProperty( "otherProperty" ), "keep me" );
            assertFalse( aRelationships.hasNext() );

            // Validate b
            assertEquals( "Those",  b.getProperty( "name" ) );
            assertEquals( "keep me",  b.getProperty( "secretProperty" ) );
            Iterator<Relationship> bRelationships = b.getRelationships( Direction.OUTGOING ).iterator();
            Relationship bKnowsC = bRelationships.next();
            assertEquals( c.getId(), bKnowsC.getEndNodeId() );
            assertEquals( "KNOWS", bKnowsC.getType().name() );
            assertThrows( NotFoundException.class, () -> bKnowsC.getProperty( "secretProperty" ) );
            assertFalse( bRelationships.hasNext() );

            // Validate c
            assertEquals( "Trays",  c.getProperty( "name" ) );
            Iterator<Relationship> cRelationships = c.getRelationships( Direction.OUTGOING ).iterator();
            Relationship cKnowsA = cRelationships.next();
            assertEquals( a.getId(), cKnowsA.getEndNodeId() );
            assertEquals( "SECRET", cKnowsA.getType().name() );
            assertEquals( cKnowsA.getProperty( "secretProperty" ), "keep me" );
            assertFalse( cRelationships.hasNext() );
            tx.commit();
        }
        managementService.dropDatabase( copyName );
    }

    @Test
    void cankeepOnlyRelationshipProperties() throws Exception
    {
        // Create some data
        try ( Transaction tx = databaseAPI.beginTx() )
        {
            Node a = tx.createNode( NUMBER_LABEL, CHARACTER_LABEL );
            a.setProperty( "name", "On" );
            Node b = tx.createNode( NUMBER_LABEL );
            b.setProperty( "name", "Those" );
            b.setProperty( "secretProperty", "keep me" );
            Node c = tx.createNode( NUMBER_LABEL );
            c.setProperty( "name", "Trays" );

            Relationship rel = a.createRelationshipTo( b, KNOWS );
            rel.setProperty( "otherProperty", "keep me" );
            rel = b.createRelationshipTo( c, KNOWS );
            rel.setProperty( "secretProperty", "Please delete me!" );
            rel = c.createRelationshipTo( a, SECRET );
            rel.setProperty( "secretProperty", "keep me" );
            tx.commit();
        }
        String databaseName = databaseAPI.databaseName();
        String copyName = getCopyName( databaseName, "copy" );
        managementService.shutdownDatabase( databaseName );

        copyDatabase( "--from-database=" + databaseName,
                "--to-database=" + copyName,
                "--keep-only-relationship-properties=SECRET.secretProperty,KNOWS.otherProperty");

        managementService.createDatabase( copyName );
        GraphDatabaseService copyDb = managementService.database( copyName );
        try ( Transaction tx = copyDb.beginTx() )
        {
            Node a = tx.getNodeById( 0 );
            Node b = tx.getNodeById( 1 );
            Node c = tx.getNodeById( 2 );
            assertThrows( NotFoundException.class, () -> tx.getNodeById( 3 ) );

            // Validate a
            assertEquals( "On",  a.getProperty( "name" ) );
            Iterator<Relationship> aRelationships = a.getRelationships( Direction.OUTGOING ).iterator();
            Relationship aKnowsB = aRelationships.next();
            assertEquals( b.getId(), aKnowsB.getEndNodeId() );
            assertEquals( "KNOWS", aKnowsB.getType().name() );
            assertEquals( aKnowsB.getProperty( "otherProperty" ), "keep me" );
            assertFalse( aRelationships.hasNext() );

            // Validate b
            assertEquals( "Those",  b.getProperty( "name" ) );
            assertEquals( "keep me",  b.getProperty( "secretProperty" ) );
            Iterator<Relationship> bRelationships = b.getRelationships( Direction.OUTGOING ).iterator();
            Relationship bKnowsC = bRelationships.next();
            assertEquals( c.getId(), bKnowsC.getEndNodeId() );
            assertEquals( "KNOWS", bKnowsC.getType().name() );
            assertThrows( NotFoundException.class, () -> bKnowsC.getProperty( "secretProperty" ) );
            assertFalse( bRelationships.hasNext() );

            // Validate c
            assertEquals( "Trays",  c.getProperty( "name" ) );
            Iterator<Relationship> cRelationships = c.getRelationships( Direction.OUTGOING ).iterator();
            Relationship cKnowsA = cRelationships.next();
            assertEquals( a.getId(), cKnowsA.getEndNodeId() );
            assertEquals( "SECRET", cKnowsA.getType().name() );
            assertEquals( cKnowsA.getProperty( "secretProperty" ), "keep me" );
            assertFalse( cRelationships.hasNext() );
            tx.commit();
        }
        managementService.dropDatabase( copyName );
    }

    @Test
    void canHandleEscapedStrings() throws Exception
    {
        Label labelWithComma = Label.label( "My,comma,label" );
        // Create some data
        try ( Transaction tx = databaseAPI.beginTx() )
        {
            Node a = tx.createNode( NUMBER_LABEL, labelWithComma );
            a.setProperty( "name", "On" );
            a.setProperty( "property.with.dots", "value" );
            tx.createNode( NUMBER_LABEL );

            tx.commit();
        }

        String databaseName = databaseAPI.databaseName();
        String copyName = getCopyName( databaseName, "copy" );
        managementService.shutdownDatabase( databaseName );

        copyDatabase( "--from-database=" + databaseName,
                "--to-database=" + copyName,
                "--keep-only-nodes-with-labels=`My,comma,label`",
                "--keep-only-node-properties=Number.`property.with.dots`"
        );

        managementService.createDatabase( copyName );
        GraphDatabaseService copyDb = managementService.database( copyName );

        try ( Transaction tx = copyDb.beginTx() )
        {
            ResourceIterable<Node> allNodes = tx.getAllNodes();
            List<Node> nodes = allNodes.stream().collect( Collectors.toList() );
            assertThat( nodes.size() ).isEqualTo( 1 );

            Node a = nodes.get( 0 );

            assertThat( a.getLabels() ).containsExactlyInAnyOrder( labelWithComma, NUMBER_LABEL );
            assertThat( a.getAllProperties() ).containsOnly( entry( "property.with.dots", "value" ) );
            assertFalse( a.getRelationships().iterator().hasNext() );

            tx.commit();
        }
        managementService.dropDatabase( copyName );
    }

    @Test
    void respectFormat() throws Exception
    {
        try ( Transaction tx = databaseAPI.beginTx() )
        {
            Node a = tx.createNode( NUMBER_LABEL, ERROR_LABEL );
            a.setProperty( "name", "Anna" );
            Node b = tx.createNode( NUMBER_LABEL, ERROR_LABEL );
            b.setProperty( "name", "Bob" );
            Node c = tx.createNode( NUMBER_LABEL, ERROR_LABEL );
            c.setProperty( "name", "Carrie" );
            a.createRelationshipTo( b, KNOWS );
            b.createRelationshipTo( c, KNOWS );
            tx.commit();
        }
        try ( Transaction tx = databaseAPI.beginTx() )
        {
            tx.getRelationshipById( 1 ).delete();
            tx.getNodeById( 2 ).delete();
            tx.commit();
        }
        String databaseName = databaseAPI.databaseName();
        managementService.shutdownDatabase( databaseName );

        String highLimitCopyName = getCopyName( databaseName, "copy-hl" );
        String standardCopyName = getCopyName( databaseName, "copy-std" );
        String standardSameCopyName = getCopyName( databaseName, "copy-same-std" );
        String highLimitSameCopyName = getCopyName( databaseName, "copy-same-hl" );

        // Standard -> high limit
        copyDatabase( "--from-database=" + databaseName, "--to-database=" + highLimitCopyName, "--to-format=high_limit" );

        // High limit -> standard
        copyDatabase( "--from-database=" + highLimitCopyName, "--to-database=" + standardCopyName, "--to-format=standard" );

        // Standard -> same
        copyDatabase( "--from-database=" + standardCopyName, "--to-database=" + standardSameCopyName, "--to-format=same" );

        // High limit -> same
        copyDatabase( "--from-database=" + highLimitCopyName, "--to-database=" + highLimitSameCopyName, "--to-format=same" );

        assertRecordFormat( highLimitCopyName, HighLimitFormatFamily.INSTANCE );
        assertRecordFormat( standardCopyName, StandardFormatFamily.INSTANCE );
        assertRecordFormat( standardSameCopyName, StandardFormatFamily.INSTANCE );
        assertRecordFormat( highLimitSameCopyName, HighLimitFormatFamily.INSTANCE );

        validateCopyContents( highLimitCopyName );
        validateCopyContents( standardCopyName );
        validateCopyContents( standardSameCopyName );
        validateCopyContents( highLimitSameCopyName );
    }

    private void validateCopyContents( String dbName )
    {
        managementService.createDatabase( dbName );
        GraphDatabaseService copyDb = managementService.database( dbName );
        try ( Transaction tx = copyDb.beginTx() )
        {
            assertEquals( "Anna", tx.getNodeById( 0 ).getProperty( "name" ) );
            assertEquals( "Bob", tx.getNodeById( 1 ).getProperty( "name" ) );
            assertEquals( KNOWS, single( tx.getNodeById( 0 ).getRelationships() ).getType() );
            assertThrows( NotFoundException.class, () -> tx.getNodeById( 2 ) );
            assertThrows( NotFoundException.class, () -> tx.getRelationshipById( 1 ) );
            tx.commit();
        }
        managementService.dropDatabase( dbName );
    }

    @Test
    void copySchema() throws Exception
    {
        // Create some data
        try ( Transaction tx = databaseAPI.beginTx() )
        {
            Node a = tx.createNode( NUMBER_LABEL );
            a.setProperty( "name", "Uno" );
            Node b = tx.createNode( CHARACTER_LABEL );
            b.setProperty( "name", "Dos" );
            Node c = tx.createNode( NUMBER_LABEL );
            c.setProperty( "name", "Tres" );

            a.createRelationshipTo( b, KNOWS );
            tx.commit();
        }
        try ( Transaction tx = databaseAPI.beginTx() )
        {
            tx.schema().indexFor( NUMBER_LABEL ).on( "name" ).withName( "myIndex" ).create();
            tx.commit();
        }

        String databaseName = databaseAPI.databaseName();
        String copyName = getCopyName( databaseName, "copy" );
        managementService.shutdownDatabase( databaseName );

        copyDatabase( "--from-database=" + databaseName, "--to-database=" + copyName );

        assertTrue( suppressOutput.getOutputVoice().containsMessage( "CALL db.createIndex('myIndex'" ) );
    }

    @Test
    void mustRepairBrokenTokens() throws Exception
    {
        // Create some data
        try ( Transaction tx = databaseAPI.beginTx() )
        {
            Node a = tx.createNode( NUMBER_LABEL );
            a.setProperty( "a", 1 );
            a.setProperty( "b", 2 );
            a.createRelationshipTo( a, KNOWS );
            tx.commit();
        }

        String databaseName = databaseAPI.databaseName();
        String copyName = getCopyName( databaseName, "copy" );

        // Create a name duplication inconsistency in the property key token store:
        TokenHolders tokens = databaseAPI.getDependencyResolver().resolveDependency( TokenHolders.class );
        RecordStorageEngine engine = databaseAPI.getDependencyResolver().resolveDependency( RecordStorageEngine.class );
        int idA = tokens.propertyKeyTokens().getIdByName( "a" );
        int idB = tokens.propertyKeyTokens().getIdByName( "b" );
        PropertyKeyTokenStore store = engine.testAccessNeoStores().getPropertyKeyTokenStore();
        var tokenA = store.getRecord( idA, store.newRecord(), RecordLoad.NORMAL, PageCursorTracer.NULL );
        var tokenB = store.getRecord( idB, store.newRecord(), RecordLoad.NORMAL, PageCursorTracer.NULL );
        tokenB.initialize( tokenA.inUse(), tokenA.getNameId(), tokenA.getPropertyCount() );
        store.updateRecord( tokenB, PageCursorTracer.NULL );

        managementService.shutdownDatabase( databaseName );

        copyDatabase( "--from-database=" + databaseName, "--to-database=" + copyName );

        managementService.createDatabase( copyName );
        GraphDatabaseService db = managementService.database( copyName );
        try ( Transaction tx = db.beginTx() )
        {
            Node node  = single( tx.getAllNodes() );
            Map<String, Object> properties = node.getAllProperties();
            assertThat( properties.remove( "a" ) ).isEqualTo( 1 );
            assertThat( single( properties.values() ) ).isEqualTo( 2 );
        }
        String output = suppressOutput.getOutputVoice().toString();
        assertTrue( output.contains( "tokens were recreated" ) );
        // One occurrence reporting the broken token. Then another reporting its invented replacement:
        assertThat( countOccurrences( output, "PropertyKey(" + idB + ")" ) ).isEqualTo( 2 );
    }

    private int countOccurrences( String haystack, String needle )
    {
        int count = 0;
        int index = 0;
        while ( ( index = haystack.indexOf( needle, index ) ) != -1 )
        {
            count++;
            index++;
        }
        return count;
    }

    @Test
    void specifyPageCacheSize() throws Exception
    {
        // Create some data
        try ( Transaction tx = databaseAPI.beginTx() )
        {
            Node a = tx.createNode( NUMBER_LABEL );
            a.setProperty( "name", "Uno" );
            Node b = tx.createNode( NUMBER_LABEL );
            b.setProperty( "name", "Dos" );
            Node c = tx.createNode( NUMBER_LABEL );
            c.setProperty( "name", "Tres" );

            a.createRelationshipTo( b, KNOWS );
            tx.commit();
        }
        String databaseName = databaseAPI.databaseName();
        String copyName = getCopyName( databaseName, "copy" );
        managementService.shutdownDatabase( databaseName );

        copyDatabase( "--from-database=" + databaseName,
                "--to-database=" + copyName,
                "--from-pagecache=6m",
                "--to-pagecache=7m" );

        assertTrue( suppressOutput.getOutputVoice().containsMessage( "(page cache 6m)" ) );
        assertTrue( suppressOutput.getOutputVoice().containsMessage( "(page cache 7m)" ) );
    }

    private void copyDatabase( String... args ) throws Exception
    {
        copyDatabase( NULL, args );
    }

    private void copyDatabase( PageCacheTracer pageCacheTracer,  String... args ) throws Exception
    {
        var context = new ExecutionContext( neo4jHome, configDir );
        var command = new StoreCopyCommand( context );

        CommandLine.populateCommand( command, args );
        command.setPageCacheTracer( pageCacheTracer );
        command.execute();
    }

    private String getDatabaseAbsolutePath( String databaseName )
    {
        return databaseAPI.databaseLayout().getNeo4jLayout().databaseLayout( databaseName ).databaseDirectory().toAbsolutePath().toString();
    }

    private void assertRecordFormat( String databaseName, FormatFamily formatFamily )
    {
        RecordFormats recordFormats = Objects.requireNonNull(
                RecordFormatSelector.selectForStore( databaseAPI.databaseLayout().getNeo4jLayout().databaseLayout( databaseName ), fs, pageCache,
                        NullLogProvider.getInstance(), NULL ) );
        assertEquals( formatFamily, recordFormats.getFormatFamily() );
    }

    static String getCopyName( String databaseName, String copySuffix )
    {
        return databaseName.substring( 0, min( databaseName.length(), 63 - copySuffix.length() ) ) + copySuffix;
    }
}
