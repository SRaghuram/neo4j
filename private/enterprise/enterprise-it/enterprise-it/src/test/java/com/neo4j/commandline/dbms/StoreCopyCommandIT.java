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
import java.util.Objects;

import org.neo4j.cli.CommandFailedException;
import org.neo4j.cli.ExecutionContext;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.impl.store.format.FormatFamily;
import org.neo4j.kernel.impl.store.format.RecordFormatSelector;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.store.format.standard.StandardFormatFamily;
import org.neo4j.kernel.internal.locker.FileLockException;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.rule.SuppressOutput;

import static java.lang.Math.min;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StoreCopyCommandIT extends AbstractCommandIT
{
    @Inject
    private FileSystemAbstraction fs;
    @Inject
    private SuppressOutput suppressOutput;
    private static final Label NUMBER_LABEL = Label.label( "Number" );
    private static final Label CHARACTER_LABEL = Label.label( "Character" );
    private static final Label ERROR_LABEL = Label.label( "Error" );
    private static final RelationshipType KNOWS_RELATIONSHIP_TYPE = RelationshipType.withName( "KNOWS" );

    @Test
    void cantCopyFromRunningDatabase()
    {
        CommandFailedException commandFailedException = assertThrows( CommandFailedException.class,
                () -> copyDatabase( "--from-database=" + databaseAPI.databaseName(), "--to-database=copy" ) );
        assertTrue( commandFailedException.getCause() instanceof FileLockException );
        assertThat( commandFailedException.getMessage(), containsString( "The database is in use") );
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
            assertThat( commandFailedException.getMessage(), containsString( "The directory is not empty" ) );
        }
        finally
        {
            Files.delete( file );
        }
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

            a.createRelationshipTo( b, RelationshipType.withName( "KNOWS" ) );
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
            assertEquals( RelationshipType.withName("KNOWS"), tx.getNodeById( 1 ).getRelationships().iterator().next().getType() );
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

            a.createRelationshipTo( b, RelationshipType.withName( "KNOWS" ) );
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
                "--from-path-tx=" + databaseAPI.databaseLayout().getTransactionLogsDirectory().getAbsolutePath(),
                "--to-database=" + copyName );

        managementService.createDatabase( copyName );
        GraphDatabaseService copyDb = managementService.database( copyName );
        try ( Transaction tx = copyDb.beginTx() )
        {
            assertEquals( "On", tx.getNodeById( 0 ).getProperty( "name" ) );
            assertEquals( "Those", tx.getNodeById( 1 ).getProperty( "name" ) );
            assertEquals( "Trays", tx.getNodeById( 2 ).getProperty( "name" ) );
            assertEquals( RelationshipType.withName("KNOWS"), tx.getNodeById( 1 ).getRelationships().iterator().next().getType() );
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

            a.createRelationshipTo( b, RelationshipType.withName( "KNOWS" ) );
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

            a.createRelationshipTo( b, RelationshipType.withName( "KNOWS" ) );
            b.createRelationshipTo( c, RelationshipType.withName( "KNOWS" ) );
            a.createRelationshipTo( c, RelationshipType.withName( "HATES" ) );
            tx.commit();
        }
        String databaseName = databaseAPI.databaseName();
        String copyName = getCopyName( databaseName, "copy" );
        managementService.shutdownDatabase( databaseName );

        copyDatabase( "--from-database=" + databaseName,
                "--to-database=" + copyName,
                "--skip-labels=Error",
                "--skip-properties=secretProperty",
                "--skip-relationships=HATES" );

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
            assertFalse( bRelationships.hasNext() );

            // Validate c
            assertEquals( "Trays",  c.getProperty( "name" ) );
            assertFalse( c.getRelationships( Direction.OUTGOING ).iterator().hasNext() );
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
            a.createRelationshipTo( b, KNOWS_RELATIONSHIP_TYPE );
            b.createRelationshipTo( c, KNOWS_RELATIONSHIP_TYPE );
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
            assertEquals( KNOWS_RELATIONSHIP_TYPE, tx.getNodeById( 0 ).getRelationships().iterator().next().getType() );
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

            a.createRelationshipTo( b, RelationshipType.withName( "KNOWS" ) );
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

        suppressOutput.getOutputVoice().containsMessage( "CALL db.createIndex('myIndex'" );
    }

    private void copyDatabase( String... args ) throws Exception
    {
        var context = new ExecutionContext( neo4jHome, configDir );
        var command = new StoreCopyCommand( context );

        CommandLine.populateCommand( command, args );

        command.execute();
    }

    private String getDatabaseAbsolutePath( String databaseName )
    {
        return databaseAPI.databaseLayout().getNeo4jLayout().databaseLayout( databaseName ).databaseDirectory().getAbsolutePath();
    }

    private void assertRecordFormat( String databaseName, FormatFamily formatFamily )
    {
        PageCache pageCache = databaseAPI.getDependencyResolver().resolveDependency( PageCache.class );
        RecordFormats recordFormats = Objects.requireNonNull(
                RecordFormatSelector.selectForStore( databaseAPI.databaseLayout().getNeo4jLayout().databaseLayout( databaseName ), fs, pageCache,
                        NullLogProvider.getInstance() ) );
        assertEquals( formatFamily, recordFormats.getFormatFamily() );
    }

    static String getCopyName( String databaseName, String copySuffix )
    {
        return databaseName.substring( 0, min( databaseName.length(), 63 - copySuffix.length() ) ) + copySuffix;
    }
}
