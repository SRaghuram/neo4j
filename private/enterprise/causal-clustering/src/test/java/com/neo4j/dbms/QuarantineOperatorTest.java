/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms;

import com.neo4j.causalclustering.common.state.ClusterStateStorageFactory;
import com.neo4j.causalclustering.core.state.ClusterStateLayout;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.util.UUID;

import org.neo4j.configuration.Config;
import org.neo4j.dbms.api.DatabaseManagementException;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.database.DatabaseIdFactory;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.database.TestDatabaseIdRepository;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.string.UTF8;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.logging.NullLogProvider.nullLogProvider;

@TestDirectoryExtension
class QuarantineOperatorTest
{
    @Inject
    private TestDirectory testDirectory;

    @Inject
    private FileSystemAbstraction fs;

    private ClusterStateLayout layout;
    private QuarantineOperator operator;

    private TestDatabaseIdRepository databaseIdRepository = new TestDatabaseIdRepository();
    private NamedDatabaseId databaseId = databaseIdRepository.defaultDatabase();
    private String databaseName = databaseId.name();

    @BeforeEach
    void beforeEach()
    {
        layout = ClusterStateLayout.of( testDirectory.homePath() );
        var storageFactory = new ClusterStateStorageFactory( fs, layout, nullLogProvider(), Config.defaults(), EmptyMemoryTracker.INSTANCE );
        operator = new QuarantineOperator( nullLogProvider(), databaseIdRepository, storageFactory );
    }

    @Test
    void checkOperatorDesiredIsFilledProperly()
    {
        // given
        var reason = "Just a reason";
        var differentReason = "Different reason";
        assertTrue( operator.desired.isEmpty() );

        // when
        var result = operator.putIntoQuarantine( databaseName, reason );

        // then
        assertEquals( reason, result );
        assertDesired( reason, databaseId );

        // when
        var secondResult = operator.putIntoQuarantine( databaseName, differentReason );

        // then
        assertTrue( secondResult.contains( reason ) );
        assertFalse( secondResult.contains( differentReason ) );
        assertDesired( reason, databaseId );

        // when
        result = operator.removeFromQuarantine( databaseName );

        // then
        assertTrue( operator.desired.isEmpty() );
        assertEquals( reason, result );

        // when
        secondResult = operator.removeFromQuarantine( databaseName );

        // then
        assertTrue( operator.desired.isEmpty() );
        assertFalse( secondResult.contains( reason ) );
    }

    @Test
    void markerFileGetsCreatedAndRemoved() throws IOException
    {
        // given
        var reason = "Just a reason";
        var markerFile = layout.quarantineMarkerStateFile( databaseName );
        assertFalse( fs.fileExists( markerFile ) );

        // when
        operator.putIntoQuarantine( databaseName, reason );
        operator.setQuarantineMarker( databaseId );

        // then
        assertTrue( fs.fileExists( markerFile ) );
        assertEquals( reason, read( markerFile ) );

        // when
        operator.removeFromQuarantine( databaseName );
        operator.removeQuarantineMarker( databaseId );

        // then
        assertFalse( fs.fileExists( markerFile ) );
    }

    @Test
    void removeMarkerShouldNotThrow() throws IOException
    {
        // given
        var reason = "Just a reason";
        var markerFile = layout.quarantineMarkerStateFile( databaseName );
        write( markerFile, reason );
        operator.removeQuarantineMarker( databaseId );
        assertFalse( fs.fileExists( markerFile ) );

        // when
        operator.removeQuarantineMarker( databaseId );

        // then
        assertFalse( fs.fileExists( markerFile ) );
    }

    @Test
    void removeMarkerShouldWorkOnDatabaseUnknownToManager() throws IOException
    {
        // given
        var reason = "Just a reason";
        var otherDatabaseName = "other";
        var otherDatabaseId = DatabaseIdFactory.from( otherDatabaseName, UUID.randomUUID() );
        var markerFile = layout.quarantineMarkerStateFile( otherDatabaseName );
        write( markerFile, reason );
        assertTrue( fs.fileExists( markerFile ) );

        // when
        operator.removeQuarantineMarker( otherDatabaseId );

        // then
        assertFalse( fs.fileExists( markerFile ) );
    }

    @Test
    void checkMarkShouldAddDesire() throws IOException
    {
        // given
        var reason = "Just a reason";
        var otherDatabaseName = "other";
        var otherDatabaseId = DatabaseIdFactory.from( otherDatabaseName, UUID.randomUUID() );
        var markerFile = layout.quarantineMarkerStateFile( otherDatabaseName );

        // when
        var state = operator.checkQuarantineMarker( otherDatabaseId );

        // then
        assertTrue( state.isEmpty() );

        // when
        write( markerFile, reason );
        assertTrue( operator.desired.isEmpty() );
        state = operator.checkQuarantineMarker( otherDatabaseId );

        // then
        assertTrue( state.isPresent() );
        assertEquals( state.get().operatorState(), EnterpriseOperatorState.QUARANTINED );
        assertTrue( state.get().failure().isPresent() );
        var failure = state.get().failure().get();
        assertEquals( reason, failure.getMessage() );

        assertDesired( reason, otherDatabaseId );
    }

    @Test
    void cannotQuarantineSystemDatabase()
    {
        // given
        var reason = "Just a reason";

        // when/then
        assertThrows( DatabaseManagementException.class, () -> operator.putIntoQuarantine( SYSTEM_DATABASE_NAME, reason ) );
    }

    private void assertDesired( String reason, NamedDatabaseId databaseId )
    {
        assertTrue( operator.desired.containsKey( databaseId.name() ) );
        var desired = operator.desired.get( databaseId.name() );
        assertEquals( databaseId, desired.databaseId() );
        assertEquals( EnterpriseOperatorState.QUARANTINED, desired.operatorState() );
        assertTrue( desired.failure().isPresent() );
        var failure = desired.failure().get();
        assertEquals( reason, failure.getMessage() );
        assertEquals( DatabaseManagementException.class, failure.getClass() );
    }

    private String read( Path markerFile ) throws IOException
    {
        try ( var is = fs.openAsInputStream( markerFile ) )
        {
            var bytes = is.readAllBytes();
            return UTF8.decode( bytes, 4, bytes.length - 4 );
        }
    }

    private void write( Path markerFile, String content ) throws IOException
    {
        fs.mkdirs( markerFile.getParent() );
        try ( var os = fs.openAsOutputStream( markerFile, false ) )
        {
            var bytes = UTF8.encode( content );
            os.write( new byte[]{0, 0, 0, (byte) bytes.length} );
            os.write( bytes );
            os.flush();
        }
    }
}
