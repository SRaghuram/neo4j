/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.logging;

import com.neo4j.causalclustering.core.consensus.RaftMessages;
import com.neo4j.causalclustering.identity.IdFactory;
import com.neo4j.causalclustering.identity.RaftMemberId;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.nio.file.Path;
import java.time.Clock;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.database.DatabaseIdRepository;
import org.neo4j.kernel.database.NamedDatabaseId;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

class BetterRaftMessageLoggerTest
{
    private static final String INCOMING_PATTERN = "^\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\\.\\d{3}\\+0000 <-- .*[\\n\\r]+";
    private static final String OUTGOING_PATTERN = "^\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\\.\\d{3}\\+0000 --> .*[\\n\\r]+";
    private final NamedDatabaseId databaseId = DatabaseIdRepository.NAMED_SYSTEM_DATABASE_ID;
    private final RaftMemberId memberId = IdFactory.randomRaftMemberId();
    private final Path logFile = Path.of( "logs/raft-messages" );
    private final FileSystemAbstraction fs = mock( FileSystemAbstraction.class );

    private final BetterRaftMessageLogger<RaftMemberId> logger = new BetterRaftMessageLogger<>( logFile, fs, Clock.systemUTC() );

    @Test
    void shouldOpenAndCloseWriter() throws Exception
    {
        var outputStream = mock( OutputStream.class );
        when( fs.openAsOutputStream( logFile, true ) ).thenReturn( outputStream );
        verify( fs, never() ).openAsOutputStream( any( Path.class ), anyBoolean() );

        logger.start();
        verify( fs ).openAsOutputStream( logFile, true );

        logger.stop();
        verify( outputStream ).close();
    }

    @Test
    void shouldLogNothingWhenStopped() throws Exception
    {
        var outputStream = mock( OutputStream.class );
        when( fs.openAsOutputStream( logFile, true ) ).thenReturn( outputStream );
        logger.start();
        verifyNoMoreInteractions( outputStream );

        logger.stop();
        verify( outputStream ).close();

        var message = new RaftMessages.Heartbeat( memberId, 1, 1, 1 );
        logger.logInbound( databaseId, memberId, message );
        logger.logOutbound( databaseId, memberId, message );

        verifyNoMoreInteractions( outputStream );
    }

    @Test
    void shouldLogInboundMessageWithDatabase() throws Exception
    {
        var outputStream = new ByteArrayOutputStream();
        when( fs.openAsOutputStream( logFile, true ) ).thenReturn( outputStream );
        logger.start();
        var message = new RaftMessages.Heartbeat( memberId, 1, 1, 1 );
        logger.logInbound( databaseId, memberId, message );
        logger.stop();

        var logLine = outputStream.toString();
        assertTrue( logLine.matches( INCOMING_PATTERN ) );
        assertTrue( logLine.contains( memberId.toString() ) );
        assertTrue( logLine.contains( databaseId.toString() ) );
        assertTrue( logLine.contains( message.type().toString() ) );
        assertTrue( logLine.contains( message.toString() ) );
    }

    @Test
    void shouldLogOutboundMessageWithDatabase() throws Exception
    {
        var outputStream = new ByteArrayOutputStream();
        when( fs.openAsOutputStream( logFile, true ) ).thenReturn( outputStream );
        logger.start();
        var message = new RaftMessages.Heartbeat( memberId, 1, 1, 1 );
        logger.logOutbound( databaseId, memberId, message );
        logger.stop();

        var logLine = outputStream.toString();
        assertTrue( logLine.matches( OUTGOING_PATTERN ) );
        assertTrue( logLine.contains( memberId.toString() ) );
        assertTrue( logLine.contains( databaseId.toString() ) );
        assertTrue( logLine.contains( message.type().toString() ) );
        assertTrue( logLine.contains( message.toString() ) );
    }

    @Test
    void shouldLogInboundWithoutMessageAndDatabase() throws Exception
    {
        var outputStream = new ByteArrayOutputStream();
        when( fs.openAsOutputStream( logFile, true ) ).thenReturn( outputStream );
        logger.start();
        logger.logInbound( null, memberId, null );
        logger.stop();

        var logLine = outputStream.toString();
        assertTrue( logLine.matches( INCOMING_PATTERN ) );
        assertTrue( logLine.contains( memberId.toString() ) );
        assertTrue( logLine.contains( " for unknownDB" ) );
        assertTrue( logLine.contains( "unknown type" ) );
        assertTrue( logLine.contains( "\"null\"" ) );
    }

    @Test
    void shouldLogOutboundWithoutMessageAndDatabase() throws Exception
    {
        var outputStream = new ByteArrayOutputStream();
        when( fs.openAsOutputStream( logFile, true ) ).thenReturn( outputStream );
        logger.start();
        logger.logOutbound( null, memberId, null );
        logger.stop();

        var logLine = outputStream.toString();
        assertTrue( logLine.matches( OUTGOING_PATTERN ) );
        assertTrue( logLine.contains( memberId.toString() ) );
        assertTrue( logLine.contains( "for unknownDB" ) );
        assertTrue( logLine.contains( "unknown type" ) );
        assertTrue( logLine.contains( "\"null\"" ) );
    }
}
