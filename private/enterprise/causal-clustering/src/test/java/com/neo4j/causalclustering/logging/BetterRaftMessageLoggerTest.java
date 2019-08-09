/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.logging;

import com.neo4j.causalclustering.core.consensus.RaftMessages;
import com.neo4j.causalclustering.identity.MemberId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.OutputStream;
import java.time.Clock;
import java.util.UUID;

import org.neo4j.io.fs.FileSystemAbstraction;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

class BetterRaftMessageLoggerTest
{
    private final MemberId memberId = new MemberId( UUID.randomUUID() );
    private final File logFile = new File( "raft-messages" );
    private final FileSystemAbstraction fs = mock( FileSystemAbstraction.class );
    private final OutputStream outputStream = mock( OutputStream.class );

    private final BetterRaftMessageLogger<MemberId> logger = new BetterRaftMessageLogger<>( memberId, logFile, fs, Clock.systemUTC() );

    @BeforeEach
    void beforeEach() throws Exception
    {
        when( fs.openAsOutputStream( logFile, true ) ).thenReturn( outputStream );
    }

    @Test
    void shouldOpenAndCloseWriter() throws Exception
    {
        verify( fs, never() ).openAsOutputStream( any( File.class ), anyBoolean() );

        logger.start();
        verify( fs ).openAsOutputStream( logFile, true );

        logger.stop();
        verify( outputStream ).close();
    }

    @Test
    void shouldLogNothingWhenStopped() throws Exception
    {
        logger.start();
        verify( outputStream ).write( any(), anyInt(), anyInt() );
        verify( outputStream ).flush();

        logger.stop();
        verify( outputStream ).close();

        var message = new RaftMessages.Heartbeat( memberId, 1, 1, 1 );
        logger.logInbound( memberId, message, memberId );
        logger.logOutbound( memberId, message, memberId );

        verifyNoMoreInteractions( outputStream );
    }
}
