/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.logging;

import com.neo4j.causalclustering.identity.MemberId;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.OutputStream;
import java.time.Clock;
import java.util.UUID;

import org.neo4j.io.fs.FileSystemAbstraction;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class BetterRaftMessageLoggerTest
{
    @Test
    void shouldOpenAndCloseWriter() throws Exception
    {
        var memberId = new MemberId( UUID.randomUUID() );
        var logFile = new File( "raft-messages" );
        var fs = mock( FileSystemAbstraction.class );
        var outputStream = mock( OutputStream.class );
        when( fs.openAsOutputStream( logFile, true ) ).thenReturn( outputStream );

        var logger = new BetterRaftMessageLogger<>( memberId, logFile, fs, Clock.systemUTC() );
        verify( fs, never() ).openAsOutputStream( any( File.class ), anyBoolean() );

        logger.start();
        verify( fs ).openAsOutputStream( logFile, true );

        logger.stop();
        verify( outputStream ).close();
    }
}
