/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.logging;

import com.neo4j.causalclustering.core.consensus.RaftMessages.RaftMessage;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Path;
import java.time.Clock;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Objects;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.util.VisibleForTesting;

import static java.lang.String.format;
import static java.lang.String.valueOf;
import static org.neo4j.io.IOUtils.closeAllSilently;
import static org.neo4j.util.Preconditions.checkState;

public class BetterRaftMessageLogger<MEMBER> extends LifecycleAdapter implements RaftMessageLogger<MEMBER>
{
    private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern( "yyyy-MM-dd HH:mm:ss.SSSZ" );

    private enum Direction
    {
        INFO( "---" ),
        OUTBOUND( "-->" ),
        INBOUND( "<--" );

        public final String arrow;

        Direction( String arrow )
        {
            this.arrow = arrow;
        }
    }

    private final MEMBER me;
    private final Path logFile;
    private final FileSystemAbstraction fs;
    private final Clock clock;
    private final ReadWriteLock lifecycleLock = new ReentrantReadWriteLock();

    private PrintWriter printWriter;

    public BetterRaftMessageLogger( MEMBER me, Path logFile, FileSystemAbstraction fs, Clock clock )
    {
        this.me = me;
        this.logFile = logFile;
        this.fs = fs;
        this.clock = clock;
    }

    @Override
    public void start() throws IOException
    {
        lifecycleLock.writeLock().lock();
        try
        {
            checkState( printWriter == null, "Already started" );
            printWriter = openPrintWriter();
            log( "", me, Direction.INFO, me, "Info", "I am " + me );
        }
        finally
        {
            lifecycleLock.writeLock().unlock();
        }
    }

    @Override
    public void stop()
    {
        lifecycleLock.writeLock().lock();
        try
        {
            closeAllSilently( printWriter );
            printWriter = null;
        }
        finally
        {
            lifecycleLock.writeLock().unlock();
        }
    }

    @Override
    public void logOutbound( NamedDatabaseId databaseId, MEMBER me, RaftMessage message, MEMBER remote )
    {
        // timestamp me --> remote for database type message
        log( convertDatabaseId( databaseId ), me, Direction.OUTBOUND, remote, nullSafeMessageType( message ), valueOf( message ) );
    }

    @Override
    public void logInbound( NamedDatabaseId databaseId, MEMBER remote, RaftMessage message, MEMBER me )
    {
        // timestamp me <-- remote for database type message
        log( convertDatabaseId( databaseId ), me, Direction.INBOUND, remote, nullSafeMessageType( message ), valueOf( message ) );
    }

    private String convertDatabaseId( NamedDatabaseId databaseId )
    {
        return databaseId == null ? "for unknownDB" : format( "for %s", databaseId );
    }

    @VisibleForTesting
    protected PrintWriter openPrintWriter() throws IOException
    {
        fs.mkdirs( logFile.getParent() );
        return new PrintWriter( fs.openAsOutputStream( logFile, true ) );
    }

    private void log( String forDatabase, MEMBER first, Direction direction, MEMBER second, String type, String message )
    {
        lifecycleLock.readLock().lock();
        try
        {
            if ( printWriter != null )
            {
                var timestamp = ZonedDateTime.now( clock ).format( DATE_TIME_FORMATTER );
                var text = format( "%s %s %s %s %s %s \"%s\"", timestamp, first, direction.arrow, second, forDatabase, type, message );
                printWriter.println( text );
                printWriter.flush();
            }
        }
        finally
        {
            lifecycleLock.readLock().unlock();
        }
    }

    private static String nullSafeMessageType( RaftMessage message )
    {
        if ( Objects.isNull( message ) )
        {
            return "null";
        }
        else
        {
            return message.type().toString();
        }
    }
}
