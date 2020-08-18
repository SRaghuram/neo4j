/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.log;

import com.neo4j.causalclustering.core.consensus.ReplicatedString;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.LifeExtension;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@TestDirectoryExtension
@ExtendWith( LifeExtension.class )
public abstract class ConcurrentStressIT
{
    private static final int MAX_CONTENT_SIZE = 2048;
    private static final CharSequence CHARS = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";

    @Inject
    private TestDirectory testDir;
    @Inject
    private LifeSupport life;

    private final ExecutorService executor = Executors.newCachedThreadPool();

    @AfterEach
    void afterEach() throws Exception
    {
        executor.shutdownNow();
        assertTrue( executor.awaitTermination( 1, MINUTES ) );
    }

    @Test
    void readAndWrite() throws Throwable
    {
        readAndWrite( 5, 2, SECONDS );
    }

    protected abstract RaftLog createRaftLog( FileSystemAbstraction fsa, Path dir );

    private void readAndWrite( int nReaders, int time, TimeUnit unit ) throws Throwable
    {
        var raftLog = createRaftLog( testDir.getFileSystem(), testDir.directoryPath( "raft-logs" ) );
        if ( raftLog instanceof Lifecycle )
        {
            life.add( (Lifecycle) raftLog );
        }

        var futures = new ArrayList<Future<Long>>();
        futures.add( executor.submit( new TimedTask( () -> write( raftLog ), time, unit ) ) );

        for ( var i = 0; i < nReaders; i++ )
        {
            futures.add( executor.submit( new TimedTask( () -> read( raftLog ), time, unit ) ) );
        }

        for ( var f : futures )
        {
            f.get();
        }
    }

    private static class TimedTask implements Callable<Long>
    {
        final Runnable task;
        final long runTimeMillis;

        TimedTask( Runnable task, int time, TimeUnit unit )
        {
            this.task = task;
            this.runTimeMillis = unit.toMillis( time );
        }

        @Override
        public Long call()
        {
            var endTime = System.currentTimeMillis() + runTimeMillis;
            long count = 0;
            while ( endTime > System.currentTimeMillis() )
            {
                task.run();
                count++;
            }
            return count;
        }
    }

    private static void read( RaftLog raftLog )
    {
        try ( var cursor = raftLog.getEntryCursor( 0 ) )
        {
            while ( cursor.next() )
            {
                var entry = cursor.get();
                var content = (ReplicatedString) entry.content();
                assertEquals( stringForIndex( cursor.index() ), content.value() );
            }
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    private static void write( RaftLog raftLog )
    {
        var index = raftLog.appendIndex();
        var term = (index + 1) * 3;
        try
        {
            var data = stringForIndex( index + 1 );
            raftLog.append( new RaftLogEntry( term, new ReplicatedString( data ) ) );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    private static String stringForIndex( long index )
    {
        var len = ((int) index) % MAX_CONTENT_SIZE + 1;
        var str = new StringBuilder( len );

        while ( len-- > 0 )
        {
            str.append( CHARS.charAt( len % CHARS.length() ) );
        }

        return str.toString();
    }
}
