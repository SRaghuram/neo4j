/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.log;

import com.neo4j.causalclustering.core.consensus.ReplicatedString;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.test.rule.TestDirectory;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;

public abstract class ConcurrentStressIT<T extends RaftLog & Lifecycle>
{
    private static final int MAX_CONTENT_SIZE = 2048;
    @Rule
    public final TestDirectory dir = TestDirectory.testDirectory();

    protected abstract T createRaftLog( FileSystemAbstraction fsa, File dir );

    @Test
    public void readAndWrite() throws Throwable
    {
        readAndWrite( 5, 2, SECONDS );
    }

    private void readAndWrite( int nReaders, int time, TimeUnit unit ) throws Throwable
    {
        try ( DefaultFileSystemAbstraction fsa = new DefaultFileSystemAbstraction() )
        {
            LifeSupport lifeSupport = new LifeSupport();
            T raftLog = createRaftLog( fsa, dir.directory() );
            lifeSupport.add( raftLog );
            lifeSupport.start();

            try
            {
                ExecutorService es = Executors.newCachedThreadPool();

                Collection<Future<Long>> futures = new ArrayList<>();
                futures.add( es.submit( new TimedTask( () -> write( raftLog ), time, unit ) ) );

                for ( int i = 0; i < nReaders; i++ )
                {
                    futures.add( es.submit( new TimedTask( () -> read( raftLog ), time, unit ) ) );
                }

                for ( Future<Long> f : futures )
                {
                    long iterations = f.get();
                }

                es.shutdown();
            }
            finally
            {
                lifeSupport.shutdown();
            }
        }
    }

    private class TimedTask implements Callable<Long>
    {
        private Runnable task;
        private final long runTimeMillis;

        TimedTask( Runnable task, int time, TimeUnit unit )
        {
            this.task = task;
            this.runTimeMillis = unit.toMillis( time );
        }

        @Override
        public Long call()
        {
            long endTime = System.currentTimeMillis() + runTimeMillis;
            long count = 0;
            while ( endTime > System.currentTimeMillis() )
            {
                task.run();
                count++;
            }
            return count;
        }
    }

    private void read( RaftLog raftLog )
    {
        try ( RaftLogCursor cursor = raftLog.getEntryCursor( 0 ) )
        {
            while ( cursor.next() )
            {
                RaftLogEntry entry = cursor.get();
                ReplicatedString content = (ReplicatedString) entry.content();
                assertEquals( stringForIndex( cursor.index() ), content.value() );
            }
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    private void write( RaftLog raftLog )
    {
        long index = raftLog.appendIndex();
        long term = (index + 1) * 3;
        try
        {
            String data = stringForIndex( index + 1 );
            raftLog.append( new RaftLogEntry( term, new ReplicatedString( data ) ) );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    private static final CharSequence CHARS = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";

    private String stringForIndex( long index )
    {
        int len = ((int) index) % MAX_CONTENT_SIZE + 1;
        StringBuilder str = new StringBuilder( len );

        while ( len-- > 0 )
        {
            str.append( CHARS.charAt( len % CHARS.length() ) );
        }

        return str.toString();
    }
}
