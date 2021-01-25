/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.log.segmented;

import java.io.IOException;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static java.util.Optional.empty;
import static java.util.Optional.of;

class ReaderPool
{
    private ArrayList<Reader> pool;
    private final int maxSize;
    private final Log log;
    private final FileNames fileNames;
    private final FileSystemAbstraction fsa;
    private final Clock clock;

    ReaderPool( int maxSize, LogProvider logProvider, FileNames fileNames, FileSystemAbstraction fsa, Clock clock )
    {
        this.pool = new ArrayList<>( maxSize );
        this.maxSize = maxSize;
        this.log = logProvider.getLog( getClass() );
        this.fileNames = fileNames;
        this.fsa = fsa;
        this.clock = clock;
    }

    Reader acquire( long version, long byteOffset ) throws IOException
    {
        Reader reader = getFromPool( version );
        if ( reader == null )
        {
            reader = createFor( version );
        }
        reader.channel().position( byteOffset );
        return reader;
    }

    void release( Reader reader )
    {
        reader.setTimeStamp( clock.millis() );
        Optional<Reader> optionalOverflow = putInPool( reader );
        optionalOverflow.ifPresent( this::dispose );
    }

    private synchronized Reader getFromPool( long version )
    {
        Iterator<Reader> itr = pool.iterator();
        while ( itr.hasNext() )
        {
            Reader reader = itr.next();
            if ( reader.version() == version )
            {
                itr.remove();
                return reader;
            }
        }
        return null;
    }

    private synchronized Optional<Reader> putInPool( Reader reader )
    {
        pool.add( reader );
        return pool.size() > maxSize ? of( pool.remove( 0 ) ) : empty();
    }

    private Reader createFor( long version ) throws IOException
    {
        return new Reader( fsa, fileNames.getForSegment( version ), version );
    }

    synchronized void prune( long maxAge, TimeUnit unit )
    {
        if ( pool == null )
        {
            return;
        }

        long endTimeMillis = clock.millis() - unit.toMillis( maxAge );

        Iterator<Reader> itr = pool.iterator();
        while ( itr.hasNext() )
        {
            Reader reader = itr.next();
            if ( reader.getTimeStamp() < endTimeMillis )
            {
                dispose( reader );
                itr.remove();
            }
        }
    }

    private void dispose( Reader reader )
    {
        try
        {
            reader.close();
        }
        catch ( IOException e )
        {
            log.error( "Failed to close reader", e );
        }
    }

    synchronized void close() throws IOException
    {
        for ( Reader reader : pool )
        {
            reader.close();
        }
        pool.clear();
        pool = null;
    }

    public synchronized void prune( long version )
    {
        if ( pool == null )
        {
            return;
        }

        Iterator<Reader> itr = pool.iterator();
        while ( itr.hasNext() )
        {
            Reader reader = itr.next();
            if ( reader.version() == version )
            {
                dispose( reader );
                itr.remove();
            }
        }
    }
}
