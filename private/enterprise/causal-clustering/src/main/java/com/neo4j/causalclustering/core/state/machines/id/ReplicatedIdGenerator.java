/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state.machines.id;

import com.neo4j.causalclustering.error_handling.Panicker;

import java.io.File;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.LongSupplier;

import org.neo4j.internal.id.IdContainer;
import org.neo4j.internal.id.IdGenerator;
import org.neo4j.internal.id.IdRange;
import org.neo4j.internal.id.IdRangeIterator;
import org.neo4j.internal.id.IdType;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static java.lang.Math.max;
import static org.neo4j.internal.id.IdRangeIterator.EMPTY_ID_RANGE_ITERATOR;
import static org.neo4j.internal.id.IdRangeIterator.VALUE_REPRESENTING_NULL;

class ReplicatedIdGenerator implements IdGenerator
{
    private final IdType idType;
    private final Log log;
    private final Panicker panicker;
    private final ReplicatedIdRangeAcquirer acquirer;
    private volatile long highId;
    private volatile IdRangeIterator idQueue = EMPTY_ID_RANGE_ITERATOR;
    private final IdContainer idContainer;
    private final ReentrantLock idContainerLock = new ReentrantLock();

    ReplicatedIdGenerator( FileSystemAbstraction fs, File file, IdType idType, LongSupplier highId, ReplicatedIdRangeAcquirer acquirer, LogProvider logProvider,
            Panicker panicker )
    {
        this.idType = idType;
        this.highId = highId.getAsLong();
        this.acquirer = acquirer;
        this.log = logProvider.getLog( getClass() );
        this.panicker = panicker;
        idContainer = new IdContainer( fs, file, 1024, false );
        idContainer.init();
    }

    @Override
    public void close()
    {
        idContainerLock.lock();
        try
        {
            idContainer.close( highId );
        }
        finally
        {
            idContainerLock.unlock();
        }
    }

    @Override
    public void freeId( long id )
    {
        idContainerLock.lock();
        try
        {
            idContainer.freeId( id );
        }
        finally
        {
            idContainerLock.unlock();
        }
    }

    @Override
    public void deleteId( long id )
    {
        throw new UnsupportedOperationException( "Not implemented yet" );
    }

    @Override
    public void markIdAsUsed( long id )
    {
        throw new UnsupportedOperationException( "Not implemented yet" );
    }

    @Override
    public ReuseMarker reuseMarker()
    {
        throw new UnsupportedOperationException( "Not implemented yet" );
    }

    @Override
    public CommitMarker commitMarker()
    {
        throw new UnsupportedOperationException( "Not implemented yet" );
    }

    @Override
    public long getHighId()
    {
        return highId;
    }

    @Override
    public void setHighId( long id )
    {
        this.highId = max( this.highId, id );
    }

    @Override
    public long getHighestPossibleIdInUse()
    {
        return highId - 1;
    }

    @Override
    public long getNumberOfIdsInUse()
    {
        return highId - getDefragCount();
    }

    @Override
    public synchronized long nextId()
    {
        long id = getReusableId();
        if ( id != IdContainer.NO_RESULT )
        {
            return id;
        }

        long nextId = idQueue.nextId();
        if ( nextId == VALUE_REPRESENTING_NULL )
        {
            acquireNextIdBatch();
            nextId = idQueue.nextId();
        }
        highId = max( highId, nextId + 1 );
        return nextId;
    }

    private void acquireNextIdBatch()
    {
        IdAllocation allocation = acquirer.acquireIds( idType );

        assert allocation.getIdRange().getRangeLength() > 0;
        log.debug( "Received id allocation " + allocation + " for " + idType );
        storeLocally( allocation );
    }

    @Override
    public synchronized IdRange nextIdBatch( int size )
    {
        IdRange idBatch = getReusableIdBatch( size );
        if ( idBatch.totalSize() > 0 )
        {
            return idBatch;
        }
        IdRange range = idQueue.nextIdBatch( size );
        if ( range.totalSize() == 0 )
        {
            acquireNextIdBatch();
            range = idQueue.nextIdBatch( size );
            setHighId( range.getHighId() );
        }
        return range;
    }

    @Override
    public long getDefragCount()
    {
        idContainerLock.lock();
        try
        {
            return idContainer.getFreeIdCount();
        }
        finally
        {
            idContainerLock.unlock();
        }
    }

    @Override
    public void start()
    {   // Nothing to do
    }

    @Override
    public void checkpoint( IOLimiter ioLimiter )
    {
        throw new UnsupportedOperationException( "Not implemented yet" );
    }

    @Override
    public void maintenance()
    {   // Nothing to do
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName() + "[" + this.idQueue + "]";
    }

    static void createGenerator( FileSystemAbstraction fs, File fileName, long highId,
            boolean throwIfFileExists )
    {
        IdContainer.createEmptyIdFile( fs, fileName, highId, throwIfFileExists );
    }

    private long getReusableId()
    {
        idContainerLock.lock();
        try
        {
            return idContainer.getReusableId();
        }
        finally
        {
            idContainerLock.unlock();
        }
    }

    private IdRange getReusableIdBatch( int maxSize )
    {
        idContainerLock.lock();
        try
        {
            return idContainer.getReusableIdBatch( maxSize );
        }
        finally
        {
            idContainerLock.unlock();
        }
    }

    private void storeLocally( IdAllocation allocation )
    {
        setHighId( allocation.getHighestIdInUse() + 1 ); // high id is certainly bigger than the highest id in use
        this.idQueue = respectingHighId( allocation.getIdRange() ).iterator();
    }

    private IdRange respectingHighId( IdRange idRange )
    {
        int adjustment = 0;
        long originalRangeStart = idRange.getRangeStart();
        if ( highId > originalRangeStart )
        {
            adjustment = (int) (highId - originalRangeStart);
        }
        long rangeStart = max( this.highId, originalRangeStart );
        int rangeLength = idRange.getRangeLength() - adjustment;
        if ( rangeLength <= 0 )
        {
            IdAllocationException idAllocationException = new IdAllocationException(
                    "IdAllocation state is probably corrupted or out of sync with the cluster. Local highId is " + highId + " and allocation range is " +
                            idRange );
            panicker.panic( idAllocationException );
            throw idAllocationException;
        }
        return new IdRange( idRange.getDefragIds(), rangeStart, rangeLength );
    }
}
