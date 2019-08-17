/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state.machines.id;

import com.neo4j.causalclustering.core.state.machines.barrier.BarrierException;
import com.neo4j.causalclustering.core.state.machines.barrier.BarrierState;

import java.io.IOException;

import org.neo4j.internal.id.FreeIds;
import org.neo4j.internal.id.IdGenerator;
import org.neo4j.internal.id.IdRange;
import org.neo4j.io.pagecache.IOLimiter;

public class BarrierAwareIdGenerator implements IdGenerator
{
    private final IdGenerator delegate;
    private final BarrierState barrierState;

    BarrierAwareIdGenerator( IdGenerator delegate, BarrierState barrierState )
    {
        this.delegate = delegate;
        this.barrierState = barrierState;
    }

    @Override
    public void setHighId( long id )
    {
        delegate.setHighId( id );
    }

    @Override
    public void markHighestWrittenAtHighId()
    {
        delegate.markHighestWrittenAtHighId();
    }

    @Override
    public long getHighId()
    {
        return delegate.getHighId();
    }

    @Override
    public long getHighestPossibleIdInUse()
    {
        return delegate.getHighestPossibleIdInUse();
    }

    @Override
    public ReuseMarker reuseMarker()
    {
        return delegate.reuseMarker();
    }

    @Override
    public CommitMarker commitMarker()
    {
        return delegate.commitMarker();
    }

    @Override
    public void close()
    {
        delegate.close();
    }

    @Override
    public long getNumberOfIdsInUse()
    {
        return delegate.getNumberOfIdsInUse();
    }

    @Override
    public long getDefragCount()
    {
        return delegate.getDefragCount();
    }

    @Override
    public void checkpoint( IOLimiter ioLimiter )
    {
        delegate.checkpoint( ioLimiter );
    }

    @Override
    public void maintenance()
    {
        delegate.maintenance();
    }

    @Override
    public void start( FreeIds freeIdsForRebuild ) throws IOException
    {
        delegate.start( freeIdsForRebuild );
    }

    @Override
    public long nextId()
    {
        ensureCanManageIds();
        return delegate.nextId();
    }

    @Override
    public IdRange nextIdBatch( int size )
    {
        ensureCanManageIds();
        return delegate.nextIdBatch( size );
    }

    private void ensureCanManageIds()
    {
        try
        {
            barrierState.ensureHoldingToken( this::clearCache );
        }
        catch ( BarrierException e )
        {
            throw new IdGenerationException( "This instance is no longer able to manage IDs because of leader re-election", e, e.status() );
        }
    }

    @Override
    public void clearCache()
    {
        delegate.clearCache();
    }
}
