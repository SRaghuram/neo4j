package org.neo4j.store;

import java.util.NoSuchElementException;
import static java.lang.String.format;

import org.neo4j.kernel.impl.store.RecordStore;

public class StoreIdIterator extends org.neo4j.kernel.impl.store.StoreIdIterator
{
    private boolean forward = true;
    public StoreIdIterator( RecordStore<?> store, boolean forward )
    {
        super( store );
        this.id = forward ? store.getNumberOfReservedLowIds() : store.getHighId();
        this.forward = forward;
    }
    
    public StoreIdIterator( RecordStore<?> store )
    {
        super( store );
    }

    @Override
    public boolean hasNext()
    {
        if ( forward )
        {
            if ( id < highId )
            {
                return true;
            }
            highId = store.getHighId();
            return id < highId;
        }
        else
        {
            if ( id > 0 )
                return true;
            return false;
        }
    }
    @Override
    public long next()
    {
        if ( !hasNext() )
        {
            throw new NoSuchElementException(
                    format( "ID [%s] has exceeded the high ID [%s] of %s.", id, highId, store ) );
        }
        if ( forward )
            return id++;
        else
            return id--;
    }

    public boolean reverseDirection()
    {
        forward = !forward;
        return forward;
    }

    public boolean isForward()
    {
        return forward;
    }
}
