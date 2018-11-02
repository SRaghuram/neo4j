/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.com;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class PortIterator implements Iterator<Integer>
{
    private final int start;
    private final int end;
    private int next;

    public PortIterator( int[] portRanges )
    {
        start = portRanges[0];
        end = portRanges[1];
        next = start;
    }

    @Override
    public boolean hasNext()
    {
        return start < end ? next <= end : next >= end;
    }

    @Override
    public Integer next()
    {
        if ( !hasNext() )
        {
            throw new NoSuchElementException();
        }
        return start < end ? next++ : next--;
    }

    @Override
    public void remove()
    {
        throw new UnsupportedOperationException();
    }
}
