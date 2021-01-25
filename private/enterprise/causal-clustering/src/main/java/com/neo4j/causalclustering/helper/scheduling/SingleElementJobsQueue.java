/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.helper.scheduling;

public class SingleElementJobsQueue<E> implements JobsQueue<E>
{
    private E next;

    @Override
    public E poll()
    {
        var polled = next;
        next = null;
        return polled;
    }

    @Override
    public void offer( E element )
    {
        if ( next == null )
        {
            next = element;
        }
    }

    @Override
    public void clear()
    {
        next = null;
    }

    @Override
    public boolean isEmpty()
    {
        return next == null;
    }
}
