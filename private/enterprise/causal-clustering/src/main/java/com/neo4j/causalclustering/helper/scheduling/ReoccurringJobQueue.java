/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.helper.scheduling;

public class ReoccurringJobQueue<T> implements JobsQueue<T>
{
    private int queuedJobs;
    private T job;

    @Override
    public T poll()
    {
        if ( queuedJobs == 0 )
        {
            return null;
        }
        queuedJobs--;
        return job;
    }

    @Override
    public void offer( T element )
    {
        if ( job == null )
        {
            job = element;
        }
        assert job == element;
        queuedJobs++;
    }

    @Override
    public void clear()
    {
        queuedJobs = 0;
    }

    @Override
    public boolean isEmpty()
    {
        return queuedJobs == 0;
    }
}
