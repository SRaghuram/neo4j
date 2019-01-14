/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.core.replication;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.neo4j.causalclustering.core.replication.session.GlobalSession;
import org.neo4j.causalclustering.core.replication.session.LocalOperationId;
import org.neo4j.causalclustering.core.state.Result;

public class ProgressTrackerImpl implements ProgressTracker
{
    private final Map<LocalOperationId,Progress> tracker = new ConcurrentHashMap<>();
    private final GlobalSession myGlobalSession;

    public ProgressTrackerImpl( GlobalSession myGlobalSession )
    {
        this.myGlobalSession = myGlobalSession;
    }

    @Override
    public Progress start( DistributedOperation operation )
    {
        assert operation.globalSession().equals( myGlobalSession );

        Progress progress = new Progress();
        tracker.put( operation.operationId(), progress );
        return progress;
    }

    @Override
    public void trackReplication( DistributedOperation operation )
    {
        if ( !operation.globalSession().equals( myGlobalSession ) )
        {
            return;
        }

        Progress progress = tracker.get( operation.operationId() );
        if ( progress != null )
        {
            progress.setReplicated();
        }
    }

    @Override
    public void trackResult( DistributedOperation operation, Result result )
    {
        if ( !operation.globalSession().equals( myGlobalSession ) )
        {
            return;
        }

        Progress progress = tracker.remove( operation.operationId() );

        if ( progress != null )
        {
            progress.registerResult(result);
        }
    }

    @Override
    public void abort( DistributedOperation operation )
    {
        tracker.remove( operation.operationId() );
    }

    @Override
    public void triggerReplicationEvent()
    {
        tracker.forEach( ( ignored, progress ) -> progress.triggerReplicationEvent() );
    }

    @Override
    public int inProgressCount()
    {
        return tracker.size();
    }
}
