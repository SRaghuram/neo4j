/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.replication;

import com.neo4j.causalclustering.identity.RaftMemberId;

import org.neo4j.logging.Log;

/**
 * A class that attempts to log only the uncommon helpful cases, for example when
 * the replicator seems stuck. The code is broken out into this helper class to
 * un-clutter the main replicator class.
 */
class ReplicationLogger
{
    private final Log log;
    private int attempts;

    ReplicationLogger( Log log )
    {
        this.log = log;
    }

    void newAttempt( DistributedOperation operation, RaftMemberId leader )
    {
        attempts++;
        if ( attempts > 1 )
        {
            log.info( String.format( "Replication attempt %d to leader %s: %s", attempts, leader, operation ) );
        }
    }

    void success( DistributedOperation operation )
    {
        if ( attempts > 1 )
        {
            log.info( String.format( "Successfully replicated after attempt %d: %s", attempts, operation ) );
        }
    }
}
