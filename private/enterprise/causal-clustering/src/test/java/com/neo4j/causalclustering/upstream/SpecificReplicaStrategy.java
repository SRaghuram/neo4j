/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.upstream;

import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.causalclustering.readreplica.ReadReplica;

import java.util.Optional;

public class SpecificReplicaStrategy extends UpstreamDatabaseSelectionStrategy
{
    // This because we need a stable point for config to inject into Service loader loaded classes
    public static final UpstreamFactory upstreamFactory = new UpstreamFactory();

    public SpecificReplicaStrategy()
    {
        super( "specific" );
    }

    @Override
    public Optional<MemberId> upstreamDatabase()
    {
        ReadReplica current = upstreamFactory.current();
        if ( current == null )
        {
            return Optional.empty();
        }
        else
        {
            return Optional.of( current.memberId() );
        }
    }

    public static class UpstreamFactory
    {
        private ReadReplica current;

        public void setCurrent( ReadReplica readReplica )
        {
            this.current = readReplica;
        }

        public ReadReplica current()
        {
            return current;
        }

        public void reset()
        {
            current = null;
        }
    }
}
