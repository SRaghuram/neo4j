/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.read_replica;

import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.causalclustering.upstream.UpstreamDatabaseSelectionStrategy;

import java.util.Optional;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.kernel.database.DatabaseId;

@ServiceProvider
public class SpecificReplicaStrategy extends UpstreamDatabaseSelectionStrategy
{
    static final String NAME = "specific";
    // This because we need a stable point for config to inject into Service loader loaded classes
    public static final UpstreamFactory upstreamFactory = new UpstreamFactory();

    public SpecificReplicaStrategy()
    {
        super( NAME );
    }

    @Override
    public Optional<MemberId> upstreamMemberForDatabase( DatabaseId databaseId )
    {
        ReadReplica current = upstreamFactory.current();
        if ( current == null )
        {
            return Optional.empty();
        }
        else
        {
            return Optional.of( current.id() );
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
