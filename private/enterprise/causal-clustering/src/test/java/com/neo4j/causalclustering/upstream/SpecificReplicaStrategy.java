/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package com.neo4j.causalclustering.upstream;

import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.causalclustering.readreplica.ReadReplica;

import java.util.Optional;

import org.neo4j.helpers.Service;

@Service.Implementation( UpstreamDatabaseSelectionStrategy.class )
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
