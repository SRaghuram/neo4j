/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.identity;

import com.neo4j.causalclustering.core.state.storage.SafeStateMarshal;
import com.neo4j.causalclustering.discovery.TopologyService;
import com.neo4j.causalclustering.upstream.UpstreamDatabaseSelectionStrategy;

import java.io.IOException;
import java.util.UUID;

import org.neo4j.dbms.identity.ServerId;
import org.neo4j.io.fs.ReadableChannel;
import org.neo4j.io.fs.WritableChannel;

/**
 * {@link MemberId} was historically used throughout causal clustering to identify:
 *     1. Neo4j servers (used in discovery, catchup and routing components).
 *     2. Membership of a Neo4j server in a raft group (used in raft components and state machines).
 *
 * In Neo4j 4.2 the {@link ServerId} interface was introduced to serve the first purpose for all editions.
 * Unfortunately, our use of MemberId as a server identifier (use 1.) is present in public api (such as
 * {@link UpstreamDatabaseSelectionStrategy}) and so cannot change until the next major version.
 *
 * As a result, we made the unintuitive decision to have MemberId extend ServerId.
 * This means that everywhere a ServerId is expected a MemberId can be used and our concrete
 * server identifier ({@link ClusteringServerId}) implements MemberId, not ServerId.
 *
 * Our prior use of MemberId to uniquely identify raft group membership (use 2.) does not exist
 * in public api. Therefore, our new concrete type {@link RaftMemberId} does *not* implement MemberId,
 * so that the compiler can prevent us from confusing the two.
 *
 * In the next major version of Neo4j, the intent is that all uses of MemberId will be replaced with ServerId.
 *
 * TL;DR Despite what the name suggests, implementers of MemberId uniquely identify Neo4j servers,
 * *not* raft group memberships. See ClusteringServerId and RaftMemberId for concrete types.
 */
public interface MemberId extends ServerId
{
    static MemberId of( UUID id )
    {
        return new ClusteringServerId( id );
    }

    @Deprecated
    static MemberId of( ServerId id )
    {
        return (id instanceof MemberId) ? (MemberId) id : MemberId.of( id.getUuid() );
    }

    /**
     * Format:
     * ┌──────────────────────────────┐
     * │mostSignificantBits    8 bytes│
     * │leastSignificantBits   8 bytes│
     * └──────────────────────────────┘
     */
    class Marshal extends SafeStateMarshal<MemberId>
    {
        public static final Marshal INSTANCE = new Marshal();

        @Override
        public void marshal( MemberId memberId, WritableChannel channel ) throws IOException
        {
            if ( memberId == null )
            {
                channel.put( (byte) 0 );
            }
            else
            {
                channel.put( (byte) 1 );
                channel.putLong( memberId.getUuid().getMostSignificantBits() );
                channel.putLong( memberId.getUuid().getLeastSignificantBits() );
            }
        }

        @Override
        public MemberId unmarshal0( ReadableChannel channel ) throws IOException
        {
            byte nullMarker = channel.get();
            if ( nullMarker == 0 )
            {
                return null;
            }
            else
            {
                long mostSigBits = channel.getLong();
                long leastSigBits = channel.getLong();
                return MemberId.of( new UUID( mostSigBits, leastSigBits ) );
            }
        }

        @Override
        public MemberId startState()
        {
            return null;
        }

        @Override
        public long ordinal( MemberId memberId )
        {
            return memberId == null ? 0 : 1;
        }
    }
}
