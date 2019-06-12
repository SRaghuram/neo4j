/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery;

import com.neo4j.causalclustering.catchup.CatchupAddressResolutionException;
import com.neo4j.causalclustering.identity.MemberId;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.neo4j.internal.helpers.AdvertisedSocketAddress;
import org.neo4j.kernel.database.DatabaseId;

public class FakeTopologyService implements TopologyService
{
    private final DatabaseCoreTopology coreTopology;
    private final DatabaseReadReplicaTopology readReplicaTopology;
    private final Map<MemberId,AdvertisedSocketAddress> catchupAddresses;

    public FakeTopologyService( DatabaseCoreTopology coreTopology, DatabaseReadReplicaTopology readReplicaTopology )
    {
        this.coreTopology = coreTopology;
        this.readReplicaTopology = readReplicaTopology;
        this.catchupAddresses = extractCatchupAddressesMap( coreTopology, readReplicaTopology );
    }

    @Override
    public void onDatabaseStart( DatabaseId databaseId )
    {
    }

    @Override
    public void onDatabaseStop( DatabaseId databaseId )
    {
    }

    @Override
    public Map<MemberId,CoreServerInfo> allCoreServers()
    {
        return coreTopology.members();
    }

    @Override
    public DatabaseCoreTopology coreTopologyForDatabase( DatabaseId databaseId )
    {
        return coreTopology;
    }

    @Override
    public Map<MemberId,ReadReplicaInfo> allReadReplicas()
    {
        return readReplicaTopology.members();
    }

    @Override
    public DatabaseReadReplicaTopology readReplicaTopologyForDatabase( DatabaseId databaseId )
    {
        return readReplicaTopology;
    }

    @Override
    public AdvertisedSocketAddress findCatchupAddress( MemberId upstream ) throws CatchupAddressResolutionException
    {
        var address = catchupAddresses.get( upstream );
        if ( address == null )
        {
            throw new CatchupAddressResolutionException( upstream );
        }
        return address;
    }

    @Override
    public RoleInfo coreRole( DatabaseId databaseId, MemberId memberId )
    {
        return RoleInfo.UNKNOWN;
    }

    @Override
    public MemberId memberId()
    {
        return new MemberId( new UUID( 0, 0 ) );
    }

    @Override
    public void init()
    {
    }

    @Override
    public void start()
    {
    }

    @Override
    public void stop()
    {
    }

    @Override
    public void shutdown()
    {
    }

    private static Map<MemberId,AdvertisedSocketAddress> extractCatchupAddressesMap( DatabaseCoreTopology coreTopology,
            DatabaseReadReplicaTopology readReplicaTopology )
    {
        var catchupAddressMap = new HashMap<MemberId,AdvertisedSocketAddress>();

        for ( var entry : coreTopology.members().entrySet() )
        {
            catchupAddressMap.put( entry.getKey(), entry.getValue().catchupServer() );
        }

        for ( var entry : readReplicaTopology.members().entrySet() )
        {
            catchupAddressMap.put( entry.getKey(), entry.getValue().catchupServer() );
        }

        return Collections.unmodifiableMap( catchupAddressMap );
    }
}
