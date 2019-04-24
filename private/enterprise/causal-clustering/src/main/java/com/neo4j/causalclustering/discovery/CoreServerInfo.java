/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery;

import com.neo4j.causalclustering.core.CausalClusteringSettings;

import java.util.Objects;
import java.util.Set;

import org.neo4j.configuration.Config;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.kernel.database.DatabaseId;

public class CoreServerInfo implements DiscoveryServerInfo
{
    private final AdvertisedSocketAddress raftServer;
    private final AdvertisedSocketAddress catchupServer;
    private final ClientConnectorAddresses clientConnectorAddresses;
    private final Set<String> groups;
    private final Set<DatabaseId> databaseIds;
    private final boolean refuseToBeLeader;

    public CoreServerInfo( AdvertisedSocketAddress raftServer, AdvertisedSocketAddress catchupServer,
            ClientConnectorAddresses clientConnectorAddresses, Set<String> groups, Set<DatabaseId> databaseIds, boolean refuseToBeLeader )
    {
        this.raftServer = raftServer;
        this.catchupServer = catchupServer;
        this.clientConnectorAddresses = clientConnectorAddresses;
        this.groups = groups;
        this.databaseIds = databaseIds;
        this.refuseToBeLeader = refuseToBeLeader;
    }

    public static CoreServerInfo from( Config config, Set<DatabaseId> databaseIds )
    {
        var raftAddress = config.get( CausalClusteringSettings.raft_advertised_address );
        var catchupAddress = config.get( CausalClusteringSettings.transaction_advertised_address );
        var connectorUris = ClientConnectorAddresses.extractFromConfig( config );
        var groups = Set.copyOf( config.get( CausalClusteringSettings.server_groups ) );
        var refuseToBeLeader = config.get( CausalClusteringSettings.refuse_to_be_leader );
        return new CoreServerInfo( raftAddress, catchupAddress, connectorUris, groups, databaseIds, refuseToBeLeader );
    }

    @Override
    public Set<DatabaseId> getDatabaseIds()
    {
        return databaseIds;
    }

    public AdvertisedSocketAddress getRaftServer()
    {
        return raftServer;
    }

    @Override
    public AdvertisedSocketAddress getCatchupServer()
    {
        return catchupServer;
    }

    @Override
    public ClientConnectorAddresses connectors()
    {
        return clientConnectorAddresses;
    }

    @Override
    public Set<String> groups()
    {
        return groups;
    }

    public boolean refusesToBeLeader()
    {
        return refuseToBeLeader;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }
        var that = (CoreServerInfo) o;
        return refuseToBeLeader == that.refuseToBeLeader &&
               Objects.equals( raftServer, that.raftServer ) &&
               Objects.equals( catchupServer, that.catchupServer ) &&
               Objects.equals( clientConnectorAddresses, that.clientConnectorAddresses ) &&
               Objects.equals( groups, that.groups ) &&
               Objects.equals( databaseIds, that.databaseIds );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( raftServer, catchupServer, clientConnectorAddresses, groups, databaseIds, refuseToBeLeader );
    }

    @Override
    public String toString()
    {
        return "CoreServerInfo{" +
               "raftServer=" + raftServer +
               ", catchupServer=" + catchupServer +
               ", clientConnectorAddresses=" + clientConnectorAddresses +
               ", groups=" + groups +
               ", databaseIds=" + databaseIds +
               ", refuseToBeLeader=" + refuseToBeLeader +
               '}';
    }
}
