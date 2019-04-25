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

public class ReadReplicaInfo implements DiscoveryServerInfo
{
    private final AdvertisedSocketAddress catchupServerAddress;
    private final ClientConnectorAddresses clientConnectorAddresses;
    private final Set<String> groups;
    private final Set<DatabaseId> databaseIds;

    public ReadReplicaInfo( Config config, Set<DatabaseId> databaseIds )
    {
        this( ClientConnectorAddresses.extractFromConfig( config ),
                config.get( CausalClusteringSettings.transaction_advertised_address ),
                Set.copyOf( config.get( CausalClusteringSettings.server_groups ) ),
                databaseIds );
    }

    public ReadReplicaInfo( ClientConnectorAddresses clientConnectorAddresses,
            AdvertisedSocketAddress catchupServerAddress, Set<String> groups, Set<DatabaseId> databaseIds )
    {
        this.clientConnectorAddresses = clientConnectorAddresses;
        this.catchupServerAddress = catchupServerAddress;
        this.groups = groups;
        this.databaseIds = databaseIds;
    }

    @Override
    public Set<DatabaseId> getDatabaseIds()
    {
        return databaseIds;
    }

    @Override
    public ClientConnectorAddresses connectors()
    {
        return clientConnectorAddresses;
    }

    @Override
    public AdvertisedSocketAddress getCatchupServer()
    {
        return catchupServerAddress;
    }

    @Override
    public Set<String> groups()
    {
        return groups;
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
        ReadReplicaInfo that = (ReadReplicaInfo) o;
        return Objects.equals( catchupServerAddress, that.catchupServerAddress ) &&
               Objects.equals( clientConnectorAddresses, that.clientConnectorAddresses ) &&
               Objects.equals( groups, that.groups ) &&
               Objects.equals( databaseIds, that.databaseIds );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( catchupServerAddress, clientConnectorAddresses, groups, databaseIds );
    }

    @Override
    public String toString()
    {
        return "ReadReplicaInfo{" +
               "catchupServerAddress=" + catchupServerAddress +
               ", clientConnectorAddresses=" + clientConnectorAddresses +
               ", groups=" + groups +
               ", databaseIds=" + databaseIds +
               '}';
    }
}
