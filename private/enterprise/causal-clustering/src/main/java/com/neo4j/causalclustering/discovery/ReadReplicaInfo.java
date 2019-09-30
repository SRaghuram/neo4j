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
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.kernel.database.DatabaseId;

public class ReadReplicaInfo implements DiscoveryServerInfo
{
    private final SocketAddress catchupServerAddress;
    private final ClientConnectorAddresses clientConnectorAddresses;
    private final Set<String> groups;
    private final Set<DatabaseId> databaseIds;

    public ReadReplicaInfo( ClientConnectorAddresses clientConnectorAddresses,
            SocketAddress catchupServerAddress, Set<String> groups, Set<DatabaseId> databaseIds )
    {
        this.clientConnectorAddresses = clientConnectorAddresses;
        this.catchupServerAddress = catchupServerAddress;
        this.groups = groups;
        this.databaseIds = databaseIds;
    }

    public static ReadReplicaInfo from( Config config, Set<DatabaseId> databaseIds )
    {
        var connectorUris = ClientConnectorAddresses.extractFromConfig( config );
        var catchupAddress = config.get( CausalClusteringSettings.transaction_advertised_address );
        var groups = Set.copyOf( config.get( CausalClusteringSettings.server_groups ) );
        return new ReadReplicaInfo( connectorUris, catchupAddress, groups, databaseIds );
    }

    @Override
    public Set<DatabaseId> databaseIds()
    {
        return databaseIds;
    }

    @Override
    public ClientConnectorAddresses connectors()
    {
        return clientConnectorAddresses;
    }

    @Override
    public SocketAddress catchupServer()
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
        var that = (ReadReplicaInfo) o;
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
