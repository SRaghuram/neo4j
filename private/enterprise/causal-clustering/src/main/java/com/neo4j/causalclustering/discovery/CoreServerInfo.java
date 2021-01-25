/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery;

import com.neo4j.configuration.CausalClusteringSettings;
import com.neo4j.configuration.ServerGroupName;

import java.util.Objects;
import java.util.Set;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.kernel.database.DatabaseId;

public class CoreServerInfo implements DiscoveryServerInfo
{
    private final SocketAddress raftServer;
    private final SocketAddress catchupServer;
    private final ConnectorAddresses connectorAddresses;
    private final Set<ServerGroupName> groups;
    private final Set<DatabaseId> startedDatabaseIds;

    public CoreServerInfo( SocketAddress raftServer, SocketAddress catchupServer, ConnectorAddresses connectorAddresses, Set<ServerGroupName> groups,
                           Set<DatabaseId> startedDatabaseIds )
    {
        this.raftServer = raftServer;
        this.catchupServer = catchupServer;
        this.connectorAddresses = connectorAddresses;
        this.groups = groups;
        this.startedDatabaseIds = startedDatabaseIds;
    }

    public static CoreServerInfo fromRaw( Config config, Set<DatabaseId> databaseIds )
    {
        var raftAddress = config.get( CausalClusteringSettings.raft_advertised_address );
        var catchupAddress = config.get( CausalClusteringSettings.transaction_advertised_address );
        var connectorUris = ConnectorAddresses.fromConfig( config );
        var groups = Set.copyOf( config.get( CausalClusteringSettings.server_groups ) );
        return new CoreServerInfo( raftAddress, catchupAddress, connectorUris, groups, databaseIds );
    }

    public static CoreServerInfo from( Config config, Set<DatabaseId> databaseIds )
    {
        return fromRaw( config, databaseIds );
    }

    @Override
    public Set<DatabaseId> startedDatabaseIds()
    {
        return startedDatabaseIds;
    }

    public SocketAddress getRaftServer()
    {
        return raftServer;
    }

    @Override
    public SocketAddress catchupServer()
    {
        return catchupServer;
    }

    @Override
    public ConnectorAddresses connectors()
    {
        return connectorAddresses;
    }

    @Override
    public Set<ServerGroupName> groups()
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
        var that = (CoreServerInfo) o;
        return Objects.equals( raftServer, that.raftServer ) &&
               Objects.equals( catchupServer, that.catchupServer ) &&
               Objects.equals( connectorAddresses, that.connectorAddresses ) &&
               Objects.equals( groups, that.groups ) &&
               Objects.equals( startedDatabaseIds, that.startedDatabaseIds );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( raftServer, catchupServer, connectorAddresses, groups, startedDatabaseIds );
    }

    @Override
    public String toString()
    {
        return "CoreServerInfo{" +
               "raftServer=" + raftServer +
               ", catchupServer=" + catchupServer +
               ", connectorAddresses=" + connectorAddresses +
               ", groups=" + groups +
               ", startedDatabaseIds=" + startedDatabaseIds +
               '}';
    }
}
