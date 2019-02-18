/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery;

import com.neo4j.causalclustering.core.CausalClusteringSettings;

import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import org.neo4j.configuration.Config;
import org.neo4j.helpers.AdvertisedSocketAddress;

import static java.util.Collections.emptySet;

public class ReadReplicaInfo implements DiscoveryServerInfo
{
    private final AdvertisedSocketAddress catchupServerAddress;
    private final ClientConnectorAddresses clientConnectorAddresses;
    private final Set<String> groups;
    private final String dbName;

    public ReadReplicaInfo( ClientConnectorAddresses clientConnectorAddresses, AdvertisedSocketAddress catchupServerAddress, String dbName )
    {
        this( clientConnectorAddresses, catchupServerAddress, emptySet(), dbName );
    }

    public ReadReplicaInfo( ClientConnectorAddresses clientConnectorAddresses,
            AdvertisedSocketAddress catchupServerAddress, Set<String> groups, String dbName )
    {
        this.clientConnectorAddresses = clientConnectorAddresses;
        this.catchupServerAddress = catchupServerAddress;
        this.groups = groups;
        this.dbName = dbName;
    }

    @Override
    public String getDatabaseName()
    {
        return dbName;
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
    public String toString()
    {
        return "ReadReplicaInfo{" +
               "catchupServerAddress=" + catchupServerAddress +
               ", clientConnectorAddresses=" + clientConnectorAddresses +
               ", groups=" + groups +
               '}';
    }

    public static ReadReplicaInfo from( Config config )
    {
        AdvertisedSocketAddress transactionSource = config.get( CausalClusteringSettings.transaction_advertised_address );
        ClientConnectorAddresses clientConnectorAddresses = ClientConnectorAddresses.extractFromConfig( config );
        String dbName = config.get( CausalClusteringSettings.database );
        List<String> groupList = config.get( CausalClusteringSettings.server_groups );
        Set<String> groups = new HashSet<>( groupList );

        return new ReadReplicaInfo( clientConnectorAddresses, transactionSource, groups, dbName );
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
        return Objects.equals( catchupServerAddress, that.catchupServerAddress ) && Objects.equals( clientConnectorAddresses, that.clientConnectorAddresses ) &&
                Objects.equals( groups, that.groups ) && Objects.equals( dbName, that.dbName );
    }

    @Override
    public int hashCode()
    {

        return Objects.hash( catchupServerAddress, clientConnectorAddresses, groups, dbName );
    }
}
