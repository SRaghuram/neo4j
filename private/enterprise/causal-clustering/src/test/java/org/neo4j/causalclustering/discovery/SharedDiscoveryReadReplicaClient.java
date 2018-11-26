/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.discovery;

import java.util.Map;
import java.util.Optional;

import org.neo4j.causalclustering.catchup.CatchupAddressResolutionException;
import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.lifecycle.SafeLifecycle;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static org.neo4j.helpers.SocketAddressParser.socketAddress;

class SharedDiscoveryReadReplicaClient extends SafeLifecycle implements TopologyService
{
    private final SharedDiscoveryService sharedDiscoveryService;
    private final ReadReplicaInfo addresses;
    private final MemberId memberId;
    private final Log log;
    private final String dbName;

    SharedDiscoveryReadReplicaClient( SharedDiscoveryService sharedDiscoveryService, Config config, MemberId memberId,
            LogProvider logProvider )
    {
        this.sharedDiscoveryService = sharedDiscoveryService;
        this.dbName = config.get( CausalClusteringSettings.database );
        this.addresses = new ReadReplicaInfo( ClientConnectorAddresses.extractFromConfig( config ),
                socketAddress( config.get( CausalClusteringSettings.transaction_advertised_address ).toString(),
                        AdvertisedSocketAddress::new ), dbName );
        this.memberId = memberId;
        this.log = logProvider.getLog( getClass() );
    }

    @Override
    public void init0()
    {
        // nothing to do
    }

    @Override
    public void start0()
    {
        sharedDiscoveryService.registerReadReplica( this );
        log.info( "Registered read replica member id: %s at %s", memberId, addresses );
    }

    @Override
    public void stop0()
    {
        sharedDiscoveryService.unRegisterReadReplica( this );
    }

    @Override
    public void shutdown0()
    {
        // nothing to do
    }

    @Override
    public CoreTopology allCoreServers()
    {
        return sharedDiscoveryService.getCoreTopology( dbName, false );
    }

    @Override
    public CoreTopology localCoreServers()
    {
        return allCoreServers().filterTopologyByDb( dbName );
    }

    @Override
    public ReadReplicaTopology allReadReplicas()
    {
        return sharedDiscoveryService.getReadReplicaTopology();
    }

    @Override
    public ReadReplicaTopology localReadReplicas()
    {
        return allReadReplicas().filterTopologyByDb( dbName );
    }

    @Override
    public AdvertisedSocketAddress findCatchupAddress( MemberId upstream ) throws CatchupAddressResolutionException
    {
        Optional<AdvertisedSocketAddress> coreAdvertisedAddress =
                sharedDiscoveryService.getCoreTopology( dbName, false ).find( upstream ).map( CoreServerInfo::getCatchupServer );
        if ( coreAdvertisedAddress.isPresent() )
        {
            return coreAdvertisedAddress.get();
        }
        return sharedDiscoveryService.getReadReplicaTopology()
                        .find( upstream ).map( ReadReplicaInfo::getCatchupServer ).orElseThrow( () -> new CatchupAddressResolutionException( upstream ) );
    }

    @Override
    public String localDBName()
    {
        return dbName;
    }

    @Override
    public Map<MemberId,RoleInfo> allCoreRoles()
    {
        return sharedDiscoveryService.getCoreRoles();
    }

    @Override
    public MemberId myself()
    {
        return memberId;
    }

    public MemberId getMemberId()
    {
        return memberId;
    }

    public ReadReplicaInfo getReadReplicainfo()
    {
        return addresses;
    }
}
