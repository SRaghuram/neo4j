/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery;

import com.neo4j.causalclustering.catchup.CatchupAddressResolutionException;
import com.neo4j.causalclustering.core.CausalClusteringSettings;
import com.neo4j.causalclustering.identity.MemberId;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.lifecycle.SafeLifecycle;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static org.neo4j.helpers.SocketAddressParser.socketAddress;

class SharedDiscoveryReadReplicaClient extends SafeLifecycle implements TopologyService
{
    private final SharedDiscoveryService sharedDiscoveryService;
    private final ReadReplicaInfo addresses;
    private final DiscoveryMember myself;
    private final Log log;
    private final DatabaseId databaseId;

    SharedDiscoveryReadReplicaClient( SharedDiscoveryService sharedDiscoveryService, Config config, DiscoveryMember myself,
            LogProvider logProvider )
    {
        this.sharedDiscoveryService = sharedDiscoveryService;
        this.databaseId = new DatabaseId( GraphDatabaseSettings.DEFAULT_DATABASE_NAME );
        this.addresses = new ReadReplicaInfo( ClientConnectorAddresses.extractFromConfig( config ),
                socketAddress( config.get( CausalClusteringSettings.transaction_advertised_address ).toString(),
                        AdvertisedSocketAddress::new ), Set.of(), Set.of( databaseId ) ); // todo: no dbName like this!
        this.myself = myself;
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
        log.info( "Registered read replica member id: %s at %s", myself(), addresses );
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
    public Map<MemberId,CoreServerInfo> allCoreServers()
    {
        return sharedDiscoveryService.allCoreServers();
    }

    @Override
    public CoreTopology coreServersForDatabase( DatabaseId databaseId )
    {
        return sharedDiscoveryService.getCoreTopology( databaseId, false );
    }

    @Override
    public Map<MemberId,ReadReplicaInfo> allReadReplicas()
    {
        return sharedDiscoveryService.allReadReplicas();
    }

    @Override
    public ReadReplicaTopology readReplicasForDatabase( DatabaseId databaseId )
    {
        return sharedDiscoveryService.getReadReplicaTopology(); // todo: this is not correct
    }

    @Override
    public AdvertisedSocketAddress findCatchupAddress( MemberId upstream ) throws CatchupAddressResolutionException
    {
        Optional<AdvertisedSocketAddress> coreAdvertisedAddress =
                sharedDiscoveryService.getCoreTopology( databaseId, false ).find( upstream ).map( CoreServerInfo::getCatchupServer );
        if ( coreAdvertisedAddress.isPresent() )
        {
            return coreAdvertisedAddress.get();
        }
        return sharedDiscoveryService.getReadReplicaTopology()
                        .find( upstream ).map( ReadReplicaInfo::getCatchupServer ).orElseThrow( () -> new CatchupAddressResolutionException( upstream ) );
    }

    @Override
    public Map<MemberId,RoleInfo> allCoreRoles()
    {
        return sharedDiscoveryService.getCoreRoles();
    }

    @Override
    public MemberId myself()
    {
        return myself.id();
    }

    public ReadReplicaInfo getReadReplicaInfo()
    {
        return addresses;
    }
}
