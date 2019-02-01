/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.test.causalclustering;

import com.neo4j.causalclustering.discovery.CommercialCluster;
import com.neo4j.causalclustering.discovery.CommercialDiscoveryServiceType;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.IntFunction;

import org.neo4j.causalclustering.common.Cluster;
import org.neo4j.causalclustering.common.EnterpriseCluster;
import org.neo4j.causalclustering.discovery.IpFamily;
import org.neo4j.causalclustering.scenarios.DiscoveryServiceType;
import org.neo4j.causalclustering.scenarios.EnterpriseDiscoveryServiceType;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;
import org.neo4j.server.security.enterprise.configuration.SecuritySettings;
import org.neo4j.test.causalclustering.ClusterRule;

public class CommercialClusterRule extends ClusterRule
{

    @Override
    public CommercialClusterRule withDatabaseNames( Set<String> dbNames )
    {
        return (CommercialClusterRule) super.withDatabaseNames( dbNames );
    }

    @Override
    public CommercialClusterRule withNumberOfCoreMembers( int noCoreMembers )
    {
        return (CommercialClusterRule) super.withNumberOfCoreMembers( noCoreMembers );
    }

    @Override
    public CommercialClusterRule withNumberOfReadReplicas( int noReadReplicas )
    {
        return (CommercialClusterRule) super.withNumberOfReadReplicas( noReadReplicas );
    }

    @Override
    public CommercialClusterRule withDiscoveryServiceType( DiscoveryServiceType discoveryServiceType )
    {
        return (CommercialClusterRule) super.withDiscoveryServiceType( discoveryServiceType );
    }

    @Override
    public CommercialClusterRule withSharedCoreParams( Map<String,String> params )
    {
        return (CommercialClusterRule) super.withSharedCoreParams( params );
    }

    @Override
    public CommercialClusterRule withSharedCoreParam( Setting<?> key, String value )
    {
        return (CommercialClusterRule) super.withSharedCoreParam( key, value );
    }

    @Override
    public CommercialClusterRule withInstanceCoreParams( Map<String,IntFunction<String>> params )
    {
        return (CommercialClusterRule) super.withInstanceCoreParams( params );
    }

    @Override
    public CommercialClusterRule withInstanceCoreParam( Setting<?> key, IntFunction<String> valueFunction )
    {
        return (CommercialClusterRule) super.withInstanceCoreParam( key, valueFunction );
    }

    @Override
    public CommercialClusterRule withSharedReadReplicaParams( Map<String,String> params )
    {
        return (CommercialClusterRule) super.withSharedReadReplicaParams( params );
    }

    @Override
    public CommercialClusterRule withSharedReadReplicaParam( Setting<?> key, String value )
    {
        return (CommercialClusterRule) super.withSharedReadReplicaParam( key, value );
    }

    @Override
    public CommercialClusterRule withInstanceReadReplicaParams( Map<String,IntFunction<String>> params )
    {
        return (CommercialClusterRule) super.withInstanceReadReplicaParams( params );
    }

    @Override
    public CommercialClusterRule withInstanceReadReplicaParam( Setting<?> key, IntFunction<String> valueFunction )
    {
        return (CommercialClusterRule) super.withInstanceReadReplicaParam( key, valueFunction );
    }

    @Override
    public CommercialClusterRule withRecordFormat( String recordFormat )
    {
        return (CommercialClusterRule) super.withRecordFormat( recordFormat );
    }

    @Override
    public CommercialClusterRule withClusterDirectory( File clusterDirectory )
    {
        return (CommercialClusterRule) super.withClusterDirectory( clusterDirectory );
    }

    @Override
    public CommercialClusterRule withIpFamily( IpFamily ipFamily )
    {
        return (CommercialClusterRule) super.withIpFamily( ipFamily );
    }

    @Override
    public CommercialClusterRule useWildcard( boolean useWildcard )
    {
        return (CommercialClusterRule) super.useWildcard( useWildcard );
    }

    @Override
    public CommercialClusterRule withTimeout( long timeout, TimeUnit unit )
    {
        return (CommercialClusterRule) super.withTimeout( timeout, unit );
    }

    @Override
    public CommercialClusterRule withNoTimeout()
    {
        return (CommercialClusterRule) super.withNoTimeout();
    }

    @Override
    public Cluster<?> startCluster() throws Exception
    {
        return startCommercialCluster();
    }

    public Cluster<?> startEnterpriseCluster() throws Exception
    {
        return startCluster( EnterpriseDiscoveryServiceType.SHARED::createFactory, EnterpriseCluster::new );
    }

    public Cluster<?> startCommercialCluster() throws Exception
    {
        return startCluster( CommercialDiscoveryServiceType.SHARED::createFactory, CommercialCluster::new );
    }

}
