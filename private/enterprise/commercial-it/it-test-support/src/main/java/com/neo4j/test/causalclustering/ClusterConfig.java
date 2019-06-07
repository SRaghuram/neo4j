/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.test.causalclustering;

import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.discovery.DiscoveryServiceType;
import com.neo4j.causalclustering.discovery.IpFamily;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.IntFunction;

import org.neo4j.graphdb.config.Setting;
import org.neo4j.kernel.impl.store.format.standard.Standard;
import org.neo4j.test.rule.VerboseTimeout;

import static com.neo4j.causalclustering.discovery.IpFamily.IPV4;
import static org.neo4j.internal.helpers.collection.MapUtil.stringMap;

public class ClusterConfig
{
    private int noCoreMembers = 3;
    private int noReadReplicas = 3;
    private DiscoveryServiceType discoveryServiceType = DiscoveryServiceType.AKKA;
    private Map<String,String> coreParams = stringMap();
    private Map<String,IntFunction<String>> instanceCoreParams = new HashMap<>();
    private Map<String,String> readReplicaParams = stringMap();
    private Map<String,IntFunction<String>> instanceReadReplicaParams = new HashMap<>();
    private String recordFormat = Standard.LATEST_NAME;
    private IpFamily ipFamily = IPV4;
    private boolean useWildcard;
    private VerboseTimeout.VerboseTimeoutBuilder timeoutBuilder = new VerboseTimeout.VerboseTimeoutBuilder().withTimeout( 15, TimeUnit.MINUTES );

    public static ClusterConfig clusterConfig()
    {
        return new ClusterConfig();
    }

    private ClusterConfig()
    {
    }

    static Cluster createCluster( File directory, ClusterConfig clusterConfig )
    {
        return new Cluster( directory, clusterConfig.noCoreMembers, clusterConfig.noReadReplicas, clusterConfig.discoveryServiceType.factory(),
                clusterConfig.coreParams, clusterConfig.instanceCoreParams, clusterConfig.readReplicaParams, clusterConfig.instanceReadReplicaParams,
                clusterConfig.recordFormat, clusterConfig.ipFamily, clusterConfig.useWildcard );
    }

    public ClusterConfig withNumberOfCoreMembers( int noCoreMembers )
    {
        this.noCoreMembers = noCoreMembers;
        return this;
    }

    public ClusterConfig withNumberOfReadReplicas( int noReadReplicas )
    {
        this.noReadReplicas = noReadReplicas;
        return this;
    }

    public ClusterConfig withDiscoveryServiceType( DiscoveryServiceType discoveryServiceType )
    {
        this.discoveryServiceType = discoveryServiceType;
        return this;
    }

    public ClusterConfig withSharedCoreParams( Map<String,String> params )
    {
        this.coreParams.putAll( params );
        return this;
    }

    public ClusterConfig withSharedCoreParam( Setting<?> key, String value )
    {
        this.coreParams.put( key.name(), value );
        return this;
    }

    public ClusterConfig withInstanceCoreParams( Map<String,IntFunction<String>> params )
    {
        this.instanceCoreParams.putAll( params );
        return this;
    }

    public ClusterConfig withInstanceCoreParam( Setting<?> key, IntFunction<String> valueFunction )
    {
        this.instanceCoreParams.put( key.name(), valueFunction );
        return this;
    }

    public ClusterConfig withSharedReadReplicaParams( Map<String,String> params )
    {
        this.readReplicaParams.putAll( params );
        return this;
    }

    public ClusterConfig withSharedReadReplicaParam( Setting<?> key, String value )
    {
        this.readReplicaParams.put( key.name(), value );
        return this;
    }

    public ClusterConfig withInstanceReadReplicaParams( Map<String,IntFunction<String>> params )
    {
        this.instanceReadReplicaParams.putAll( params );
        return this;
    }

    public ClusterConfig withInstanceReadReplicaParam( Setting<?> key, IntFunction<String> valueFunction )
    {
        this.instanceReadReplicaParams.put( key.name(), valueFunction );
        return this;
    }

    public ClusterConfig withRecordFormat( String recordFormat )
    {
        this.recordFormat = recordFormat;
        return this;
    }

    public ClusterConfig withIpFamily( IpFamily ipFamily )
    {
        this.ipFamily = ipFamily;
        return this;
    }

    public ClusterConfig useWildcard( boolean useWildcard )
    {
        this.useWildcard = useWildcard;
        return this;
    }

    public ClusterConfig withTimeout( long timeout, TimeUnit unit )
    {
        this.timeoutBuilder = new VerboseTimeout.VerboseTimeoutBuilder().withTimeout( timeout, unit );
        return this;
    }

    public ClusterConfig withNoTimeout()
    {
        this.timeoutBuilder = null;
        return this;
    }
}
