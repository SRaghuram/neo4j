/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.test.causalclustering;

import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.discovery.DiscoveryServiceType;
import com.neo4j.causalclustering.discovery.IpFamily;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.function.IntFunction;
import java.util.function.Supplier;

import org.neo4j.graphdb.config.Setting;
import org.neo4j.kernel.impl.store.format.standard.Standard;
import org.neo4j.logging.LogProvider;

import static com.neo4j.causalclustering.discovery.IpFamily.IPV4;

public class ClusterConfig
{
    public enum ClusterType
    {
        CORES,
        STANDALONE,
    }

    private int noCoreMembers = 3;
    private int noReadReplicas = 2;

    private DiscoveryServiceType discoveryServiceType = DiscoveryServiceType.AKKA;
    private final Map<String,String> primaryParams = new HashMap<>();
    private final Map<String,IntFunction<String>> instancePrimaryParams = new HashMap<>();
    private final Map<String,String> readReplicaParams = new HashMap<>();
    private final Map<String,IntFunction<String>> instanceReadReplicaParams = new HashMap<>();
    private String recordFormat = Standard.LATEST_NAME;
    private IpFamily ipFamily = IPV4;
    private Supplier<LogProvider> logProviderSupplier;
    private boolean useWildcard;
    private ClusterType type = ClusterType.CORES;

    public ClusterConfig withStandalone()
    {
        return withClusterType( ClusterType.STANDALONE );
    }

    public static ClusterConfig clusterConfig()
    {
        return new ClusterConfig();
    }

    private ClusterConfig()
    {
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

    public ClusterConfig withSharedPrimaryParams( Map<String,String> params )
    {
        this.primaryParams.putAll( params );
        return this;
    }

    public ClusterConfig withSharedPrimaryParam( Setting<?> key, String value )
    {
        this.primaryParams.put( key.name(), value );
        return this;
    }

    public ClusterConfig withInstanceCoreParams( Map<String,IntFunction<String>> params )
    {
        this.instancePrimaryParams.putAll( params );
        return this;
    }

    public ClusterConfig withInstanceCoreParam( Setting<?> key, IntFunction<String> valueFunction )
    {
        this.instancePrimaryParams.put( key.name(), valueFunction );
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

    public ClusterConfig withLogProvider( Supplier<LogProvider> logProviderSupplier )
    {
        this.logProviderSupplier = logProviderSupplier;
        return this;
    }

    public ClusterConfig useWildcard( boolean useWildcard )
    {
        this.useWildcard = useWildcard;
        return this;
    }

    public ClusterConfig withClusterType( ClusterType type )
    {
        this.type = type;
        return this;
    }

    public Cluster build( Path parentDir )
    {
        switch ( type )
        {
        case CORES:
            return Cluster.createWithCores( parentDir, noCoreMembers, noReadReplicas, discoveryServiceType.factory(),
                                            primaryParams, instancePrimaryParams, readReplicaParams, instanceReadReplicaParams,
                                            recordFormat, ipFamily, logProviderSupplier, useWildcard );
        case STANDALONE:
            return Cluster.createWithStandalone( parentDir, noReadReplicas, discoveryServiceType.factory(),
                                                 primaryParams, readReplicaParams, instanceReadReplicaParams,
                                                 recordFormat, ipFamily, logProviderSupplier, useWildcard );
        default:
            throw new IllegalStateException( "Type " + type + " not supported" );
        }
    }
}
