/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.test.causalclustering;

import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.IntFunction;
import java.util.stream.Collectors;

import org.neo4j.causalclustering.common.EnterpriseCluster;
import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.causalclustering.discovery.IpFamily;
import org.neo4j.causalclustering.helpers.CausalClusteringTestHelpers;
import org.neo4j.causalclustering.scenarios.DiscoveryServiceType;
import org.neo4j.causalclustering.scenarios.EnterpriseDiscoveryServiceType;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.kernel.impl.store.format.standard.Standard;
import org.neo4j.test.rule.VerboseTimeout;

import static org.neo4j.causalclustering.discovery.IpFamily.IPV4;
import static org.neo4j.helpers.collection.MapUtil.stringMap;

public class ClusterConfig
{
    private int noCoreMembers = 3;
    private int noReadReplicas = 3;
    private DiscoveryServiceType discoveryServiceType = EnterpriseDiscoveryServiceType.SHARED;
    private Map<String,String> coreParams = stringMap();
    private Map<String,IntFunction<String>> instanceCoreParams = new HashMap<>();
    private Map<String,String> readReplicaParams = stringMap();
    private Map<String,IntFunction<String>> instanceReadReplicaParams = new HashMap<>();
    private String recordFormat = Standard.LATEST_NAME;
    private IpFamily ipFamily = IPV4;
    private boolean useWildcard;
    private VerboseTimeout.VerboseTimeoutBuilder timeoutBuilder = new VerboseTimeout.VerboseTimeoutBuilder().withTimeout( 15, TimeUnit.MINUTES );
    private Set<String> dbNames = Collections.singleton( CausalClusteringSettings.database.getDefaultValue() );

    public static ClusterConfig clusterConfig()
    {
        return new ClusterConfig();
    }

    private ClusterConfig()
    {
    }

    static EnterpriseCluster createCluster( File directory, ClusterConfig clusterConfig )
    {
        return new EnterpriseCluster( directory, clusterConfig.noCoreMembers, clusterConfig.noReadReplicas, clusterConfig.discoveryServiceType.createFactory(),
                clusterConfig.coreParams, clusterConfig.instanceCoreParams, clusterConfig.readReplicaParams, clusterConfig.instanceReadReplicaParams,
                clusterConfig.recordFormat, clusterConfig.ipFamily, clusterConfig.useWildcard, clusterConfig.dbNames );
    }

    public ClusterConfig withDatabaseNames( Set<String> dbNames )
    {
        this.dbNames = dbNames;
        Map<Integer,String> coreDBMap = CausalClusteringTestHelpers.distributeDatabaseNamesToHostNums( noCoreMembers, dbNames );
        Map<Integer,String> rrDBMap = CausalClusteringTestHelpers.distributeDatabaseNamesToHostNums( noReadReplicas, dbNames );

        Map<String,Long> minCoresPerDb = coreDBMap.entrySet().stream().collect( Collectors.groupingBy( Map.Entry::getValue, Collectors.counting() ) );

        Map<Integer,String> minCoresSettingsMap = new HashMap<>();

        for ( Map.Entry<Integer,String> entry : coreDBMap.entrySet() )
        {
            Optional<Long> minNumCores = Optional.ofNullable( minCoresPerDb.get( entry.getValue() ) );
            minNumCores.ifPresent( n -> minCoresSettingsMap.put( entry.getKey(), n.toString() ) );
        }

        withInstanceCoreParam( CausalClusteringSettings.database, coreDBMap::get );
        withInstanceCoreParam( CausalClusteringSettings.minimum_core_cluster_size_at_formation, minCoresSettingsMap::get );
        withInstanceReadReplicaParam( CausalClusteringSettings.database, rrDBMap::get );
        return this;
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
