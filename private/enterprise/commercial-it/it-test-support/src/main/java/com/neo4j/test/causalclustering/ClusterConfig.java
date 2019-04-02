/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.test.causalclustering;

import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.core.CausalClusteringSettings;
import com.neo4j.causalclustering.discovery.DiscoveryServiceType;
import com.neo4j.causalclustering.discovery.IpFamily;
import com.neo4j.causalclustering.helpers.CausalClusteringTestHelpers;

import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.IntFunction;
import java.util.stream.Collectors;

import org.neo4j.graphdb.config.Setting;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.impl.store.format.standard.Standard;
import org.neo4j.test.rule.VerboseTimeout;

import static com.neo4j.causalclustering.discovery.IpFamily.IPV4;
import static org.neo4j.helpers.collection.MapUtil.stringMap;

public class ClusterConfig
{
    private int noCoreMembers = 3;
    private int noReadReplicas = 3;
    private DiscoveryServiceType discoveryServiceType = DiscoveryServiceType.SHARED;
    private Map<String,String> coreParams = stringMap();
    private Map<String,IntFunction<String>> instanceCoreParams = new HashMap<>();
    private Map<String,String> readReplicaParams = stringMap();
    private Map<String,IntFunction<String>> instanceReadReplicaParams = new HashMap<>();
    private String recordFormat = Standard.LATEST_NAME;
    private IpFamily ipFamily = IPV4;
    private boolean useWildcard;
    private VerboseTimeout.VerboseTimeoutBuilder timeoutBuilder = new VerboseTimeout.VerboseTimeoutBuilder().withTimeout( 15, TimeUnit.MINUTES );
    private Set<DatabaseId> dbNames = Collections.singleton( new DatabaseId( CausalClusteringSettings.database.getDefaultValue() ) );

    public static ClusterConfig clusterConfig()
    {
        return new ClusterConfig();
    }

    private ClusterConfig()
    {
    }

    static Cluster createCluster( File directory, ClusterConfig clusterConfig )
    {
        return new Cluster( directory, clusterConfig.noCoreMembers, clusterConfig.noReadReplicas, clusterConfig.discoveryServiceType.createFactory(),
                clusterConfig.coreParams, clusterConfig.instanceCoreParams, clusterConfig.readReplicaParams, clusterConfig.instanceReadReplicaParams,
                clusterConfig.recordFormat, clusterConfig.ipFamily, clusterConfig.useWildcard, clusterConfig.dbNames );
    }

    public ClusterConfig withDatabaseIds( Set<DatabaseId> dbNames )
    {
        this.dbNames = dbNames;
        Map<Integer,DatabaseId> coreDBMap = CausalClusteringTestHelpers.distributeDatabaseNamesToHostNums( noCoreMembers, dbNames );
        Map<Integer,DatabaseId> rrDBMap = CausalClusteringTestHelpers.distributeDatabaseNamesToHostNums( noReadReplicas, dbNames );

        Map<DatabaseId,Long> minCoresPerDb = coreDBMap.entrySet().stream().collect( Collectors.groupingBy( Map.Entry::getValue, Collectors.counting() ) );

        Map<Integer,String> minCoresSettingsMap = new HashMap<>();

        for ( var entry : coreDBMap.entrySet() )
        {
            Optional<Long> minNumCores = Optional.ofNullable( minCoresPerDb.get( entry.getValue() ) );
            minNumCores.ifPresent( n -> minCoresSettingsMap.put( entry.getKey(), n.toString() ) );
        }

        withInstanceCoreParam( CausalClusteringSettings.database, idx -> coreDBMap.get( idx ).name() );
        withInstanceCoreParam( CausalClusteringSettings.minimum_core_cluster_size_at_formation, minCoresSettingsMap::get );
        withInstanceReadReplicaParam( CausalClusteringSettings.database, idx -> rrDBMap.get( idx ).name() );
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
