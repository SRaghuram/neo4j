/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.test.causalclustering;

import org.junit.rules.ExternalResource;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.IntFunction;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.causalclustering.common.Cluster;
import org.neo4j.causalclustering.common.EnterpriseCluster;
import org.neo4j.causalclustering.discovery.DiscoveryServiceFactory;
import org.neo4j.causalclustering.discovery.IpFamily;
import org.neo4j.causalclustering.helpers.CausalClusteringTestHelpers;
import org.neo4j.causalclustering.scenarios.DiscoveryServiceType;
import org.neo4j.causalclustering.scenarios.EnterpriseDiscoveryServiceType;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.kernel.impl.store.format.standard.Standard;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.VerboseTimeout;

import static org.neo4j.causalclustering.discovery.IpFamily.IPV4;
import static org.neo4j.helpers.collection.MapUtil.stringMap;

/**
 * Includes a {@link VerboseTimeout} rule with a long default timeout. Use {@link #withTimeout(long, TimeUnit)} to customise
 * or {@link #withNoTimeout()} to disable.
 */
public class ClusterRule extends ExternalResource
{
    public interface TestClusterFactory<T extends DiscoveryServiceFactory>
    {
        Cluster<T> create( File parentDir, int numCores, int numRRs, T discoveryServiceFactory, Map<String,String> coreParams,
                Map<String,IntFunction<String>> instanceCoreParams, Map<String,String> rrParams, Map<String,IntFunction<String>> instanceRRParams,
                String recordFormat, IpFamily ipFamily, boolean useWildCard, Set<String> dbNames );
    }

    protected final TestDirectory testDirectory = TestDirectory.testDirectory();
    protected File clusterDirectory;
    protected Cluster<?> cluster;

    protected int noCoreMembers = 3;
    protected int noReadReplicas = 3;
    protected DiscoveryServiceType discoveryServiceType = EnterpriseDiscoveryServiceType.SHARED;
    protected Map<String,String> coreParams = stringMap();
    protected Map<String,IntFunction<String>> instanceCoreParams = new HashMap<>();
    protected Map<String,String> readReplicaParams = stringMap();
    protected Map<String,IntFunction<String>> instanceReadReplicaParams = new HashMap<>();
    protected String recordFormat = Standard.LATEST_NAME;
    protected IpFamily ipFamily = IPV4;
    protected boolean useWildcard;
    protected VerboseTimeout.VerboseTimeoutBuilder timeoutBuilder = new VerboseTimeout.VerboseTimeoutBuilder().withTimeout( 15, TimeUnit.MINUTES );
    protected Set<String> dbNames = Collections.singleton( CausalClusteringSettings.database.getDefaultValue() );

    public ClusterRule()
    {
    }

    @Override
    public Statement apply( final Statement base, final Description description )
    {
        Statement timeoutStatement;
        if ( timeoutBuilder != null )
        {
            timeoutStatement = timeoutBuilder.build().apply( base, description );
        }
        else
        {
            timeoutStatement = base;
        }

        Statement testMethod = new Statement()
        {
            @Override
            public void evaluate() throws Throwable
            {
                // If this is used as class rule then getMethodName() returns null, so use
                // getClassName() instead.
                String name =
                        description.getMethodName() != null ? description.getMethodName() : description.getClassName();
                clusterDirectory = testDirectory.directory( name );
                timeoutStatement.evaluate();
            }
        };

        Statement testMethodWithBeforeAndAfter = super.apply( testMethod, description );

        return testDirectory.apply( testMethodWithBeforeAndAfter, description );
    }

    @Override
    protected void after()
    {
        if ( cluster != null )
        {
            cluster.shutdown();
        }
    }

    /**
     * Starts cluster with the configuration provided at instantiation time. This method will not return until the
     * cluster is up and all members report each other as available.
     */
    public Cluster<?> startCluster() throws Exception
    {
        return startCluster( discoveryServiceType::createFactory, EnterpriseCluster::new );
    }

    protected <T extends DiscoveryServiceFactory> Cluster<?> startCluster( Supplier<T> discoveryServiceFactory, TestClusterFactory<T> factory ) throws Exception
    {
        createCluster( discoveryServiceFactory, factory );
        cluster.start();
        for ( String dbName : dbNames )
        {
            cluster.awaitLeader( dbName );
        }
        return cluster;
    }

    private  <T extends DiscoveryServiceFactory> Cluster<?> createCluster( Supplier<T> discoveryServiceFactory, TestClusterFactory<T> factory )
    {
        if ( cluster == null )
        {
            cluster = factory.create( clusterDirectory, noCoreMembers, noReadReplicas, discoveryServiceFactory.get(), coreParams, instanceCoreParams,
                    readReplicaParams, instanceReadReplicaParams, recordFormat, ipFamily, useWildcard, dbNames );
        }
        return cluster;
    }

    public Cluster<?> createCluster()
    {
        return createCluster( discoveryServiceType::createFactory, EnterpriseCluster::new );
    }

    public TestDirectory testDirectory()
    {
        return testDirectory;
    }

    public File clusterDirectory()
    {
        return clusterDirectory;
    }

    public ClusterRule withDatabaseNames( Set<String> dbNames )
    {
        this.dbNames = dbNames;
        Map<Integer, String> coreDBMap = CausalClusteringTestHelpers.distributeDatabaseNamesToHostNums( noCoreMembers, dbNames );
        Map<Integer, String> rrDBMap = CausalClusteringTestHelpers.distributeDatabaseNamesToHostNums( noReadReplicas, dbNames );

        Map<String,Long> minCoresPerDb = coreDBMap.entrySet().stream()
                .collect( Collectors.groupingBy( Map.Entry::getValue, Collectors.counting() ) );

        Map<Integer,String> minCoresSettingsMap = new HashMap<>();

        for ( Map.Entry<Integer,String> entry: coreDBMap.entrySet() )
        {
            Optional<Long> minNumCores = Optional.ofNullable( minCoresPerDb.get( entry.getValue() ) );
            minNumCores.ifPresent( n -> minCoresSettingsMap.put( entry.getKey(), n.toString() ) );
        }

        withInstanceCoreParam( CausalClusteringSettings.database, coreDBMap::get );
        withInstanceCoreParam( CausalClusteringSettings.minimum_core_cluster_size_at_formation, minCoresSettingsMap::get );
        withInstanceReadReplicaParam( CausalClusteringSettings.database, rrDBMap::get );
        return this;
    }

    public ClusterRule withNumberOfCoreMembers( int noCoreMembers )
    {
        this.noCoreMembers = noCoreMembers;
        return this;
    }

    public ClusterRule withNumberOfReadReplicas( int noReadReplicas )
    {
        this.noReadReplicas = noReadReplicas;
        return this;
    }

    public ClusterRule withDiscoveryServiceType( DiscoveryServiceType discoveryServiceType )
    {
        this.discoveryServiceType = discoveryServiceType;
        return this;
    }

    public ClusterRule withSharedCoreParams( Map<String,String> params )
    {
        this.coreParams.putAll( params );
        return this;
    }

    public ClusterRule withSharedCoreParam( Setting<?> key, String value )
    {
        this.coreParams.put( key.name(), value );
        return this;
    }

    public ClusterRule withInstanceCoreParams( Map<String,IntFunction<String>> params )
    {
        this.instanceCoreParams.putAll( params );
        return this;
    }

    public ClusterRule withInstanceCoreParam( Setting<?> key, IntFunction<String> valueFunction )
    {
        this.instanceCoreParams.put( key.name(), valueFunction );
        return this;
    }

    public ClusterRule withSharedReadReplicaParams( Map<String,String> params )
    {
        this.readReplicaParams.putAll( params );
        return this;
    }

    public ClusterRule withSharedReadReplicaParam( Setting<?> key, String value )
    {
        this.readReplicaParams.put( key.name(), value );
        return this;
    }

    public ClusterRule withInstanceReadReplicaParams( Map<String,IntFunction<String>> params )
    {
        this.instanceReadReplicaParams.putAll( params );
        return this;
    }

    public ClusterRule withInstanceReadReplicaParam( Setting<?> key, IntFunction<String> valueFunction )
    {
        this.instanceReadReplicaParams.put( key.name(), valueFunction );
        return this;
    }

    public ClusterRule withRecordFormat( String recordFormat )
    {
        this.recordFormat = recordFormat;
        return this;
    }

    public ClusterRule withClusterDirectory( File clusterDirectory )
    {
        this.clusterDirectory = clusterDirectory;
        return this;
    }

    public ClusterRule withIpFamily( IpFamily ipFamily )
    {
        this.ipFamily = ipFamily;
        return this;
    }

    public ClusterRule useWildcard( boolean useWildcard )
    {
        this.useWildcard = useWildcard;
        return this;
    }

    public ClusterRule withTimeout( long timeout, TimeUnit unit )
    {
        this.timeoutBuilder = new VerboseTimeout.VerboseTimeoutBuilder().withTimeout( timeout, unit );
        return this;
    }

    public ClusterRule withNoTimeout()
    {
        this.timeoutBuilder = null;
        return this;
    }
}
