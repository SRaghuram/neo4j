/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.routing;

import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.core.CoreClusterMember;
import com.neo4j.causalclustering.read_replica.ReadReplica;
import com.neo4j.test.causalclustering.ClusterConfig;
import com.neo4j.test.causalclustering.ClusterExtension;
import com.neo4j.test.causalclustering.ClusterFactory;
import com.neo4j.test.driver.DriverExtension;
import com.neo4j.test.driver.DriverFactory;
import org.assertj.core.api.ThrowableAssert;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.platform.commons.logging.Logger;
import org.junit.platform.commons.logging.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Predicate;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.driver.Driver;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.exceptions.SessionExpiredException;
import org.neo4j.driver.net.ServerAddress;
import org.neo4j.fabric.config.FabricSettings;
import org.neo4j.test.assertion.Assert;
import org.neo4j.test.extension.Inject;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.fail;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_METHOD;

@ClusterExtension
@DriverExtension
@TestInstance( PER_METHOD )
public class ReadReplicaRoutingIT
{
    private static final DriverFactory.InstanceConfig driverConfig =
            DriverFactory.instanceConfig()
                         .withAdditionalConfig( configBuilder -> configBuilder.withMaxTransactionRetryTime( 10, TimeUnit.SECONDS )
                                                                              .withMaxConnectionPoolSize( 2 )
                         );
    @Inject
    private ClusterFactory clusterFactory;

    @Inject
    private DriverFactory driverFactory;

    private Cluster cluster;

    private final Logger log = LoggerFactory.getLogger( this.getClass() );

    @ParameterizedTest
    @EnumSource( value = FabricByDefault.class )
    void testWriteOnReadReplicaWithRoutingEnabled( FabricByDefault fabric ) throws Exception
    {
        for ( var clusterType : ClusterConfig.ClusterType.values() )
        {
            //given
            SimulateLoadBalancedReadReplicas simulateLoadBalancer = SimulateLoadBalancedReadReplicas.OFF;
            cluster = createCluster( clusterType, ServerSideRouting.ENABLED, fabric, simulateLoadBalancer );

            // when/then...
            // Run multiple checks using the same cluster. If run separately the tests take much longer.
            checkReadReplicaBoltDriverCannotWrite();

            checkReadReplicaNeo4jDriverBehavesCorrectly( fabric );

            checkCoreNeo4jDriverCanWrite( simulateLoadBalancer );

            cluster.shutdown();
        }
    }

    @ParameterizedTest
    @EnumSource( value = FabricByDefault.class )
    void testWriteOnReadReplicaWithRoutingEnabledAndSimulatedLoadBalancer( FabricByDefault fabric ) throws Exception
    {
        for ( var clusterType : ClusterConfig.ClusterType.values() )
        {
            // given
            SimulateLoadBalancedReadReplicas simulateLoadBalancer = SimulateLoadBalancedReadReplicas.LOAD_BALANCED;
            cluster = createCluster( clusterType, ServerSideRouting.ENABLED, fabric, simulateLoadBalancer );

            // when/then...
            // Run multiple checks using the same cluster. If run separately the tests take much longer.
            checkReadReplicaBoltDriverCannotWrite();

            checkReadReplicaNeo4jDriverBehavesCorrectly( fabric );

            try ( Driver coreNeo4jDriver = getCoreNeo4jDriver( cluster, simulateLoadBalancer ) )
            {
                if ( fabric == FabricByDefault.ENABLED )
                {
                    // In this situation write should work
                    assertNoException( tryWriteTx( coreNeo4jDriver, true ) );
                }
                else
                {
                    // fail because fabric is not enabled
                    assertThatExceptionOfType( SessionExpiredException.class )
                            .isThrownBy( tryWriteTx( coreNeo4jDriver, false ) )
                            .withMessageContaining( "Server at localhost:" )
                            .withMessageContaining( "no longer accepts writes" );
                }
            }

            cluster.shutdown();
        }
    }

    @ParameterizedTest
    @EnumSource( value = FabricByDefault.class )
    void testWriteOnReadReplicaWithRoutingDisabled( FabricByDefault fabric ) throws Exception
    {
        for ( var clusterType : ClusterConfig.ClusterType.values() )
        {
            // given
            SimulateLoadBalancedReadReplicas simulateLoadBalancer = SimulateLoadBalancedReadReplicas.OFF;
            cluster = createCluster( clusterType, ServerSideRouting.DISABLED, fabric, simulateLoadBalancer );

            // when/then...
            // Run multiple checks using the same cluster. If run separately the tests take much longer.
            checkReadReplicaBoltDriverCannotWrite();

            checkReadReplicaNeo4jDriverCannotWrite();

            checkCoreNeo4jDriverCanWrite( simulateLoadBalancer );

            cluster.shutdown();
        }
    }

    @ParameterizedTest
    @EnumSource( value = FabricByDefault.class )
    void testWriteOnReadReplicaWithRoutingDisabledAndSimulatedLoadBalancer( FabricByDefault fabric ) throws Exception
    {
        for ( var clusterType : ClusterConfig.ClusterType.values() )
        {
            // given
            SimulateLoadBalancedReadReplicas simulateLoadBalancer = SimulateLoadBalancedReadReplicas.LOAD_BALANCED;
            cluster = createCluster( clusterType, ServerSideRouting.DISABLED, fabric, simulateLoadBalancer );

            // when/then...
            // Run multiple checks using the same cluster. If run separately the tests take much longer.
            checkReadReplicaBoltDriverCannotWrite();

            try ( Driver coreNeo4jDriver = getCoreNeo4jDriver( cluster, simulateLoadBalancer ) )
            {
                log.info( () -> "neo4j:// accidental write (simulated Load Balancer)" );
                assertThatExceptionOfType( SessionExpiredException.class )
                        .isThrownBy( tryWriteTx( coreNeo4jDriver, false ) )
                        .withMessageContaining( "Server at localhost:" )
                        .withMessageContaining( "no longer accepts writes" );
            }

            checkReadReplicaNeo4jDriverCannotWrite();

            cluster.shutdown();
        }
    }

    private enum ServerSideRouting
    {
        ENABLED,
        DISABLED;
    }

    private enum FabricByDefault
    {
        ENABLED,
        DISABLED;
    }

    private enum SimulateLoadBalancedReadReplicas
    {
        LOAD_BALANCED,
        OFF;
    }

    private Cluster createCluster( ClusterConfig.ClusterType type, ServerSideRouting ssr, FabricByDefault fabric,
                                   SimulateLoadBalancedReadReplicas loadBalanced ) throws Exception
    {
        log.info( () -> String.format( "Using cluster type: %s", type ) );

        ClusterConfig clusterConfig = ClusterConfig
                .clusterConfig()
                .withClusterType( type )
                .withSharedPrimaryParam( GraphDatabaseSettings.routing_enabled, ssr == ServerSideRouting.ENABLED ? "true" : "false" )
                .withSharedPrimaryParam( FabricSettings.enabled_by_default, fabric == FabricByDefault.ENABLED ? "true" : "false" )
                .withSharedReadReplicaParam( GraphDatabaseSettings.routing_enabled, ssr == ServerSideRouting.ENABLED ? "true" : "false" )
                .withSharedReadReplicaParam( FabricSettings.enabled_by_default, fabric == FabricByDefault.ENABLED ? "true" : "false" );

        switch ( type )
        {
        case CORES:
            clusterConfig = clusterConfig.withNumberOfCoreMembers( 3 ).withNumberOfReadReplicas( 2 );
            break;
        case STANDALONE:
            clusterConfig = clusterConfig.withNumberOfReadReplicas( 1 );
            break;
        default:
            throw new IllegalStateException( "Unknown state :" + type );
        }

        var cluster = clusterFactory.createCluster( clusterConfig );
        cluster.start();
        cluster.awaitLeader( "neo4j" );

        var readReplica = cluster.readReplicas().stream().findFirst().get();

        // Here we "hack" the routing table to point at a read replica. This allows us to simulate what happens if writes get directed to a RR.
        // this would happen in the wild if the RR and the leader were both being load balanced over by a naive load balancer.
        if ( loadBalanced == SimulateLoadBalancedReadReplicas.LOAD_BALANCED )
        {
            switch ( type )
            {
            case CORES:
                cluster.primaryMembers().forEach( advertiseReadReplicaBoltAddress( readReplica ) );
                break;
            case STANDALONE:
                advertiseReadReplicaBoltAddress( readReplica ).accept( cluster.getStandaloneMember() );
                break;
            default:
                throw new IllegalStateException( "Unknown state :" + type );
            }
        }

        cluster.awaitLeader( "neo4j" );
        // We have restarted the cores but read replicas may not have re-connected to them which can cause flakiness, so we wait until the RRs look OK
        Assert.assertEventually(
                () -> cluster.readReplicas().stream().allMatch( ReadReplica::isConnectedToAtLeastOnePrimary ),
                Predicate.isEqual( true ),
                30, TimeUnit.SECONDS
        );
        return cluster;
    }

    private void checkCoreNeo4jDriverCanWrite( SimulateLoadBalancedReadReplicas simulateLoadBalancer ) throws IOException
    {
        try ( Driver coreNeo4jDriver = getCoreNeo4jDriver( cluster, simulateLoadBalancer ) )
        {
            // this is just a regular write to a core
            log.info( () -> "neo4j:// to core" );
            assertNoException( tryWriteTx( coreNeo4jDriver, true ) );
        }
    }

    private void checkReadReplicaNeo4jDriverCannotWrite() throws IOException
    {
        try ( Driver readReplicaNeo4jDriver = getReadReplicaNeo4jDriver( cluster ) )
        {
            log.info( () -> "neo4j:// to read replica" );
            assertThatExceptionOfType( SessionExpiredException.class )
                    .isThrownBy( tryWriteTx( readReplicaNeo4jDriver, false ) )
                    .withMessageContaining( "Failed to obtain connection towards WRITE server" );
        }
    }

    private void checkReadReplicaBoltDriverCannotWrite() throws IOException
    {
        try ( Driver readReplicaBoltDriver = getReadReplicaBoltDriver( cluster ) )
        {
            log.info( () -> "bolt:// driver direct to read replica" );
            assertThatExceptionOfType( ClientException.class )
                    .isThrownBy( tryWriteTx( readReplicaBoltDriver, false ) )
                    .withMessage( "No write operations are allowed on this database. This is a read only Neo4j instance." );
        }
    }

    private void checkReadReplicaNeo4jDriverBehavesCorrectly( FabricByDefault fabric ) throws IOException
    {
        try ( Driver readReplicaNeo4jDriver = getReadReplicaNeo4jDriver( cluster ) )
        {
            log.info( () -> "neo4j:// to read replica" );
            if ( fabric == FabricByDefault.ENABLED )
            {
                // In this situation write should work
                assertNoException( tryWriteTx( readReplicaNeo4jDriver, true ) );
            }
            else
            {
                // fail because fabric is not enabled
                assertThatExceptionOfType( SessionExpiredException.class )
                        .isThrownBy( tryWriteTx( readReplicaNeo4jDriver, false ) )
                        .withMessageContaining( "Server at localhost:" )
                        .withMessageContaining( "no longer accepts writes" );
            }
        }
    }

    private void assertNoException( ThrowableAssert.ThrowingCallable tryWriteTx )
    {
        try
        {
            tryWriteTx.call();
        }
        catch ( Throwable throwable )
        {
            fail( "Method should not throw", throwable );
        }
    }

    private ThrowableAssert.ThrowingCallable tryWriteTx( Driver driver, boolean printDriverLogsOnError )
    {
        var printDriverLogsOnSuccess = !printDriverLogsOnError;

        return () ->
        {
            try ( var session = driver.session() )
            {
                // execute count before in a write transaction to guarantee that it reads latest state.
                var countBefore = session.writeTransaction( tx -> tx.run( "MATCH (n) RETURN COUNT(n)" ).single().get( 0 ).asLong() );
                var result = session.writeTransaction( tx -> tx.run( "CREATE (n) RETURN id(n)" ).single().get( 0 ).asLong() );
                assertThat( result ).isGreaterThanOrEqualTo( 0 );
                var result2 = session.run( "CREATE (n) RETURN id(n)" ).single().get( 0 ).asLong();
                assertThat( result2 ).isGreaterThanOrEqualTo( 0 );
                // count after can use a read transaction because it is in the same session.
                var countAfter = session.readTransaction( tx -> tx.run( "MATCH (n) RETURN COUNT(n)" ).single().get( 0 ).asLong() );
                assertThat( countAfter ).isEqualTo( countBefore + 2 );

                if ( printDriverLogsOnSuccess )
                {
                    printDriverLogs( driver );
                }
            }
            catch ( Exception e )
            {
                if ( printDriverLogsOnError )
                {
                    printDriverLogs( driver );
                }
                throw e;
            }
        };
    }

    private void printDriverLogs( Driver driver ) throws IOException
    {
        var driverLog = Files.readString( driverFactory.getLogFile( driver ) );
        log.info( () -> driverLog );
    }

    private Consumer<CoreClusterMember> advertiseReadReplicaBoltAddress( com.neo4j.causalclustering.read_replica.ReadReplica readReplica )
    {
        return c ->
        {
            c.shutdown();
            c.config().set( BoltConnector.advertised_address, readReplica.settingValue( BoltConnector.advertised_address ) );
            c.start();
        };
    }

    private Driver getReadReplicaBoltDriver( Cluster cluster ) throws IOException
    {
        var readReplica = cluster.findAnyReadReplica();
        return driverFactory.graphDatabaseDriver( readReplica.directURI(), driverConfig );
    }

    private Driver getCoreNeo4jDriver( Cluster cluster, SimulateLoadBalancedReadReplicas simulateLoadBalancer ) throws IOException
    {
        var routingURI = URI.create( cluster.randomCoreMember( true ).get().routingURI() );

        // If simulating the load balancer scenario it is necessary to use a port that's <= 0 here so that the standalone / read replica instances use their
        // advertised addresses in the routing table if a positive port is used then the routing table will echo the address that the caller used back to them.
        return simulateLoadBalancer == SimulateLoadBalancedReadReplicas.OFF ?
               driverFactory.graphDatabaseDriver( routingURI, driverConfig ) :
               driverFactory.graphDatabaseDriver( URI.create( "neo4j://bootstrap:0" ),
                                                  ignored -> Set.of( ServerAddress.of( routingURI.getHost(), routingURI.getPort() ) ),
                                                  driverConfig );
    }

    private Driver getReadReplicaNeo4jDriver( Cluster cluster ) throws IOException
    {
        var readReplica = cluster.findAnyReadReplica();
        return driverFactory.graphDatabaseDriver( readReplica.neo4jURI(), driverConfig );
    }
}
