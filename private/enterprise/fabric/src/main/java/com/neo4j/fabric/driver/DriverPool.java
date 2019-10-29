/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.driver;

import com.neo4j.fabric.auth.CredentialsProvider;
import com.neo4j.fabric.config.FabricConfig;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;

import java.net.URI;
import java.net.URISyntaxException;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import org.neo4j.driver.AuthToken;
import org.neo4j.driver.internal.DriverFactory;
import org.neo4j.driver.internal.async.connection.EventLoopGroupFactory;
import org.neo4j.driver.internal.cluster.RoutingSettings;
import org.neo4j.driver.internal.retry.RetrySettings;
import org.neo4j.driver.internal.shaded.io.netty.channel.EventLoopGroup;
import org.neo4j.internal.kernel.api.security.AuthSubject;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.scheduler.JobScheduler;

import static org.neo4j.scheduler.Group.TRANSACTION_TIMEOUT_MONITOR;

public class DriverPool extends LifecycleAdapter
{
    private final ConcurrentHashMap<Key,PooledDriver> driversInUse = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Key,PooledDriver> idleDrivers = new ConcurrentHashMap<>();
    private final CredentialsProvider credentialsProvider;
    private final JobScheduler jobScheduler;
    private final FabricConfig fabricConfig;
    private final Clock clock;
    private final DriverConfigFactory driverConfigFactory;
    private final EventLoopGroup eventLoopGroup;

    public DriverPool( JobScheduler jobScheduler,
            FabricConfig fabricConfig,
            org.neo4j.configuration.Config serverConfig,
            Clock clock,
            CredentialsProvider credentialsProvider )
    {
        this.jobScheduler = jobScheduler;
        this.fabricConfig = fabricConfig;
        this.clock = clock;
        this.credentialsProvider = credentialsProvider;

        driverConfigFactory = new DriverConfigFactory( fabricConfig, serverConfig );

        var eventLoopCount = fabricConfig.getGlobalDriverConfig().getEventLoopCount();
        eventLoopGroup = EventLoopGroupFactory.newEventLoopGroup( eventLoopCount );
    }

    public PooledDriver getDriver( FabricConfig.Graph location, AuthSubject subject )
    {

        var authToken = credentialsProvider.credentialsFor( subject );
        Key key = new Key( location.getUri(), authToken );
        return driversInUse.compute( key, ( k, presentValue ) ->
        {
            if ( presentValue != null )
            {
                presentValue.getReferenceCounter().incrementAndGet();
                return presentValue;
            }

            AtomicReference<PooledDriver> idleDriverRef = new AtomicReference<>();
            idleDrivers.computeIfPresent( key, ( k2, oldValue ) ->
            {
                idleDriverRef.set( oldValue );
                return null;
            } );

            PooledDriver pooledDriver;
            if ( idleDriverRef.get() != null )
            {
                pooledDriver = idleDriverRef.get();
            }
            else
            {
                pooledDriver = createDriver( key, location, authToken );
            }

            pooledDriver.getReferenceCounter().incrementAndGet();
            return pooledDriver;
        });
    }

    private void release( Key key, PooledDriver pooledDriver )
    {
        driversInUse.computeIfPresent( key, ( k, value ) ->
        {
            if ( pooledDriver.getReferenceCounter().decrementAndGet() != 0 )
            {
                return pooledDriver;
            }

            idleDrivers.put( key, pooledDriver );
            pooledDriver.setLastUsedTimestamp( clock.instant() );
            return null;
        } );
    }

    @Override
    public void start()
    {
        long checkInterval = fabricConfig.getGlobalDriverConfig().getDriverIdleCheckInterval().toSeconds();
        Duration idleTimeout = fabricConfig.getGlobalDriverConfig().getIdleTimeout();
        jobScheduler.schedule( TRANSACTION_TIMEOUT_MONITOR, () ->
        {
            List<Key> timeoutCandidates = idleDrivers.entrySet().stream()
                    .filter( entry -> Duration.between( entry.getValue().getLastUsedTimestamp(), clock.instant() ).compareTo( idleTimeout ) > 0 )
                    .map( Map.Entry::getKey )
                    .collect( Collectors.toList() );

            timeoutCandidates.forEach( key -> idleDrivers.computeIfPresent( key, ( k, pooledDriver ) ->
            {
                pooledDriver.close();
                return null;
            } ) );
        }, checkInterval, TimeUnit.SECONDS );
    }

    @Override
    public void stop()
    {
        idleDrivers.values().forEach( PooledDriver::close );
        driversInUse.values().forEach( PooledDriver::close );
        eventLoopGroup.shutdownGracefully( 1, 4,  TimeUnit.SECONDS);
    }

    private PooledDriver createDriver( Key key, FabricConfig.Graph location, AuthToken token )
    {
        var config = driverConfigFactory.createConfig( location );

        var driverFactory = new DriverFactory();

        var driverUri = constructDriverUri( location.getUri() );
        var databaseDriver = driverFactory.newInstance( driverUri, token, RoutingSettings.DEFAULT, RetrySettings.DEFAULT, config, eventLoopGroup );

        var driverApi = driverConfigFactory.getProperty( location, FabricConfig.DriverConfig::getDriverApi );
        switch ( driverApi )
        {
        case RX:
            return new RxPooledDriver( databaseDriver, pd -> release( key, pd ) );
        case ASYNC:
            return new AsyncPooledDriver( databaseDriver, pd -> release( key, pd ) );
        default:
            throw new IllegalArgumentException( "Unexpected Driver API value: " + driverApi );
        }
    }

    private URI constructDriverUri( FabricConfig.RemoteUri uri )
    {
        var address = uri.getAddresses().get( 0 );
        try
        {
            return new URI( uri.getScheme(), null, address.getHostname(), address.getPort(), null, uri.getQuery(), null );
        }
        catch ( URISyntaxException e )
        {
            throw new IllegalArgumentException( e.getMessage(), e );
        }
    }

    private class Key
    {

        private final FabricConfig.RemoteUri uri;
        private final AuthToken auth;

        Key( FabricConfig.RemoteUri uri, AuthToken auth )
        {
            this.uri = uri;
            this.auth = auth;
        }

        @Override
        public boolean equals( Object that )
        {
            return EqualsBuilder.reflectionEquals( this, that );
        }

        @Override
        public int hashCode()
        {
            return HashCodeBuilder.reflectionHashCode( this );
        }
    }
}
