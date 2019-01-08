/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.bolt;

import java.time.Clock;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

import io.netty.channel.Channel;
import io.netty.handler.ssl.SslContext;
import io.netty.util.internal.logging.InternalLoggerFactory;

import org.neo4j.bolt.security.auth.Authentication;
import org.neo4j.bolt.security.auth.BasicAuthentication;
import org.neo4j.bolt.transport.BoltProtocol;
import org.neo4j.bolt.transport.Netty4LoggerFactory;
import org.neo4j.bolt.transport.NettyServer;
import org.neo4j.bolt.transport.NettyServer.ProtocolInitializer;
import org.neo4j.bolt.transport.SocketTransport;
import org.neo4j.bolt.v1.runtime.BoltChannelAutoReadLimiter;
import org.neo4j.bolt.v1.runtime.BoltConnectionDescriptor;
import org.neo4j.bolt.v1.runtime.BoltFactory;
import org.neo4j.bolt.v1.runtime.BoltFactoryImpl;
import org.neo4j.bolt.v1.runtime.BoltWorker;
import org.neo4j.bolt.v1.runtime.MonitoredWorkerFactory;
import org.neo4j.bolt.v1.runtime.WorkerFactory;
import org.neo4j.bolt.v1.runtime.concurrent.ThreadedWorkerFactory;
import org.neo4j.bolt.v1.transport.BoltProtocolV1;
import org.neo4j.configuration.Description;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.ListenSocketAddress;
import org.neo4j.helpers.Service;
import org.neo4j.kernel.api.bolt.BoltConnectionTracker;
import org.neo4j.kernel.api.security.AuthManager;
import org.neo4j.kernel.api.security.UserManagerSupplier;
import org.neo4j.kernel.configuration.BoltConnector;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.ssl.SslPolicyLoader;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.spi.KernelContext;
import org.neo4j.kernel.impl.util.JobScheduler;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.Log;
import org.neo4j.udc.UsageData;

import static java.lang.String.format;
import static java.util.stream.Collectors.toList;
import static org.neo4j.kernel.configuration.Settings.STRING;
import static org.neo4j.kernel.configuration.Settings.setting;
import static org.neo4j.kernel.configuration.ssl.LegacySslPolicyConfig.LEGACY_POLICY_NAME;
import static org.neo4j.kernel.impl.util.JobScheduler.Groups.boltNetworkIO;

/**
 * Wraps Bolt and exposes it as a Kernel Extension.
 */
@Service.Implementation( KernelExtensionFactory.class )
public class BoltKernelExtension extends KernelExtensionFactory<BoltKernelExtension.Dependencies>
{
    public static class Settings
    {
        @Description( "SSL policy to use" )
        public static Setting<String> ssl_policy = setting( "bolt.ssl_policy", STRING, LEGACY_POLICY_NAME );
    }

    public interface Dependencies
    {
        LogService logService();

        Config config();

        GraphDatabaseService db();

        JobScheduler scheduler();

        UsageData usageData();

        Monitors monitors();

        ThreadToStatementContextBridge txBridge();

        BoltConnectionTracker sessionTracker();

        Clock clock();

        AuthManager authManager();

        UserManagerSupplier userManagerSupplier();

        SslPolicyLoader sslPolicyFactory();
    }

    public BoltKernelExtension()
    {
        super( "bolt-server" );
    }

    @Override
    public Lifecycle newInstance( KernelContext context, Dependencies dependencies ) throws Throwable
    {
        Config config = dependencies.config();
        GraphDatabaseService gdb = dependencies.db();
        GraphDatabaseAPI api = (GraphDatabaseAPI) gdb;
        LogService logService = dependencies.logService();
        Clock clock = dependencies.clock();
        SslPolicyLoader sslPolicyFactory = dependencies.sslPolicyFactory();
        Log log = logService.getInternalLog( WorkerFactory.class );

        LifeSupport life = new LifeSupport();

        JobScheduler scheduler = dependencies.scheduler();

        InternalLoggerFactory.setDefaultFactory( new Netty4LoggerFactory( logService.getInternalLogProvider() ) );

        Authentication authentication = authentication( dependencies.authManager(), dependencies.userManagerSupplier() );

        BoltFactory boltFactory = life.add( new BoltFactoryImpl( api, dependencies.usageData(),
                logService, dependencies.txBridge(), authentication, dependencies.sessionTracker(), config ) );
        WorkerFactory workerFactory = createWorkerFactory( boltFactory, scheduler, dependencies, logService, clock );

        List<ProtocolInitializer> connectors = config.enabledBoltConnectors().stream()
                .map( ( connConfig ) ->
                {
                    ListenSocketAddress listenAddress = config.get( connConfig.listen_address );
                    SslContext sslCtx;
                    boolean requireEncryption;
                    final BoltConnector.EncryptionLevel encryptionLevel = config.get( connConfig.encryption_level );
                    switch ( encryptionLevel )
                    {
                    case REQUIRED:
                        // Encrypted connections are mandatory, a self-signed certificate may be generated.
                        requireEncryption = true;
                        sslCtx = createSslContext( sslPolicyFactory, config );
                        break;
                    case OPTIONAL:
                        // Encrypted connections are optional, a self-signed certificate may be generated.
                        requireEncryption = false;
                        sslCtx = createSslContext( sslPolicyFactory, config );
                        break;
                    case DISABLED:
                        // Encryption is turned off, no self-signed certificate will be generated.
                        requireEncryption = false;
                        sslCtx = null;
                        break;
                    default:
                        // In the unlikely event that we happen to fall through to the default option here,
                        // there is a mismatch between the BoltConnector.EncryptionLevel enum and the options
                        // handled in this switch statement. In this case, we'll log a warning and default to
                        // disabling encryption, since this mirrors the functionality introduced in 3.0.
                        log.warn( format( "Unhandled encryption level %s - assuming DISABLED.", encryptionLevel.name() ) );
                        requireEncryption = false;
                        sslCtx = null;
                        break;
                    }

                    final Map<Long, BiFunction<Channel, Boolean, BoltProtocol>> versions =
                            newVersions( logService, workerFactory );
                    return new SocketTransport( listenAddress, sslCtx, requireEncryption, logService.getInternalLogProvider(), versions );
                } )
                .collect( toList() );

        if ( connectors.size() > 0 && !config.get( GraphDatabaseSettings.disconnected ) )
        {
            life.add( new NettyServer( scheduler.threadFactory( boltNetworkIO ), connectors ) );
            log.info( "Bolt Server extension loaded." );
            for ( ProtocolInitializer connector : connectors )
            {
                logService.getUserLog( WorkerFactory.class ).info( "Bolt enabled on %s.", connector.address() );
            }
        }

        return life;
    }

    protected WorkerFactory createWorkerFactory( BoltFactory boltFactory, JobScheduler scheduler,
            Dependencies dependencies, LogService logService, Clock clock )
    {
        WorkerFactory threadedWorkerFactory = new ThreadedWorkerFactory( boltFactory, scheduler, logService, clock );
        return new MonitoredWorkerFactory( dependencies.monitors(), threadedWorkerFactory, clock );
    }

    private SslContext createSslContext( SslPolicyLoader sslPolicyFactory, Config config )
    {
        try
        {
            String policyName = config.get( Settings.ssl_policy );
            if ( policyName == null )
            {
                throw new IllegalArgumentException( "No SSL policy has been configured for bolt" );
            }
            return sslPolicyFactory.getPolicy( policyName ).nettyServerContext();
        }
        catch ( Exception e )
        {
            throw new RuntimeException( "Failed to initialize SSL encryption support, which is required to start " +
                    "this connector. Error was: " + e.getMessage(), e );
        }
    }

    private Map<Long, BiFunction<Channel, Boolean, BoltProtocol>> newVersions(
            LogService logging, WorkerFactory workerFactory )
    {
        Map<Long, BiFunction<Channel, Boolean, BoltProtocol>> availableVersions = new HashMap<>();
        availableVersions.put(
                (long) BoltProtocolV1.VERSION,
                ( channel, isEncrypted ) ->
                {
                    BoltConnectionDescriptor descriptor = new BoltConnectionDescriptor(
                            channel.remoteAddress(), channel.localAddress() );
                    BoltChannelAutoReadLimiter limiter =
                            new BoltChannelAutoReadLimiter( channel, logging.getInternalLog( BoltChannelAutoReadLimiter.class ) );
                    BoltWorker worker = workerFactory.newWorker( descriptor, limiter, channel::close );
                    return new BoltProtocolV1( worker, channel, logging );
                }
        );
        return availableVersions;
    }

    private Authentication authentication( AuthManager authManager, UserManagerSupplier userManagerSupplier )
    {
        return new BasicAuthentication( authManager, userManagerSupplier );
    }
}
