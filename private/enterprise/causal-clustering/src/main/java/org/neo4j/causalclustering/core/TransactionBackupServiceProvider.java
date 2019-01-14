/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.core;

import com.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;
import io.netty.channel.ChannelInboundHandler;

import java.util.Collection;
import java.util.Optional;

import org.neo4j.causalclustering.catchup.CatchupServerBuilder;
import org.neo4j.causalclustering.catchup.CatchupServerHandler;
import org.neo4j.causalclustering.net.Server;
import org.neo4j.causalclustering.protocol.NettyPipelineBuilderFactory;
import org.neo4j.causalclustering.protocol.handshake.ApplicationSupportedProtocols;
import org.neo4j.causalclustering.protocol.handshake.ModifierSupportedProtocols;
import org.neo4j.helpers.ListenSocketAddress;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.ConnectorPortRegister;
import org.neo4j.logging.LogProvider;
import org.neo4j.scheduler.JobScheduler;

public class TransactionBackupServiceProvider
{
    public static final String BACKUP_SERVER_NAME = "backup-server";

    private final LogProvider logProvider;
    private final LogProvider userLogProvider;
    private final ChannelInboundHandler parentHandler;
    private final ApplicationSupportedProtocols catchupProtocols;
    private final Collection<ModifierSupportedProtocols> supportedModifierProtocols;
    private final NettyPipelineBuilderFactory serverPipelineBuilderFactory;
    private final CatchupServerHandler catchupServerHandler;
    private final JobScheduler scheduler;
    private final ConnectorPortRegister portRegister;
    private final String activeDatabaseName;

    public TransactionBackupServiceProvider( LogProvider logProvider, LogProvider userLogProvider, ApplicationSupportedProtocols catchupProtocols,
            Collection<ModifierSupportedProtocols> supportedModifierProtocols, NettyPipelineBuilderFactory serverPipelineBuilderFactory,
            CatchupServerHandler catchupServerHandler, ChannelInboundHandler parentHandler, String activeDatabaseName, JobScheduler scheduler,
            ConnectorPortRegister portRegister )
    {
        this.logProvider = logProvider;
        this.userLogProvider = userLogProvider;
        this.parentHandler = parentHandler;
        this.catchupProtocols = catchupProtocols;
        this.supportedModifierProtocols = supportedModifierProtocols;
        this.serverPipelineBuilderFactory = serverPipelineBuilderFactory;
        this.catchupServerHandler = catchupServerHandler;
        this.activeDatabaseName = activeDatabaseName;
        this.scheduler = scheduler;
        this.portRegister = portRegister;
    }

    public Optional<Server> resolveIfBackupEnabled( Config config )
    {
        if ( config.get( OnlineBackupSettings.online_backup_enabled ) )
        {
            ListenSocketAddress backupAddress = config.get( OnlineBackupSettings.online_backup_listen_address );
            logProvider.getLog( TransactionBackupServiceProvider.class ).info( "Binding backup service on address %s", backupAddress );
            Server catchupServer = CatchupServerBuilder.builder()
                    .catchupServerHandler( catchupServerHandler )
                    .defaultDatabaseName( activeDatabaseName )
                    .catchupProtocols( catchupProtocols )
                    .modifierProtocols( supportedModifierProtocols )
                    .pipelineBuilder( serverPipelineBuilderFactory )
                    .installedProtocolsHandler( parentHandler )
                    .listenAddress( backupAddress )
                    .scheduler( scheduler )
                    .userLogProvider( userLogProvider )
                    .debugLogProvider( logProvider )
                    .portRegister( portRegister )
                    .serverName( BACKUP_SERVER_NAME )
                    .build();
            return Optional.of( catchupServer );
        }
        else
        {
            return Optional.empty();
        }
    }
}
