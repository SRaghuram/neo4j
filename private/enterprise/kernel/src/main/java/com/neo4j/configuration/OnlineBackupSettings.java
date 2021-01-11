/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.configuration;

import org.neo4j.annotations.api.PublicApi;
import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.configuration.Description;
import org.neo4j.configuration.SettingsDeclaration;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.graphdb.config.Setting;

import static org.neo4j.configuration.SettingImpl.newBuilder;
import static org.neo4j.configuration.SettingValueParsers.BOOL;
import static org.neo4j.configuration.SettingValueParsers.SOCKET_ADDRESS;
import static org.neo4j.configuration.SettingValueParsers.ofEnum;

@ServiceProvider
@PublicApi
public class OnlineBackupSettings implements SettingsDeclaration
{
    public static final String DEFAULT_BACKUP_HOST = "localhost";
    public static final int DEFAULT_BACKUP_PORT = 6362;

    @Description( "Enable support for running online backups." )
    public static final Setting<Boolean> online_backup_enabled = newBuilder( "dbms.backup.enabled", BOOL, true ).build();

    @Description( "Strategy for incremental backup. StartTime means that this server will send transactions until the time of when the backup started has " +
                  "been reached. Aggressive will keep sending until all committed transactions have been sent, even if they where committed after the backup " +
                  "job started. " )
    public static final Setting<TransactionStreamingStrategy> incremental_backup_strategy =
            newBuilder( "dbms.backup.incremental.strategy", ofEnum( TransactionStreamingStrategy.class ), TransactionStreamingStrategy.Aggressive ).dynamic()
                    .build();

    @Description( "Network interface and port for the backup server to listen on." )
    public static final Setting<SocketAddress> online_backup_listen_address =
            newBuilder( "dbms.backup.listen_address", SOCKET_ADDRESS, new SocketAddress( "127.0.0.1", DEFAULT_BACKUP_PORT ) ).build();
}
