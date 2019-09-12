/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.config;

import java.net.URI;
import java.time.Duration;
import java.util.List;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.configuration.GroupSetting;
import org.neo4j.configuration.SettingValueParsers;
import org.neo4j.configuration.SettingsDeclaration;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.graphdb.config.Setting;

import static java.time.Duration.ofMinutes;
import static org.neo4j.configuration.SettingImpl.newBuilder;
import static org.neo4j.configuration.SettingValueParsers.DURATION;
import static org.neo4j.configuration.SettingValueParsers.INT;
import static org.neo4j.configuration.SettingValueParsers.LONG;
import static org.neo4j.configuration.SettingValueParsers.STRING;

@ServiceProvider
public class FabricSettings implements SettingsDeclaration
{
    static Setting<List<SocketAddress>> fabricServersSetting = newBuilder( "fabric.routing.servers",
            SettingValueParsers.listOf( SettingValueParsers.SOCKET_ADDRESS ),
            List.of(new SocketAddress( "localhost", 7687 )))
            .build();
    static Setting<String> databaseName = newBuilder( "fabric.database.name", STRING, null ).build();
    static Setting<Long> routingTtlSetting = newBuilder( "fabric.routing.ttl", LONG, 1000L ).build();
    static Setting<Duration> driverIdleTimeout = newBuilder( "fabric.driver.timeout", DURATION, ofMinutes( 1 ) ).build();
    static Setting<Duration> driverIdleCheckInterval = newBuilder( "fabric.driver.idle.check.interval", DURATION, ofMinutes( 1 ) ).build();
    static Setting<Integer> bufferLowWatermarkSetting = newBuilder( "fabric.stream.buffer.low.watermark",
            INT,
            300 )
            .build();
    static Setting<Integer> bufferSizeSetting = newBuilder( "fabric.stream.buffer.size", INT, 1000 ).build();
    static Setting<Integer> syncBatchSizeSetting = newBuilder( "fabric.stream.sync.batch.size", INT, 50 ).build();

    @ServiceProvider
    public static class GraphSetting extends GroupSetting
    {

        public final Setting<URI> uri = getBuilder( "uri", SettingValueParsers.URI, null ).build();

        public final Setting<String> database = getBuilder( "database", SettingValueParsers.STRING, "neo4j" ).build();

        public final Setting<String> name = getBuilder( "name", SettingValueParsers.STRING, null ).build();

        protected GraphSetting( String name )
        {
            super( name );
        }

        public GraphSetting()
        {
            super( null ); // For ServiceLoader
        }

        @Override
        public String getPrefix()
        {
            return "fabric.graph";
        }
    }
}
