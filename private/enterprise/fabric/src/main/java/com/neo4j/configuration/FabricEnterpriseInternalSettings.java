/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.configuration;

import java.time.Duration;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.configuration.Description;
import org.neo4j.configuration.DocumentedDefaultValue;
import org.neo4j.configuration.Internal;
import org.neo4j.configuration.SettingsDeclaration;
import org.neo4j.graphdb.config.Setting;

import static java.time.Duration.ofMinutes;
import static org.neo4j.configuration.SettingImpl.newBuilder;
import static org.neo4j.configuration.SettingValueParsers.BOOL;
import static org.neo4j.configuration.SettingValueParsers.DURATION;
import static org.neo4j.configuration.SettingValueParsers.INT;

@SuppressWarnings( "WeakerAccess" )
@ServiceProvider
public class FabricEnterpriseInternalSettings implements SettingsDeclaration
{
    @Internal
    @Description( "Time interval of inactivity after which a driver will be closed." )
    public static final Setting<Duration> driver_idle_timeout = newBuilder( "fabric.driver.timeout", DURATION, ofMinutes( 1 ) ).build();

    @Internal
    @Description( "Time interval between driver idleness check." )
    public static final Setting<Duration> driver_idle_check_interval = newBuilder( "fabric.driver.idle_check_interval", DURATION, ofMinutes( 1 ) ).build();

    @Internal
    @Description( " Number of event loops used by drivers. Event loops are shard between drivers, so this is the total number of event loops created." )
    @DocumentedDefaultValue( "Number of available processors" )
    public static final Setting<Integer> driver_event_loop_count =
            newBuilder( "fabric.driver.event_loop_count", INT, Runtime.getRuntime().availableProcessors() ).build();

    @Internal
    @Description( "Enables logging of leaked driver session" )
    public static final Setting<Boolean> driver_log_leaked_sessions =
            newBuilder( "fabric." + FabricEnterpriseSettings.DRIVER_LOG_LEAKED_SESSIONS, BOOL, false ).build();
}
