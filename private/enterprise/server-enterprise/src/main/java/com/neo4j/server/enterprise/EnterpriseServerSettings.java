/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.enterprise;

import java.time.Duration;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.configuration.Description;
import org.neo4j.configuration.SettingsDeclaration;
import org.neo4j.graphdb.config.Setting;

import static org.neo4j.configuration.SettingImpl.newBuilder;
import static org.neo4j.configuration.SettingValueParsers.BOOL;
import static org.neo4j.configuration.SettingValueParsers.DURATION;

@Description( "Settings available in the Enterprise server" )
@ServiceProvider
public class EnterpriseServerSettings implements SettingsDeclaration
{
    @SuppressWarnings( "unused" ) // accessed from the browser
    @Description( "Configure the Neo4j Browser to time out logged in users after this idle period. " +
            "Setting this to 0 indicates no limit." )
    public static final Setting<Duration> browser_credential_timeout =
            newBuilder( "browser.credential_timeout", DURATION, Duration.ZERO  ).build();

    @SuppressWarnings( "unused" ) // accessed from the browser
    @Description( "Configure the Neo4j Browser to store or not store user credentials." )
    public static final Setting<Boolean> browser_retain_connection_credentials =
            newBuilder( "browser.retain_connection_credentials", BOOL,  true  ).build();

    @SuppressWarnings( "unused" ) // accessed from the browser
    @Description( "Configure the policy for outgoing Neo4j Browser connections." )
    public static final Setting<Boolean> browser_allow_outgoing_browser_connections =
            newBuilder( "browser.allow_outgoing_connections", BOOL,  true  ).build();
}
