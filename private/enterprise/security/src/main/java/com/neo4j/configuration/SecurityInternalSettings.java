/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.configuration;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.configuration.Description;
import org.neo4j.configuration.Internal;
import org.neo4j.configuration.SettingsDeclaration;
import org.neo4j.graphdb.config.Setting;

import static org.neo4j.configuration.SettingImpl.newBuilder;
import static org.neo4j.configuration.SettingValueParsers.BOOL;
import static org.neo4j.configuration.SettingValueParsers.STRING;

@ServiceProvider
public class SecurityInternalSettings implements SettingsDeclaration
{
    @Internal
    @Description( "Set to true if connection pooling should be used for authorization searches using the system account." )
    public static final Setting<Boolean> ldap_authorization_connection_pooling =
            newBuilder( "unsupported.dbms.security.ldap.authorization.connection_pooling", BOOL, true ).build();

    @Internal
    @Description( "This has been replaced by privilege management on roles. Setting it to true will prevent the server from starting." )
    @Deprecated
    public static final Setting<Boolean> property_level_authorization_enabled =
            newBuilder( "dbms.security.property_level.enabled", BOOL, false ).build();

    @Internal
    @Description( "This can be achieved with `DENY READ {property} ON GRAPH * ELEMENTS * TO role`. " +
                  "Using this setting will prevent the server from starting." )
    @Deprecated
    public static final Setting<String> property_level_authorization_permissions =
            newBuilder( "dbms.security.property_level.blacklist", STRING, null ).build();
}
