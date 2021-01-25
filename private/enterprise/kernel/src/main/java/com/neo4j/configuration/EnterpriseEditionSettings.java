/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.configuration;

import java.util.List;

import org.neo4j.annotations.api.PublicApi;
import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.configuration.Description;
import org.neo4j.configuration.SettingsDeclaration;
import org.neo4j.graphdb.config.Setting;

import static org.neo4j.configuration.SettingConstraints.min;
import static org.neo4j.configuration.SettingImpl.newBuilder;
import static org.neo4j.configuration.SettingValueParsers.BOOL;
import static org.neo4j.configuration.SettingValueParsers.LONG;
import static org.neo4j.configuration.SettingValueParsers.STRING;
import static org.neo4j.configuration.SettingValueParsers.listOf;

/**
 * Enterprise edition specific settings
 */
@ServiceProvider
@PublicApi
public class EnterpriseEditionSettings implements SettingsDeclaration
{
    @Description( "The maximum number of databases." )
    public static final Setting<Long> max_number_of_databases = newBuilder( "dbms.max_databases", LONG, 100L ).addConstraint( min( 2L ) ).build();

    @Deprecated( since = "4.2.0", forRemoval = true )
    @Description( "A list of setting name patterns (comma separated) that are allowed to be dynamically changed. " +
            "The list may contain both full setting names, and partial names with the wildcard '*'. " +
            "If this setting is left empty all dynamic settings updates will be blocked. " +
            "Deprecated, use dbms.dynamic.setting.allowlist" )
    public static final Setting<List<String>> dynamic_setting_whitelist =
            newBuilder( "dbms.dynamic.setting.whitelist", listOf( STRING ), List.of( "*" ) ).build();

    @Description( "A list of setting name patterns (comma separated) that are allowed to be dynamically changed. " +
            "The list may contain both full setting names, and partial names with the wildcard '*'. " +
            "If this setting is left empty all dynamic settings updates will be blocked." )
    public static final Setting<List<String>> dynamic_setting_allowlist =
            newBuilder( "dbms.dynamic.setting.allowlist", listOf( STRING ), List.of( "*" ) ).build();

    @Description( "If there is a Database Management System Panic (an irrecoverable error) should the neo4j process shut down or continue running. " +
                  "Following a DbMS panic it is likely that a significant amount of functionality will be lost. " +
                  "Recovering full functionality will require a Neo4j restart"
    )
    public static final Setting<Boolean> shutdown_on_dbms_panic =
            newBuilder( "dbms.panic.shutdown_on_panic", BOOL, false ).build();
}
