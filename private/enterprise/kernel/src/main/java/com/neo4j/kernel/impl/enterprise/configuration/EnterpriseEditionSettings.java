/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.impl.enterprise.configuration;

import java.util.List;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.configuration.Description;
import org.neo4j.configuration.SettingsDeclaration;
import org.neo4j.graphdb.config.Setting;

import static org.neo4j.configuration.SettingConstraints.min;
import static org.neo4j.configuration.SettingImpl.newBuilder;
import static org.neo4j.configuration.SettingValueParsers.LONG;
import static org.neo4j.configuration.SettingValueParsers.STRING;
import static org.neo4j.configuration.SettingValueParsers.listOf;
import static org.neo4j.configuration.SettingValueParsers.ofEnum;

/**
 * Enterprise edition specific settings
 */
@ServiceProvider
public class EnterpriseEditionSettings implements SettingsDeclaration
{
    @Description( "The maximum number of databases." )
    public static final Setting<Long> maxNumberOfDatabases = newBuilder( "dbms.max_databases", LONG, 100L ).addConstraint( min( 2L ) ).build();

    @Description( "A list of setting name patterns (comma separated) that are allowed to be dynamically changed. " +
            "The list may contain both full setting names, and partial names with the wildcard '*'. " +
            "If this setting is left empty all dynamic settings updates will be blocked." )
    public static final Setting<List<String>> dynamic_setting_whitelist =
            newBuilder( "dbms.dynamic.setting.whitelist", listOf( STRING ), List.of( "*" ) ).build();
}
