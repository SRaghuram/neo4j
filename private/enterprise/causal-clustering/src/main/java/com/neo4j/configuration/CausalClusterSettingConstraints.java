/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.configuration;

import com.typesafe.config.ConfigException;
import com.typesafe.config.ConfigFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.stream.Collectors;

import org.neo4j.configuration.SettingConstraint;
import org.neo4j.configuration.SettingConstraints;
import org.neo4j.graphdb.config.Configuration;
import org.neo4j.graphdb.config.Setting;

import static com.neo4j.configuration.CausalClusteringInternalSettings.middleware_akka_external_config;
import static java.lang.String.format;
import static java.util.stream.Collectors.toList;

final class CausalClusterSettingConstraints
{
    private CausalClusterSettingConstraints()
    {
    }

    static SettingConstraint<Path> validateMiddlewareConfig()
    {
        return SettingConstraints.ifCluster( new SettingConstraint<>()
        {
            @Override
            public void validate( Path akkaConfigFile, Configuration config )
            {
                if ( akkaConfigFile != null )
                {
                    if ( Files.notExists( akkaConfigFile ) || !Files.isRegularFile( akkaConfigFile ) )
                    {
                        throw new IllegalArgumentException( format( "'%s' must be a file or empty", middleware_akka_external_config.name() ) );
                    }
                    try
                    {
                        ConfigFactory.parseFileAnySyntax( akkaConfigFile.toFile() );
                    }
                    catch ( ConfigException e )
                    {
                        throw new IllegalArgumentException( format( "'%s' could not be parsed", akkaConfigFile ), e );
                    }
                }
            }

            @Override
            public String getDescription()
            {
                return "must be a parsable file or empty";
            }
        } );
    }

    static SettingConstraint<DiscoveryType> validateInitialDiscoveryMembers()
    {
        return SettingConstraints.ifCluster( new SettingConstraint<>()
        {
            @Override
            public void validate( DiscoveryType discoveryType, Configuration config )
            {
                discoveryType.requiredSettings().forEach( setting ->
                {
                    Object requiredSetting;
                    try
                    {
                        requiredSetting = config.get( setting );
                    }
                    catch ( IllegalArgumentException e )
                    {
                        throwMissing( discoveryType, setting );
                        return;
                    }
                    if ( requiredSetting == null )
                    {
                        throwMissing( discoveryType, setting );
                    }
                } );
            }

            private void throwMissing( DiscoveryType discoveryType, Setting<?> setting )
            {
                throw new IllegalArgumentException( format( "Missing value for '%s', which is mandatory with '%s=%s'",
                        setting.name(), CausalClusteringSettings.discovery_type.name(), discoveryType ) );
            }

            @Override
            public String getDescription()
            {
                return "may require different settings depending on the discovery type: `" + Arrays.stream( DiscoveryType.values() )
                        .map( discoveryType -> discoveryType.name() + " requires " +
                                               discoveryType.requiredSettings().stream().map( Setting::name ).collect( toList() ) )
                        .collect( Collectors.joining( ", " ) ) + "`";
            }
        } );
    }
}
