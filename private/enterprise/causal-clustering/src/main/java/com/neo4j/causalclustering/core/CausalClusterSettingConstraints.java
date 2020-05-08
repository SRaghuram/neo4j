/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core;

import com.typesafe.config.ConfigException;
import com.typesafe.config.ConfigFactory;

import java.io.File;
import java.nio.file.Path;

import org.neo4j.configuration.SettingConstraint;
import org.neo4j.configuration.SettingConstraints;
import org.neo4j.graphdb.config.Configuration;
import org.neo4j.graphdb.config.Setting;

import static com.neo4j.causalclustering.core.CausalClusteringSettings.middleware_akka_external_config;
import static java.lang.String.format;

class CausalClusterSettingConstraints
{
    private CausalClusterSettingConstraints()
    {
    }

    static SettingConstraint<Path> validateMiddlewareConfig()
    {
        return SettingConstraints.ifCluster( new SettingConstraint<>()
        {
            @Override
            public void validate( Path value, Configuration config )
            {
                if ( value != null )
                {
                    File akkaConfigFile = value.toFile();

                    if ( !akkaConfigFile.exists() || !akkaConfigFile.isFile() )
                    {
                        throw new IllegalArgumentException( format( "'%s' must be a file or empty", middleware_akka_external_config.name() ) );
                    }
                    try
                    {
                        ConfigFactory.parseFileAnySyntax( akkaConfigFile );
                    }
                    catch ( ConfigException e )
                    {
                        throw new IllegalArgumentException( format( "'%s' could not be parsed", value ), e );
                    }
                }
            }

            @Override
            public String getDescription()
            {
                return "must be parsable file or empty";
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
                return "Different discovery types requires may require different settings.";
            }
        } );
    }
}
