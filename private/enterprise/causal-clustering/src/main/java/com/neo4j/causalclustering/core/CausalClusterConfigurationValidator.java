/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core;

import com.neo4j.causalclustering.routing.load_balancing.LoadBalancingPluginLoader;
import com.typesafe.config.ConfigException;
import com.typesafe.config.ConfigFactory;

import java.io.File;
import java.nio.file.Path;
import java.util.Map;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.GraphDatabaseSettings.Mode;
import org.neo4j.configuration.GroupSettingValidator;
import org.neo4j.configuration.SettingConstraint;
import org.neo4j.graphdb.config.Configuration;
import org.neo4j.graphdb.config.Setting;

import static com.neo4j.causalclustering.core.CausalClusteringSettings.middleware_akka_external_config;
import static java.lang.String.format;

public class CausalClusterConfigurationValidator implements GroupSettingValidator
{
    @Override
    public String getPrefix()
    {
        return "causal_clustering";
    }

    @Override
    public String getDescription()
    {
        return "Validates causal clustering settings";
    }

    @Override
    public void validate( Map<Setting<?>,Object> values, Config config )
    {
        Mode mode = config.get( GraphDatabaseSettings.mode );
        if ( mode.equals( GraphDatabaseSettings.Mode.CORE ) || mode.equals( GraphDatabaseSettings.Mode.READ_REPLICA ) )
        {
            LoadBalancingPluginLoader.validate( config, null );
        }
    }
}
