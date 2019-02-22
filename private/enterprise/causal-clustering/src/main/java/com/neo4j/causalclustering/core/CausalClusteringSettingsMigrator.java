/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core;

import java.util.Map;

import org.neo4j.configuration.BaseConfigurationMigrator;
import org.neo4j.configuration.GraphDatabaseSettings;

class CausalClusteringSettingsMigrator extends BaseConfigurationMigrator
{
    CausalClusteringSettingsMigrator()
    {
        add( new RoutingTtlMigrator() );
    }

    private static class RoutingTtlMigrator extends SpecificPropertyMigration
    {
        static final String OLD_SETTING = "causal_clustering.cluster_routing_ttl";
        static final String NEW_SETTING = GraphDatabaseSettings.routing_ttl.name();

        RoutingTtlMigrator()
        {
            super( OLD_SETTING, OLD_SETTING + " has been replaced with " + NEW_SETTING + "." );
        }

        @Override
        public void setValueWithOldSetting( String value, Map<String,String> rawConfiguration )
        {
            if ( value != null && value.length() != 0 )
            {
                rawConfiguration.put( NEW_SETTING, value );
            }
        }
    }
}
