/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core;

import org.neo4j.configuration.GroupSetting;

public abstract class LoadBalancingPluginGroup extends GroupSetting
{
    private final String pluginName;

    protected LoadBalancingPluginGroup( String name, String pluginName )
    {
        super( name );
        this.pluginName = pluginName;
    }

    @Override
    public String getPrefix()
    {
        return "causal_clustering.load_balancing.config." + pluginName;
    }
}
