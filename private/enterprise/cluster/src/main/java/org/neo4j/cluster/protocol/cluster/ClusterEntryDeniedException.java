/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cluster.protocol.cluster;

import org.neo4j.cluster.InstanceId;
import org.neo4j.cluster.protocol.cluster.ClusterMessage.ConfigurationResponseState;

public class ClusterEntryDeniedException extends IllegalStateException
{
    private final ConfigurationResponseState configurationResponseState;

    public ClusterEntryDeniedException( InstanceId me, ConfigurationResponseState configurationResponseState )
    {
        super( "I was denied entry. I am " + me + ", configuration response:" + configurationResponseState );
        this.configurationResponseState = configurationResponseState;
    }

    public ConfigurationResponseState getConfigurationResponseState()
    {
        return configurationResponseState;
    }
}
