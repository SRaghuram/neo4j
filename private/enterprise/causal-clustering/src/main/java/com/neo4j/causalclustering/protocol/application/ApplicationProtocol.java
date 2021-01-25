/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.protocol.application;

import com.neo4j.causalclustering.protocol.Protocol;
import com.neo4j.configuration.ApplicationProtocolVersion;

public interface ApplicationProtocol extends Protocol<ApplicationProtocolVersion>
{
    default boolean lessOrEquals( ApplicationProtocol applicationProtocol )
    {
        if ( !this.category().equals( applicationProtocol.category() ) )
        {
            throw new IllegalArgumentException( "Can't compare protocol with different category" );
        }
        else
        {
            return this.implementation().compareTo( applicationProtocol.implementation() ) <= 0;
        }
    }
}
