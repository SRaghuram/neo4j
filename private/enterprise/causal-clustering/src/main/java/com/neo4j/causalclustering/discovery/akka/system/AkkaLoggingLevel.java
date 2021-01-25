/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.system;

import org.neo4j.logging.Level;

enum AkkaLoggingLevel
{
    DEBUG( Level.DEBUG ),
    INFO( Level.INFO ),
    WARNING( Level.WARN ),
    ERROR( Level.ERROR ),
    OFF( Level.NONE );

    private final Level neo4jLevel;

    AkkaLoggingLevel( Level neo4jLevel )
    {
        this.neo4jLevel = neo4jLevel;
    }

    static AkkaLoggingLevel fromNeo4jLevel( Level neo4jLevel )
    {
        for ( var akkaLevel : values() )
        {
            if ( akkaLevel.neo4jLevel == neo4jLevel )
            {
                return akkaLevel;
            }
        }
        throw new IllegalArgumentException( "Unknown Neo4j logging level: " + neo4jLevel );
    }

    public Level neo4jLevel()
    {
        return neo4jLevel;
    }
}
