/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.system;

import akka.actor.ActorSystem;
import com.typesafe.config.ConfigFactory;

import java.util.HashMap;

public class TestActorSystem
{

    public static ActorSystem withDefaults( String name )
    {
        HashMap<String,Object> configMap = new HashMap<>();
        configMap.put( "akka.logger-startup-timeout", "15s" );
        var overrideConfig = ConfigFactory.parseMap( configMap );

        var finalConfig = ConfigFactory.defaultReference()
                                         .withFallback( overrideConfig );

        return ActorSystem.create( name, finalConfig );
    }
}
