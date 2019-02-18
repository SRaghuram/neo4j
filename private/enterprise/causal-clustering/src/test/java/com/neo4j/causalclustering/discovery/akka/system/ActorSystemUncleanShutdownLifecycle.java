/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.system;

import com.neo4j.causalclustering.discovery.RemoteMembersResolver;
import scala.concurrent.Await;
import scala.concurrent.duration.FiniteDuration;

import java.util.concurrent.TimeUnit;

import org.neo4j.configuration.Config;
import org.neo4j.logging.LogProvider;

/**
 * Terminate actor system without going through coordinated shutdown. Will not leave cluster.
 */
public class ActorSystemUncleanShutdownLifecycle extends ActorSystemLifecycle
{
    public ActorSystemUncleanShutdownLifecycle( ActorSystemFactory actorSystemFactory, RemoteMembersResolver resolver, Config config, LogProvider logProvider )
    {
        super( actorSystemFactory, resolver, new JoinMessageFactory( resolver ), config, logProvider );
    }

    @Override
    public void doShutdown( ActorSystemComponents actorSystemComponents ) throws Exception
    {
        Await.result( actorSystemComponents.actorSystem().terminate(), new FiniteDuration( SYSTEM_SHUTDOWN_TIMEOUT_S, TimeUnit.SECONDS ) );
    }
}
