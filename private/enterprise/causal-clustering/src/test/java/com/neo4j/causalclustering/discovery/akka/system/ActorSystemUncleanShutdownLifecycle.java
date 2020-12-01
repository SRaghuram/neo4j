/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.system;

import com.neo4j.causalclustering.discovery.RemoteMembersResolver;
import com.neo4j.configuration.CausalClusteringInternalSettings;
import scala.concurrent.Await;
import scala.concurrent.duration.FiniteDuration;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import org.neo4j.configuration.Config;
import org.neo4j.logging.LogProvider;

/**
 * Terminate actor system without going through coordinated shutdown. Will not leave cluster.
 */
public class ActorSystemUncleanShutdownLifecycle extends ActorSystemLifecycle
{
    private final Duration shutdownTimeout;
    public ActorSystemUncleanShutdownLifecycle( ActorSystemFactory actorSystemFactory, RemoteMembersResolver resolver, Config config, LogProvider logProvider )
    {
        super( actorSystemFactory, resolver, new JoinMessageFactory( resolver ), config, logProvider );
        shutdownTimeout = config.get( CausalClusteringInternalSettings.akka_shutdown_timeout );
    }

    @Override
    public void doShutdown( ActorSystemComponents actorSystemComponents ) throws Exception
    {
        Await.result( actorSystemComponents.actorSystem().terminate(), new FiniteDuration( shutdownTimeout.toSeconds(), TimeUnit.SECONDS ) );
    }
}
