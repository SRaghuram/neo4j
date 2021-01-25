/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.system;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.CoordinatedShutdown;
import akka.actor.ProviderSelection;
import akka.cluster.Cluster;
import akka.cluster.client.ClusterClientReceptionist;
import akka.cluster.ddata.DistributedData;
import akka.stream.ActorMaterializer;
import akka.stream.ActorMaterializerSettings;

public class ActorSystemComponents
{
    private final ActorSystem actorSystem;

    private Cluster cluster;
    private ActorRef replicator;
    private ActorMaterializer materializer;
    private ClusterClientReceptionist clusterClientReceptionist;
    private CoordinatedShutdown coordinatedShutdown;

    public ActorSystemComponents( ActorSystemFactory actorSystemFactory, ProviderSelection providerSelection )
    {
        this.actorSystem = actorSystemFactory.createActorSystem( providerSelection );
        this.coordinatedShutdown = CoordinatedShutdown.get( actorSystem );
    }

    public ActorSystem actorSystem()
    {
        return actorSystem;
    }

    public Cluster cluster()
    {
        if ( cluster == null )
        {
            cluster = Cluster.get( actorSystem );
        }
        return cluster;
    }

    public ActorRef replicator()
    {
        if ( replicator == null )
        {
            replicator = DistributedData.get( actorSystem ).replicator();
        }
        return replicator;
    }

    ClusterClientReceptionist clusterClientReceptionist()
    {
        if ( clusterClientReceptionist == null )
        {
            clusterClientReceptionist = ClusterClientReceptionist.get( actorSystem );
        }
        return clusterClientReceptionist;
    }

    ActorMaterializer materializer()
    {
        if ( materializer == null )
        {
            ActorMaterializerSettings settings = ActorMaterializerSettings.create( actorSystem )
                    .withDispatcher( TypesafeConfigService.DISCOVERY_SINK_DISPATCHER );
            materializer = ActorMaterializer.create( settings, actorSystem );
        }
        return materializer;
    }

    public CoordinatedShutdown coordinatedShutdown()
    {
        return coordinatedShutdown;
    }
}
