/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.coretopology;

import akka.actor.AbstractLoggingActor;
import akka.actor.Props;
import akka.cluster.Cluster;
import akka.cluster.Member;
import akka.japi.pf.ReceiveBuilder;

public class ClusterDowningActor extends AbstractLoggingActor
{
    public static Props props( Cluster cluster )
    {
        return Props.create( ClusterDowningActor.class, () -> new ClusterDowningActor( cluster ) );
    }

    private final Cluster cluster;

    public ClusterDowningActor( Cluster cluster )
    {
        this.cluster = cluster;
    }

    @Override
    public Receive createReceive()
    {
        return ReceiveBuilder
                .create()
                .match( ClusterViewMessage.class, this::handle )
                .build();
    }

    private void handle( ClusterViewMessage clusterView )
    {
        if ( clusterView.mostAreReachable() )
        {
            log().info( "Downing members: {}", clusterView.unreachable() );

            clusterView
                    .unreachable()
                    .stream()
                    .map( Member::address )
                    .forEach( cluster::down );
        }
        else
        {
            log().info( "In minority side of network partition? {}", clusterView );
        }
    }
}
