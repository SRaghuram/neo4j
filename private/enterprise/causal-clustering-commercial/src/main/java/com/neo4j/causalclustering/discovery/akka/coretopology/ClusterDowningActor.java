/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.coretopology;

import akka.actor.AbstractActor;
import akka.actor.Props;
import akka.cluster.Cluster;
import akka.cluster.Member;
import akka.japi.pf.ReceiveBuilder;

import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

public class ClusterDowningActor extends AbstractActor
{
    public static Props props( Cluster cluster, LogProvider logProvider )
    {
        return Props.create( ClusterDowningActor.class, () -> new ClusterDowningActor( cluster, logProvider ) );
    }

    private final Cluster cluster;
    private final Log log;

    public ClusterDowningActor( Cluster cluster, LogProvider logProvider )
    {
        this.cluster = cluster;
        this.log = logProvider.getLog( getClass() );
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
            log.info( "Downing members: %s", clusterView.unreachable() );

            clusterView
                    .unreachable()
                    .stream()
                    .map( Member::address )
                    .forEach( cluster::down );
        }
        else
        {
            log.info( "In minority side of network partition? %s", clusterView );
        }
    }
}
