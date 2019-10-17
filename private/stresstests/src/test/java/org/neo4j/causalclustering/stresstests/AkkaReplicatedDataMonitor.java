/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.stresstests;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

import org.neo4j.causalclustering.common.ClusterMember;
import org.neo4j.causalclustering.discovery.akka.monitoring.ReplicatedDataIdentifier;
import org.neo4j.causalclustering.discovery.akka.monitoring.ReplicatedDataMonitor;
import org.neo4j.logging.Log;

class AkkaReplicatedDataMonitor implements ReplicatedDataMonitor, AutoCloseable
{
    private final ClusterMember member;
    private final int alertLevel;
    private final Log log;

    private final Map<ReplicatedDataIdentifier,Integer> visibleSizes = new ConcurrentHashMap<>();
    private final Map<ReplicatedDataIdentifier,Integer> invisibleSizes = new ConcurrentHashMap<>();

    static AkkaReplicatedDataMonitor install( ClusterMember member, int alertLevel, Log log )
    {
        AkkaReplicatedDataMonitor monitor = new AkkaReplicatedDataMonitor( member, alertLevel, log );
        member.monitors().addMonitorListener( monitor );
        return monitor;
    }

    private AkkaReplicatedDataMonitor( ClusterMember member, int alertLevel, Log log )
    {
        this.member = member;
        this.log = log;
        this.alertLevel = alertLevel;
    }

    @Override
    public void setVisibleDataSize( ReplicatedDataIdentifier key, int size )
    {
        visibleSizes.put( key, size );
        if ( size >= alertLevel )
        {
            log.warn( member + " akka:" + key.keyName() + " has visible size: " + size );
        }
    }

    @Override
    public void setInvisibleDataSize( ReplicatedDataIdentifier key, int size )
    {
        invisibleSizes.put( key, size );
        if ( size >= alertLevel )
        {
            log.warn( member + " akka:" + key.keyName() + " has invisible size: " + size );
        }
    }

    void dump()
    {
        for ( Map.Entry<ReplicatedDataIdentifier,Integer> entry : visibleSizes.entrySet() )
        {
            log.info( member + " akka:" + entry.getKey().keyName() + " has visible size: " + entry.getValue() );
        }
        for ( Map.Entry<ReplicatedDataIdentifier,Integer> entry : invisibleSizes.entrySet() )
        {
            log.info( member + " akka:" + entry.getKey().keyName() + " has invisible size: " + entry.getValue() );
        }
    }

    int maxSize()
    {
        return Stream
                .concat( visibleSizes.values().stream(), invisibleSizes.values().stream() )
                .max( Integer::compareTo )
                .orElseThrow( IllegalStateException::new );
    }

    @Override
    public void close()
    {
        member.monitors().removeMonitorListener( this );
        visibleSizes.clear();
        invisibleSizes.clear();
    }
}
