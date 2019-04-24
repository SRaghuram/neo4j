/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery;

import com.neo4j.causalclustering.core.consensus.LeaderInfo;
import com.neo4j.causalclustering.identity.MemberId;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import org.neo4j.configuration.Config;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.lifecycle.SafeLifecycle;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static java.util.Collections.unmodifiableMap;

public abstract class AbstractCoreTopologyService extends SafeLifecycle implements CoreTopologyService
{
    protected final CoreTopologyListenerService listenerService = new CoreTopologyListenerService();
    protected final Config config;
    protected final DiscoveryMember myself;
    protected final Log log;
    protected final Log userLog;

    private final Map<DatabaseId,LeaderInfo> localLeadersByDatabaseName = new ConcurrentHashMap<>();

    protected AbstractCoreTopologyService( Config config, DiscoveryMember myself, LogProvider logProvider, LogProvider userLogProvider )
    {
        this.config = config;
        this.myself = myself;
        this.log = logProvider.getLog( getClass() );
        this.userLog = userLogProvider.getLog( getClass() );
    }

    @Override
    public final synchronized void addLocalCoreTopologyListener( Listener listener )
    {
        this.listenerService.addCoreTopologyListener( listener );
        listener.onCoreTopologyChange( coreTopologyForDatabase( listener.databaseId() ) );
    }

    @Override
    public final void removeLocalCoreTopologyListener( Listener listener )
    {
        listenerService.removeCoreTopologyListener( listener );
    }

    @Override
    public final void setLeader( LeaderInfo newLeader, DatabaseId databaseId )
    {
        LeaderInfo currentLeaderInfo = getLeader( databaseId );

        if ( currentLeaderInfo.term() < newLeader.term() )
        {
            log.info( "Leader %s updating leader info for database %s and term %s", memberId(), databaseId.name(), newLeader.term() );
            localLeadersByDatabaseName.put( databaseId, newLeader );
            setLeader0( newLeader, databaseId );
        }
    }

    protected abstract void setLeader0( LeaderInfo newLeader, DatabaseId databaseId );

    @Override
    public final void handleStepDown( long term, DatabaseId databaseId )
    {
        LeaderInfo currentLeaderInfo = getLeader( databaseId );

        boolean wasLeaderForTerm =
                Objects.equals( memberId(), currentLeaderInfo.memberId() ) &&
                term == currentLeaderInfo.term();

        if ( wasLeaderForTerm )
        {
            log.info( "Step down event detected. This topology member, with MemberId %s, was leader for database %s in term %s, now moving " +
                      "to follower.", memberId(), databaseId.name(), currentLeaderInfo.term() );
            localLeadersByDatabaseName.put( databaseId, currentLeaderInfo.stepDown() );
            handleStepDown0( currentLeaderInfo.stepDown(), databaseId );
        }
    }

    protected abstract void handleStepDown0( LeaderInfo steppingDown, DatabaseId databaseId );

    @Override
    public MemberId memberId()
    {
        return myself.id();
    }

    final Map<DatabaseId,LeaderInfo> getLocalLeadersByDatabaseName()
    {
        return unmodifiableMap( localLeadersByDatabaseName );
    }

    private LeaderInfo getLeader( DatabaseId databaseId )
    {
        return localLeadersByDatabaseName.getOrDefault( databaseId, LeaderInfo.INITIAL );
    }
}
