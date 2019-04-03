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
import org.neo4j.kernel.lifecycle.SafeLifecycle;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static java.util.Collections.unmodifiableMap;

public abstract class AbstractCoreTopologyService extends SafeLifecycle implements CoreTopologyService
{
    protected final CoreTopologyListenerService listenerService = new CoreTopologyListenerService();
    protected final Config config;
    protected final MemberId myself;
    protected final Log log;
    protected final Log userLog;

    private final Map<String,LeaderInfo> localLeadersByDatabaseName = new ConcurrentHashMap<>();

    protected AbstractCoreTopologyService( Config config, MemberId myself, LogProvider logProvider, LogProvider userLogProvider )
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
        listener.onCoreTopologyChange( localCoreServers() );
    }

    @Override
    public final void removeLocalCoreTopologyListener( Listener listener )
    {
        listenerService.removeCoreTopologyListener( listener );
    }

    @Override
    public final void setLeader( LeaderInfo newLeader, String dbName )
    {
        LeaderInfo currentLeaderInfo = getLeader( dbName );

        if ( currentLeaderInfo.term() < newLeader.term() )
        {
            log.info( "Leader %s updating leader info for database %s and term %s", myself, dbName, newLeader.term() );
            localLeadersByDatabaseName.put( dbName, newLeader );
            setLeader0( newLeader, dbName );
        }
    }

    protected abstract void setLeader0( LeaderInfo newLeader, String dbName );

    @Override
    public final void handleStepDown( long term, String dbName )
    {
        LeaderInfo currentLeaderInfo = getLeader( dbName );

        boolean wasLeaderForTerm =
                Objects.equals( myself, currentLeaderInfo.memberId() ) &&
                term == currentLeaderInfo.term();

        if ( wasLeaderForTerm )
        {
            log.info( "Step down event detected. This topology member, with MemberId %s, was leader for database %s in term %s, now moving " +
                      "to follower.", myself, dbName, currentLeaderInfo.term() );
            localLeadersByDatabaseName.put( dbName, currentLeaderInfo.stepDown() );
            handleStepDown0( currentLeaderInfo.stepDown(), dbName );
        }
    }

    protected abstract void handleStepDown0( LeaderInfo steppingDown, String dbName );

    @Override
    public MemberId myself()
    {
        return myself;
    }

    final Map<String,LeaderInfo> getLocalLeadersByDatabaseName()
    {
        return unmodifiableMap( localLeadersByDatabaseName );
    }

    private LeaderInfo getLeader( String dbName )
    {
        return localLeadersByDatabaseName.getOrDefault( dbName, LeaderInfo.INITIAL );
    }
}
