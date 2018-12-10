/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery;

import com.neo4j.causalclustering.core.consensus.LeaderInfo;
import com.neo4j.causalclustering.identity.MemberId;

import java.util.Objects;

import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.lifecycle.SafeLifecycle;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

public abstract class AbstractCoreTopologyService extends SafeLifecycle implements CoreTopologyService
{
    protected final CoreTopologyListenerService listenerService = new CoreTopologyListenerService();
    protected final Config config;
    protected final MemberId myself;
    protected final Log log;
    protected final Log userLog;

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
        LeaderInfo currentLeaderInfo = getLeader();

        if ( currentLeaderInfo.term() < newLeader.term() && localDBName().equals( dbName ) )
        {
            log.info( "Leader %s updating leader info for database %s and term %s", myself, dbName, newLeader.term() );
            setLeader0( newLeader );
        }
    }

    protected abstract void setLeader0( LeaderInfo newLeader );

    @Override
    public final void handleStepDown( long term, String dbName )
    {
        LeaderInfo localLeaderInfo = getLeader();

        boolean wasLeaderForDbAndTerm =
                Objects.equals( myself, localLeaderInfo.memberId() ) &&
                localDBName().equals( dbName ) &&
                term == localLeaderInfo.term();

        if ( wasLeaderForDbAndTerm )
        {
            log.info( "Step down event detected. This topology member, with MemberId %s, was leader in term %s, now moving " +
                    "to follower.", myself, localLeaderInfo.term() );
            handleStepDown0( localLeaderInfo.stepDown() );
        }
    }

    protected abstract void handleStepDown0( LeaderInfo steppingDown );

    @Override
    public MemberId myself()
    {
        return myself;
    }
}
