/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka;

import com.neo4j.causalclustering.core.consensus.LeaderInfo;
import com.neo4j.causalclustering.discovery.RetryStrategy;
import com.neo4j.causalclustering.discovery.RoleInfo;
import com.neo4j.causalclustering.discovery.akka.directory.LeaderInfoSettingMessage;
import com.neo4j.causalclustering.discovery.akka.system.ActorSystemLifecycle;
import com.neo4j.causalclustering.discovery.member.ServerSnapshot;
import com.neo4j.causalclustering.discovery.member.ServerSnapshotFactory;
import com.neo4j.causalclustering.error_handling.Panicker;
import com.neo4j.causalclustering.identity.RaftMemberId;
import com.neo4j.dbms.EnterpriseOperatorState;

import java.time.Clock;
import java.util.stream.Collectors;

import org.neo4j.configuration.Config;
import org.neo4j.dbms.DatabaseState;
import org.neo4j.dbms.DatabaseStateService;
import org.neo4j.dbms.identity.ServerId;
import org.neo4j.dbms.identity.ServerIdentity;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.logging.LogProvider;
import org.neo4j.monitoring.Monitors;
import org.neo4j.scheduler.JobScheduler;

import static akka.actor.ActorRef.noSender;
import static java.util.function.Function.identity;

public class AkkaStandaloneTopologyService extends AkkaTopologyService
{
    private final LeaderInfo defaultLeader;

    public AkkaStandaloneTopologyService( Config config,
                                          ServerIdentity myIdentity,
                                          ActorSystemLifecycle actorSystemLifecycle,
                                          LogProvider logProvider,
                                          LogProvider userLogProvider,
                                          RetryStrategy catchupAddressRetryStrategy,
                                          ActorSystemRestarter actorSystemRestarter,
                                          ServerSnapshotFactory serverSnapshotFactory,
                                          JobScheduler jobScheduler,
                                          Clock clock,
                                          Monitors monitors,
                                          DatabaseStateService databaseStateService,
                                          Panicker panicker )
    {
        super( config,
               myIdentity,
               actorSystemLifecycle,
               logProvider,
               userLogProvider,
               catchupAddressRetryStrategy,
               actorSystemRestarter,
               serverSnapshotFactory,
               jobScheduler,
               clock,
               monitors,
               databaseStateService,
               panicker );
        this.defaultLeader = new LeaderInfo( new RaftMemberId( myIdentity.serverId().uuid() ), 1 );
    }

    @Override
    protected ServerSnapshot createServerSnapshot()
    {
        var collect = databaseStateService.stateOfAllDatabases().keySet().stream()
                                          .map( NamedDatabaseId::databaseId )
                                          .collect( Collectors.toMap( identity(), ignored -> defaultLeader ) );

        return serverSnapshotFactory.createSnapshot( databaseStateService, collect );
    }

    @Override
    public void stateChange( DatabaseState previousState, DatabaseState newState )
    {
        if ( newState.operatorState() == EnterpriseOperatorState.STARTED )
        {
            super.onDatabaseStart( newState.databaseId() );
            sendLeaderInfo( defaultLeader, newState.databaseId() );
        }
        if ( newState.operatorState() == EnterpriseOperatorState.STOPPED )
        {
            sendLeaderInfo( LeaderInfo.INITIAL, newState.databaseId() );
            super.onDatabaseStop( newState.databaseId() );
        }
        super.stateChange( previousState, newState );
    }

    @Override
    public RoleInfo lookupRole( NamedDatabaseId namedDatabaseId, ServerId serverId )
    {
        return globalTopologyState.role( namedDatabaseId, serverId );
    }

    @Override
    protected void removeLeaderInfo( NamedDatabaseId namedDatabaseId )
    {
        // no-op
    }

    private void sendLeaderInfo( LeaderInfo leaderInfo, NamedDatabaseId namedDatabaseId )
    {
        var directoryActor = directoryActorRef;
        if ( directoryActor != null )
        {
            directoryActor.tell( new LeaderInfoSettingMessage( leaderInfo, namedDatabaseId ), noSender() );
        }
    }
}
