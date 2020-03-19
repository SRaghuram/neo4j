/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.leader_transfer;

import com.neo4j.causalclustering.catchup.CatchupComponentsRepository;
import com.neo4j.causalclustering.common.ClusteredDatabase;
import com.neo4j.causalclustering.core.CausalClusteringSettings;
import com.neo4j.causalclustering.core.consensus.LeaderListener;
import com.neo4j.causalclustering.core.consensus.LeaderLocator;
import com.neo4j.causalclustering.core.consensus.RaftMessages;
import com.neo4j.causalclustering.discovery.FakeTopologyService;
import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.causalclustering.messaging.Inbound;
import com.neo4j.dbms.database.ClusteredDatabaseContext;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.neo4j.configuration.Config;
import org.neo4j.dbms.api.DatabaseManagementException;
import org.neo4j.dbms.api.DatabaseNotFoundException;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.database.DatabaseIdRepository;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.monitoring.Monitors;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.time.Clocks;

import static java.util.UUID.randomUUID;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.kernel.database.TestDatabaseIdRepository.randomNamedDatabaseId;

class TransferLeaderTest
{
    private final MemberId myself = new MemberId( randomUUID() );
    private final MemberId core1 = new MemberId( randomUUID() );
    private final Set<NamedDatabaseId> databaseIds = Set.of( randomNamedDatabaseId() );
    private final FakeTopologyService fakeTopologyService =
            new FakeTopologyService( Set.of( myself, core1 ), Set.of(), myself, databaseIds );
    private final Config config = Config.defaults();
    private final TrackingMessageHandler messageHandler = new TrackingMessageHandler();
    private final StubDatabaseManager databaseManager = new StubDatabaseManager( databaseIds );
    private final DatabasePenalties databasePenalties = new DatabasePenalties( 1, TimeUnit.MILLISECONDS, Clocks.fakeClock() );

    @Test
    void shouldChooseToTransferIfIAmNotInPriority()
    {
        TransferLeader transferLeader =
                new TransferLeader( fakeTopologyService, config, databaseManager, messageHandler, myself, databasePenalties, new RandomStrategy() );
        var databaseId = databaseIds.iterator().next();
        // I am leader
        databaseManager.setLeaderFor( databaseId, myself );
        // Priority group exist and I am not in it
        config.set( CausalClusteringSettings.leadership_priority_groups, List.of( "prio" ) );

        // when
        transferLeader.run();

        // then
        assertEquals( messageHandler.proposals.size(), 1 );
        var propose = messageHandler.proposals.get( 0 );
        assertEquals( propose.raftId().uuid(), databaseId.databaseId().uuid() );
        assertEquals( propose.message().proposed(), core1 );
    }

    @Test
    void shouldChooseToNotTransferLeaderIfIamNotLeader()
    {
        TransferLeader transferLeader =
                new TransferLeader( fakeTopologyService, config, databaseManager, messageHandler, myself, databasePenalties,
                                    ( validTopologies, myself ) -> null );
        var databaseId = databaseIds.iterator().next();
        // I am not leader
        databaseManager.setLeaderFor( databaseId, null );
        // Priority group exist and I am not in it
        config.set( CausalClusteringSettings.leadership_priority_groups, List.of( "prio" ) );

        // when
        transferLeader.run();

        // then
        assertTrue( messageHandler.proposals.isEmpty() );
    }

    @Test
    void shouldChooseToNotTransferLeaderIfIamLeaderAndInPrioritisedGroup()
    {
        TransferLeader transferLeader =
                new TransferLeader( fakeTopologyService, config, databaseManager, messageHandler, myself, databasePenalties,
                                    ( validTopologies, myself ) -> null );
        var databaseId = databaseIds.iterator().next();
        // I am leader
        databaseManager.setLeaderFor( databaseId, myself );
        // Priority group exist and I am not in it
        config.set( CausalClusteringSettings.leadership_priority_groups, List.of( "prio" ) );
        // I am of group prio
        config.set( CausalClusteringSettings.server_groups, List.of( "prio" ) );

        // when
        transferLeader.run();

        // then
        assertTrue( messageHandler.proposals.isEmpty() );
    }

    // TODO add more tests!

    private static class StubDatabaseManager implements DatabaseManager<ClusteredDatabaseContext>
    {

        private final Map<NamedDatabaseId,ControllableLeaderLocator> databases;

        StubDatabaseManager( Set<NamedDatabaseId> databaseIds )
        {
            databases = databaseIds.stream().collect( Collectors.toMap( d -> d, d -> new ControllableLeaderLocator() ) );
        }

        void setLeaderFor( NamedDatabaseId databaseId, MemberId leader )
        {
            databases.get( databaseId ).setLeader( leader );
        }

        @Override
        public Optional<ClusteredDatabaseContext> getDatabaseContext( NamedDatabaseId namedDatabaseId )
        {
            return Optional.empty();
        }

        @Override
        public ClusteredDatabaseContext createDatabase( NamedDatabaseId namedDatabaseId ) throws DatabaseManagementException
        {
            return null;
        }

        @Override
        public void dropDatabase( NamedDatabaseId namedDatabaseId ) throws DatabaseNotFoundException
        {

        }

        @Override
        public void stopDatabase( NamedDatabaseId namedDatabaseId ) throws DatabaseNotFoundException
        {

        }

        @Override
        public void startDatabase( NamedDatabaseId namedDatabaseId ) throws DatabaseNotFoundException
        {

        }

        @Override
        public SortedMap<NamedDatabaseId,ClusteredDatabaseContext> registeredDatabases()
        {
            return new TreeMap<>( databases.entrySet().stream()
                                          .collect( Collectors.toMap( Map.Entry::getKey,
                                                                      entry -> new TransferLeaderTest.StubClusteredDatabaseContext( entry.getValue(),
                                                                                                                                    entry.getKey() ) ) ) );
        }

        @Override
        public DatabaseIdRepository.Caching databaseIdRepository()
        {
            return null;
        }

        @Override
        public void init()
        {

        }

        @Override
        public void start()
        {

        }

        @Override
        public void stop()
        {

        }

        @Override
        public void shutdown()
        {

        }
    }

    private static class ControllableLeaderLocator implements LeaderLocator
    {
        private MemberId leader;

        void setLeader( MemberId memberId )
        {
            leader = memberId;
        }

        @Override
        public MemberId getLeader()
        {
            return leader;
        }

        @Override
        public void registerListener( LeaderListener listener )
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void unregisterListener( LeaderListener listener )
        {
            throw new UnsupportedOperationException();
        }
    }

    private static class TrackingMessageHandler implements Inbound.MessageHandler<RaftMessages.ReceivedInstantRaftIdAwareMessage<?>>
    {
        private final ArrayList<RaftMessages.RaftIdAwareMessage<RaftMessages.LeadershipTransfer.Proposal>> proposals = new ArrayList<>();

        @Override
        public void handle( RaftMessages.ReceivedInstantRaftIdAwareMessage<?> message )
        {
            if ( message.message() instanceof RaftMessages.LeadershipTransfer.Proposal )
            {
                proposals.add( (RaftMessages.ReceivedInstantRaftIdAwareMessage<RaftMessages.LeadershipTransfer.Proposal>) message );
            }
            else
            {
                throw new IllegalArgumentException( "Unexpected message type " + message );
            }
        }
    }

    private static class StubClusteredDatabaseContext implements ClusteredDatabaseContext
    {
        private LeaderLocator leaderLocator;
        private NamedDatabaseId databaseId;

        StubClusteredDatabaseContext( LeaderLocator leaderLocator, NamedDatabaseId databaseId )
        {
            this.leaderLocator = leaderLocator;
            this.databaseId = databaseId;
        }

        @Override
        public StoreId storeId()
        {
            return null;
        }

        @Override
        public Monitors monitors()
        {
            return null;
        }

        @Override
        public void delete()
        {

        }

        @Override
        public boolean isEmpty()
        {
            return false;
        }

        @Override
        public DatabaseLayout databaseLayout()
        {
            return null;
        }

        @Override
        public void replaceWith( File sourceDir )
        {

        }

        @Override
        public NamedDatabaseId databaseId()
        {
            return databaseId;
        }

        @Override
        public CatchupComponentsRepository.CatchupComponents catchupComponents()
        {
            return null;
        }

        @Override
        public ClusteredDatabase clusteredDatabase()
        {
            return null;
        }

        @Override
        public Optional<LeaderLocator> leaderLocator()
        {
            return Optional.of( leaderLocator );
        }

        @Override
        public Database database()
        {
            return null;
        }

        @Override
        public GraphDatabaseFacade databaseFacade()
        {
            return null;
        }
    }
}
