/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus;

import com.neo4j.causalclustering.core.consensus.log.InMemoryRaftLog;
import com.neo4j.causalclustering.core.consensus.log.RaftLog;
import com.neo4j.causalclustering.core.consensus.log.RaftLogEntry;
import com.neo4j.causalclustering.core.consensus.membership.MemberIdSet;
import com.neo4j.causalclustering.core.consensus.membership.MembershipEntry;
import com.neo4j.causalclustering.core.consensus.roles.Role;
import com.neo4j.causalclustering.core.consensus.schedule.OnDemandTimerService;
import com.neo4j.causalclustering.core.consensus.schedule.TimerService;
import com.neo4j.causalclustering.core.state.snapshot.RaftCoreState;
import com.neo4j.causalclustering.identity.CoreServerIdentity;
import com.neo4j.causalclustering.identity.RaftMemberId;
import com.neo4j.causalclustering.identity.RaftTestMemberSetBuilder;
import com.neo4j.causalclustering.logging.BetterRaftMessageLogger;
import com.neo4j.causalclustering.messaging.LoggingInbound;
import com.neo4j.causalclustering.messaging.LoggingOutbound;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.UncheckedIOException;
import java.time.Clock;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.neo4j.configuration.helpers.NormalizedDatabaseName;
import org.neo4j.dbms.identity.ServerId;
import org.neo4j.function.Suppliers.Lazy;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.database.DatabaseIdRepository;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.time.Clocks;
import org.neo4j.time.FakeClock;

import static java.lang.String.format;
import static org.neo4j.function.Suppliers.lazySingleton;
import static org.neo4j.internal.helpers.collection.Iterators.asSet;

public class RaftTestFixture
{
    private Members members = new Members();
    // Does not need to be closed
    private StringWriter writer = new StringWriter();
    private NamedDatabaseId namedDatabaseId = DatabaseIdRepository.NAMED_SYSTEM_DATABASE_ID;

    public RaftTestFixture( DirectNetworking net, int expectedClusterSize, RaftMemberId... ids )
    {
        for ( RaftMemberId id : ids )
        {
            MemberFixture fixtureMember = new MemberFixture();

            FakeClock clock = Clocks.fakeClock();
            fixtureMember.timerService = new OnDemandTimerService( clock );

            fixtureMember.raftLog = new InMemoryRaftLog();
            fixtureMember.member = id;

            RaftTestFixtureLogger raftMessageLogger = new RaftTestFixtureLogger( id, new PrintWriter( writer ) );
            DatabaseIdRepository fakeDatabaseIdRepository = new DatabaseIdRepository()
            {
                @Override
                public Optional<NamedDatabaseId> getByName( NormalizedDatabaseName databaseName )
                {
                    return Optional.of( namedDatabaseId );
                }

                @Override
                public Optional<NamedDatabaseId> getById( DatabaseId databaseId )
                {
                    return Optional.of( namedDatabaseId );
                }
            };

            var inbound = new LoggingInbound( net.new Inbound( fixtureMember.member ), raftMessageLogger, coreIdentity( id ), fakeDatabaseIdRepository );
            var outbound = new LoggingOutbound<>( net.new Outbound( id ), namedDatabaseId, fixtureMember.lazyMember(), raftMessageLogger );

            fixtureMember.raftMachine = new RaftMachineBuilder( fixtureMember.member, expectedClusterSize, RaftTestMemberSetBuilder.INSTANCE, clock )
                    .inbound( inbound )
                    .outbound( outbound )
                    .raftLog( fixtureMember.raftLog )
                    .timerService( fixtureMember.timerService )
                    .build();

            members.put( fixtureMember );
        }
    }

    private CoreServerIdentity coreIdentity( RaftMemberId id )
    {
        return new CoreServerIdentity()
        {
            @Override
            public RaftMemberId raftMemberId( DatabaseId databaseId )
            {
                return id;
            }

            @Override
            public RaftMemberId raftMemberId( NamedDatabaseId namedDatabaseId )
            {
                return id;
            }

            @Override
            public void createMemberId( NamedDatabaseId databaseId, RaftMemberId raftMemberId )
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public RaftMemberId loadMemberId( NamedDatabaseId databaseId )
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public ServerId serverId()
            {
                throw new UnsupportedOperationException();
            }
        };
    }

    public Members members()
    {
        return members;
    }

    public void bootstrap( RaftMemberId[] members ) throws IOException
    {
        for ( MemberFixture member : members() )
        {
            member.raftLog().append( new RaftLogEntry( 0, new MemberIdSet( asSet( members ) ) ) );
            member.raftInstance().installCoreState( new RaftCoreState( new MembershipEntry( 0, asSet( members ) ) ) );
            member.raftInstance().postRecoveryActions();
        }
    }

    public String messageLog()
    {
        return writer.toString();
    }

    public static class Members implements Iterable<MemberFixture>
    {
        private Map<RaftMemberId,MemberFixture> memberMap = new HashMap<>();

        private MemberFixture put( MemberFixture value )
        {
            return memberMap.put( value.member, value );
        }

        public MemberFixture withId( RaftMemberId id )
        {
            return memberMap.get( id );
        }

        public Members withIds( RaftMemberId... ids )
        {
            Members filteredMembers = new Members();
            for ( RaftMemberId id : ids )
            {
                if ( memberMap.containsKey( id ) )
                {
                    filteredMembers.put( memberMap.get( id ) );
                }
            }
            return filteredMembers;
        }

        public Members withRole( Role role )
        {
            Members filteredMembers = new Members();

            for ( Map.Entry<RaftMemberId,MemberFixture> entry : memberMap.entrySet() )
            {
                if ( entry.getValue().raftInstance().currentRole() == role )
                {
                    filteredMembers.put( entry.getValue() );
                }
            }
            return filteredMembers;
        }

        public void setTargetMembershipSet( Set<RaftMemberId> targetMembershipSet )
        {
            for ( MemberFixture memberFixture : memberMap.values() )
            {
                memberFixture.raftMachine.setTargetMembershipSet( targetMembershipSet );
            }
        }

        public void invokeTimeout( TimerService.TimerName name )
        {
            for ( MemberFixture memberFixture : memberMap.values() )
            {
                memberFixture.timerService.invoke( name );
            }
        }

        @Override
        public Iterator<MemberFixture> iterator()
        {
            return memberMap.values().iterator();
        }

        public int size()
        {
            return memberMap.size();
        }

        @Override
        public String toString()
        {
            return format( "Members%s", memberMap );
        }
    }

    public class MemberFixture
    {
        private RaftMemberId member;
        private RaftMachine raftMachine;
        private OnDemandTimerService timerService;
        private RaftLog raftLog;

        public RaftMemberId member()
        {
            return member;
        }

        public RaftMachine raftInstance()
        {
            return raftMachine;
        }

        public OnDemandTimerService timerService()
        {
            return timerService;
        }

        public RaftLog raftLog()
        {
            return raftLog;
        }

        public Lazy<RaftMemberId> lazyMember()
        {
            var lazyMember = lazySingleton( () -> member );
            lazyMember.get();
            return lazyMember;
        }

        @Override
        public String toString()
        {
            return "FixtureMember{" +
                   "raftInstance=" + raftMachine +
                   ", timeoutService=" + timerService +
                   ", raftLog=" + raftLog +
                   '}';
        }
    }

    private static class RaftTestFixtureLogger extends BetterRaftMessageLogger<RaftMemberId>
    {
        final PrintWriter printWriter;

        RaftTestFixtureLogger( RaftMemberId me, PrintWriter printWriter )
        {
            super( null, null, Clock.systemUTC() );
            this.printWriter = printWriter;

            try
            {
                start();
            }
            catch ( IOException e )
            {
                throw new UncheckedIOException( e );
            }
        }

        @Override
        protected PrintWriter openPrintWriter()
        {
            return printWriter;
        }
    }
}
