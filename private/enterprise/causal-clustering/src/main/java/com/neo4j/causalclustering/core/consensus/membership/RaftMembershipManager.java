/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.membership;

import com.neo4j.causalclustering.core.consensus.log.RaftLogCursor;
import com.neo4j.causalclustering.core.consensus.log.RaftLogEntry;
import com.neo4j.causalclustering.core.consensus.log.ReadableRaftLog;
import com.neo4j.causalclustering.core.consensus.outcome.RaftLogCommand;
import com.neo4j.causalclustering.core.consensus.roles.Role;
import com.neo4j.causalclustering.core.consensus.roles.follower.FollowerStates;
import com.neo4j.causalclustering.core.replication.SendToMyself;
import com.neo4j.causalclustering.identity.RaftMemberId;

import java.io.IOException;
import java.time.Clock;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.function.LongSupplier;

import org.neo4j.function.Suppliers.Lazy;
import org.neo4j.io.state.StateStorage;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static java.util.Collections.emptySet;
import static org.neo4j.internal.helpers.collection.Iterables.first;

/**
 * This class drives raft membership changes by glueing together various components:
 * - target membership from discovery service
 * - raft membership state machine
 * - raft log events
 */
public class RaftMembershipManager extends LifecycleAdapter implements RaftMembership, RaftLogCommand.Handler
{
    private RaftMembershipChanger membershipChanger;

    private Set<RaftMemberId> targetMembers;

    private final SendToMyself sendToMyself;
    private final Lazy<RaftMemberId> myself;
    private final RaftMembers.Builder<RaftMemberId> memberSetBuilder;
    private final ReadableRaftLog raftLog;
    private final Log log;
    private final StateStorage<RaftMembershipState> storage;

    private LongSupplier recoverFromIndexSupplier;
    private RaftMembershipState state;

    private final int minimumConsensusGroupSize;

    private volatile Set<RaftMemberId> votingMembers = Collections.unmodifiableSet( new HashSet<>() );
    // votingMembers + additionalReplicationMembers
    private volatile Set<RaftMemberId> replicationMembers = Collections.unmodifiableSet( new HashSet<>() );

    private Set<Listener> listeners = new HashSet<>();
    private Set<RaftMemberId> additionalReplicationMembers = new HashSet<>();

    public RaftMembershipManager( SendToMyself sendToMyself, Lazy<RaftMemberId> myself, RaftMembers.Builder<RaftMemberId> memberSetBuilder,
            ReadableRaftLog raftLog, LogProvider logProvider, int minimumConsensusGroupSize, long catchupLagTimeout,
            Clock clock, long catchupTimeout, StateStorage<RaftMembershipState> membershipStorage )
    {
        this.sendToMyself = sendToMyself;
        this.myself = myself;
        this.memberSetBuilder = memberSetBuilder;
        this.raftLog = raftLog;
        this.minimumConsensusGroupSize = minimumConsensusGroupSize;
        this.storage = membershipStorage;
        this.log = logProvider.getLog( getClass() );
        this.membershipChanger =
                new RaftMembershipChanger( raftLog, clock, catchupLagTimeout, logProvider, catchupTimeout, this );
    }

    public void setRecoverFromIndexSupplier( LongSupplier recoverFromIndexSupplier )
    {
        this.recoverFromIndexSupplier = recoverFromIndexSupplier;
    }

    @Override
    public void start() throws IOException
    {
        this.state = storage.getInitialState();
        long recoverFromIndex = recoverFromIndexSupplier.getAsLong();
        log.info( "Membership state before recovery: " + state );
        log.info( "Recovering from: " + recoverFromIndex + " to: " + raftLog.appendIndex() );

        try ( RaftLogCursor cursor = raftLog.getEntryCursor( recoverFromIndex ) )
        {
            while ( cursor.next() )
            {
                append( cursor.index(), cursor.get() );
            }
        }

        log.info( "Membership state after recovery: " + state );
        updateMemberSets();
    }

    public void setTargetMembershipSet( Set<RaftMemberId> targetMembers )
    {
        boolean targetMembershipChanged = !targetMembers.equals( this.targetMembers );

        this.targetMembers = new HashSet<>( targetMembers );

        if ( targetMembershipChanged )
        {
            log.info( "Target membership: " + targetMembers );
        }

        membershipChanger.onTargetChanged( targetMembers );

        var intersection = new HashSet<>( targetMembers );
        var votingMembers = votingMembers();
        intersection.retainAll( votingMembers );

        var majority = ( votingMembers.size() / 2 ) + 1;

        if ( intersection.size() < majority )
        {
            log.warn( "Target membership %s does not contain a majority of existing raft members %s. " +
                    "It is likely an operator removed too many core members too quickly. If not, this issue should be transient.",
                    this.targetMembers, this.votingMembers );
        }

        checkForStartCondition();
    }

    private Set<RaftMemberId> missingMembers()
    {
        if ( targetMembers == null || votingMembers() == null )
        {
            return emptySet();
        }
        Set<RaftMemberId> missingMembers = new HashSet<>( targetMembers );
        missingMembers.removeAll( votingMembers() );

        return missingMembers;
    }

    /**
     * All the externally published sets are derived from the committed and appended sets.
     */
    private void updateMemberSets()
    {
        votingMembers = Collections.unmodifiableSet( state.getLatest() );

        HashSet<RaftMemberId> newReplicationMembers = new HashSet<>( votingMembers );
        newReplicationMembers.addAll( additionalReplicationMembers );

        replicationMembers = Collections.unmodifiableSet( newReplicationMembers );
        listeners.forEach( Listener::onMembershipChanged );
    }

    /**
     * Adds an additional member to replicate to. Members that are joining need to
     * catch up sufficiently before they become part of the voting group.
     *
     * @param member The member which will be added to the replication group.
     */
    void addAdditionalReplicationMember( RaftMemberId member )
    {
        additionalReplicationMembers.add( member );
        updateMemberSets();
    }

    /**
     * Removes a member previously part of the additional replication member group.
     *
     * This either happens because they caught up sufficiently and became part of the
     * voting group or because they failed to catch up in time.
     *
     * @param member The member to remove from the replication group.
     */
    void removeAdditionalReplicationMember( RaftMemberId member )
    {
        additionalReplicationMembers.remove( member );
        updateMemberSets();
    }

    private boolean isSafeToRemoveMember()
    {
        Set<RaftMemberId> votingMembers = votingMembers();
        boolean safeToRemoveMember = votingMembers != null && votingMembers.size() > minimumConsensusGroupSize;

        if ( !safeToRemoveMember )
        {
            Set<RaftMemberId> membersToRemove = superfluousMembers();

            log.info( "Not safe to remove %s %s because it would reduce the number of voting members below the expected " +
                            "cluster size of %d. Voting members: %s",
                    membersToRemove.size() > 1 ? "members" : "member",
                    membersToRemove, minimumConsensusGroupSize, votingMembers  );
        }

        return safeToRemoveMember;
    }

    private Set<RaftMemberId> superfluousMembers()
    {
        if ( targetMembers == null || votingMembers() == null )
        {
            return emptySet();
        }
        Set<RaftMemberId> superfluousMembers = new HashSet<>( votingMembers() );
        superfluousMembers.removeAll( targetMembers );
        superfluousMembers.remove( myself.getIfPresent() );

        return superfluousMembers;
    }

    private void checkForStartCondition()
    {
        if ( missingMembers().size() > 0 )
        {
            membershipChanger.onMissingMember( first( missingMembers() ) );
        }
        else if ( superfluousMembers().size() > 0 && isSafeToRemoveMember() )
        {
            membershipChanger.onSuperfluousMember( first( superfluousMembers() ) );
        }
    }

    /**
     * Used by the membership changer for getting consensus on a new set of members.
     *
     * @param newVotingMemberSet The new set of members.
     */
    void doConsensus( Set<RaftMemberId> newVotingMemberSet )
    {
        log.info( "Getting consensus on new voting member set %s", newVotingMemberSet );
        sendToMyself.replicate( memberSetBuilder.build( newVotingMemberSet ) );
    }

    /**
     * Called by the membership changer when it has changed state and in response
     * the membership manager potentially feeds it back with an event to start
     * a new membership change operation.
     */
    void stateChanged()
    {
        checkForStartCondition();
    }

    public void onFollowerStateChange( FollowerStates<RaftMemberId> followerStates )
    {
        membershipChanger.onFollowerStateChange( followerStates );
    }

    public void onRole( Role role )
    {
        membershipChanger.onRole( role );
    }

    public void handleLeadershipTransfers( boolean areTransferring )
    {
        membershipChanger.onLeadershipTransfer( areTransferring );
    }

    @Override
    public Set<RaftMemberId> votingMembers()
    {
        return votingMembers;
    }

    @Override
    public Set<RaftMemberId> replicationMembers()
    {
        return replicationMembers;
    }

    @Override
    public void registerListener( Listener listener )
    {
        listeners.add( listener );
    }

    boolean uncommittedMemberChangeInLog()
    {
        return state.uncommittedMemberChangeInLog();
    }

    public void processLog( long commitIndex, Collection<RaftLogCommand> logCommands ) throws IOException
    {
        for ( RaftLogCommand logCommand : logCommands )
        {
            logCommand.dispatch( this );
        }

        if ( state.commit( commitIndex ) )
        {
            membershipChanger.onRaftGroupCommitted();
            storage.writeState( state );
            updateMemberSets();
        }
    }

    @Override
    public void append( long baseIndex, RaftLogEntry... entries ) throws IOException
    {
        /* The warnings in this method are rarely expected occurrences which warrant to be logged with significance. */

        for ( RaftLogEntry entry : entries )
        {
            if ( entry.content() instanceof RaftMembers )
            {
                //noinspection unchecked
                RaftMembers<RaftMemberId> raftMembers = (RaftMembers<RaftMemberId>) entry.content();

                if ( state.uncommittedMemberChangeInLog() )
                {
                    log.warn( "Appending with uncommitted membership change in log" );
                }

                if ( state.append( baseIndex, new HashSet<>( raftMembers.getMembers() ) ) )
                {
                    log.info( "Appending new member set %s", state );
                    storage.writeState( state );
                    updateMemberSets();
                }
                else
                {
                    log.warn( "Appending member set was ignored. Current state: %s, Appended set: %s, Log index: %d%n",
                            state, raftMembers, baseIndex );
                }
            }
            baseIndex++;
        }
    }

    @Override
    public void truncate( long fromIndex ) throws IOException
    {
        if ( state.truncate( fromIndex ) )
        {
            storage.writeState( state );
            updateMemberSets();
        }
    }

    @Override
    public void prune( long pruneIndex )
    {
        // only the actual log prunes
    }

    public MembershipEntry getCommitted()
    {
        return state.committed();

    }

    public void install( MembershipEntry committed ) throws IOException
    {
        state = new RaftMembershipState( committed.logIndex(), committed, null );
        storage.writeState( state );
        updateMemberSets();
    }
}
