/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus;

import com.neo4j.causalclustering.core.consensus.outcome.Outcome;

public interface LeaderListener
{
    /**
     * Allows listeners to handle a leader step down for the given term.
     * Note: actions taken as a result of a step down should typically happen *before* any
     * actions taken as a result of the leader switch which has also, implicitly, taken place.
     *
     * @param stepDownTerm the term in which the the step down event occurred.
     */
    default void onLeaderStepDown( long stepDownTerm )
    {
    }

    void onLeaderSwitch( LeaderInfo leaderInfo );

    /**
     * Standard catch-all method which delegates leader events to their appropriate handlers
     * in the appropriate order, i.e. calls step down logic (if necessary) before leader switch
     * logic.
     *
     * @param outcome The outcome which contains details of the leader event
     */
    default void onLeaderEvent( Outcome outcome )
    {
        outcome.stepDownTerm().ifPresent( this::onLeaderStepDown );
        onLeaderSwitch( new LeaderInfo( outcome.getLeader(), outcome.getTerm() ) );
    }

    /**
     * Trigger an event when this listener is being unregistered.
     */
    default void onUnregister()
    {
        // do nothing
    }
}
