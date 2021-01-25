/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.replication;

import com.neo4j.causalclustering.core.state.StateMachineResult;

import static com.neo4j.causalclustering.core.replication.ReplicationResult.Outcome.APPLIED;
import static com.neo4j.causalclustering.core.replication.ReplicationResult.Outcome.MAYBE_REPLICATED;
import static com.neo4j.causalclustering.core.replication.ReplicationResult.Outcome.NOT_REPLICATED;
import static java.util.Objects.requireNonNull;

public class ReplicationResult
{
    public enum Outcome
    {
        NOT_REPLICATED,
        MAYBE_REPLICATED,
        APPLIED
    }

    private final Outcome outcome;
    private final Throwable failure;
    private final StateMachineResult stateMachineResult;

    private ReplicationResult( Outcome outcome, StateMachineResult stateMachineResult )
    {
        requireNonNull( stateMachineResult, "Illegal result: " + stateMachineResult );

        this.outcome = outcome;
        this.failure = null;
        this.stateMachineResult = stateMachineResult;
    }

    private ReplicationResult( Outcome outcome, Throwable failure )
    {
        requireNonNull( failure, "Illegal failure: " + failure );

        this.outcome = outcome;
        this.stateMachineResult = null;
        this.failure = failure;
    }

    public static ReplicationResult notReplicated( Throwable failure )
    {
        return new ReplicationResult( NOT_REPLICATED, failure );
    }

    public static ReplicationResult maybeReplicated( Throwable failure )
    {
        return new ReplicationResult( MAYBE_REPLICATED, failure );
    }

    public static ReplicationResult applied( StateMachineResult stateMachineResult )
    {
        return new ReplicationResult( APPLIED, stateMachineResult );
    }

    public Outcome outcome()
    {
        return outcome;
    }

    public Throwable failure()
    {
        return failure;
    }

    public StateMachineResult stateMachineResult()
    {
        return stateMachineResult;
    }
}
