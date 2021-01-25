/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.roles;

import com.neo4j.causalclustering.core.consensus.RaftMessages;
import com.neo4j.causalclustering.core.consensus.log.RaftLogEntry;
import com.neo4j.causalclustering.core.consensus.outcome.OutcomeBuilder;
import com.neo4j.causalclustering.core.consensus.state.ReadableRaftState;
import com.neo4j.causalclustering.core.replication.DistributedOperation;
import com.neo4j.causalclustering.core.replication.ReplicatedContent;
import com.neo4j.causalclustering.core.state.machines.status.Status;
import com.neo4j.causalclustering.core.state.machines.status.StatusRequest;

import java.util.Collection;

public class StatusAppender
{
    private StatusAppender()
    {
    }

    static void statusResponse( ReadableRaftState state, OutcomeBuilder outcomeBuilder, RaftMessages.AppendEntries.Request request )
    {
        for ( RaftLogEntry entry : request.entries() )
        {
            statusResponse( state, outcomeBuilder, entry.content() );
        }
    }

    private static void statusResponse( ReadableRaftState state, OutcomeBuilder outcomeBuilder, ReplicatedContent replicatedContent )
    {
        if ( replicatedContent instanceof DistributedOperation )
        {
            var distributedOperation = (DistributedOperation) replicatedContent;
            if ( distributedOperation.content() instanceof StatusRequest )
            {

                var content = (StatusRequest) distributedOperation.content();
                RaftMessages.StatusResponse response = new RaftMessages.StatusResponse( state.myself(),
                                                                                        new Status( Status.Message.OK ),
                                                                                        content.messageId() );

                outcomeBuilder.addOutgoingMessage( new RaftMessages.Directed( content.memberId(), response ) );
            }
        }
    }

    static void statusResponse( ReadableRaftState state, OutcomeBuilder outcomeBuilder, Collection<ReplicatedContent> contents )
    {
        contents.forEach( c -> statusResponse( state, outcomeBuilder, c ) );
    }
}
