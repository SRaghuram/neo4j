/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cluster.protocol.atomicbroadcast.multipaxos;

import org.neo4j.cluster.com.message.Message;
import org.neo4j.cluster.com.message.MessageHolder;
import org.neo4j.cluster.statemachine.State;

/**
 * State machine for Paxos Acceptor
 */
public enum AcceptorState
        implements State<AcceptorContext, AcceptorMessage>
{
    start
            {
                @Override
                public AcceptorState handle( AcceptorContext context,
                                             Message<AcceptorMessage> message,
                                             MessageHolder outgoing
                )
                {
                    if ( message.getMessageType() == AcceptorMessage.join )
                    {
                        return acceptor;
                    }

                    return this;
                }
            },

    acceptor
            {
                @Override
                public AcceptorState handle( AcceptorContext context,
                                             Message<AcceptorMessage> message,
                                             MessageHolder outgoing
                )
                {
                    switch ( message.getMessageType() )
                    {
                        case prepare:
                        {
                            AcceptorMessage.PrepareState incomingState = message.getPayload();
                            InstanceId instanceId = new InstanceId( message );

                            // This creates the instance if not already present
                            AcceptorInstance localState = context.getAcceptorInstance( instanceId );

                            /*
                             * If the incoming messages has a ballot greater than the local one, send back a promise.
                             * This is always true for newly seen instances, the local state has ballot initialized
                             * to -1
                             */
                            if ( incomingState.getBallot() >= localState.getBallot() )
                            {
                                context.promise( localState, incomingState.getBallot() );

                                outgoing.offer( message.copyHeadersTo( Message.respond( ProposerMessage.promise,
                                        message, new ProposerMessage.PromiseState( incomingState.getBallot(),
                                                localState.getValue() ) ), InstanceId.INSTANCE ) );
                            }
                            else
                            {
                                // Optimization - explicit reject
                                context.getLog( AcceptorState.class ).debug("Rejecting prepare from "
                                        + message.getHeader( Message.HEADER_FROM ) + " for instance "
                                        + message.getHeader( InstanceId.INSTANCE ) + " and ballot "
                                        + incomingState.getBallot() + " (i had a prepare state ballot = "
                                        + localState.getBallot() + ")" );
                                outgoing.offer( message.copyHeadersTo( Message.respond( ProposerMessage
                                        .rejectPrepare, message,
                                        new ProposerMessage.RejectPrepare( localState.getBallot() ) ),
                                        InstanceId.INSTANCE ) );
                            }
                            break;
                        }

                        case accept:
                        {
                            // Task 4
                            AcceptorMessage.AcceptState acceptState = message.getPayload();
                            InstanceId instanceId = new InstanceId( message );
                            AcceptorInstance instance = context.getAcceptorInstance( instanceId );

                            if ( acceptState.getBallot() == instance.getBallot() )
                            {
                                context.accept( instance, acceptState.getValue() );
                                instance.accept( acceptState.getValue() );

                                outgoing.offer( message.copyHeadersTo( Message.respond( ProposerMessage.accepted,
                                        message,
                                        new ProposerMessage.AcceptedState() ), InstanceId.INSTANCE ) );
                            }
                            else
                            {
                                context.getLog( AcceptorState.class ).debug( "Reject " + instanceId
                                        + " accept ballot:" + acceptState.getBallot() + " actual ballot:" +
                                        instance.getBallot() );
                                outgoing.offer( message.copyHeadersTo( Message.respond( ProposerMessage
                                        .rejectAccept, message,
                                        new ProposerMessage.RejectAcceptState() ), InstanceId.INSTANCE ) );
                            }
                            break;
                        }

                        case leave:
                        {
                            context.leave();
                            return start;
                        }

                        default:
                            break;
                    }

                    return this;
                }
            },
}
