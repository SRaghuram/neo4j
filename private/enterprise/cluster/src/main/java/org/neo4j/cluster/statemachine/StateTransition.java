/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cluster.statemachine;

import org.neo4j.cluster.com.message.Message;
import org.neo4j.cluster.com.message.MessageType;

/**
 * A single state transition that occurred in
 * a state machine as a consequence of handling a message.
 */
public class StateTransition
{
    private State<?,?> oldState;
    private Message<? extends MessageType> message;
    private State<?,?> newState;

    public StateTransition( State<?,?> oldState, Message<? extends MessageType> message, State<?,?> newState )
    {
        this.oldState = oldState;
        this.message = message;
        this.newState = newState;
    }

    public State<?,?> getOldState()
    {
        return oldState;
    }

    public Message<? extends MessageType> getMessage()
    {
        return message;
    }

    public State<?,?> getNewState()
    {
        return newState;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }

        StateTransition that = (StateTransition) o;

        if ( !message.equals( that.message ) )
        {
            return false;
        }
        if ( !newState.equals( that.newState ) )
        {
            return false;
        }
        return oldState.equals( that.oldState );
    }

    @Override
    public int hashCode()
    {
        int result = oldState.hashCode();
        result = 31 * result + message.hashCode();
        result = 31 * result + newState.hashCode();
        return result;
    }

    @Override
    public String toString()
    {
        if ( message.getPayload() instanceof String )
        {
            return getOldState().toString() + "-[" + getMessage().getMessageType() + ":" + getMessage().getPayload() +
                    "]->" + getNewState().toString();
        }
        else
        {
            return getOldState().toString() + "-[" + getMessage().getMessageType() + "]->" + getNewState().toString();
        }
    }
}
