/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.system;

import akka.actor.ActorRef;
import akka.actor.ActorSelectionMessage;
import akka.actor.ExtendedActorSystem;
import akka.cluster.ClusterMessage;
import akka.cluster.InternalClusterAction;
import akka.remote.artery.RemoteInstrument;

import java.nio.ByteBuffer;

import org.neo4j.util.VisibleForTesting;

public class JoinMessageSpy extends RemoteInstrument
{
    private final ExtendedActorSystem actorSystem;

    private static final byte remoteInstrumentId = AkkaRemoteInstrumentIdentifiers.getIdentifier();

    public JoinMessageSpy( ExtendedActorSystem actorSystem )
    {
        this.actorSystem = actorSystem;
    }

    @Override
    public byte identifier()
    {
        return remoteInstrumentId;
    }

    @Override
    public void remoteWriteMetadata( ActorRef recipient, Object message, ActorRef sender, ByteBuffer buffer )
    {
        // do nothing
    }

    @Override
    public void remoteMessageSent( ActorRef recipient, Object message, ActorRef sender, int size, long time )
    {
        // do nothing
    }

    @Override
    public void remoteReadMetadata( ActorRef recipient, Object message, ActorRef sender, ByteBuffer buffer )
    {
        // do nothing
    }

    @Override
    public void remoteMessageReceived( ActorRef recipient, Object message, ActorRef sender, int size, long time )
    {
        message = unwrapMessage( message );

        if ( message instanceof InternalClusterAction.InitJoin )
        {
            InitJoinRequestObserved event = new InitJoinRequestObserved( (InternalClusterAction.InitJoin) message, sender );
            tryPublishEvent( event );
        }
    }

    /**
     * unwrap ActorSelectionMessage objects to get the actual message inside
     */
    private Object unwrapMessage( Object message )
    {
        while ( message instanceof ActorSelectionMessage )
        {
            message = ((ActorSelectionMessage) message).msg();
        }
        return message;
    }

    private void tryPublishEvent( ClusterActionWrapper<?> event )
    {
        if ( actorSystem == null )
        {
            // System.out.println( "Actor system is null" );
            return;
        }
        if ( actorSystem.eventStream() == null )
        {
            // System.out.println( "Event stream is null" );
            return;
        }
        actorSystem.eventStream().publish( event );
    }

    public interface OriginalSender
    {
        ActorRef getOriginalSender();
    }

    private static class ClusterActionWrapper<T extends ClusterMessage> implements OriginalSender
    {
        private final T originalMessage;
        private final ActorRef originalSender;

        ClusterActionWrapper( T originalMessage, ActorRef originalSender )
        {
            this.originalMessage = originalMessage;
            this.originalSender = originalSender;
        }

        @Override
        public ActorRef getOriginalSender()
        {
            return originalSender;
        }

        public T getOriginalMessage()
        {
            return originalMessage;
        }
    }

    /**
     * Event sent when we observe an incoming InitJoin Request from aa remote instance
     */
    public static class InitJoinRequestObserved extends ClusterActionWrapper<InternalClusterAction.InitJoin>
    {
        public InitJoinRequestObserved( InternalClusterAction.InitJoin msg, ActorRef originalSender )
        {
            super( msg, originalSender );
        }

        @VisibleForTesting
        public static InitJoinRequestObserved dummy()
        {
            return new InitJoinRequestObserved( new InternalClusterAction.InitJoin( null ), null );
        }
    }
}
