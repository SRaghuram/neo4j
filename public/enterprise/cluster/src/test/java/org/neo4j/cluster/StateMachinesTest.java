/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.cluster;

import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Executor;

import org.neo4j.cluster.com.message.Message;
import org.neo4j.cluster.com.message.MessageHolder;
import org.neo4j.cluster.com.message.MessageSender;
import org.neo4j.cluster.com.message.MessageSource;
import org.neo4j.cluster.com.message.MessageType;
import org.neo4j.cluster.statemachine.State;
import org.neo4j.cluster.statemachine.StateMachine;
import org.neo4j.cluster.timeout.Timeouts;
import org.neo4j.logging.NullLogProvider;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.cluster.com.message.Message.internal;

public class StateMachinesTest
{
    @Test
    public void whenMessageHandlingCausesNewMessagesThenEnsureCorrectOrder() throws Exception
    {
        // Given
        StateMachines stateMachines = new StateMachines( NullLogProvider.getInstance(), mock( StateMachines.Monitor.class ),
                mock( MessageSource.class ),
                Mockito.mock( MessageSender.class ), Mockito.mock( Timeouts.class ),
                Mockito.mock( DelayedDirectExecutor.class ), new Executor()
        {
            @Override
            public void execute( Runnable command )
            {
                command.run();
            }
        }, mock( InstanceId.class )
        );

        ArrayList<TestMessage> handleOrder = new ArrayList<>();
        StateMachine stateMachine = new StateMachine( handleOrder, TestMessage.class, TestState.test, NullLogProvider.getInstance() );

        stateMachines.addStateMachine( stateMachine );

        // When
        stateMachines.process( internal( TestMessage.message1 ) );

        // Then
        assertThat( handleOrder.toString(), equalTo( "[message1, message2, message4, message5, message3]" ) );
    }

    @Test
    public void shouldAlwaysAddItsInstanceIdToOutgoingMessages() throws Exception
    {
        InstanceId me = new InstanceId( 42 );
        final List<Message> sentOut = new LinkedList<Message>();

        /*
         * Lots of setup required. Must have a sender that keeps messages so we can see what the machine sent out.
         * We must have the StateMachines actually delegate the incoming message and retrieve the generated outgoing.
         * That means we need an actual StateMachine with a registered MessageType. And most of those are void
         * methods, which means lots of Answer objects.
         */
        // Given
        MessageSender sender = mock( MessageSender.class );
        // The sender, which adds messages outgoing to the list above.
        doAnswer( new Answer()
        {
            @Override
            public Object answer( InvocationOnMock invocation ) throws Throwable
            {
                sentOut.addAll( (Collection<? extends Message>) invocation.getArguments()[0] );
                return null;
            }
        } ).when( sender ).process( Matchers.<List<Message<? extends MessageType>>>any() );

        StateMachines stateMachines = new StateMachines( NullLogProvider.getInstance(), mock( StateMachines.Monitor.class ),
                mock( MessageSource.class ), sender,
                mock( Timeouts.class ), mock( DelayedDirectExecutor.class ), new Executor()
        {
            @Override
            public void execute( Runnable command )
            {
                command.run();
            }
        }, me
        );

        // The state machine, which has a TestMessage message type and simply adds a TO header to the messages it
        // is handed to handle.
        StateMachine machine = mock( StateMachine.class );
        when( machine.getMessageType() ).then( new Answer<Object>()
        {
            @Override
            public Object answer( InvocationOnMock invocation ) throws Throwable
            {
                return TestMessage.class;
            }
        } );
        doAnswer( new Answer<Object>()
        {
            @Override
            public Object answer( InvocationOnMock invocation ) throws Throwable
            {
                Message message = (Message) invocation.getArguments()[0];
                MessageHolder holder = (MessageHolder) invocation.getArguments()[1];
                message.setHeader( Message.TO, "to://neverland" );
                holder.offer( message );
                return null;
            }
        } ).when( machine ).handle( any( Message.class ), any( MessageHolder.class ) );
        stateMachines.addStateMachine( machine );

        // When
        stateMachines.process( Message.internal( TestMessage.message1 ) );

        // Then
        assertEquals( "StateMachines should not make up messages from thin air", 1, sentOut.size() );
        Message sent = sentOut.get( 0 );
        assertTrue( "StateMachines should add the instance-id header", sent.hasHeader( Message.INSTANCE_ID ) );
        assertEquals( "StateMachines should add instance-id header that has the correct value",
                me.toString(), sent.getHeader( Message.INSTANCE_ID ) );
    }

    public enum TestMessage
            implements MessageType
    {
        message1, message2, message3, message4, message5;
    }

    public enum TestState
            implements State<List, TestMessage>
    {
        test
                {
                    @Override
                    public State<?, ?> handle( List context, Message<TestMessage> message,
                                               MessageHolder outgoing ) throws Throwable
                    {
                        context.add( message.getMessageType() );

                        switch ( message.getMessageType() )
                        {
                            case message1:
                            {
                                outgoing.offer( internal( TestMessage.message2 ) );
                                outgoing.offer( internal( TestMessage.message3 ) );
                                break;
                            }

                            case message2:
                            {
                                outgoing.offer( internal( TestMessage.message4 ) );
                                outgoing.offer( internal( TestMessage.message5 ) );
                                break;
                            }

                            default:
                                break;
                        }

                        return this;
                    }
                }
    }
}
