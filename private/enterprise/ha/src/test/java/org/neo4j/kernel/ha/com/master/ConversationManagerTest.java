/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.kernel.ha.com.master;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.time.Duration;

import org.neo4j.com.RequestContext;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.ha.HaSettings;
import org.neo4j.kernel.ha.cluster.ConversationSPI;
import org.neo4j.kernel.impl.util.collection.TimedRepository;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobHandle;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith( MockitoJUnitRunner.class )
public class ConversationManagerTest
{

    @Mock
    private ConversationSPI conversationSPI;
    @Mock
    private Config config;
    private ConversationManager conversationManager;

    @Test
    public void testStart()
    {
        JobHandle reaperJobHandle = mock( JobHandle.class );
        when( config.get( HaSettings.lock_read_timeout ) ).thenReturn( Duration.ofMillis( 1 ) );
        when( conversationSPI.scheduleRecurringJob( any( Group.class ), any( Long.class ),
                any( Runnable.class ) ) ).thenReturn( reaperJobHandle );
        conversationManager = getConversationManager();

        conversationManager.start();

        assertNotNull( conversationManager.conversations );
        verify( conversationSPI ).scheduleRecurringJob(any( Group.class ), any( Long.class ),
                any( Runnable.class ) );
    }

    @Test
    public void testStop()
    {
        JobHandle reaperJobHandle = mock( JobHandle.class );
        when( config.get( HaSettings.lock_read_timeout ) ).thenReturn( Duration.ofMillis( 1 ) );
        when( conversationSPI.scheduleRecurringJob( any( Group.class ), any( Long.class ),
                any( Runnable.class ) ) ).thenReturn( reaperJobHandle );
        conversationManager = getConversationManager();

        conversationManager.start();
        conversationManager.stop();

        assertNull( conversationManager.conversations );
        verify( reaperJobHandle ).cancel( false );
    }

    @Test
    public void testConversationWorkflow() throws Exception
    {
        RequestContext requestContext = getRequestContext();
        conversationManager = getConversationManager();
        TimedRepository conversationStorage = mock( TimedRepository.class );
        conversationManager.conversations = conversationStorage;

        conversationManager.begin( requestContext );
        conversationManager.acquire( requestContext );
        conversationManager.release( requestContext );
        conversationManager.end( requestContext );

        InOrder conversationOrder = inOrder( conversationStorage);
        conversationOrder.verify(conversationStorage).begin( requestContext );
        conversationOrder.verify(conversationStorage).acquire( requestContext );
        conversationOrder.verify(conversationStorage).release( requestContext );
        conversationOrder.verify(conversationStorage).end( requestContext );
    }

    @Test
    public void testConversationStop()
    {
        RequestContext requestContext = getRequestContext();
        conversationManager = getConversationManager();

        Conversation conversation = mock( Conversation.class );
        when( conversation.isActive() ).thenReturn( true );

        TimedRepository conversationStorage = mock( TimedRepository.class );
        when( conversationStorage.end( requestContext ) ).thenReturn( conversation );
        conversationManager.conversations = conversationStorage;

        conversationManager.stop( requestContext );

        verify( conversationStorage ).end( requestContext );
        verify( conversation ).stop();
    }

    private RequestContext getRequestContext()
    {
        return new RequestContext( 1L, 1, 1, 1L, 1L );
    }

    private ConversationManager getConversationManager()
    {
        return new ConversationManager( conversationSPI, config, 1000, 5000 );
    }

}
