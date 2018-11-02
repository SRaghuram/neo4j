/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.kernel.ha.com.master;

import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.test.rule.concurrent.ThreadingRule;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

@RunWith( MockitoJUnitRunner.class )
public class ConversationTest
{
    @Mock
    private Locks.Client client;
    @InjectMocks
    private Conversation conversation;
    @Rule
    public ThreadingRule threadingRule = new ThreadingRule();

    @Test
    public void stopAlreadyClosedConversationDoNotTouchLocks()
    {
        conversation.close();
        conversation.stop();
        conversation.stop();
        conversation.stop();

        verify( client ).close();
        assertFalse( conversation.isActive() );
        verifyNoMoreInteractions( client );
    }

    @Test
    public void stopCloseConversation()
    {
        conversation.stop();
        conversation.close();

        verify( client ).stop();
        verify( client ).close();
        assertFalse( conversation.isActive() );
    }

    @Test( timeout = 3000 )
    public void conversationCanNotBeStoppedAndClosedConcurrently() throws InterruptedException
    {
        final CountDownLatch answerLatch = new CountDownLatch( 1 );
        final CountDownLatch stopLatch = new CountDownLatch( 1 );
        final CountDownLatch stopReadyLatch = new CountDownLatch( 1 );
        final int sleepTime = 1000;
        doAnswer( invocation ->
        {
            stopReadyLatch.countDown();
            stopLatch.await();
            TimeUnit.MILLISECONDS.sleep( sleepTime );
            return null;
        } ).when( client ).stop();
        doAnswer( invocation ->
        {
            answerLatch.countDown();
            return null;
        } ).when( client ).close();

        threadingRule.execute( conversation ->
        {
            conversation.stop();
            return null;
        }, conversation );

        stopReadyLatch.await();
        threadingRule.execute( conversation ->
        {
            conversation.close();
            return null;
        }, conversation );

        long raceStartTime = System.currentTimeMillis();
        stopLatch.countDown();
        answerLatch.await();
        // execution time should be at least 1000 millis
        long executionTime = System.currentTimeMillis() - raceStartTime;
        assertTrue(String.format( "Execution time should be at least equal to %d, but was %d.", sleepTime, executionTime),
                executionTime  >= sleepTime);
    }
}
