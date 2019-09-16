/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.bolt.runtime;

import org.neo4j.bolt.runtime.statemachine.impl.AbstractBoltStateMachine;
import org.neo4j.bolt.testing.BoltResponseRecorder;
import org.neo4j.bolt.testing.RecordedBoltResponse;
import org.neo4j.bolt.v3.BoltProtocolV3;
import org.neo4j.bolt.v3.BoltStateMachineV3;
import org.neo4j.bolt.v3.messaging.request.BeginMessage;
import org.neo4j.bolt.v3.messaging.request.PullAllMessage;
import org.neo4j.bolt.v3.messaging.request.ResetMessage;
import org.neo4j.bolt.v3.messaging.request.RunMessage;
import org.neo4j.bolt.v3.runtime.FailedState;
import org.neo4j.bolt.v3.runtime.ReadyState;
import org.neo4j.bolt.v3.runtime.StreamingState;
import org.neo4j.bolt.v3.runtime.TransactionReadyState;
import org.neo4j.bolt.v3.runtime.TransactionStreamingState;
import org.neo4j.kernel.api.exceptions.Status;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.neo4j.bolt.runtime.statemachine.StatementProcessor.EMPTY;
import static org.neo4j.bolt.testing.BoltMatchers.failedWithStatus;
import static org.neo4j.bolt.testing.BoltMatchers.succeeded;
import static org.neo4j.bolt.testing.BoltMatchers.wasIgnored;
import static org.neo4j.bolt.testing.NullResponseHandler.nullResponseHandler;
import static org.neo4j.bolt.v3.messaging.request.CommitMessage.COMMIT_MESSAGE;

class MultiDatabaseBoltStateMachineV3IT extends MultiDatabaseBoltStateMachineTestBase
{
    @Override
    protected void reset( AbstractBoltStateMachine machine ) throws Throwable
    {
        BoltResponseRecorder recorder = new BoltResponseRecorder();
        machine.interrupt();
        machine.process( ResetMessage.INSTANCE, recorder );
        RecordedBoltResponse response = recorder.nextResponse();
        assertThat( response, succeeded() );
        assertThat( machine.state(), instanceOf( ReadyState.class ) );
        assertThat( machine.connectionState().getStatementProcessor(), equalTo( EMPTY ) );
    }

    @Override
    protected RecordedBoltResponse sessionRun( String query, AbstractBoltStateMachine machine ) throws Throwable
    {
        BoltResponseRecorder recorder = new BoltResponseRecorder();
        // RUN
        machine.process( new RunMessage( query ), recorder );
        assertThat( recorder.nextResponse(), succeeded() );
        assertThat( machine.state(), instanceOf( StreamingState.class ) );
        verifyStatementProcessorNotEmpty( machine );

        // PULL_ALL
        machine.process( PullAllMessage.INSTANCE, recorder );
        RecordedBoltResponse response = recorder.nextResponse();
        assertThat( response, succeeded() );
        assertThat( machine.state(), instanceOf( ReadyState.class ) );
        verifyStatementProcessorIsEmpty( machine );
        return response;
    }

    @Override
    protected void runWithFailure( String query, AbstractBoltStateMachine machine, Status status, boolean isEmpty ) throws Throwable
    {
        BoltResponseRecorder recorder = new BoltResponseRecorder();
        // RUN
        machine.process( new RunMessage( query ), recorder );
        assertThat( recorder.nextResponse(), failedWithStatus( status ) );
        assertThat( machine.state(), instanceOf( FailedState.class ) );
        verifyStatementProcessor( machine, isEmpty );

        // PULL_ALL
        machine.process( PullAllMessage.INSTANCE, recorder );
        RecordedBoltResponse response = recorder.nextResponse();
        assertThat( response, wasIgnored() );
        assertThat( machine.state(), instanceOf( FailedState.class ) );
        verifyStatementProcessor( machine, isEmpty );
    }

    @Override
    protected RecordedBoltResponse txRun( String query, AbstractBoltStateMachine machine ) throws Throwable
    {
        // BEGIN
        sessionBeginTx( machine );

        // RUN
        BoltResponseRecorder recorder = new BoltResponseRecorder();
        machine.process( new RunMessage( query ), recorder );
        assertThat( recorder.nextResponse(), succeeded() );
        assertThat( machine.state(), instanceOf( TransactionStreamingState.class ) );
        verifyStatementProcessorNotEmpty( machine );

        // PULL_ALL
        machine.process( PullAllMessage.INSTANCE, recorder );
        RecordedBoltResponse response = recorder.nextResponse();
        assertThat( response, succeeded() );
        assertThat( machine.state(), instanceOf( TransactionReadyState.class ) );
        verifyStatementProcessorNotEmpty( machine );

        // COMMIT
        machine.process( COMMIT_MESSAGE, recorder );
        assertThat( recorder.nextResponse(), succeeded() );
        assertThat( machine.state(), instanceOf( ReadyState.class ) );
        verifyStatementProcessorIsEmpty( machine );
        return response;
    }

    @Override
    protected void sessionBeginTx( AbstractBoltStateMachine machine ) throws Throwable
    {
        BoltResponseRecorder recorder = new BoltResponseRecorder();
        machine.process( new BeginMessage(), recorder );
        assertThat( recorder.nextResponse(), succeeded() );
        assertThat( machine.state(), instanceOf( TransactionReadyState.class ) );
        verifyStatementProcessorNotEmpty( machine );
    }

    @Override
    protected AbstractBoltStateMachine newStateMachineInReadyState() throws Throwable
    {
        BoltStateMachineV3 machine = (BoltStateMachineV3) env.newMachine( BoltProtocolV3.VERSION, BOLT_CHANNEL );
        machine.process( newHelloMessage(), nullResponseHandler() );
        return machine;
    }
}
