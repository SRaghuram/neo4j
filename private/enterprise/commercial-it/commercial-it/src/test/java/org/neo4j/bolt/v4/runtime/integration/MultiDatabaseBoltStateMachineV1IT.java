/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.bolt.v4.runtime.integration;

import org.neo4j.bolt.testing.BoltResponseRecorder;
import org.neo4j.bolt.testing.RecordedBoltResponse;
import org.neo4j.bolt.v1.BoltProtocolV1;
import org.neo4j.bolt.v1.messaging.request.DiscardAllMessage;
import org.neo4j.bolt.v1.messaging.request.InitMessage;
import org.neo4j.bolt.v1.messaging.request.PullAllMessage;
import org.neo4j.bolt.v1.messaging.request.ResetMessage;
import org.neo4j.bolt.v1.messaging.request.RunMessage;
import org.neo4j.bolt.v1.runtime.BoltStateMachineV1;
import org.neo4j.bolt.v1.runtime.FailedState;
import org.neo4j.bolt.v1.runtime.ReadyState;
import org.neo4j.bolt.v1.runtime.StreamingState;
import org.neo4j.kernel.api.exceptions.Status;

import static java.util.Collections.emptyMap;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.neo4j.bolt.runtime.StatementProcessor.EMPTY;
import static org.neo4j.bolt.testing.BoltMatchers.failedWithStatus;
import static org.neo4j.bolt.testing.BoltMatchers.succeeded;
import static org.neo4j.bolt.testing.BoltMatchers.wasIgnored;
import static org.neo4j.bolt.testing.NullResponseHandler.nullResponseHandler;

class MultiDatabaseBoltStateMachineV1IT extends MultiDatabaseBoltStateMachineTestBase
{
    @Override
    protected void reset( BoltStateMachineV1 machine ) throws Throwable
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
    protected RecordedBoltResponse sessionRun( String query, BoltStateMachineV1 machine ) throws Throwable
    {
        BoltResponseRecorder recorder = new BoltResponseRecorder();
        // RUN
        machine.process( new RunMessage( query ), recorder );
        assertThat( recorder.nextResponse(), succeeded() );
        assertThat( machine.state(), instanceOf( StreamingState.class ) );
        verifyStatementProcessorNotEmpty( machine );

        // PULL_ALL
        return pullAll( machine, recorder, true );
    }

    @Override
    protected void runWithFailure( String query, BoltStateMachineV1 machine, Status status, boolean isEmpty ) throws Throwable
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
    protected RecordedBoltResponse txRun( String query, BoltStateMachineV1 machine ) throws Throwable
    {
        sessionBeginTx( machine );
        BoltResponseRecorder recorder = new BoltResponseRecorder();
        // RUN
        machine.process( new RunMessage( query ), recorder );
        assertThat( recorder.nextResponse(), succeeded() );
        assertThat( machine.state(), instanceOf( StreamingState.class ) );
        verifyStatementProcessorNotEmpty( machine );

        RecordedBoltResponse response = pullAll( machine, recorder, false );

        // COMMIT
        machine.process( newCommitMessage(), recorder );
        assertThat( recorder.nextResponse(), succeeded() );
        assertThat( machine.state(), instanceOf( ReadyState.class ) );
        verifyStatementProcessorIsEmpty( machine );

        discardAll( machine, recorder );
        return response;
    }

    @Override
    void sessionBeginTx( BoltStateMachineV1 machine ) throws Throwable
    {
        BoltResponseRecorder recorder = new BoltResponseRecorder();
        // BEGIN
        machine.process( newBeginMessage(), recorder );
        assertThat( recorder.nextResponse(), succeeded() );
        assertThat( machine.state(), instanceOf( ReadyState.class ) );
        verifyStatementProcessorNotEmpty( machine );

        pullAll( machine, recorder, false );
    }

    private RecordedBoltResponse pullAll( BoltStateMachineV1 machine, BoltResponseRecorder recorder, boolean isEmpty ) throws Throwable
    {
        // PULL_ALL
        machine.process( PullAllMessage.INSTANCE, recorder );
        RecordedBoltResponse response = recorder.nextResponse();
        assertThat( response, succeeded() );
        assertThat( machine.state(), instanceOf( ReadyState.class ) );

        if ( isEmpty )
        {
            verifyStatementProcessorIsEmpty( machine );
        }
        else
        {
            verifyStatementProcessorNotEmpty( machine );
        }

        return response;
    }

    private void discardAll( BoltStateMachineV1 machine, BoltResponseRecorder recorder ) throws Throwable
    {
        machine.process( DiscardAllMessage.INSTANCE, recorder );
        RecordedBoltResponse response = recorder.nextResponse();
        assertThat( response, succeeded() );
        assertThat( machine.state(), instanceOf( ReadyState.class ) );
        verifyStatementProcessorIsEmpty( machine );
    }

    protected BoltStateMachineV1 newStateMachineInReadyState() throws Throwable
    {
        BoltStateMachineV1 machine = (BoltStateMachineV1) env.newMachine( BoltProtocolV1.VERSION, BOLT_CHANNEL );
        machine.process( newInitMessage(), nullResponseHandler() );
        assertThat( machine.state(), instanceOf( ReadyState.class ) );
        return machine;
    }

    private static InitMessage newInitMessage()
    {
        return new InitMessage( USER_AGENT, emptyMap() );
    }

    private static RunMessage newBeginMessage()
    {
        return new RunMessage( "BEGIN" );
    }

    private static RunMessage newCommitMessage()
    {
        return new RunMessage( "COMMIT" );
    }
}
