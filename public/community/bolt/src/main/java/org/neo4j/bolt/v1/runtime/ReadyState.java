/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.bolt.v1.runtime;

import org.neo4j.bolt.messaging.RequestMessage;
import org.neo4j.bolt.runtime.BoltConnectionFatality;
import org.neo4j.bolt.runtime.BoltResult;
import org.neo4j.bolt.runtime.BoltStateMachineState;
import org.neo4j.bolt.runtime.Bookmark;
import org.neo4j.bolt.runtime.StateMachineContext;
import org.neo4j.bolt.runtime.StatementMetadata;
import org.neo4j.bolt.runtime.StatementProcessor;
import org.neo4j.bolt.v1.ResultConsumerV1Adaptor;
import org.neo4j.bolt.v1.messaging.request.DiscardAllMessage;
import org.neo4j.bolt.v1.messaging.request.InterruptSignal;
import org.neo4j.bolt.v1.messaging.request.PullAllMessage;
import org.neo4j.bolt.v1.messaging.request.ResetMessage;
import org.neo4j.bolt.v1.messaging.request.RunMessage;
import org.neo4j.bolt.v1.runtime.bookmarking.BookmarksParserV1;
import org.neo4j.bolt.v1.runtime.spi.BookmarkResult;
import org.neo4j.bolt.v4.messaging.ResultConsumer;
import org.neo4j.graphdb.security.AuthorizationExpiredException;
import org.neo4j.values.storable.Values;

import static org.neo4j.bolt.v1.runtime.RunMessageChecker.isBegin;
import static org.neo4j.bolt.v1.runtime.RunMessageChecker.isCommit;
import static org.neo4j.bolt.v1.runtime.RunMessageChecker.isRollback;
import static org.neo4j.bolt.v4.messaging.MessageMetadataParser.ABSENT_DB_NAME;
import static org.neo4j.util.Preconditions.checkState;
import static org.neo4j.values.storable.Values.stringArray;

/**
 * The READY state indicates that the connection is ready to accept a
 * new RUN request. This is the "normal" state for a connection and
 * becomes available after successful authorisation and when not
 * executing another statement. It is this that ensures that statements
 * must be executed in series and each must wait for the previous
 * statement to complete.
 */
public class ReadyState implements BoltStateMachineState
{
    private BoltStateMachineState streamingState;
    private BoltStateMachineState interruptedState;
    private BoltStateMachineState failedState;
    private BoltResult txBeginCommitRollbackResponse;

    @Override
    public BoltStateMachineState process( RequestMessage message, StateMachineContext context ) throws BoltConnectionFatality
    {
        assertInitialized();
        if ( message instanceof RunMessage )
        {
            return processRunMessage( (RunMessage) message, context );
        }
        if ( message instanceof PullAllMessage )
        {
            return processStreamingMessageAfterRunBeginCommitRollback( context, true );
        }
        else if ( message instanceof DiscardAllMessage )
        {
            return processStreamingMessageAfterRunBeginCommitRollback( context, false );
        }
        if ( message instanceof ResetMessage )
        {
            return processResetMessage( context );
        }
        if ( message instanceof InterruptSignal )
        {
            return interruptedState;
        }
        return null;
    }

    @Override
    public String name()
    {
        return "READY";
    }

    public void setStreamingState( BoltStateMachineState streamingState )
    {
        this.streamingState = streamingState;
    }

    public void setInterruptedState( BoltStateMachineState interruptedState )
    {
        this.interruptedState = interruptedState;
    }

    public void setFailedState( BoltStateMachineState failedState )
    {
        this.failedState = failedState;
    }

    private BoltStateMachineState processRunMessage( RunMessage message, StateMachineContext context ) throws BoltConnectionFatality
    {
        try
        {
            long start = context.clock().millis();
            StatementProcessor statementProcessor = context.setCurrentStatementProcessorForDatabase( ABSENT_DB_NAME );
            BoltStateMachineState state = processRunMessage( message, statementProcessor, context );
            long end = context.clock().millis();
            context.connectionState().onMetadata( "result_available_after", Values.longValue( end - start ) );

            return state;
        }
        catch ( AuthorizationExpiredException e )
        {
            context.handleFailure( e, true );
            return failedState;
        }
        catch ( Throwable t )
        {
            context.handleFailure( t, false );
            return failedState;
        }
    }

    private BoltStateMachineState processRunMessage( RunMessage message, StatementProcessor statementProcessor, StateMachineContext context ) throws Exception
    {
        BoltStateMachineState nextState = this;
        StatementMetadata statementMetadata = StatementMetadata.EMPTY;
        if ( isBegin( message ) )
        {
            var bookmarks = BookmarksParserV1.INSTANCE.parseBookmarks( message.params() );
            statementProcessor.beginTransaction( bookmarks );
            txBeginCommitRollbackResponse = BoltResult.EMPTY;
        }
        else if ( isCommit( message ) )
        {
            Bookmark bookmark = statementProcessor.commitTransaction();
            txBeginCommitRollbackResponse = new BookmarkResult( bookmark );
        }
        else if ( isRollback( message ) )
        {
            statementProcessor.rollbackTransaction();
            txBeginCommitRollbackResponse = BoltResult.EMPTY;
        }
        else
        {
            statementMetadata = statementProcessor.run( message.statement(), message.params() );
            nextState = streamingState;
        }
        context.connectionState().onMetadata( "fields", stringArray( statementMetadata.fieldNames() ) );
        return nextState;
    }

    private BoltStateMachineState processStreamingMessageAfterRunBeginCommitRollback( StateMachineContext context, boolean pull )
            throws BoltConnectionFatality
    {
        try
        {
            if ( txBeginCommitRollbackResponse == null )
            {
                // This means a PULL_ALL or DISCARD_ALL happens before Begin/Commit/Rollback
                return null;
            }
            ResultConsumer resultConsumer = new ResultConsumerV1Adaptor( context, pull );
            resultConsumer.consume( txBeginCommitRollbackResponse );
            txBeginCommitRollbackResponse = null;
            return this;
        }
        catch ( Throwable e )
        {
            context.handleFailure( e, false );
            return failedState;
        }
    }

    private BoltStateMachineState processResetMessage( StateMachineContext context ) throws BoltConnectionFatality
    {
        txBeginCommitRollbackResponse = null;
        boolean success = context.resetMachine();
        return success ? this : failedState;
    }

    private void assertInitialized()
    {
        checkState( streamingState != null, "Streaming state not set" );
        checkState( interruptedState != null, "Interrupted state not set" );
        checkState( failedState != null, "Failed state not set" );
    }
}
