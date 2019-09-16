/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.bolt;

import com.neo4j.fabric.config.FabricConfig;
import com.neo4j.fabric.executor.FabricException;
import com.neo4j.fabric.stream.Record;
import com.neo4j.fabric.stream.Rx2SyncStream;
import com.neo4j.fabric.stream.StatementResult;
import com.neo4j.fabric.stream.summary.EmptySummary;

import java.util.List;

import org.neo4j.bolt.dbapi.BoltQueryExecution;
import org.neo4j.graphdb.ExecutionPlanDescription;
import org.neo4j.graphdb.Notification;
import org.neo4j.graphdb.QueryExecutionType;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.query.QueryExecution;
import org.neo4j.kernel.impl.query.QuerySubscriber;

public class BoltQueryExecutionImpl implements BoltQueryExecution
{

    private final QueryExecutionImpl queryExecution;
    private final EmptySummary summary = new EmptySummary();

    public BoltQueryExecutionImpl( StatementResult statementResult, QuerySubscriber subscriber, FabricConfig fabricConfig )
    {
        var config = fabricConfig.getDataStream();
        var rx2SyncStream = new Rx2SyncStream( statementResult, config.getBufferLowWatermark(), config.getBufferSize(), config.getSyncBatchSize() );
        queryExecution = new QueryExecutionImpl( rx2SyncStream, subscriber );
    }

    @Override
    public QueryExecution getQueryExecution()
    {
        return queryExecution;
    }

    @Override
    public void close()
    {
        queryExecution.cancel();
    }

    @Override
    public void terminate()
    {
        queryExecution.cancel();
    }

    private class QueryExecutionImpl implements QueryExecution
    {

        private final Rx2SyncStream rx2SyncStream;
        private final QuerySubscriber subscriber;
        private final List<String> columns;
        private boolean hasMore = true;
        private boolean initialised;

        private QueryExecutionImpl( Rx2SyncStream rx2SyncStream, QuerySubscriber subscriber )
        {
            this.rx2SyncStream = rx2SyncStream;
            this.subscriber = subscriber;
            columns = rx2SyncStream.getColumns();
        }

        @Override
        public QueryExecutionType executionType()
        {
            return summary.executionType();
        }

        @Override
        public ExecutionPlanDescription executionPlanDescription()
        {
            return summary.executionPlanDescription();
        }

        @Override
        public Iterable<Notification> getNotifications()
        {
            return summary.getNotifications();
        }

        @Override
        public String[] fieldNames()
        {
            return columns.toArray( new String[0] );
        }

        @Override
        public void request( long numberOfRecords ) throws Exception
        {
            if ( !hasMore )
            {
                return;
            }

            if ( !initialised )
            {
                initialised = true;
                subscriber.onResult( rx2SyncStream.getColumns().size() );
            }

            try
            {
                for ( int i = 0; i < numberOfRecords; i++ )
                {
                    Record record = rx2SyncStream.readRecord();

                    if ( record == null )
                    {
                        hasMore = false;
                        subscriber.onResultCompleted( summary.getQueryStatistics() );
                        return;
                    }

                    subscriber.onRecord();
                    publishFields( record );
                    subscriber.onRecordCompleted();
                }
            }
            catch ( Exception e )
            {
                if ( e instanceof FabricException )
                {
                    throw e;
                }
                else
                {
                    throw new FabricException( Status.Statement.ExecutionFailed, e );
                }
            }
        }

        private void publishFields( Record record ) throws Exception
        {
            for ( int i = 0; i < columns.size(); i++ )
            {
                subscriber.onField( record.getValue( i ) );
            }
        }

        @Override
        public void cancel()
        {
            rx2SyncStream.close();
        }

        @Override
        public boolean await()
        {
            return hasMore;
        }
    }
}
