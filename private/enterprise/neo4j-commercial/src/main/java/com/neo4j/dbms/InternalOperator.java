/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import org.neo4j.kernel.database.DatabaseId;

import static com.neo4j.dbms.OperatorState.STOPPED;

/**
 * Database operator for system internal components.
 */
// TODO: Use this from internal components, e.g. for store copy.
public class InternalOperator implements Operator
{
    private final List<StoppingContext> stoppers = new CopyOnWriteArrayList<>();
    private final OperatorConnector connector;

    public InternalOperator( OperatorConnector connector )
    {
        this.connector = connector;
        connector.register( this );
    }

    @Override
    public Map<DatabaseId,OperatorState> getDesired()
    {
        var stopped = new HashMap<DatabaseId,OperatorState>();
        for ( StoppingContext context : stoppers )
        {
            stopped.put( context.databaseId, STOPPED );
        }
        return stopped;
    }

    public Startable stopDatabase( DatabaseId databaseId )
    {
        StoppingContext stoppingContext = new StoppingContext( databaseId );
        stoppers.add( stoppingContext );

        connector.trigger();

        return stoppingContext;
    }

    private class StoppingContext implements Startable
    {
        private final DatabaseId databaseId;

        private StoppingContext( DatabaseId databaseId )
        {
            this.databaseId = databaseId;
        }

        @Override
        public void start()
        {
            boolean exists = stoppers.remove( this );
            if ( !exists )
            {
                throw new IllegalStateException( "Start was already called for " + databaseId );
            }
            connector.trigger();
        }
    }
}
