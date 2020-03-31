/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.utils;

import reactor.core.publisher.Mono;

import java.util.function.Consumer;
import java.util.function.Function;

import org.neo4j.driver.AccessMode;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.reactive.RxTransaction;

public final class DriverUtils
{
    private final String defaultDatabaseName;

    public DriverUtils( String defaultDatabaseName )
    {
        this.defaultDatabaseName = defaultDatabaseName;
    }

    public SessionConfig sessionConfig()
    {
        return sessionConfig( null );
    }

    public SessionConfig sessionConfig( AccessMode accessMode )
    {
        SessionConfig.Builder builder = SessionConfig.builder();
        if ( defaultDatabaseName != null )
        {
            builder.withDatabase( defaultDatabaseName );
        }
        if ( accessMode != null )
        {
            builder.withDefaultAccessMode( accessMode );
        }
        return builder.build();
    }

    public <T> T inTx( Driver driver, Function<Transaction,T> workload )
    {
        try ( var session = driver.session( sessionConfig() ) )
        {
            return session.writeTransaction( workload::apply );
        }
    }

    public <T> T inTx( Driver driver, AccessMode accessMode, Function<Transaction,T> workload )
    {
        try ( var session = driver.session( sessionConfig( accessMode ) );
                var tx = session.beginTransaction() )
        {
            T value = workload.apply( tx );
            tx.commit();
            return value;
        }
    }

    public void doInTx( Driver driver, Consumer<Transaction> workload )
    {
        inTx( driver, tx ->
        {
            workload.accept( tx );
            return null;
        } );
    }

    public void doInTx( Driver driver, AccessMode accessMode, Consumer<Transaction> workload )
    {
        inTx( driver, accessMode, tx ->
        {
            workload.accept( tx );
            return null;
        } );
    }

    public <T> T inSession( Driver driver, Function<Session,T> workload )
    {
        try ( var session = driver.session( sessionConfig() ) )
        {
            return workload.apply( session );
        }
    }

    public void doInSession( Driver driver, Consumer<Session> workload )
    {
        try ( var session = driver.session( sessionConfig() ) )
        {
            workload.accept( session );
        }
    }

    public void doInSession( Driver driver, AccessMode accessMode, Consumer<Session> workload )
    {
        try ( var session = driver.session( sessionConfig( accessMode ) ) )
        {
            workload.accept( session );
        }
    }

    public <T> T inRxTx( Driver driver, Function<RxTransaction,T> workload )
    {
        var session = driver.rxSession( sessionConfig() );
        try
        {
            var tx = Mono.from( session.beginTransaction() ).block();
            try
            {
                return workload.apply( tx );
            }
            finally
            {
                Mono.from( tx.commit() ).block();
            }
        }
        finally
        {
            session.close();
        }
    }
}
