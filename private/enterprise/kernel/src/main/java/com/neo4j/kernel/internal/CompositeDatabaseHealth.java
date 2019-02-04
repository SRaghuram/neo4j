/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.internal;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

import org.neo4j.collection.Streams;
import org.neo4j.monitoring.SingleDatabaseHealth;
import org.neo4j.logging.Log;
import org.neo4j.monitoring.DatabaseHealth;
import org.neo4j.monitoring.DatabasePanicEventGenerator;
import org.neo4j.util.VisibleForTesting;

/**
 * Composite database health service that makes decisions about its health based on multiple underlying database specific health services.
 * Any panics, asserts and other checks are redistributed to all underlying health services
 *
 * TODO: cleanup on database shutdown
 */
public class CompositeDatabaseHealth implements DatabaseHealth, HealthService
{
    private final ConcurrentMap<String,SingleDatabaseHealth> healthServices;
    private Throwable rootCauseOfPanic;

    public CompositeDatabaseHealth()
    {
        this.healthServices = new ConcurrentHashMap<>();
    }

    @VisibleForTesting
    CompositeDatabaseHealth( Map<String,SingleDatabaseHealth> healthServices )
    {
        this.healthServices = new ConcurrentHashMap<>( healthServices );
    }

    @Override
    public SingleDatabaseHealth createDatabaseHealth( String databaseName, DatabasePanicEventGenerator dbpe, Log log )
    {
        SingleDatabaseHealth databaseHealth = new SingleDatabaseHealth( dbpe, log );
        healthServices.putIfAbsent( databaseName, databaseHealth );
        return databaseHealth;
    }

    @Override
    public void removeDatabaseHealth( String databaseName )
    {
        healthServices.remove( databaseName );
    }

    @Override
    public Optional<SingleDatabaseHealth> getDatabaseHealth( String databaseName )
    {
        return Optional.ofNullable( healthServices.get( databaseName ) );
    }

    @Override
    public <EXCEPTION extends Throwable> void assertHealthy( Class<EXCEPTION> panicDisguise ) throws EXCEPTION
    {
        for ( SingleDatabaseHealth healthService : healthServices.values() )
        {
            healthService.assertHealthy( panicDisguise );
        }
    }

    @Override
    public void panic( Throwable cause )
    {
        rootCauseOfPanic = cause;
        for ( SingleDatabaseHealth healthService : healthServices.values() )
        {
            healthService.panic( cause );
        }
    }

    @Override
    public boolean isHealthy()
    {
        return healthServices.values().stream().allMatch( DatabaseHealth::isHealthy );
    }

    @Override
    public boolean healed()
    {
        return healthServices.values().stream().allMatch( DatabaseHealth::healed );
    }

    @Override
    public Throwable cause()
    {
        List<Throwable> exceptions = healthServices.values().stream().flatMap( h -> Streams.ofNullable( h.cause() ) ).collect( Collectors.toList() );
        if ( exceptions.isEmpty() )
        {
            return null;
        }
        Throwable identity = rootCauseOfPanic != null ? rootCauseOfPanic : new Exception( "Some of the databases have panicked!" );
        return exceptions.stream()
                .reduce( identity, ( acc, next ) ->
                {
                    acc.addSuppressed( next );
                    return acc;
                } );
    }
}
