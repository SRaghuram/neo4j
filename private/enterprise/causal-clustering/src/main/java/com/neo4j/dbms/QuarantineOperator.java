/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms;

import com.neo4j.causalclustering.common.state.ClusterStateStorageFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Optional;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementException;
import org.neo4j.dbms.api.DatabaseNotFoundException;
import org.neo4j.io.state.SimpleStorage;
import org.neo4j.kernel.database.DatabaseIdRepository;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static com.neo4j.dbms.EnterpriseOperatorState.QUARANTINED;

public class QuarantineOperator extends DbmsOperator
{
    private final Log log;
    private final DatabaseIdRepository databaseIdRepository;
    private final ClusterStateStorageFactory storageFactory;

    public QuarantineOperator( LogProvider logProvider, DatabaseIdRepository databaseIdRepository, ClusterStateStorageFactory storageFactory )
    {
        this.log = logProvider.getLog( getClass() );
        this.databaseIdRepository = databaseIdRepository;
        this.storageFactory = storageFactory;
    }

    public String putIntoQuarantine( String databaseName, String message )
    {
        if ( GraphDatabaseSettings.SYSTEM_DATABASE_NAME.equals( databaseName ) )
        {
            throw new DatabaseManagementException( "System database can't be quarantined. Please shutdown this cluster instance, or the entire DBMS instead." );
        }
        var alreadyInQuarantine = desired.get( databaseName );
        if ( alreadyInQuarantine != null )
        {
            return String.format( "Already in quarantine: %s", alreadyInQuarantine.failure().map( Throwable::getMessage ).orElseThrow() );
        }
        var databaseId = databaseIdRepository.getByName( databaseName );
        if ( databaseId.isEmpty() )
        {
            throw new DatabaseNotFoundException( "Cannot find database: " + databaseName );
        }
        addToDesires( databaseId.get(), message );
        trigger( databaseId.get() );
        return message;
    }

    public String removeFromQuarantine( String databaseName )
    {
        var previousState = Optional.ofNullable( desired.remove( databaseName ) );
        if ( previousState.isPresent() )
        {
            var databaseId = databaseIdRepository.getByName( databaseName )
                    .orElseGet( () -> previousState.map( EnterpriseDatabaseState::databaseId )
                            .orElseThrow( () -> new DatabaseNotFoundException( "Cannot find database: " + databaseName ) ) );
            trigger( databaseId );
        }
        return getStoredMessage( previousState ).orElse( "Not quarantined previously" );
    }

    void setQuarantineMarker( NamedDatabaseId databaseId )
    {
        var message = getStoredMessage( Optional.ofNullable( desired.get( databaseId.name() ) ) )
                .orElseThrow( () -> new IllegalStateException( "Setting quarantine without message is not possible" ) );
        var ctx = new QuarantineMarkerContext( databaseId );
        ctx.writeQuarantineMarker( message );
    }

    void removeQuarantineMarker( NamedDatabaseId databaseId )
    {
        var ctx = new QuarantineMarkerContext( databaseId );
        ctx.removeQuarantineMarker();
    }

    Optional<EnterpriseDatabaseState> checkQuarantineMarker( NamedDatabaseId databaseId )
    {
        var ctx = new QuarantineMarkerContext( databaseId );
        var message = ctx.readQuarantineMarker();
        return message.map( text -> addToDesires( ctx.id, text ) );
    }

    private Optional<String> getStoredMessage( @SuppressWarnings( "OptionalUsedAsFieldOrParameterType" ) Optional<EnterpriseDatabaseState> previousState )
    {
        return previousState.flatMap( EnterpriseDatabaseState::failure ).map( Throwable::getMessage );
    }

    private EnterpriseDatabaseState addToDesires( NamedDatabaseId id, String message )
    {
        var state = new EnterpriseDatabaseState( id, QUARANTINED ).failed( new DatabaseManagementException( message ) );
        desired.put( id.name(), state );
        return state;
    }

    private void trigger( NamedDatabaseId id )
    {
        trigger( ReconcilerRequest.priorityTarget( id ).build() );
    }

    private class QuarantineMarkerContext
    {
        NamedDatabaseId id;
        SimpleStorage<QuarantineMarker> markerStorage;

        QuarantineMarkerContext( NamedDatabaseId id )
        {
            this.id = id;
            this.markerStorage = storageFactory.createQuarantineMarkerStorage( id.name() );
        }

        void writeQuarantineMarker( String message )
        {
            try
            {
                markerStorage.writeState( new QuarantineMarker( message ) );
            }
            catch ( IOException e )
            {
                var error = String.format( "Quarantine marker file cannot be written for database %s", id );
                log.warn( error, e );
                throw new UncheckedIOException( error, e );
            }
        }

        Optional<String> readQuarantineMarker()
        {
            if ( !markerStorage.exists() )
            {
                return Optional.empty();
            }
            try
            {
                var marker = markerStorage.readState();
                if ( marker.message().isEmpty() )
                {
                    log.info( "Quarantine marker file cannot be read for database %s", id );
                    return Optional.of( "Quarantine marker is present, but empty" );
                }
                return Optional.of( marker.message() );
            }
            catch ( IOException e )
            {
                log.warn( String.format( "Quarantine marker file cannot be read for database %s", id ), e );
                return Optional.of( "Quarantine marker is present, but unable to read" );
            }
        }

        void removeQuarantineMarker()
        {
            if ( markerStorage.exists() )
            {
                try
                {
                    markerStorage.removeState();
                }
                catch ( IOException e )
                {
                    var error = String.format( "Quarantine marker file cannot be removed for database %s", id );
                    log.warn( error, e );
                    throw new UncheckedIOException( error, e );
                }
            }
        }
    }
}
