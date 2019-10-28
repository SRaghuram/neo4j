/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.dbms.database.SystemGraphDbmsModel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.event.PropertyEntry;
import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.database.DatabaseIdFactory;

import static com.neo4j.dbms.OperatorState.DROPPED;
import static com.neo4j.dbms.OperatorState.STARTED;
import static com.neo4j.dbms.OperatorState.STOPPED;

/**
 * Utility class for accessing the DBMS model in the system database.
 */
public class EnterpriseSystemGraphDbmsModel extends SystemGraphDbmsModel
{
    protected final Supplier<GraphDatabaseService> systemDatabase;

    public EnterpriseSystemGraphDbmsModel( Supplier<GraphDatabaseService> systemDatabase )
    {
        this.systemDatabase = systemDatabase;
    }

    Collection<DatabaseId> updatedDatabases( TransactionData transactionData )
    {
        Collection<DatabaseId> updatedDatabases;

        try ( var tx = systemDatabase.get().beginTx() )
        {
            var changedDatabases = Iterables.stream( transactionData.assignedNodeProperties() )
                    .map( PropertyEntry::entity )
                    .map( n -> tx.getNodeById( n.getId() ) )
                    .filter( n -> n.hasLabel( DATABASE_LABEL ) )
                    .map( this::getDatabaseId )
                    .distinct();

            var deletedDatabases = Iterables.stream( transactionData.assignedLabels() )
                    .filter( l -> l.label().equals( DELETED_DATABASE_LABEL ) )
                    .map( e -> tx.getNodeById( e.node().getId() ) )
                    .map( this::getDatabaseId );

            updatedDatabases = Stream.concat( changedDatabases, deletedDatabases ).collect( Collectors.toList() );
            tx.commit();
        }

        return updatedDatabases;
    }

    Map<String,DatabaseState> getDatabaseStates()
    {
        Map<String,DatabaseState> databases = new HashMap<>();

        try ( var tx = systemDatabase.get().beginTx() )
        {
            var deletedDatabases = tx.findNodes( DELETED_DATABASE_LABEL ).stream().collect( Collectors.toList() );
            deletedDatabases.forEach( node -> databases.put( getDatabaseName( node ), new DatabaseState( getDatabaseId( node ), DROPPED ) ) );

            // existing databases supersede dropped databases of the same name, because they represent a later state
            // there can only ever be exactly 0 or 1 existing database for a particular name and
            // database nodes can only ever go from the existing to the dropped state
            var existingDatabases = tx.findNodes( DATABASE_LABEL ).stream().collect( Collectors.toList() );
            existingDatabases.forEach( node -> databases.put( getDatabaseName( node ), new DatabaseState( getDatabaseId( node ), getOnlineStatus( node ) ) ) );

            tx.commit();
        }

        // TODO: Declare exceptions!
        return databases;
    }

    Optional<OperatorState> getStatus( DatabaseId databaseId )
    {
        Optional<OperatorState> result;
        try ( var tx = systemDatabase.get().beginTx() )
        {
            var uuid = databaseId.uuid().toString();

            var databaseNode = tx.findNode( DATABASE_LABEL, DATABASE_UUID_PROPERTY, uuid );
            var deletedDatabaseNode = tx.findNode( DELETED_DATABASE_LABEL, DATABASE_UUID_PROPERTY, uuid );

            if ( databaseNode != null )
            {
                 result = Optional.of( getOnlineStatus( databaseNode ) );
            }
            else if ( deletedDatabaseNode != null )
            {
                result =  Optional.of( DROPPED );
            }
            else
            {
                result = Optional.empty();
            }

            tx.commit();
        }
        return result;
    }

    private OperatorState getOnlineStatus( Node node )
    {
        String onlineStatus = (String) node.getProperty( DATABASE_STATUS_PROPERTY );

        switch ( onlineStatus )
        {
        case "online":
            return STARTED;
        case "offline":
            return STOPPED;
        default:
            throw new IllegalArgumentException( "Unsupported database status: " + onlineStatus );
        }
    }

    private DatabaseId getDatabaseId( Node node )
    {
        var name = (String) node.getProperty( DATABASE_NAME_PROPERTY );
        var uuid = UUID.fromString( (String) node.getProperty( DATABASE_UUID_PROPERTY ) );
        return DatabaseIdFactory.from( name, uuid );
    }

    private String getDatabaseName( Node node )
    {
        return (String) node.getProperty( DATABASE_NAME_PROPERTY );
    }
}
