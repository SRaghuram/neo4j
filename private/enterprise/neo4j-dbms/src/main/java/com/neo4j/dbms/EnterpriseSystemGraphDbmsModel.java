/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.neo4j.dbms.OperatorState;
import org.neo4j.dbms.database.SystemGraphDbmsModel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.event.PropertyEntry;
import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.kernel.database.DatabaseIdFactory;
import org.neo4j.kernel.database.NamedDatabaseId;

import static com.neo4j.dbms.EnterpriseOperatorState.DROPPED;
import static com.neo4j.dbms.EnterpriseOperatorState.DROPPED_DUMPED;
import static com.neo4j.dbms.EnterpriseOperatorState.STARTED;
import static com.neo4j.dbms.EnterpriseOperatorState.STOPPED;

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

    DatabaseUpdates updatedDatabases( TransactionData transactionData )
    {
        Set<NamedDatabaseId> changedDatabases;
        Set<NamedDatabaseId> droppedDatabases;

        var updatedNodeIds  = extractUpdatedNodes( transactionData );
        var changedNodes = updatedNodeIds.changedNodes;
        var droppedNodes = updatedNodeIds.deletedDatabaseNodes;

        try ( var tx = systemDatabase.get().beginTx() )
        {
            changedDatabases = changedNodes.stream()
                                               .map( tx::getNodeById )
                                               .filter( n -> n.hasLabel( DATABASE_LABEL ) || n.hasLabel( DELETED_DATABASE_LABEL ) )
                                               .map( this::getDatabaseId )
                                               .collect( Collectors.toSet() );

            droppedDatabases = droppedNodes.stream()
                                        .map( tx::getNodeById )
                                        .filter( n -> n.hasLabel( DELETED_DATABASE_LABEL ) )
                                        .map( this::getDatabaseId )
                                        .collect( Collectors.toSet() );

            tx.commit();
        }

        return new DatabaseUpdates( changedDatabases, droppedDatabases );
    }

    private UpdatedNodeIds extractUpdatedNodes( TransactionData transactionData )
    {
        var propertiesByNode = Iterables.stream( transactionData.assignedNodeProperties() )
                                        .collect( Collectors.groupingBy( PropertyEntry::entity ) );
        var newDeletedDatabaseNodes = Iterables.stream( transactionData.assignedLabels() )
                                               .filter( l -> l.label().equals( DELETED_DATABASE_LABEL ) )
                                               .map( e -> e.node().getId() )
                                               .collect( Collectors.toSet() );

        var changedNodes = propertiesByNode.keySet().stream()
                        .map( Node::getId )
                        .filter( Predicate.not( newDeletedDatabaseNodes::contains ) )
                        .collect( Collectors.toSet() );

        return new UpdatedNodeIds( newDeletedDatabaseNodes, changedNodes );
    }

    Map<String,EnterpriseDatabaseState> getDatabaseStates()
    {
        Map<String,EnterpriseDatabaseState> databases = new HashMap<>();

        try ( var tx = systemDatabase.get().beginTx() )
        {
            var deletedDatabases = tx.findNodes( DELETED_DATABASE_LABEL ).stream().collect( Collectors.toList() );
            deletedDatabases.forEach( node -> databases.put( getDatabaseName( node ),
                    new EnterpriseDatabaseState( getDatabaseId( node ), getDroppedStatus( node ) ) ) );

            // existing databases supersede dropped databases of the same name, because they represent a later state
            // there can only ever be exactly 0 or 1 existing database for a particular name and
            // database nodes can only ever go from the existing to the dropped state
            var existingDatabases = tx.findNodes( DATABASE_LABEL ).stream().collect( Collectors.toList() );
            existingDatabases.forEach( node ->
                    databases.put( getDatabaseName( node ), new EnterpriseDatabaseState( getDatabaseId( node ), getOnlineStatus( node ) ) ) );

            tx.commit();
        }
        return databases;
    }

    Optional<OperatorState> getStatus( NamedDatabaseId namedDatabaseId )
    {
        Optional<OperatorState> result;
        try ( var tx = systemDatabase.get().beginTx() )
        {
            var uuid = namedDatabaseId.databaseId().uuid().toString();

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

    private EnterpriseOperatorState getOnlineStatus( Node node )
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

    private EnterpriseOperatorState getDroppedStatus( Node node )
    {
        if ( node.hasProperty( DUMP_DATA_PROPERTY ) && (Boolean) node.getProperty( DUMP_DATA_PROPERTY ) )
        {
            return DROPPED_DUMPED;
        }
        return DROPPED;
    }

    private NamedDatabaseId getDatabaseId( Node node )
    {
        var name = (String) node.getProperty( DATABASE_NAME_PROPERTY );
        var uuid = UUID.fromString( (String) node.getProperty( DATABASE_UUID_PROPERTY ) );
        return DatabaseIdFactory.from( name, uuid );
    }

    private String getDatabaseName( Node node )
    {
        return (String) node.getProperty( DATABASE_NAME_PROPERTY );
    }

    private static class UpdatedNodeIds
    {
        private final Set<Long> deletedDatabaseNodes;
        private final Set<Long> changedNodes;

        private UpdatedNodeIds( Set<Long> deletedDatabaseNodes, Set<Long> changedNodes )
        {
            this.deletedDatabaseNodes = deletedDatabaseNodes;
            this.changedNodes = changedNodes;
        }
    }
}
