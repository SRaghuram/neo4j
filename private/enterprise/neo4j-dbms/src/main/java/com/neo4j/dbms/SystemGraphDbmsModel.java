/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.event.LabelEntry;
import org.neo4j.graphdb.event.PropertyEntry;
import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.database.DatabaseIdRepository;

import static com.neo4j.dbms.SystemGraphDbmsModel.DatabaseState.DELETED;
import static com.neo4j.dbms.SystemGraphDbmsModel.DatabaseState.OFFLINE;
import static com.neo4j.dbms.SystemGraphDbmsModel.DatabaseState.ONLINE;
import static org.neo4j.graphdb.Label.label;

/**
 * Utility class for accessing the DBMS model in the system database.
 */
public class SystemGraphDbmsModel
{
    enum DatabaseState
    {
        ONLINE,
        OFFLINE,
        DELETED
    }

    static final Label DATABASE_LABEL = label( "Database" );
    static final Label DELETED_DATABASE_LABEL = label( "DeletedDatabase" );

    private final DatabaseIdRepository databaseIdRepository;

    private GraphDatabaseService systemDatabase;

    SystemGraphDbmsModel( DatabaseIdRepository databaseIdRepository )
    {
        this.databaseIdRepository = databaseIdRepository;
    }

    public void setSystemDatabase( GraphDatabaseService systemDatabase )
    {
        this.systemDatabase = systemDatabase;
    }

    Collection<DatabaseId> updatedDatabases( TransactionData transactionData )
    {
        Collection<DatabaseId> updatedDatabases;

        try ( var tx = systemDatabase.beginTx() )
        {
            var changedDatabases = Iterables.stream( transactionData.assignedNodeProperties() )
                    .map( PropertyEntry::entity )
                    .filter( n -> n.hasLabel( DATABASE_LABEL ) )
                    .map( this::getDatabaseId )
                    .distinct();

            var deletedDatabases = Iterables.stream( transactionData.assignedLabels() )
                    .filter( l -> l.label().equals( DELETED_DATABASE_LABEL ) )
                    .map( LabelEntry::node )
                    .map( this::getDatabaseId );

            updatedDatabases = Stream.concat( changedDatabases, deletedDatabases ).collect(Collectors.toList() );
            tx.success();
        }

        return updatedDatabases;
    }

    Map<DatabaseId,DatabaseState> getDatabaseStates()
    {
        Map<DatabaseId,DatabaseState> databases = new HashMap<>();

        try ( var tx = systemDatabase.beginTx() )
        {
            var existingDatabases = systemDatabase.findNodes( DATABASE_LABEL ).stream().collect( Collectors.toList() );
            existingDatabases.forEach( node -> databases.put( getDatabaseId( node ), getOnlineStatus( node ) ) );

            var deletedDatabases = systemDatabase.findNodes( DELETED_DATABASE_LABEL ).stream().collect( Collectors.toList() );
            deletedDatabases.forEach( node -> databases.put( getDatabaseId( node ), DELETED ) );

            tx.success();
        }

        // TODO: Declare exceptions!
        return databases;
    }

    private DatabaseState getOnlineStatus( Node node )
    {
        String onlineStatus = (String) node.getProperty( "status" );

        switch ( onlineStatus )
        {
        case "online":
            return ONLINE;
        case "offline":
            return OFFLINE;
        default:
            throw new IllegalArgumentException( "Unsupported database status: " + onlineStatus );
        }
    }

    private DatabaseId getDatabaseId( Node node )
    {
        // TODO: Get the actual database ID.
        return databaseIdRepository.get( (String) node.getProperty( "name" ) );
    }
}
