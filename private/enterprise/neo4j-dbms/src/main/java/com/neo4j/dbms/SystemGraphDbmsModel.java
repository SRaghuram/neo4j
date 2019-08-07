/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.event.LabelEntry;
import org.neo4j.graphdb.event.PropertyEntry;
import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.database.DatabaseIdFactory;

import static com.neo4j.dbms.SystemGraphDbmsModel.DatabaseState.DELETED;
import static com.neo4j.dbms.SystemGraphDbmsModel.DatabaseState.OFFLINE;
import static com.neo4j.dbms.SystemGraphDbmsModel.DatabaseState.ONLINE;
import static org.neo4j.dbms.database.SystemGraphInitializer.DATABASE_LABEL;
import static org.neo4j.dbms.database.SystemGraphInitializer.DATABASE_NAME_PROPERTY;
import static org.neo4j.dbms.database.SystemGraphInitializer.DATABASE_STATUS_PROPERTY;
import static org.neo4j.dbms.database.SystemGraphInitializer.DATABASE_UUID_PROPERTY;
import static org.neo4j.dbms.database.SystemGraphInitializer.DELETED_DATABASE_LABEL;

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

    private GraphDatabaseService systemDatabase;

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
            tx.commit();
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

            tx.commit();
        }

        // TODO: Declare exceptions!
        return databases;
    }

    private DatabaseState getOnlineStatus( Node node )
    {
        String onlineStatus = (String) node.getProperty( DATABASE_STATUS_PROPERTY );

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
        var name = (String) node.getProperty( DATABASE_NAME_PROPERTY );
        var uuid = UUID.fromString( (String) node.getProperty( DATABASE_UUID_PROPERTY ) );
        return DatabaseIdFactory.from( name, uuid );
    }
}
