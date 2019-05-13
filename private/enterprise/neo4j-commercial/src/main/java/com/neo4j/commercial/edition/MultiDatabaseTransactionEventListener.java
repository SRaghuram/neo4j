/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.commercial.edition;

import com.neo4j.dbms.database.MultiDatabaseManager;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.database.DatabaseContext;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.dbms.database.DatabaseNotFoundException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.event.LabelEntry;
import org.neo4j.graphdb.event.PropertyEntry;
import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.graphdb.event.TransactionEventListenerAdapter;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;

import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;

public class MultiDatabaseTransactionEventListener extends TransactionEventListenerAdapter<Object>
{
    private static final Label DATABASE_LABEL = Label.label( "Database" );
    private static final Label DELETED_DATABASE_LABEL = Label.label( "DeletedDatabase" );
    private static final String STATUS_PROPERTY_NAME = "status";
    private static final String ONLINE_STATUS = "online";
    private static final String OFFLINE_STATUS = "offline";
    private static final String NAME_PROPERTY_NAME = "name";

    private final DatabaseManager<?> databaseManager;

    MultiDatabaseTransactionEventListener( DatabaseManager databaseManager )
    {
        this.databaseManager = databaseManager;
    }

    @Override
    public Object beforeCommit( TransactionData data, GraphDatabaseService databaseService )
    {
        inspectCreatedDatabases( data );
        for ( PropertyEntry<Node> assignedNodeProperty : data.assignedNodeProperties() )
        {
            Node node = assignedNodeProperty.entity();
            if ( node.hasLabel( DATABASE_LABEL ) )
            {
                String name = getDatabaseName( node );
                if ( STATUS_PROPERTY_NAME.equals( assignedNodeProperty.key() ) )
                {
                    DatabaseId databaseId = new DatabaseId( name );
                    String newStatus = (String) assignedNodeProperty.value();
                    switch ( newStatus )
                    {
                    case ONLINE_STATUS:
                        databaseManager.startDatabase( databaseId );
                        break;
                    case OFFLINE_STATUS:
                        databaseManager.stopDatabase( databaseId );
                        break;
                    default:
                        throw new IllegalArgumentException( "Unsupported database status: " + newStatus );
                    }
                }
            }
        }
        for ( LabelEntry label : data.assignedLabels() )
        {
            if ( label.label().equals( DELETED_DATABASE_LABEL ) )
            {
                String name = getDatabaseName( label.node() );
                databaseManager.dropDatabase( new DatabaseId( name ) );
            }
        }
        return null;
    }

    private static String getDatabaseName( Node node )
    {
        return (String) node.getProperty( NAME_PROPERTY_NAME );
    }

    private void inspectCreatedDatabases( TransactionData data )
    {
        for ( Node createdNode : data.createdNodes() )
        {
            if ( createdNode.hasLabel( DATABASE_LABEL ) )
            {
                databaseManager.createDatabase( new DatabaseId( getDatabaseName( createdNode ) ) );
            }
        }
    }

    static void createDatabasesFromSystem( DatabaseManager<?> databaseManager, Config config )
    {
        final String defaultDatabase = config.get( GraphDatabaseSettings.default_database );
        GraphDatabaseFacade systemFacade = databaseManager.getDatabaseContext( new DatabaseId( SYSTEM_DATABASE_NAME ) )
                .map( DatabaseContext::databaseFacade ).orElseThrow( () -> new DatabaseNotFoundException( SYSTEM_DATABASE_NAME ) );
        try ( Transaction tx = systemFacade.beginTx() )
        {
            //TODO:extract common validators
            systemFacade.findNodes( DATABASE_LABEL ).stream()
                    .filter( node -> ONLINE_STATUS.equals( node.getProperty( STATUS_PROPERTY_NAME ) ) )
                    .map( MultiDatabaseTransactionEventListener::getDatabaseName )
                    .filter( s -> !s.equals( defaultDatabase ) && !s.equals( SYSTEM_DATABASE_NAME ) )
                    .forEach( name -> databaseManager.createDatabase( new DatabaseId( name ) ) );
            systemFacade.findNodes( DATABASE_LABEL ).stream()
                    .filter( node -> OFFLINE_STATUS.equals( node.getProperty( STATUS_PROPERTY_NAME ) ) )
                    .map( MultiDatabaseTransactionEventListener::getDatabaseName )
                    .filter( s -> !s.equals( defaultDatabase ) && !s.equals( GraphDatabaseSettings.SYSTEM_DATABASE_NAME ) )
                    .forEach( name -> ((MultiDatabaseManager<?>) databaseManager).createStoppedDatabase( new DatabaseId( name ) ) );
            tx.success();
        }
    }
}
