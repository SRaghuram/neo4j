/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.ha.correctness;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.collection.Pair;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.neo4j.graphdb.Label.label;

public class ProofDatabase
{
    private final GraphDatabaseService gds;
    private final Map<ClusterState, Node> stateNodes = new HashMap<>();

    public ProofDatabase( String location )
    {
        File dbDir = new File( location );
        cleanDbDir( dbDir );
        this.gds = new TestGraphDatabaseFactory().newEmbeddedDatabase( dbDir );
    }

    public Node newState( ClusterState state )
    {
        try ( Transaction tx = gds.beginTx() )
        {
            Node node = gds.createNode( label( "State" ) );
            node.setProperty( "description", state.toString() );
            tx.success();

            stateNodes.put( state, node );
            return node;
        }
    }

    public void newStateTransition( ClusterState originalState,
                            Pair<ClusterAction, ClusterState> transition )
    {
        try ( Transaction tx = gds.beginTx() )
        {
            Node stateNode = stateNodes.get( originalState );

            Node subStateNode = newState( transition.other() );

            Relationship msg = stateNode.createRelationshipTo( subStateNode, RelationshipType.withName( "MESSAGE" ) );
            msg.setProperty( "description", transition.first().toString() );
            tx.success();
        }
    }

    private void cleanDbDir( File dbDir )
    {
        if ( dbDir.exists() )
        {
            try
            {
                FileUtils.deleteRecursively( dbDir );
            }
            catch ( IOException e )
            {
                throw new RuntimeException( e );
            }
        }
        else
        {
            dbDir.getParentFile().mkdirs();
        }
    }

    public void shutdown()
    {
        gds.shutdown();
    }

    public boolean isKnownState( ClusterState state )
    {
        return stateNodes.containsKey( state );
    }

    public long numberOfKnownStates()
    {
        return stateNodes.size();
    }

    public long id( ClusterState nextState )
    {
        return stateNodes.get(nextState).getId();
    }

    public void export( GraphVizExporter graphVizExporter ) throws IOException
    {
        graphVizExporter.export( gds );
    }
}
