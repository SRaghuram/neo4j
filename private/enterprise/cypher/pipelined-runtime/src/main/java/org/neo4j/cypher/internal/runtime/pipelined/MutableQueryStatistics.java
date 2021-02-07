/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined;

import org.neo4j.graphdb.QueryStatistics;

public class MutableQueryStatistics implements QueryStatistics
{
    private int createdNodes, deletedNodes, createdRelationships, deletedRelationships, setProperties, addedLabels, removedLabels;

    public void createNode()
    {
        createdNodes++;
    }

    public void deleteNode()
    {
        deletedNodes++;
    }

    public void createRelationship()
    {
        createdRelationships++;
    }

    public void deleteRelationship()
    {
        deletedRelationships++;
    }

    public void deleteRelationships( int deleteCount )
    {
        deletedRelationships += deleteCount;
    }

    public void setProperty()
    {
        setProperties++;
    }

    public void setProperties( long numberOfProperties )
    {
        setProperties += numberOfProperties;
    }

    public void addLabel()
    {
        addedLabels++;
    }

    public void addLabels( int n )
    {
        addedLabels += n;
    }

    public void removeLabel()
    {
        removedLabels++;
    }

    @Override
    public int getNodesCreated()
    {
        return createdNodes;
    }

    @Override
    public int getNodesDeleted()
    {
        return deletedNodes;
    }

    @Override
    public int getRelationshipsCreated()
    {
        return createdRelationships;
    }

    @Override
    public int getRelationshipsDeleted()
    {
        return deletedRelationships;
    }

    @Override
    public int getPropertiesSet()
    {
        return setProperties;
    }

    @Override
    public int getLabelsAdded()
    {
        return addedLabels;
    }

    @Override
    public int getLabelsRemoved()
    {
        return removedLabels;
    }

    @Override
    public int getIndexesAdded()
    {
        return 0;
    }

    @Override
    public int getIndexesRemoved()
    {
        return 0;
    }

    @Override
    public int getConstraintsAdded()
    {
        return 0;
    }

    @Override
    public int getConstraintsRemoved()
    {
        return 0;
    }

    @Override
    public int getSystemUpdates()
    {
        return 0;
    }

    @Override
    public boolean containsUpdates()
    {
       return createdNodes > 0 || deletedNodes > 0 || createdRelationships > 0 ||
              deletedRelationships > 0 || setProperties > 0 || addedLabels > 0 || removedLabels > 0;
    }

    @Override
    public boolean containsSystemUpdates()
    {
        return false;
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder();
        includeIfNonZero( builder, "Nodes created: ", createdNodes );
        includeIfNonZero( builder, "Relationships created: ", createdRelationships );
        includeIfNonZero( builder, "Properties set: ", setProperties );
        includeIfNonZero( builder, "Nodes deleted: ", deletedNodes );
        includeIfNonZero( builder, "Relationships deleted: ", deletedRelationships );
        includeIfNonZero( builder, "Labels added: ", addedLabels );
        includeIfNonZero( builder, "Labels removed: ", removedLabels );
        String result = builder.toString();

        return result.isEmpty() ? "<Nothing happened>" : result;
    }

    private void includeIfNonZero( StringBuilder builder, String message, long count )
    {
        if ( count > 0 )
        {
            builder.append( message ).append( count ).append( System.lineSeparator() );
        }
    }
}
