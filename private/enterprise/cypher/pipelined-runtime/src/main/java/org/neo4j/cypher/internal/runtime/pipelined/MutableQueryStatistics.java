/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

    public void setProperty()
    {
        setProperties++;
    }

    public void addLabel()
    {
        addedLabels++;
    }

    public void removeLabel()
    {
        removedLabels++;
    }

    @Override
    public int getNodesCreated()
    {
        return createdNodes++;
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
}
