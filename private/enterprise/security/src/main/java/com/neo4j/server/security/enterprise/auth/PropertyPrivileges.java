/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.auth;

import org.eclipse.collections.api.map.primitive.IntObjectMap;
import org.eclipse.collections.api.set.primitive.IntSet;

class PropertyPrivileges
{
    private boolean allPropertiesAllLabels;
    private boolean allPropertiesAllRelTypes;
    private IntSet labelsForAllProperties;
    private IntSet relTypesForAllProperties;
    private IntSet nodePropertiesForAllLabels;
    private IntSet relationshipPropertiesForAllTypes;
    private IntObjectMap<IntSet> labelsForProperty;
    private IntObjectMap<IntSet> relTypesForProperty;

    PropertyPrivileges( boolean allPropertiesAllLabels, boolean allPropertiesAllRelTypes,
                        IntSet labelsForAllProperties, IntSet relTypesForAllProperties,
                        IntSet nodePropertiesForAllLabels, IntSet relationshipPropertiesForAllTypes,
                        IntObjectMap<IntSet> labelsForProperty,
                        IntObjectMap<IntSet> relTypesForProperty )
    {

        this.allPropertiesAllLabels = allPropertiesAllLabels;
        this.allPropertiesAllRelTypes = allPropertiesAllRelTypes;
        this.labelsForAllProperties = labelsForAllProperties;
        this.relTypesForAllProperties = relTypesForAllProperties;
        this.nodePropertiesForAllLabels = nodePropertiesForAllLabels;
        this.relationshipPropertiesForAllTypes = relationshipPropertiesForAllTypes;
        this.labelsForProperty = labelsForProperty;
        this.relTypesForProperty = relTypesForProperty;
    }

    public boolean isAllPropertiesAllLabels()
    {
        return allPropertiesAllLabels;
    }

    public boolean isAllPropertiesAllRelTypes()
    {
        return allPropertiesAllRelTypes;
    }

    public IntSet getLabelsForAllProperties()
    {
        return labelsForAllProperties;
    }

    public IntSet getRelTypesForAllProperties()
    {
        return relTypesForAllProperties;
    }

    public IntSet getNodePropertiesForAllLabels()
    {
        return nodePropertiesForAllLabels;
    }

    public IntSet getRelationshipPropertiesForAllTypes()
    {
        return relationshipPropertiesForAllTypes;
    }

    public IntObjectMap<IntSet> getLabelsForProperty()
    {
        return labelsForProperty;
    }

    public IntObjectMap<IntSet> getRelTypesForProperty()
    {
        return relTypesForProperty;
    }
}
