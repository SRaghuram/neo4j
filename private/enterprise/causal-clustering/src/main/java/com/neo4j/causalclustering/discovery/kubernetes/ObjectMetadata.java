/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.kubernetes;

/**
 * See <a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.11/#objectmeta-v1-meta">ObjectMeta</a>
 */
public class ObjectMetadata
{
    private String deletionTimestamp;
    private String name;

    public ObjectMetadata()
    {
    }

    public String deletionTimestamp()
    {
        return deletionTimestamp;
    }

    public String name()
    {
        return name;
    }

    public void setDeletionTimestamp( String deletionTimestamp )
    {
        this.deletionTimestamp = deletionTimestamp;
    }

    public void setName( String name )
    {
        this.name = name;
    }
}
