/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.kubernetes;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo( use = JsonTypeInfo.Id.NAME, property = "kind" )
@JsonSubTypes( {
        @JsonSubTypes.Type( value = ServiceList.class, name = "ServiceList" ),
        @JsonSubTypes.Type( value = Status.class, name = "Status" )
} )
public abstract class KubernetesType
{
    private String kind;

    public String kind()
    {
        return kind;
    }

    public void setKind( String kind )
    {
        this.kind = kind;
    }

    public abstract <T> T handle( Visitor<T> visitor );

    public interface Visitor<T>
    {
        T visit( Status status );

        T visit( ServiceList serviceList );
    }
}
