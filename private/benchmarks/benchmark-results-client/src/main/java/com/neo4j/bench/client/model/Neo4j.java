/**
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.client.model;

import java.util.Map;

import org.neo4j.driver.v1.Value;

public class Neo4j extends Project
{
    public Neo4j()
    {
        super();
    }

    public Neo4j( Value value )
    {
        super( value );
    }

    public Neo4j( Map<String,Object> map )
    {
        super( map );
    }

    public Neo4j( String commit, String version, Edition edition, String branch, String owner )
    {
        super( Repository.NEO4J, commit, version, edition, branch, owner );
    }
}
