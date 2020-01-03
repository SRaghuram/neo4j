/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.model;

import com.neo4j.bench.common.options.Edition;

import java.util.Map;

public class Neo4j extends Project
{
    private Neo4j()
    {
        super();
    }

    public static Neo4j fromCypherMap( Map<String,Object> map )
    {
        Repository repository = Repository.forName( (String) map.get( NAME ) );
        if ( !repository.equals( Repository.NEO4J ) )
        {
            throw new RuntimeException( "Tried to create a Neo4j model instance with repository: " + repository );
        }
        String commit = (String) map.get( COMMIT );
        String version = (String) map.get( VERSION );
        Edition edition = Edition.valueOf( ((String) map.get( EDITION )).toUpperCase() );
        String branch = (String) map.get( BRANCH );
        String owner = (String) map.get( OWNER );
        return new Neo4j( commit, version, edition, branch, owner );
    }

    public Neo4j( String commit, String version, Edition edition, String branch, String owner )
    {
        super( Repository.NEO4J, commit, version, edition, branch, owner );
    }
}
