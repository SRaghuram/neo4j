/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.model;

import com.neo4j.bench.common.options.Edition;

public class Neo4j extends Project
{
    public Neo4j( String commit, String version, Edition edition, String branch, String owner )
    {
        super( Repository.NEO4J, commit, version, edition, branch, owner );
    }
}
