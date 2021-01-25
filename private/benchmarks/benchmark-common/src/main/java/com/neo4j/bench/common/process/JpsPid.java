/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.process;

import com.neo4j.bench.common.util.Jvm;

public class JpsPid extends PidStrategy
{
    @Override
    protected String[] getCommand( Jvm jvm )
    {
        return new String[]{jvm.launchJps(), "-v"};
    }
}
