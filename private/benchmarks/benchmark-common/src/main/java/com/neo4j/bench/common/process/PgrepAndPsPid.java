/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.process;

import com.neo4j.bench.common.util.Jvm;

public class PgrepAndPsPid extends PidStrategy
{
    @Override
    protected String[] getCommand( Jvm jvm )
    {
        return new String[]{"bash", "-c", "pgrep java | xargs ps -p"};
    }
}
