/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.macro.workload;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class LoopingParametersReader implements ParametersReader
{
    private static final int MAX_SIZE = 10_000;
    private final List<Map<String,Object>> parameters;
    private int offset;

    public static LoopingParametersReader from( ParametersReader parametersReader ) throws Exception
    {
        List<Map<String,Object>> parameterList = new ArrayList<>();
        while ( parametersReader.hasNext() )
        {
            parameterList.add( parametersReader.next() );
            if ( parameterList.size() > MAX_SIZE )
            {
                throw new RuntimeException( "Number of parametersReader should not exceed " + MAX_SIZE + " but was: " + parameterList.size() + "\n" +
                                            "NOTE: is this a fair constraint?" );
            }
        }
        if ( parameterList.isEmpty() )
        {
            throw new RuntimeException( "No parametersReader found!" );
        }
        return new LoopingParametersReader( parameterList );
    }

    private LoopingParametersReader( List<Map<String,Object>> parameters )
    {
        this.parameters = parameters;
    }

    @Override
    public boolean hasNext()
    {
        return true;
    }

    @Override
    public Map<String,Object> next() throws Exception
    {
        return parameters.get( nextOffset() );
    }

    private int nextOffset()
    {
        return ++offset % parameters.size();
    }

    @Override
    public void close()
    {
    }
}
