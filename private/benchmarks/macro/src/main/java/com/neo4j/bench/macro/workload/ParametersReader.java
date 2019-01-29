package com.neo4j.bench.macro.workload;

import java.util.Map;

public interface ParametersReader extends AutoCloseable
{
    boolean hasNext();

    Map<String,Object> next() throws Exception;
}
