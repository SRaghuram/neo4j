package com.neo4j.bench.client.process;

public interface BaseProcess
{
    void waitFor();

    void stop();
}
