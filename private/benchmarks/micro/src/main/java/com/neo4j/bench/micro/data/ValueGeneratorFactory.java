package com.neo4j.bench.micro.data;

public interface ValueGeneratorFactory<T>
{
    ValueGeneratorFun<T> create();
}
