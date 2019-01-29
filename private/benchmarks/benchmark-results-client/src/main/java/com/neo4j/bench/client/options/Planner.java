package com.neo4j.bench.client.options;

public enum Planner
{
    DEFAULT,
    RULE,
    COST;

    public String value()
    {
        return name().toLowerCase();
    }
}
