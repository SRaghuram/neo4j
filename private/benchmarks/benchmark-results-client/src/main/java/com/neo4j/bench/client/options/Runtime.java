package com.neo4j.bench.client.options;

public enum Runtime
{
    DEFAULT,
    INTERPRETED,
    SLOTTED,
    COMPILED,
    MORSEL;

    public String value()
    {
        return name().toLowerCase();
    }
}
