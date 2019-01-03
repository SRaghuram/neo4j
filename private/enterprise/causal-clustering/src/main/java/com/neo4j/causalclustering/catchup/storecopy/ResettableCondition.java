package com.neo4j.causalclustering.catchup.storecopy;

public interface ResettableCondition
{
    boolean canContinue();

    void reset();
}
