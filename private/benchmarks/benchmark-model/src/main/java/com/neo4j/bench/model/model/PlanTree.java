/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.model.model;

import com.google.common.hash.Hashing;

import java.util.Objects;

import static java.nio.charset.StandardCharsets.UTF_8;

public class PlanTree
{
    public static final String PLAN_DESCRIPTION = "description";
    public static final String PLAN_DESCRIPTION_HASH = "description_hash";
    private final String asciiPlanDescription;
    private final String hashedPlanDescription;
    private final PlanOperator planRoot;

    /**
     * WARNING: Never call this explicitly.
     * No-params constructor is only used for JSON (de)serialization.
     */
    public PlanTree()
    {
        this( "-1", new PlanOperator() );
    }

    public PlanTree( String asciiPlanDescription, PlanOperator planRoot )
    {
        this.asciiPlanDescription = asciiPlanDescription;
        this.hashedPlanDescription = sha256( asciiPlanDescription );
        this.planRoot = planRoot;
    }

    public String asciiPlanDescription()
    {
        return asciiPlanDescription;
    }

    public String hashedPlanDescription()
    {
        return hashedPlanDescription;
    }

    public PlanOperator planRoot()
    {
        return planRoot;
    }

    private static String sha256( String input )
    {
        return Hashing.sha256().hashString( input, UTF_8 ).toString();
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }
        PlanTree planTree = (PlanTree) o;
        return Objects.equals( asciiPlanDescription, planTree.asciiPlanDescription ) &&
               Objects.equals( hashedPlanDescription, planTree.hashedPlanDescription ) &&
               Objects.equals( planRoot, planTree.planRoot );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( asciiPlanDescription, hashedPlanDescription, planRoot );
    }

    @Override
    public String toString()
    {
        return "PlanTree{" +
               "asciiPlanDescription='" + asciiPlanDescription + '\'' +
               ", hashedPlanDescription='" + hashedPlanDescription + '\'' +
               ", planRoot=" + planRoot +
               '}';
    }
}
