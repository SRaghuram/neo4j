package com.neo4j.bench.client.model;

import com.google.common.collect.Sets;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static java.util.Collections.emptyMap;
import static java.util.stream.Collectors.toList;

public class PlanCompilationMetrics
{
    private static final Set<String> VALID_KEYS = Sets.newHashSet(
            "AST_REWRITE",
            "SEMANTIC_CHECK",
            "PARSING",
            "PIPE_BUILDING",
            "LOGICAL_PLANNING",
            "CODE_GENERATION",
            "OVERHEAD",
            "DEPRECATION_WARNINGS",
            "TOTAL"
    );

    private final Map<String,Long> compilationTimeComponents;

    /**
     * WARNING: Never call this explicitly.
     * No-params constructor is only used for JSON (de)serialization.
     */
    public PlanCompilationMetrics()
    {
        this( emptyMap() );
    }

    public PlanCompilationMetrics( Map<String,Long> compilationTimeComponents )
    {
        assertValidKeys( compilationTimeComponents );
        this.compilationTimeComponents = compilationTimeComponents;
    }

    public Map<String,Long> asMap()
    {
        return compilationTimeComponents;
    }

    private static void assertValidKeys( Map<String,Long> compilationTimeComponents )
    {
        List<String> invalidKeys = invalidKeys( compilationTimeComponents );
        if ( !invalidKeys.isEmpty() )
        {
            throw new RuntimeException( "Invalid compilation time metrics: " + invalidKeys );
        }
    }

    private static List<String> invalidKeys( Map<String,Long> compilationTimeComponents )
    {
        return compilationTimeComponents.keySet().stream()
                .filter( key -> !VALID_KEYS.contains( key.toUpperCase() ) )
                .collect( toList() );
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
        PlanCompilationMetrics that = (PlanCompilationMetrics) o;
        return Objects.equals( compilationTimeComponents, that.compilationTimeComponents );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( compilationTimeComponents );
    }

    @Override
    public String toString()
    {
        return "PlanCompilationMetrics{" +
               "compilationTimeComponents=" + compilationTimeComponents +
               '}';
    }
}
