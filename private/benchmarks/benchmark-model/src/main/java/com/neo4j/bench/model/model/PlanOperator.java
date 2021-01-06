/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.model.model;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.IntStream;

import static java.lang.String.format;
import static java.util.stream.Collectors.toList;

public class PlanOperator
{
    private static final String OPERATOR_TYPE = "operator_type";
    private static final String ESTIMATED_ROWS = "estimated_rows";
    private static final String DB_HITS = "db_hits";
    private static final String ROWS = "rows";

    private final int id;
    private final String operatorType;
    private final Long estimatedRows;
    private Long dbHits;
    private Long rows;

    private final List<String> identifiers;
    private final List<PlanOperator> children;

    /**
     * WARNING: Never call this explicitly.
     * No-params constructor is only used for JSON (de)serialization.
     */
    public PlanOperator()
    {
        this( 1, "-1", -1L, -1L, -1L );
    }

    public PlanOperator(
            int id,
            String operatorType,
            Long estimatedRows,
            Long dbHits,
            Long rows )
    {
        this( id, operatorType, estimatedRows, dbHits, rows, new ArrayList<>(), new ArrayList<>() );
    }

    public PlanOperator(
            int id,
            String operatorType,
            Long estimatedRows,
            Long dbHits,
            Long rows,
            List<String> identifiers,
            List<PlanOperator> children )
    {
        this.id = id;
        this.operatorType = operatorType;
        this.estimatedRows = estimatedRows;
        this.dbHits = dbHits;
        this.rows = rows;
        this.identifiers = identifiers;
        this.children = children;
    }

    /**
     * Sums the profiled counts (e.g., rows & db-hits) of both plans.
     * This call will mutate the plan.
     */
    public void addProfiledCountsFrom( PlanOperator other )
    {
        if ( !isEquivalent( other ) )
        {
            throw new IllegalStateException( format( "Can not add profile counts from a different plan%n" +
                                                     "This:  %s%n" +
                                                     "Other: %s%n", this, other ) );
        }
        dbHits += other.dbHits;
        rows += other.rows;
        List<PlanOperator> thisSortedChildren = this.childrenSorted();
        List<PlanOperator> otherSortedChildren = other.childrenSorted();
        for ( int i = 0; i < thisSortedChildren.size(); i++ )
        {
            thisSortedChildren.get( i ).addProfiledCountsFrom( otherSortedChildren.get( i ) );
        }
    }

    /**
     * Divides the profiled counts (e.g., rows & db-hits) of this operator and all of its children by the specified count.
     * This call will mutate the plan.
     */
    public void divideProfiledCountsBy( int count )
    {
        dbHits /= count;
        rows /= count;
        for ( PlanOperator child : children )
        {
            child.divideProfiledCountsBy( count );
        }
    }

    public void addChild( PlanOperator child )
    {
        children.add( child );
    }

    public int id()
    {
        return id;
    }

    public String operatorType()
    {
        return operatorType;
    }

    public Long estimatedRows()
    {
        return estimatedRows;
    }

    public Optional<Long> dbHits()
    {
        return Optional.ofNullable( dbHits );
    }

    public Optional<Long> rows()
    {
        return Optional.ofNullable( rows );
    }

    public List<String> identifiers()
    {
        return identifiers;
    }

    public List<PlanOperator> children()
    {
        return children;
    }

    public Map<String,Object> asMap()
    {
        Map<String,Object> map = new HashMap<>();
        map.put( OPERATOR_TYPE, operatorType );
        map.put( ESTIMATED_ROWS, estimatedRows );
        map.put( DB_HITS, dbHits );
        map.put( ROWS, rows );
        return map;
    }

    /**
     * Compares plans in the same way as equals(), with one exception.
     * Only considers fields that are present when running EXPLAIN.
     * Any additional/PROFILE fields (e.g., rows) is ignored.
     */
    public boolean isEquivalent( PlanOperator other )
    {
        boolean shallowEquivalent = EqualsBuilder.reflectionEquals( other, this, "dbHits", "rows", "children" );
        return shallowEquivalent && isEquivalentChildren( other );
    }

    private boolean isEquivalentChildren( PlanOperator other )
    {
        List<PlanOperator> thisChildrenSorted = childrenSorted();
        List<PlanOperator> otherChildrenSorted = other.childrenSorted();
        return thisChildrenSorted.size() == otherChildrenSorted.size() &&
               IntStream.range( 0, thisChildrenSorted.size() )
                        .allMatch( i -> thisChildrenSorted.get( i ).isEquivalent( otherChildrenSorted.get( i ) ) );
    }

    private List<PlanOperator> childrenSorted()
    {
        return children.stream().sorted( Comparator.comparingInt( o -> o.id ) ).collect( toList() );
    }

    @Override
    public boolean equals( Object o )
    {
        return EqualsBuilder.reflectionEquals( o, this );
    }

    @Override
    public int hashCode()
    {
        return id;
    }

    @Override
    public String toString()
    {
        return ToStringBuilder.reflectionToString( this, ToStringStyle.SHORT_PREFIX_STYLE );
    }
}
