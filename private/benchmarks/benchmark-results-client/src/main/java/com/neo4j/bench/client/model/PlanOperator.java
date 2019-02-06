/**
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.client.model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class PlanOperator
{
    private static final String OPERATOR_TYPE = "operator_type";
    private static final String ESTIMATED_ROWS = "estimated_rows";
    private static final String DB_HITS = "db_hits";
    private static final String ROWS = "rows";

    private final String operatorType;
    private final Number estimatedRows;
    private final Number dbHits;
    private final Number rows;
    /*
    Example 'arguments':
    LabelName: 'Track'
    KeyNames: 't, y, val, count'
    ExpandExpression: '(t)-[  UNNAMED33 APPEARS_ON]->(al)'
     */
    private final Map<String,String> arguments;
    private final List<PlanOperator> children;

    /**
     * WARNING: Never call this explicitly.
     * No-params constructor is only used for JSON (de)serialization.
     */
    public PlanOperator()
    {
        this( "-1", -1, -1, -1 );
    }

    public PlanOperator(
            String operatorType,
            Number estimatedRows,
            Number dbHits,
            Number rows )
    {
        this( operatorType, estimatedRows, dbHits, rows, new HashMap<>(), new ArrayList<>() );
    }

    public PlanOperator(
            String operatorType,
            Number estimatedRows,
            Number dbHits,
            Number rows,
            Map<String,String> arguments,
            List<PlanOperator> children )
    {
        this.operatorType = operatorType;
        this.estimatedRows = estimatedRows;
        this.dbHits = dbHits;
        this.rows = rows;
        this.arguments = arguments;
        this.children = children;
    }

    public void addArgument( String key, String value )
    {
        arguments.put( key, value );
    }

    public void addChild( PlanOperator child )
    {
        children.add( child );
    }

    public String operatorType()
    {
        return operatorType;
    }

    public Number estimatedRows()
    {
        return estimatedRows;
    }

    public Number dbHits()
    {
        return dbHits;
    }

    public Number rows()
    {
        return rows;
    }

    public Map<String,String> arguments()
    {
        return arguments;
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
        map.putAll( arguments );
        return map;
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
        PlanOperator that = (PlanOperator) o;
        return Objects.equals( operatorType, that.operatorType ) &&
               Objects.equals( estimatedRows, that.estimatedRows ) &&
               Objects.equals( dbHits, that.dbHits ) &&
               Objects.equals( rows, that.rows ) &&
               Objects.equals( arguments, that.arguments ) &&
               Objects.equals( children, that.children );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( operatorType, estimatedRows, dbHits, rows, arguments, children );
    }

    @Override
    public String toString()
    {
        return "PlanOperator{" +
               "operatorType='" + operatorType + '\'' +
               ", estimatedRows=" + estimatedRows +
               ", dbHits=" + dbHits +
               ", rows=" + rows +
               ", arguments=" + arguments +
               ", children=" + children +
               '}';
    }
}
