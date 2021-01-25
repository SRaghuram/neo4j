/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.importer;

import com.ldbc.driver.DbException;
import com.neo4j.bench.ldbc.connection.LdbcDateCodec;
import com.neo4j.bench.ldbc.connection.Neo4jSchema;

public class GraphMetadataTracker
{
    private final LdbcDateCodec.Format dateFormat;
    private final LdbcDateCodec.Resolution timestampResolution;
    private final Neo4jSchema neo4jSchema;
    private long commentHasCreatorMinDateAtResolution = Long.MAX_VALUE;
    private long commentHasCreatorMaxDateAtResolution = Long.MIN_VALUE;
    private long postHasCreatorMinDateAtResolution = Long.MAX_VALUE;
    private long postHasCreatorMaxDateAtResolution = Long.MIN_VALUE;
    private int workFromMinYear = Integer.MAX_VALUE;
    private int workFromMaxYear = Integer.MIN_VALUE;
    private long commentIsLocatedInMinDateAtResolution = Long.MAX_VALUE;
    private long commentIsLocatedInMaxDateAtResolution = Long.MIN_VALUE;
    private long postIsLocatedInMinDateAtResolution = Long.MAX_VALUE;
    private long postIsLocatedInMaxDateAtResolution = Long.MIN_VALUE;
    private long hasMemberMinDateAtResolution = Long.MAX_VALUE;
    private long hasMemberMaxDateAtResolution = Long.MIN_VALUE;

    public GraphMetadataTracker(
            LdbcDateCodec.Format dateFormat,
            LdbcDateCodec.Resolution timestampResolution,
            Neo4jSchema neo4jSchema )
    {
        this.dateFormat = dateFormat;
        this.timestampResolution = timestampResolution;
        this.neo4jSchema = neo4jSchema;
    }

    public void recordCommentHasCreatorDateAtResolution( long dateAtResolution )
    {
        commentHasCreatorMinDateAtResolution = (dateAtResolution < commentHasCreatorMinDateAtResolution)
                                               ? dateAtResolution :
                                               commentHasCreatorMinDateAtResolution;
        commentHasCreatorMaxDateAtResolution = (dateAtResolution > commentHasCreatorMaxDateAtResolution)
                                               ? dateAtResolution
                                               : commentHasCreatorMaxDateAtResolution;
    }

    public void recordPostHasCreatorDateAtResolution( long dateAtResolution )
    {
        postHasCreatorMinDateAtResolution = (dateAtResolution < postHasCreatorMinDateAtResolution)
                                            ? dateAtResolution
                                            : postHasCreatorMinDateAtResolution;
        postHasCreatorMaxDateAtResolution = (dateAtResolution > postHasCreatorMaxDateAtResolution)
                                            ? dateAtResolution
                                            : postHasCreatorMaxDateAtResolution;
    }

    public void recordWorkFromYear( int year )
    {
        workFromMinYear = (year < workFromMinYear) ? year : workFromMinYear;
        workFromMaxYear = (year > workFromMaxYear) ? year : workFromMaxYear;
    }

    public void recordCommentIsLocatedInDateAtResolution( long dateAtResolution )
    {
        commentIsLocatedInMinDateAtResolution = (dateAtResolution < commentIsLocatedInMinDateAtResolution)
                                                ? dateAtResolution :
                                                commentIsLocatedInMinDateAtResolution;
        commentIsLocatedInMaxDateAtResolution = (dateAtResolution > commentIsLocatedInMaxDateAtResolution)
                                                ? dateAtResolution
                                                : commentIsLocatedInMaxDateAtResolution;
    }

    public void recordPostIsLocatedInDateAtResolution( long dateAtResolution )
    {
        postIsLocatedInMinDateAtResolution = (dateAtResolution < postIsLocatedInMinDateAtResolution)
                                             ? dateAtResolution
                                             : postIsLocatedInMinDateAtResolution;
        postIsLocatedInMaxDateAtResolution = (dateAtResolution > postIsLocatedInMaxDateAtResolution)
                                             ? dateAtResolution
                                             : postIsLocatedInMaxDateAtResolution;
    }

    public void recordHasMemberDateAtResolution( long dateAtResolution )
    {
        hasMemberMinDateAtResolution = (dateAtResolution < hasMemberMinDateAtResolution)
                                       ? dateAtResolution
                                       : hasMemberMinDateAtResolution;
        hasMemberMaxDateAtResolution = (dateAtResolution > hasMemberMaxDateAtResolution)
                                       ? dateAtResolution
                                       : hasMemberMaxDateAtResolution;
    }

    public Long commentHasCreatorMinDateAtResolution() throws DbException
    {
        return (Long.MAX_VALUE == commentHasCreatorMinDateAtResolution)
               ? null
               : commentHasCreatorMinDateAtResolution;
    }

    public Long commentHasCreatorMaxDateAtResolution() throws DbException
    {
        return (Long.MIN_VALUE == commentHasCreatorMaxDateAtResolution)
               ? null
               : commentHasCreatorMaxDateAtResolution;
    }

    public Long postHasCreatorMinDateAtResolution() throws DbException
    {
        return (Long.MAX_VALUE == postHasCreatorMinDateAtResolution)
               ? null
               : postHasCreatorMinDateAtResolution;
    }

    public Long postHasCreatorMaxDateAtResolution() throws DbException
    {
        return (Long.MIN_VALUE == postHasCreatorMaxDateAtResolution)
               ? null : postHasCreatorMaxDateAtResolution;
    }

    public Integer workFromMinYear() throws DbException
    {
        return (Integer.MAX_VALUE == workFromMinYear)
               ? null
               : workFromMinYear;
    }

    public Integer workFromMaxYear() throws DbException
    {
        return (Integer.MIN_VALUE == workFromMaxYear)
               ? null
               : workFromMaxYear;
    }

    public Long commentIsLocatedInMinDateAtResolution() throws DbException
    {
        return (Long.MAX_VALUE == commentIsLocatedInMinDateAtResolution)
               ? null
               : commentIsLocatedInMinDateAtResolution;
    }

    public Long commentIsLocatedInMaxDateAtResolution() throws DbException
    {
        return (Long.MIN_VALUE == commentIsLocatedInMaxDateAtResolution)
               ? null
               : commentIsLocatedInMaxDateAtResolution;
    }

    public Long postIsLocatedInMinDateAtResolution() throws DbException
    {
        return (Long.MAX_VALUE == postIsLocatedInMinDateAtResolution)
               ? null
               : postIsLocatedInMinDateAtResolution;
    }

    public Long postIsLocatedInMaxDateAtResolution() throws DbException
    {
        return (Long.MIN_VALUE == postIsLocatedInMaxDateAtResolution)
               ? null : postIsLocatedInMaxDateAtResolution;
    }

    public Long hasMemberMinDateAtResolution() throws DbException
    {
        return (Long.MAX_VALUE == hasMemberMinDateAtResolution)
               ? null
               : hasMemberMinDateAtResolution;
    }

    public Long hasMemberMaxDateAtResolution() throws DbException
    {
        return (Long.MIN_VALUE == hasMemberMaxDateAtResolution)
               ? null : hasMemberMaxDateAtResolution;
    }

    public LdbcDateCodec.Format dateFormat()
    {
        return dateFormat;
    }

    public LdbcDateCodec.Resolution timestampResolution()
    {
        return timestampResolution;
    }

    public Neo4jSchema neo4jSchema()
    {
        return neo4jSchema;
    }
}
