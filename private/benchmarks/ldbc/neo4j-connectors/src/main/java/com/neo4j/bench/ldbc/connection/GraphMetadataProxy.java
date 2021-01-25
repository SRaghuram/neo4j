/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.connection;

import com.ldbc.driver.DbException;
import com.neo4j.bench.ldbc.Domain.GraphMetadata;
import com.neo4j.bench.ldbc.Domain.Nodes;
import com.neo4j.bench.ldbc.importer.GraphMetadataTracker;
import com.neo4j.bench.ldbc.operators.Operators;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

public class GraphMetadataProxy
{
    public static void writeTo( GraphDatabaseService db, GraphMetadataProxy proxy )
    {
        try ( Transaction tx = db.beginTx() )
        {
            Node node;
            try
            {
                node = Operators.findNode( tx, Nodes.GraphMetaData,
                        GraphMetadata.ID,
                        GraphMetadata.ID_VALUE );
            }
            catch ( DbException e )
            {
                node = tx.createNode( Nodes.GraphMetaData );
                node.setProperty( GraphMetadata.ID, GraphMetadata.ID_VALUE );
            }
            if ( proxy.hasCommentHasCreatorMinDateAtResolution() )
            {
                node.setProperty(
                        GraphMetadata.COMMENT_HAS_CREATOR_MIN_DATE,
                        proxy.commentHasCreatorMinDateAtResolution() );
            }
            if ( proxy.hasCommentHasCreatorMaxDateAtResolution() )
            {
                node.setProperty(
                        GraphMetadata.COMMENT_HAS_CREATOR_MAX_DATE,
                        proxy.commentHasCreatorMaxDateAtResolution() );
            }
            if ( proxy.hasPostHasCreatorMinDateAtResolution() )
            {
                node.setProperty(
                        GraphMetadata.POST_HAS_CREATOR_MIN_DATE,
                        proxy.postHasCreatorMinDateAtResolution() );
            }
            if ( proxy.hasPostHasCreatorMaxDateAtResolution() )
            {
                node.setProperty(
                        GraphMetadata.POST_HAS_CREATOR_MAX_DATE,
                        proxy.postHasCreatorMaxDateAtResolution() );
            }
            if ( proxy.hasWorkFromMinYear() )
            {
                node.setProperty(
                        GraphMetadata.WORK_FROM_MIN_YEAR,
                        proxy.workFromMinYear() );
            }
            if ( proxy.hasWorkFromMaxYear() )
            {
                node.setProperty(
                        GraphMetadata.WORK_FROM_MAX_YEAR,
                        proxy.workFromMaxYear() );
            }
            if ( proxy.hasCommentIsLocatedInMinDateAtResolution() )
            {
                node.setProperty(
                        GraphMetadata.COMMENT_IS_LOCATED_IN_MIN_DATE,
                        proxy.commentIsLocatedInMinDateAtResolution() );
            }
            if ( proxy.hasCommentIsLocatedInMaxDateAtResolution() )
            {
                node.setProperty(
                        GraphMetadata.COMMENT_IS_LOCATED_IN_MAX_DATE,
                        proxy.commentIsLocatedInMaxDateAtResolution() );
            }
            if ( proxy.hasPostIsLocatedInMinDateAtResolution() )
            {
                node.setProperty(
                        GraphMetadata.POST_IS_LOCATED_IN_MIN_DATE,
                        proxy.postIsLocatedInMinDateAtResolution() );
            }
            if ( proxy.hasPostIsLocatedInMaxDateAtResolution() )
            {
                node.setProperty(
                        GraphMetadata.POST_IS_LOCATED_IN_MAX_DATE,
                        proxy.postIsLocatedInMaxDateAtResolution() );
            }
            if ( proxy.hasHasMemberMinDateAtResolution() )
            {
                node.setProperty(
                        GraphMetadata.HAS_MEMBER_MIN_DATE,
                        proxy.hasMemberMinDateAtResolution() );
            }
            if ( proxy.hasHasMemberMaxDateAtResolution() )
            {
                node.setProperty(
                        GraphMetadata.HAS_MEMBER_MAX_DATE,
                        proxy.hasMemberMaxDateAtResolution() );
            }
            node.setProperty( GraphMetadata.DATE_FORMAT, proxy.dateFormat().name() );
            node.setProperty( GraphMetadata.TIMESTAMP_RESOLUTION, proxy.timestampResolution().name() );
            node.setProperty( GraphMetadata.NEO4J_SCHEMA, proxy.neo4jSchema().name() );
            tx.commit();
        }
    }

    public static GraphMetadataProxy loadFrom( GraphDatabaseService db ) throws DbException
    {
        try ( Transaction tx = db.beginTx() )
        {
            Node node = Operators.findNode( tx, Nodes.GraphMetaData,
                    GraphMetadata.ID,
                    GraphMetadata.ID_VALUE );
            Long commentHasCreatorMinDate =
                    (Long) node.getProperty( GraphMetadata.COMMENT_HAS_CREATOR_MIN_DATE, null );
            Long commentHasCreatorMaxDate =
                    (Long) node.getProperty( GraphMetadata.COMMENT_HAS_CREATOR_MAX_DATE, null );
            Long postHasCreatorMinDate =
                    (Long) node.getProperty( GraphMetadata.POST_HAS_CREATOR_MIN_DATE, null );
            Long postHasCreatorMaxDate =
                    (Long) node.getProperty( GraphMetadata.POST_HAS_CREATOR_MAX_DATE, null );
            Integer workFromMinYear =
                    (null == node.getProperty( GraphMetadata.WORK_FROM_MIN_YEAR, null ))
                    ? null
                    : ((Number) node.getProperty( GraphMetadata.WORK_FROM_MIN_YEAR, null )).intValue();
            Integer workFromMaxYear =
                    (null == node.getProperty( GraphMetadata.WORK_FROM_MAX_YEAR, null ))
                    ? null
                    : ((Number) node.getProperty( GraphMetadata.WORK_FROM_MAX_YEAR, null )).intValue();
            Long commentIsLocatedInMinDate =
                    (Long) node.getProperty( GraphMetadata.COMMENT_IS_LOCATED_IN_MIN_DATE, null );
            Long commentIsLocatedInMaxDate =
                    (Long) node.getProperty( GraphMetadata.COMMENT_IS_LOCATED_IN_MAX_DATE, null );
            Long postIsLocatedInMinDate =
                    (Long) node.getProperty( GraphMetadata.POST_IS_LOCATED_IN_MIN_DATE, null );
            Long postIsLocatedInMaxDate =
                    (Long) node.getProperty( GraphMetadata.POST_IS_LOCATED_IN_MAX_DATE, null );
            Long hasMemberMinDate =
                    (Long) node.getProperty( GraphMetadata.HAS_MEMBER_MIN_DATE, null );
            Long hasMemberMaxDate =
                    (Long) node.getProperty( GraphMetadata.HAS_MEMBER_MAX_DATE, null );
            return new GraphMetadataProxy(
                    commentHasCreatorMinDate,
                    commentHasCreatorMaxDate,
                    postHasCreatorMinDate,
                    postHasCreatorMaxDate,
                    workFromMinYear,
                    workFromMaxYear,
                    commentIsLocatedInMinDate,
                    commentIsLocatedInMaxDate,
                    postIsLocatedInMinDate,
                    postIsLocatedInMaxDate,
                    hasMemberMinDate,
                    hasMemberMaxDate,
                    LdbcDateCodec.Format.valueOf( (String) node.getProperty( GraphMetadata.DATE_FORMAT ) ),
                    LdbcDateCodec.Resolution.valueOf( (String) node.getProperty( GraphMetadata.TIMESTAMP_RESOLUTION ) ),
                    Neo4jSchema.valueOf( (String) node.getProperty( GraphMetadata.NEO4J_SCHEMA ) )
            );
        }
    }

    public static GraphMetadataProxy createFrom( GraphMetadataTracker tracker ) throws DbException
    {
        return new GraphMetadataProxy(
                tracker.commentHasCreatorMinDateAtResolution(),
                tracker.commentHasCreatorMaxDateAtResolution(),
                tracker.postHasCreatorMinDateAtResolution(),
                tracker.postHasCreatorMaxDateAtResolution(),
                tracker.workFromMinYear(),
                tracker.workFromMaxYear(),
                tracker.commentIsLocatedInMinDateAtResolution(),
                tracker.commentIsLocatedInMaxDateAtResolution(),
                tracker.postIsLocatedInMinDateAtResolution(),
                tracker.postIsLocatedInMaxDateAtResolution(),
                tracker.hasMemberMinDateAtResolution(),
                tracker.hasMemberMaxDateAtResolution(),
                tracker.dateFormat(),
                tracker.timestampResolution(),
                tracker.neo4jSchema()
        );
    }

    private final Long commentHasCreatorMinDate;
    private final Long commentHasCreatorMaxDate;
    private final Long postHasCreatorMinDate;
    private final Long postHasCreatorMaxDate;
    private final Integer workFromMinYear;
    private final Integer workFromMaxYear;
    private final Long commentIsLocatedInMinDate;
    private final Long commentIsLocatedInMaxDate;
    private final Long postIsLocatedInMinDate;
    private final Long postIsLocatedInMaxDate;
    private final Long hasMemberMinDate;
    private final Long hasMemberMaxDate;
    private final LdbcDateCodec.Format dateFormat;
    private final LdbcDateCodec.Resolution timestampResolution;
    private final Neo4jSchema neo4jSchema;

    private GraphMetadataProxy(
            Long commentHasCreatorMinDate,
            Long commentHasCreatorMaxDate,
            Long postHasCreatorMinDate,
            Long postHasCreatorMaxDate,
            Integer workFromMinYear,
            Integer workFromMaxYear,
            Long commentIsLocatedInMinDate,
            Long commentIsLocatedInMaxDate,
            Long postIsLocatedInMinDate,
            Long postIsLocatedInMaxDate,
            Long hasMemberMinDate,
            Long hasMemberMaxDate,
            LdbcDateCodec.Format dateFormat,
            LdbcDateCodec.Resolution timestampResolution,
            Neo4jSchema neo4jSchema )
    {
        this.commentHasCreatorMinDate = commentHasCreatorMinDate;
        this.commentHasCreatorMaxDate = commentHasCreatorMaxDate;
        this.postHasCreatorMinDate = postHasCreatorMinDate;
        this.postHasCreatorMaxDate = postHasCreatorMaxDate;
        this.workFromMinYear = workFromMinYear;
        this.workFromMaxYear = workFromMaxYear;
        this.commentIsLocatedInMinDate = commentIsLocatedInMinDate;
        this.commentIsLocatedInMaxDate = commentIsLocatedInMaxDate;
        this.postIsLocatedInMinDate = postIsLocatedInMinDate;
        this.postIsLocatedInMaxDate = postIsLocatedInMaxDate;
        this.hasMemberMinDate = hasMemberMinDate;
        this.hasMemberMaxDate = hasMemberMaxDate;
        this.dateFormat = dateFormat;
        this.timestampResolution = timestampResolution;
        this.neo4jSchema = neo4jSchema;
    }

    public boolean hasCommentHasCreatorMinDateAtResolution()
    {
        return null != commentHasCreatorMinDate;
    }

    public long commentHasCreatorMinDateAtResolution()
    {
        if ( !hasCommentHasCreatorMinDateAtResolution() )
        {
            throw new RuntimeException( "No value has been set, possibly because no value was written to database" );
        }
        return commentHasCreatorMinDate;
    }

    public boolean hasCommentHasCreatorMaxDateAtResolution()
    {
        return null != commentHasCreatorMaxDate;
    }

    public long commentHasCreatorMaxDateAtResolution()
    {
        if ( !hasCommentHasCreatorMaxDateAtResolution() )
        {
            throw new RuntimeException( "No value has been set, possibly because no value was written to database" );
        }
        return commentHasCreatorMaxDate;
    }

    public boolean hasPostHasCreatorMinDateAtResolution()
    {
        return null != postHasCreatorMinDate;
    }

    public long postHasCreatorMinDateAtResolution()
    {
        if ( !hasPostHasCreatorMinDateAtResolution() )
        {
            throw new RuntimeException( "No value has been set, possibly because no value was written to database" );
        }
        return postHasCreatorMinDate;
    }

    public boolean hasPostHasCreatorMaxDateAtResolution()
    {
        return null != postHasCreatorMaxDate;
    }

    public long postHasCreatorMaxDateAtResolution()
    {
        if ( !hasPostHasCreatorMaxDateAtResolution() )
        {
            throw new RuntimeException( "No value has been set, possibly because no value was written to database" );
        }
        return postHasCreatorMaxDate;
    }

    public boolean hasWorkFromMinYear()
    {
        return null != workFromMinYear;
    }

    public int workFromMinYear()
    {
        if ( !hasWorkFromMinYear() )
        {
            throw new RuntimeException( "No value has been set, possibly because no value was written to database" );
        }
        return workFromMinYear;
    }

    public boolean hasWorkFromMaxYear()
    {
        return null != workFromMaxYear;
    }

    public int workFromMaxYear()
    {
        if ( !hasWorkFromMaxYear() )
        {
            throw new RuntimeException( "No value has been set, possibly because no value was written to database" );
        }
        return workFromMaxYear;
    }

    public boolean hasCommentIsLocatedInMinDateAtResolution()
    {
        return null != commentIsLocatedInMinDate;
    }

    public long commentIsLocatedInMinDateAtResolution()
    {
        if ( !hasCommentIsLocatedInMinDateAtResolution() )
        {
            throw new RuntimeException( "No value has been set, possibly because no value was written to database" );
        }
        return commentIsLocatedInMinDate;
    }

    public boolean hasCommentIsLocatedInMaxDateAtResolution()
    {
        return null != commentIsLocatedInMaxDate;
    }

    public long commentIsLocatedInMaxDateAtResolution()
    {
        if ( !hasCommentIsLocatedInMaxDateAtResolution() )
        {
            throw new RuntimeException( "No value has been set, possibly because no value was written to database" );
        }
        return commentIsLocatedInMaxDate;
    }

    public boolean hasPostIsLocatedInMinDateAtResolution()
    {
        return null != postIsLocatedInMinDate;
    }

    public long postIsLocatedInMinDateAtResolution()
    {
        if ( !hasPostIsLocatedInMinDateAtResolution() )
        {
            throw new RuntimeException( "No value has been set, possibly because no value was written to database" );
        }
        return postIsLocatedInMinDate;
    }

    public boolean hasPostIsLocatedInMaxDateAtResolution()
    {
        return null != postIsLocatedInMaxDate;
    }

    public long postIsLocatedInMaxDateAtResolution()
    {
        if ( !hasPostIsLocatedInMaxDateAtResolution() )
        {
            throw new RuntimeException( "No value has been set, possibly because no value was written to database" );
        }
        return postIsLocatedInMaxDate;
    }

    public boolean hasHasMemberMinDateAtResolution()
    {
        return null != hasMemberMinDate;
    }

    public long hasMemberMinDateAtResolution()
    {
        if ( !hasHasMemberMinDateAtResolution() )
        {
            throw new RuntimeException( "No value has been set, possibly because no value was written to database" );
        }
        return hasMemberMinDate;
    }

    public boolean hasHasMemberMaxDateAtResolution()
    {
        return null != hasMemberMaxDate;
    }

    public long hasMemberMaxDateAtResolution()
    {
        if ( !hasHasMemberMaxDateAtResolution() )
        {
            throw new RuntimeException( "No value has been set, possibly because no value was written to database" );
        }
        return hasMemberMaxDate;
    }

    public LdbcDateCodec.Format dateFormat()
    {
        if ( null == dateFormat )
        {
            throw new RuntimeException( "No value has been set, possibly because no value was written to database" );
        }
        return dateFormat;
    }

    public LdbcDateCodec.Resolution timestampResolution()
    {
        if ( null == timestampResolution )
        {
            throw new RuntimeException( "No value has been set, possibly because no value was written to database" );
        }
        return timestampResolution;
    }

    public Neo4jSchema neo4jSchema()
    {
        if ( null == neo4jSchema )
        {
            throw new RuntimeException( "No value has been set, possibly because no value was written to database" );
        }
        return neo4jSchema;
    }

    @Override
    public String toString()
    {
        return "GraphMetadataProxy{" + "\n" +
               "  hasCommentHasCreatorMinDate=" + hasCommentHasCreatorMinDateAtResolution() + "\n" +
               "  commentHasCreatorMinDate=" + commentHasCreatorMinDate + "\n" +
               "  hasCommentHasCreatorMaxDate=" + hasCommentHasCreatorMaxDateAtResolution() + "\n" +
               "  commentHasCreatorMaxDate=" + commentHasCreatorMaxDate + "\n" +
               "  hasPostHasCreatorMinDate=" + hasPostHasCreatorMinDateAtResolution() + "\n" +
               "  postHasCreatorMinDate=" + postHasCreatorMinDate + "\n" +
               "  hasPostHasCreatorMaxDate=" + hasPostHasCreatorMaxDateAtResolution() + "\n" +
               "  postHasCreatorMaxDate=" + postHasCreatorMaxDate + "\n" +
               "  hasWorkFromMinYear=" + hasWorkFromMinYear() + "\n" +
               "  workFromMinYear=" + workFromMinYear + "\n" +
               "  hasWorkFromMaxYear=" + hasWorkFromMaxYear() + "\n" +
               "  workFromMaxYear=" + workFromMaxYear + "\n" +
               "  hasCommentIsLocatedInMinDate=" + hasCommentIsLocatedInMinDateAtResolution() + "\n" +
               "  commentIsLocatedInMinDate=" + commentIsLocatedInMinDate + "\n" +
               "  hasCommentIsLocatedInMaxDate=" + hasCommentIsLocatedInMaxDateAtResolution() + "\n" +
               "  commentIsLocatedInMaxDate=" + commentIsLocatedInMaxDate + "\n" +
               "  hasPostIsLocatedInMinDate=" + hasPostIsLocatedInMinDateAtResolution() + "\n" +
               "  postIsLocatedInMinDate=" + postIsLocatedInMinDate + "\n" +
               "  hasPostIsLocatedInMaxDate=" + hasPostIsLocatedInMaxDateAtResolution() + "\n" +
               "  postIsLocatedInMaxDate=" + postIsLocatedInMaxDate + "\n" +
               "  hasHasMemberMinDate=" + hasHasMemberMinDateAtResolution() + "\n" +
               "  hasMemberMinDate=" + hasMemberMinDate + "\n" +
               "  hasHasMemberMaxDate=" + hasHasMemberMaxDateAtResolution() + "\n" +
               "  hasMemberMaxDate=" + hasMemberMaxDate + "\n" +
               "  dateFormat=" + dateFormat + "\n" +
               "  timestampResolution=" + timestampResolution + "\n" +
               "  neo4jSchema=" + neo4jSchema + "\n" +
               '}';
    }
}
