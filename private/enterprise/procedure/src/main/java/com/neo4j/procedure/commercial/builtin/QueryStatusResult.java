/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.procedure.commercial.builtin;

import java.time.ZoneId;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.spatial.CRS;
import org.neo4j.graphdb.spatial.Coordinate;
import org.neo4j.graphdb.spatial.Point;
import org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo;
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.kernel.api.query.ExecutingQuery;
import org.neo4j.kernel.api.query.QuerySnapshot;
import org.neo4j.kernel.impl.core.EmbeddedProxySPI;
import org.neo4j.kernel.impl.util.BaseToObjectValueWriter;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.virtual.MapValue;

import static java.util.Collections.singletonList;

public class QueryStatusResult
{
    public final String queryId;
    public final String username;
    public final Map<String,Object> metaData;
    public final String query;
    public final Map<String,Object> parameters;
    /** @since Neo4j 3.2 */
    public final String planner;
    /** @since Neo4j 3.2 */
    public final String runtime;
    /** @since Neo4j 3.2 */
    public final List<Map<String,String>> indexes;
    public final String startTime;
    /** @since Neo4j 3.2 */
    public final String protocol;
    /** @since Neo4j 3.2 */
    public final String clientAddress;
    /** @since Neo4j 3.2 */
    public final String requestUri;
    /** @since Neo4j 3.2 */
    public final String status;
    /** @since Neo4j 3.2 */
    public final Map<String,Object> resourceInformation;
    /** @since Neo4j 3.2 */
    public final long activeLockCount;
    /** @since Neo4j 3.2 */
    public final long elapsedTimeMillis; // TODO: this field should be of a Duration type (when Cypher supports that)
    /** @since Neo4j 3.2, will be {@code null} if measuring CPU time is not supported. */
    public final Long cpuTimeMillis; // TODO: we want this field to be of a Duration type (when Cypher supports that)
    /** @since Neo4j 3.2 */
    public final long waitTimeMillis; // TODO: we want this field to be of a Duration type (when Cypher supports that)
    /** @since Neo4j 3.2 */
    public final Long idleTimeMillis; // TODO: we want this field to be of a Duration type (when Cypher supports that)
    /** @since Neo4j 3.2, will be {@code null} if measuring allocation is not supported. */
    public final Long allocatedBytes;
    /** @since Neo4j 3.2 */
    public final long pageHits;
    /** @since Neo4j 3.2 */
    public final long pageFaults;
    /** @since Neo4j 3.5 */
    public final String connectionId;

    QueryStatusResult( ExecutingQuery query, EmbeddedProxySPI manager, ZoneId zoneId ) throws InvalidArgumentsException
    {
        this( query.snapshot(), manager, zoneId );
    }

    private QueryStatusResult( QuerySnapshot query, EmbeddedProxySPI manager, ZoneId zoneId ) throws InvalidArgumentsException
    {
        this.queryId = QueryId.ofInternalId( query.internalQueryId() ).toString();
        this.username = query.username();
        this.query = query.queryText();
        this.parameters = asRawMap( query.queryParameters(), new ParameterWriter( manager ) );
        this.startTime = ProceduresTimeFormatHelper.formatTime( query.startTimestampMillis(), zoneId );
        this.elapsedTimeMillis = asMillis( query.elapsedTimeMicros() );
        ClientConnectionInfo clientConnection = query.clientConnection();
        this.protocol = clientConnection.protocol();
        this.clientAddress = clientConnection.clientAddress();
        this.requestUri = clientConnection.requestURI();
        this.metaData = query.transactionAnnotationData();
        this.cpuTimeMillis = asMillis( query.cpuTimeMicros() );
        this.status = query.status();
        this.resourceInformation = query.resourceInformation();
        this.activeLockCount = query.activeLockCount();
        this.waitTimeMillis = asMillis( query.waitTimeMicros() );
        this.idleTimeMillis = asMillis( query.idleTimeMicros() );
        this.planner = query.planner();
        this.runtime = query.runtime();
        this.indexes = query.indexes();
        this.allocatedBytes = query.allocatedBytes().orElse( null );
        this.pageHits = query.pageHits();
        this.pageFaults = query.pageFaults();
        this.connectionId = clientConnection.connectionId();
    }

    private Long asMillis( Long micros )
    {
        return micros == null ? null : TimeUnit.MICROSECONDS.toMillis( micros );
    }

    private static Map<String,Object> asRawMap( MapValue mapValue, ParameterWriter writer )
    {
        HashMap<String,Object> map = new HashMap<>();
        mapValue.foreach( ( s, value ) ->
        {
            value.writeTo( writer );
            map.put( s, writer.value() );
        } );
        return map;
    }

    private static class ParameterWriter extends BaseToObjectValueWriter<RuntimeException>
    {
        private final EmbeddedProxySPI nodeManager;

        private ParameterWriter( EmbeddedProxySPI nodeManager )
        {
            this.nodeManager = nodeManager;
        }

        @Override
        protected Node newNodeProxyById( long id )
        {
            return nodeManager.newNodeProxy( id );
        }

        @Override
        protected Relationship newRelationshipProxyById( long id )
        {
            return nodeManager.newRelationshipProxy( id );
        }

        @Override
        protected Point newPoint( CoordinateReferenceSystem crs, double[] coordinate )
        {
            return new Point()
            {
                @Override
                public String getGeometryType()
                {
                    return "Point";
                }

                @Override
                public List<Coordinate> getCoordinates()
                {
                    return singletonList( new Coordinate( coordinate ) );
                }

                @Override
                public CRS getCRS()
                {
                    return crs;
                }
            };
        }
    }
}
