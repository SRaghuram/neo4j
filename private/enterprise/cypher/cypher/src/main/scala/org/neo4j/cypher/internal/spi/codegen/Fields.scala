/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.spi.codegen

import org.neo4j.codegen.FieldReference

case class Fields(entityAccessor: FieldReference,
                  executionMode: FieldReference,
                  description: FieldReference,
                  tracer: FieldReference,
                  params: FieldReference,
                  queryContext: FieldReference,
                  skip: FieldReference,
                  cursors: FieldReference,
                  nodeCursor: FieldReference,
                  relationshipScanCursor: FieldReference,
                  propertyCursor: FieldReference,
                  dataRead: FieldReference,
                  tokenRead: FieldReference,
                  schemaRead: FieldReference
                 )
