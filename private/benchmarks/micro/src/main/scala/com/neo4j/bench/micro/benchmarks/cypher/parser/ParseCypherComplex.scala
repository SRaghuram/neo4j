/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.cypher.parser

import com.neo4j.bench.jmh.api.config.BenchmarkEnabled
import com.neo4j.bench.jmh.api.config.ParamValues
import com.neo4j.bench.micro.Main
import com.neo4j.bench.micro.benchmarks.RNGState
import org.neo4j.cypher.internal.ast.Statement
import org.openjdk.jmh.annotations.Benchmark
import org.openjdk.jmh.annotations.BenchmarkMode
import org.openjdk.jmh.annotations.Mode
import org.openjdk.jmh.annotations.Param
import org.openjdk.jmh.annotations.Scope
import org.openjdk.jmh.annotations.Setup
import org.openjdk.jmh.annotations.State
import org.openjdk.jmh.annotations.TearDown

@BenchmarkEnabled(true)
class ParseCypherComplex extends BaseParserBenchmark {

  @ParamValues(
    allowed = Array("parboiled", "javacc"),
    base = Array("parboiled", "javacc"))
  @Param(Array[String]())
  var impl: String = _

  override def description = "Parse complex Cypher queries"

  @Benchmark
  @BenchmarkMode(Array(Mode.SampleTime))
  def parseQuery1(threadState: ParseCypherComplexState): Statement = {
    threadState.parseQuery(ParseCypherComplex.QUERY1)
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.SampleTime))
  def parseQuery2(threadState: ParseCypherComplexState): Statement = {
    threadState.parseQuery(ParseCypherComplex.QUERY2)
  }
}

object ParseCypherComplex {

  val QUERY1 =
    """
      |match (root:Component{cID:'7000-08001-0300150'})
      |
      |// step 1: recursively fetch full bom tree of given component and store it in `bomTree`
      |call apoc.path.expandConfig(root, {
      |   bfs:true,
      |   sequence: "Component,hasBOM>,BOM,hasBOMElement>,BOMElement,is>",
      |   beginSequenceAtStart:true
      |}) yield path
      |with nodes(path)[-1] as componentNode, [x in nodes(path) WHERE x:BOM] as boms, path, root
      |where componentNode:Component   // keine Zwischenknoten
      |	and length(path)>0  // ignoriere startknoten
      |	and all(x in boms WHERE not (x)-[:next]->() )  // nur aktuellsten BOM
      |match (componentNode)-[:hasComponentType]->(ct:ComponentType)
      |with collect({level:length(path)/3, id: componentNode.id, cID: componentNode.cID, name: componentNode.name, ctName: ct.name, ctId: ct.id}) as bomTree, root
      |
      |// step 2: find relevant components and their component type chains
      |match (root)-[:hasComponentType]->(:ComponentType)-[:hasExport]->(:Export)-[:basedOnExportMapping]->(em:ExportMapping)-[:hasRelevantComponent]->(rc:RelevantComponent)-[:nextPathItem*]->(lpi:PathItem)-[:with]->(ct:ComponentType)
      |// aggregate on 2 differnt levels
      |with rc, lpi, collect(ct.id) as ctids, bomTree
      |with rc, collect(lpi{.level, .number, ctids} ) as specs, bomTree
      |// filter for matching bomtree elements
      |with rc, [spec in specs | {spec:spec, components: [x in bomTree WHERE x.ctId in spec.ctids ]}] as componentsAndSpecs
      |// filter remaining bomtree elements by level and number specifiction
      |with rc, reduce(r=[], cs in componentsAndSpecs | r +
      |  coalesce(
      |      [x in cs.components WHERE
      |         x.level >= case when cs.spec.level in ["n","*"] then 1 else toInteger(cs.spec.level) end + coalesce(r[-1].level,0)
      |         AND x.level <= case when cs.spec.level in ["n","*"] then 999 else toInteger(cs.spec.level) end + coalesce(r[-1].level,0)
      |      ][cs.spec.number-1]
      |      ,[]  // safety-net with coalesce
      |  )
      |)[-1] as component  // pick end of chain
      |return rc.id, rc.name, component.id, component.cID, component.name, component.level
      |""".stripMargin

  val QUERY2 =
    """
      |with {
      |    exportMappingId: toInteger($exportMappingId),
      |    externalSystemId: toInteger($externalSystemId),
      |    cID: toString($cID),
      |    uomSetName: toUpper(toString($uomSetName)),
      |    workingLanguage: toUpper(toString($workingLanguage)),
      |    includeUoms: apoc.convert.toBoolean(false),
      |    templateShortId: $templateShortId
      |} as params
      |
      |// find the export mapping
      |match (exportMapping:ExportMapping)
      |where id(exportMapping)=params.exportMappingId
      |
      |match (exportMapping)-[:hasSection*]->(:EMSection)-[:hasEMItem]->(:EMItem)-[:linkedTo]-(usage:Usage)-[:in]->(externalSystem:ExternalSystem)
      |where id(externalSystem) = params.externalSystemId
      |
      |with params, exportMapping, collect(usage.uuid) as usageUuids
      |
      |// build section tree
      |// use OPTIONAL because not all nodes have :Term or :EMItem
      |MATCH sectionPath=(exportMapping)-[:hasSection*]->(emSection:EMSection)
      |OPTIONAL MATCH (emSection)-[:withName]->(sectionTerm:Term)-[:hasTranslation]->(sectionTranslation:Translation)-[:inLanguage]->(:Language {name: params.workingLanguage})
      |
      |// get for each section the linked :EMItem, use the tabIndex for sort.
      |OPTIONAL MATCH (emSection)-[hasEMItem:hasEMItem]->(emItem:EMItem)<-[:usedInEMItem]-(asElement:ASElement)
      |
      |WITH params,usageUuids,sectionPath,sectionTerm,sectionTranslation,emSection,emItem,hasEMItem.tabIndex AS hasEMItemTabIndex,asElement ORDER BY hasEMItemTabIndex
      |
      |OPTIONAL MATCH (emItem)-[:withName]->(emItemTerm:Term)-[:hasTranslation]->(emItemTranslation:Translation)-[:inLanguage]->(:Language {name: params.workingLanguage})
      |
      |// build the collection of :ASElement uuids to be inserted into the section tree
      |WITH params,usageUuids,sectionPath,sectionTerm,sectionTranslation,emSection,
      |     CASE WHEN COUNT(asElement)=0 THEN []
      |          ELSE COLLECT(distinct {id:id(asElement),uuid:asElement.uuid,name:COALESCE(emItemTranslation.name,emItemTerm.name,emItem.name)})
      |     END AS asElements
      |
      |// sort the sectionPaths, based on the concatenated tabIndex on the  [:hasSection] rel
      |WITH params,usageUuids,sectionPath,sectionTerm,sectionTranslation,emSection,asElements,
      |     REDUCE(s='',relation IN relationships(sectionPath)| s+RIGHT('0000'+toString(relation.tabIndex),4 )) AS sectionSort
      |     ORDER BY sectionSort
      |
      |// build the tree based on the sorted paths
      |// build the collection of Details that has to be inserted
      |WITH params,usageUuids,COLLECT(sectionPath) AS sectionPaths,
      |     COLLECT([id(emSection),
      |             CASE WHEN SIZE(asElements)>0 THEN {name:COALESCE(sectionTranslation.name,sectionTerm.name,emSection.name),elements:asElements}
      |                  ELSE {name:COALESCE(sectionTranslation.name,sectionTerm.name,emSection.name)}
      |             END
      |             ]) AS sectionDetails,
      |    COLLECT([id(emSection),
      |             apoc.map.fromPairs([[emSection.name, true]])
      |             ]) AS sectionType
      |CALL apoc.convert.toTree(sectionPaths) YIELD value AS sectionTree
      |
      |WITH params,usageUuids,apoc.map.updateTree(apoc.map.updateTree(sectionTree,'_id',sectionDetails),'_id',sectionType) AS sectionTree,sectionDetails
      |
      |// create output for the next process
      |// which is the sectionTree, and the list of uuid corresponding to the ASElements
      |// that have to be looked up in the BOM tree
      |UNWIND sectionDetails AS sectionDetail
      |
      |WITH params,usageUuids,sectionTree,sectionDetail WHERE SIZE(sectionDetail[1].elements)>0
      |
      |UNWIND sectionDetail[1].elements AS asElement
      |
      |WITH params,usageUuids,sectionTree,COLLECT(DISTINCT asElement.uuid) AS aseUuids
      |
      |MATCH (component:Component)
      |WHERE toString(component.cID)=params.cID
      |
      |WITH params,usageUuids,sectionTree,aseUuids,component
      |
      |// 2019-01-04 MR: needs to be adapted to the new BOM schema
      |match (bomElement:BOMElement)-[:hasUsage]->(usage:Usage)
      |where (COALESCE(bomElement.validFrom,'1999-01-01')) <= apoc.date.format(timestamp(),'ms','yyyy-MM-dd') <= (COALESCE(bomElement.validTo,'2999-12-31'))
      |  and usage.uuid in usageUuids
      |
      |//getting components
      |OPTIONAL MATCH path=(component)-[:has|is*1..]->(bomElement:BOMElement)-[:is]->(child:Component)
      |
      |//where ALL(m in [n IN nodes(path) WHERE n:BOMElement]
      |//          WHERE (COALESCE(m.validFrom,'1999-01-01')) <= beDate <= (COALESCE(m.validTo,'2999-12-31'))
      |//            AND (m)-[:hasUsage]->(:Usage)<-[:linkedTo]-(emItem)
      |//         )
      |
      |WITH params,sectionTree,aseUuids,[component]+COLLECT(child) AS components
      |
      |UNWIND components AS component
      |MATCH (uomSet:UomSet {name:params.uomSetName}),(wl:Language {name:params.workingLanguage})
      |MATCH path=(component)-[:hasComponentType]->(componentType:ComponentType)-[hasAttributeSet:hasAttributeSet]->(:AttributeSet)-[hasASElement:hasASElement]->(asElement:ASElement)-[:isAttribute]->(componentAttribute:ComponentAttribute)
      |WHERE asElement.uuid IN aseUuids
      |
      |OPTIONAL MATCH (componentAttribute)-[:hasType]->(at)
      |OPTIONAL MATCH (componentAttribute)-[:onCondition]-(cond:CACondition)
      |OPTIONAL MATCH (componentAttribute)-[:is]->(:Term)-[:hasTranslation]->(attrTr:Translation)-[:inLanguage]->(wl)
      |WITH params,sectionTree,component,componentAttribute,attrTr,uomSet,wl,hasASElement,asElement.uuid AS aseUuid,
      |     right('000'+COALESCE(toString(hasAttributeSet.sequence),''),3)
      |                    +right('000'+COALESCE(toString(hasASElement.sequence),''),3) AS attrseq,
      |
      |     collect(at.name) as attrType,
      |     collect(cond.name) as conditions
      |
      |MATCH aPath=(componentAttribute)-[:nextAtom*1..]->(da:DataAtom)
      |WHERE NOT (da)-[:nextAtom]->()
      |
      |UNWIND nodes(aPath)[0..] AS atom
      |
      |WITH DISTINCT params,sectionTree,component,componentAttribute,attrTr,attrType,atom,uomSet,wl,hasASElement,attrseq,conditions,aseUuid
      |
      |OPTIONAL MATCH (atom)-[:useFrom]->(sourceAtom:DataAtom)
      |
      |WITH params,sectionTree,component,componentAttribute,attrTr,attrType,hasASElement,attrseq,conditions,aseUuid,
      |     COLLECT(COALESCE(sourceAtom,atom))[1..] AS atoms,
      |     // atomsToHide is a collection of nodes to be hidden in the final output, e.g. in a formula
      |     [x IN COLLECT(CASE WHEN toBoolean(atom.hide) THEN id(sourceAtom) ELSE -1 END ) WHERE x > 0 ] AS atomsToHide,
      |     uomSet,wl
      |
      |UNWIND atoms AS atom
      |OPTIONAL MATCH (atom)<-[:is]-(dAV:DataAtomValue)<-[:nextAtomValue*1..]-(:ComponentAttributeValue)<-[:hasAttributeValue]-(component)
      |WHERE atom.type IN ['number','text']
      |OPTIONAL MATCH (atom {type:'term'})-[:is]->(t:Term)
      |OPTIONAL MATCH (atom)<-[:is]-(dAV2:DataAtomValue)<-[:nextAtomValue*1..]-(:ComponentAttributeValue)<-[:hasAttributeValue]-(component),(dAV2)-[:is]->(o:Option)
      |WHERE atom.type IN ['selection','selectionMulti']
      |OPTIONAL MATCH (atom)<-[:is]-(dAV3:DataAtomValue)<-[:nextAtomValue]-(:ComponentAttributeValue)<-[:hasAttributeValue]-(c),(dAV3)-[:is]->(t2:Term)
      |WHERE atom.type IN ['termLookup']
      |OPTIONAL MATCH (atom {type:'formula'})-[:hasUom]->(formulaUomSet:Uom)<-[:has]-(uomSet)
      |
      |WITH params,sectionTree,component,componentAttribute,attrTr,attrType,atom,atoms,dAV,t,t2,o,uomSet,formulaUomSet,atomsToHide,wl,hasASElement,attrseq,conditions,aseUuid
      |
      |OPTIONAL MATCH (t)-[:hasTranslation]->(tTr:Translation)-[:inLanguage]->(wl)
      |OPTIONAL MATCH (t2)-[:hasTranslation]->(t2Tr:Translation)-[:inLanguage]->(wl)
      |OPTIONAL MATCH (o)-[:is]->(ot:Term)-[:hasTranslation]->(otTr:Translation)-[:inLanguage]->(wl)
      |
      |WITH params,sectionTree,component,componentAttribute,attrTr,attrType,tTr,t2Tr,otTr,atom,atoms,dAV,t,o,ot,uomSet,formulaUomSet,atomsToHide,hasASElement,attrseq,conditions,aseUuid
      |ORDER BY o.sequence
      |
      |OPTIONAL MATCH uomPath=(dAV)-[:hasUom]->(uomDAV:Uom)-[r:convert*0..1]->(uomUomSet:Uom)<-[:has]-(uomSet)
      |
      |WITH params,sectionTree,component,componentAttribute,attrTr,attrType,tTr,t2Tr,atom,atoms,dAV,t,hasASElement,attrseq,conditions,aseUuid,
      |     COALESCE(r[0].formula,'{dAV}.value') as formula,uomUomSet,formulaUomSet,atomsToHide,COLLECT(COALESCE(otTr.name,ot.id,o.name)) as optionNames
      |ORDER BY apoc.coll.indexOf(atoms,atom)
      |
      |CALL apoc.cypher.doIt('RETURN '+formula+' AS value',{dAV:dAV}) YIELD value AS convertedValue
      |
      |WITH params,sectionTree,component,componentAttribute,attrTr,attrType,hasASElement,attrseq,conditions,aseUuid,
      |     COLLECT(
      |       {
      |         atom:id(atom),
      |         // value:COALESCE(convertedValue.value,t.id),
      |         value: CASE WHEN atom.type='number' THEN apoc.number.format(convertedValue.value,atom.formatPattern,toLower(params.workingLanguage)) // atom.formatLang)
      |                     WHEN atom.type='term' THEN COALESCE(tTr.name,t.id)
      |                     WHEN atom.type='termLookup' THEN COALESCE(t2Tr.name,t.id)
      |                     WHEN atom.type='selection' THEN optionNames[0]
      |                     WHEN atom.type='selectionMulti' THEN optionNames
      |                     WHEN atom.type='formula' THEN convertedValue.value
      |                     WHEN atom.type='text' THEN convertedValue.value
      |                END,
      |         element:atom.element,
      |         type:atom.type,
      |                               separator: atom.separator,
      |         formula:COALESCE(atom.formula,atom.cypher,'{value}'),
      |         pattern:atom.formatPattern,
      |         lang:atom.formatLang,
      |         uom:COALESCE(uomUomSet.name,formulaUomSet.name)
      |       }
      |     )  as attrElements,
      |     //create a map to be used for the formula DataAtom
      |     apoc.map.fromPairs(
      |       [
      |         x IN COLLECT(
      |           [
      |             atom.element, COALESCE(convertedValue.value,t.id)
      |           ]
      |         )
      |         WHERE NOT x[0] IS NULL
      |       ]
      |     ) as param,
      |     COLLECT(DISTINCT
      |       {
      |         hide: id(atom) IN atomsToHide,
      |         uom: COALESCE(uomUomSet.name,formulaUomSet.name)
      |       }
      |     ) AS Uoms,
      |     atomsToHide
      |
      |UNWIND attrElements AS ae
      |
      |CALL apoc.cypher.doIt(
      |  CASE
      |    WHEN ae.type='cypher' THEN ae.formula
      |    WHEN ae.type='formula' THEN 'RETURN apoc.number.format('
      |                                   + ae.formula
      |                                   + ',"' + ae.pattern
      |                                   + '","' + toLower(params.workingLanguage) // ae.lang
      |                                   + '") AS value'
      |    ELSE 'RETURN '+ae.formula+' AS value'
      |  END,
      |  {
      |    param: param,
      |    value: ae.value,
      |    cID: params.cID,
      |    workingLanguage: params.workingLanguage
      |  }
      |) YIELD value AS result
      |
      |WITH params,
      |     sectionTree,
      |     component,
      |     componentAttribute,
      |     attrTr,
      |     attrType,
      |     hasASElement,
      |     attrseq,
      |     conditions,
      |     aseUuid,
      |     atomsToHide,
      |     [x IN COLLECT(apoc.map.setKey(ae,'value',result.value)) WHERE NOT (x.atom IN atomsToHide)] AS attrElements,
      |     [x IN Uoms WHERE NOT (x.hide OR (x.uom IS NULL)) | x.uom] AS Uoms
      |
      |WITH params,
      |     sectionTree,
      |     id(component) AS componentid,
      |     id(componentAttribute) AS attributeid,
      |     aseUuid,
      |     COALESCE(attrTr.name,componentAttribute.name) AS attribute,
      |     REDUCE(result='', s in attrType | result + " " + s) as type,
      |     REDUCE(result='', s in conditions | result + " " + s) as condition,
      |     attrseq as index,
      |     REDUCE (
      |       s='',
      |       ae in attrElements |
      |       CASE WHEN ae.type <> 'cypher'
      |            THEN s + ae.separator +
      |                 CASE
      |                      WHEN ae.type="selectionMulti"
      |                      THEN REDUCE (s = '', i IN ae.value |  s + CASE WHEN s='' THEN '' ELSE ' | ' END ++i )
      |                      ELSE ae.value
      |                 END +
      |                 CASE
      |                      WHEN toBoolean(params.includeUoms)
      |                      THEN  ' ' + COALESCE(ae.uom,'')
      |                                                                                 ELSE ''
      |                 END
      |            ELSE ae.value
      |        END
      |     ) AS value,
      |                COALESCE(Uoms[0],'') AS uom
      |
      |ORDER BY componentid,index
      |
      |WITH params,
      |     sectionTree,
      |     COLLECT(
      |       [
      |         aseUuid,
      |         CASE
      |              WHEN toBoolean(params.includeUoms)
      |              THEN {value:value}
      |              ELSE {value:value,uom:uom}
      |         END
      |       ]
      |     ) AS attributeValues
      |
      |with apoc.map.updateTree(sectionTree,'uuid',attributeValues) AS exportMap,
      |     {shortid: params.templateShortId} as template
      |
      |return apoc.convert.toJson({template: template, data: exportMap}) AS JSON
      |""".stripMargin

  def main(args: Array[String]): Unit = {
    Main.run(classOf[ParseCypherComplex], args:_*)
  }
}

@State(Scope.Thread)
class ParseCypherComplexState extends BaseParserState {

  @Setup
  def setUp(benchmarkState: ParseCypherComplex, rngState: RNGState): Unit = {
    setUpParser(benchmarkState.impl)
  }

  @TearDown
  def tearDown(): Unit = {
    tearDownParser()
  }
}
