#
# Copyright (c) 2002-2018 "Neo4j,"
# Neo4j Sweden AB [http://neo4j.com]
# This file is a commercial add-on to Neo4j Enterprise Edition.
#

$here = Split-Path -Parent $MyInvocation.MyCommand.Path
$sut = (Split-Path -Leaf $MyInvocation.MyCommand.Path).Replace(".Tests.", ".")
$common = Join-Path (Split-Path -Parent $here) 'Common.ps1'
. $common

Import-Module "$src\Neo4j-Management.psm1"

InModuleScope Neo4j-Management {
  Describe "Get-Neo4jWindowsServiceName" {

    Mock Get-Neo4jEnv { $global:mockNeo4jHome } -ParameterFilter { $Name -eq 'NEO4J_HOME' }
    Mock Set-Neo4jEnv { }

    Context "Missing service name in configuration files" {
      $serverObject = global:New-MockNeo4jInstall -WindowsService ''

      It "throws error for missing service name in configuration file" {
        { Get-Neo4jWindowsServiceName -Neo4jServer $serverObject -ErrorAction Stop } | Should Throw
      }
    }

    Context "Service name in configuration files" {
      $serverObject = global:New-MockNeo4jInstall

      It "returns Service name in configuration file" {
        Get-Neo4jWindowsServiceName -Neo4jServer $serverObject | Should be $global:mockServiceName
      }
    }
  }
}
