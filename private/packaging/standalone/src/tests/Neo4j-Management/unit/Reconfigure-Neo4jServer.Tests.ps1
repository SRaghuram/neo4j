#
# Copyright (c) 2002-2018 "Neo Technology,"
# Network Engine for Objects in Lund AB [http://neotechnology.com]
# This file is a commercial add-on to Neo4j Enterprise Edition.
#

$here = Split-Path -Parent $MyInvocation.MyCommand.Path
$sut = (Split-Path -Leaf $MyInvocation.MyCommand.Path).Replace(".Tests.", ".")
$common = Join-Path (Split-Path -Parent $here) 'Common.ps1'
. $common

Import-Module "$src\Neo4j-Management.psm1"

InModuleScope Neo4j-Management {
  Describe "Reconfigure-Neo4jServer" {

    # Setup mocking environment
    # Mock Java environment
    $javaHome = global:New-MockJavaHome
    Mock Get-Neo4jEnv { $javaHome } -ParameterFilter { $Name -eq 'JAVA_HOME' }
    Mock Set-Neo4jEnv { }
    Mock Test-Path { $false } -ParameterFilter {
      $Path -like 'Registry::*\JavaSoft\Java Runtime Environment'
    }
    Mock Get-ItemProperty { $null } -ParameterFilter {
      $Path -like 'Registry::*\JavaSoft\Java Runtime Environment*'
    }
    # Mock Neo4j environment
    Mock Get-Neo4jEnv { $global:mockNeo4jHome } -ParameterFilter { $Name -eq 'NEO4J_HOME' }
    Mock Start-Process { throw "Should not call Start-Process mock" }

    Context "Invalid or missing specified neo4j installation" {
      $serverObject = global:New-InvalidNeo4jInstall

      It "throws if invalid or missing neo4j directory" {
        { Reconfigure-Neo4jServer -Neo4jServer $serverObject -ErrorAction Stop } | Should Throw
      }
    }

    Context "Non-existing service" {
      Mock Get-Service -Verifiable { return $null }
      $serverObject = global:New-MockNeo4jInstall
      $result = Reconfigure-Neo4jServer -Neo4jServer $serverObject

      It "returns 1 for service that does not exist" {
        $result | Should Be 1
        Assert-VerifiableMocks
      }
    }

    Context "Reconfigure service failure" {
      Mock Get-Service -Verifiable { return "Fake service" }
      Mock Start-Process -Verifiable { throw "Error reconfiguring" }
      $serverObject = global:New-MockNeo4jInstall

      It "throws when reconfigure encounters an error" {
        { Reconfigure-Neo4jServer -Neo4jServer $serverObject } | Should Throw
        Assert-VerifiableMocks
      }
    }

    Context "Reconfigure service success" {
      Mock Get-Service -Verifiable { return "Fake service" }
      Mock Start-Process -Verifiable { @{'ExitCode' = 0} }
      $serverObject = global:New-MockNeo4jInstall
      $result = Reconfigure-Neo4jServer -Neo4jServer $serverObject

      It "returns 0 when successfully reconfigured" {
        $result | Should Be 0
        Assert-VerifiableMocks
      }
    }

  }
}
