@description('Location for all resources')
param location string = resourceGroup().location

@description('Prefix for resource names')
param namePrefix string = 'openemr'

@description('Environment name (dev, staging, prod)')
param environment string = 'dev'

@description('OpenEMR container image')
param openEmrImage string = 'openemr/openemr:latest'

@description('MySQL administrator login')
param mysqlAdminLogin string = 'openemradmin'

@secure()
@description('MySQL administrator password')
param mysqlAdminPassword string

@description('Allow public access to MySQL')
param allowPublicAccess bool = false

@description('Virtual network CIDR')
param vnetCidr string = '10.0.0.0/16'

@description('Container Apps subnet CIDR')
param containerAppsSubnetCidr string = '10.0.1.0/23'

@description('MySQL subnet CIDR')
param mysqlSubnetCidr string = '10.0.2.0/24'

@description('Enable zone redundancy')
param enableZoneRedundancy bool = false

@description('Enable TLS ingress')
param enableTlsIngress bool = true

// Variables
var uniqueSuffix = uniqueString(resourceGroup().id)
var containerAppEnvName = '${namePrefix}-env-${environment}-${uniqueSuffix}'
var containerAppName = '${namePrefix}-app-${environment}-${uniqueSuffix}'
var mysqlServerName = '${namePrefix}-mysql-${environment}-${uniqueSuffix}'
var logAnalyticsName = '${namePrefix}-logs-${environment}-${uniqueSuffix}'az 
var appInsightsName = '${namePrefix}-insights-${environment}-${uniqueSuffix}'
var vnetName = '${namePrefix}-vnet-${environment}-${uniqueSuffix}'
var mysqlDatabaseName = 'openemr'

// Log Analytics Workspace
resource logAnalytics 'Microsoft.OperationalInsights/workspaces@2023-09-01' = {
  name: logAnalyticsName
  location: location
  properties: {
    sku: {
      name: 'PerGB2018'
    }
    retentionInDays: 30
    features: {
      enableLogAccessUsingOnlyResourcePermissions: true
    }
    publicNetworkAccessForIngestion: 'Enabled'
    publicNetworkAccessForQuery: 'Enabled'
  }
  tags: {
    Environment: environment
    Application: 'OpenEMR'
  }
}

// Application Insights
resource appInsights 'Microsoft.Insights/components@2020-02-02' = {
  name: appInsightsName
  location: location
  kind: 'web'
  properties: {
    Application_Type: 'web'
    WorkspaceResourceId: logAnalytics.id
    IngestionMode: 'LogAnalytics'
    publicNetworkAccessForIngestion: 'Enabled'
    publicNetworkAccessForQuery: 'Enabled'
  }
  tags: {
    Environment: environment
    Application: 'OpenEMR'
  }
}

// Virtual Network
resource vnet 'Microsoft.Network/virtualNetworks@2023-09-01' = {
  name: vnetName
  location: location
  properties: {
    addressSpace: {
      addressPrefixes: [
        vnetCidr
      ]
    }
    subnets: [
      {
        name: 'container-apps-subnet'
        properties: {
          addressPrefix: containerAppsSubnetCidr
          delegations: [
            {
              name: 'Microsoft.App.environments'
              properties: {
                serviceName: 'Microsoft.App/environments'
              }
            }
          ]
        }
      }
      {
        name: 'mysql-subnet'
        properties: {
          addressPrefix: mysqlSubnetCidr
          delegations: [
            {
              name: 'Microsoft.DBforMySQL.flexibleServers'
              properties: {
                serviceName: 'Microsoft.DBforMySQL/flexibleServers'
              }
            }
          ]
        }
      }
    ]
  }
  tags: {
    Environment: environment
    Application: 'OpenEMR'
  }
}

// Private DNS Zone for MySQL
resource mysqlPrivateDnsZone 'Microsoft.Network/privateDnsZones@2020-06-01' = if (!allowPublicAccess) {
  name: '${namePrefix}.mysql.database.azure.com'
  location: 'global'
  tags: {
    Environment: environment
    Application: 'OpenEMR'
  }
}

// Link Private DNS Zone to VNet
resource mysqlPrivateDnsZoneLink 'Microsoft.Network/privateDnsZones/virtualNetworkLinks@2020-06-01' = if (!allowPublicAccess) {
  parent: mysqlPrivateDnsZone
  name: '${vnetName}-link'
  location: 'global'
  properties: {
    registrationEnabled: false
    virtualNetwork: {
      id: vnet.id
    }
  }
}

// MySQL Flexible Server
resource mysqlServer 'Microsoft.DBforMySQL/flexibleServers@2023-12-30' = {
  name: mysqlServerName
  location: location
  sku: {
    name: 'Standard_B2s'
    tier: 'Burstable'
  }
  properties: {
    administratorLogin: mysqlAdminLogin
    administratorLoginPassword: mysqlAdminPassword
    version: '8.0.21'
    storage: {
      storageSizeGB: 32
      iops: 120
      autoGrow: 'Enabled'
      logOnDisk: 'Disabled'
    }
    backup: {
      backupRetentionDays: 7
      geoRedundantBackup: enableZoneRedundancy ? 'Enabled' : 'Disabled'
    }
    highAvailability: {
      mode: enableZoneRedundancy ? 'ZoneRedundant' : 'Disabled'
    }
    network: {
      delegatedSubnetResourceId: allowPublicAccess ? null : '${vnet.id}/subnets/mysql-subnet'
      privateDnsZoneResourceId: allowPublicAccess ? null : mysqlPrivateDnsZone.id
      publicNetworkAccess: allowPublicAccess ? 'Enabled' : 'Disabled'
    }
    createMode: 'Default'
  }
  tags: {
    Environment: environment
    Application: 'OpenEMR'
  }

  dependsOn: [
    mysqlPrivateDnsZoneLink
  ]
}

// MySQL Database
resource mysqlDatabase 'Microsoft.DBforMySQL/flexibleServers/databases@2023-12-30' = {
  parent: mysqlServer
  name: mysqlDatabaseName
  properties: {
    charset: 'utf8mb4'
    collation: 'utf8mb4_unicode_ci'
  }
}

// Container Apps Environment
resource containerAppEnv 'Microsoft.App/managedEnvironments@2025-01-01' = {
  name: containerAppEnvName
  location: location
  properties: {
    appLogsConfiguration: {
      destination: 'log-analytics'
      logAnalyticsConfiguration: {
        customerId: logAnalytics.properties.customerId
        sharedKey: logAnalytics.listKeys().primarySharedKey
      }
    }
    vnetConfiguration: {
      infrastructureSubnetId: '${vnet.id}/subnets/container-apps-subnet'
      internal: false
    }
    zoneRedundant: enableZoneRedundancy
    daprAIConnectionString: appInsights.properties.ConnectionString
  }
  tags: {
    Environment: environment
    Application: 'OpenEMR'
  }
}

// OpenEMR Container App
resource openEmrContainerApp 'Microsoft.App/containerApps@2025-01-01' = {
  name: containerAppName
  location: location
  properties: {
    environmentId: containerAppEnv.id
    configuration: {
      activeRevisionsMode: 'Single'
      ingress: {
        external: true
        targetPort: 80
        transport: 'auto'
        allowInsecure: !enableTlsIngress
        traffic: [
          {
            weight: 100
            latestRevision: true
          }
        ]
      }
      secrets: [
        {
          name: 'mysql-password'
          value: mysqlAdminPassword
        }
      ]
    }
    template: {
      containers: [
        {
          name: 'openemr'
          image: openEmrImage
          resources: {
            cpu: 1
            memory: '2Gi'
          }
          env: [
            {
              name: 'MYSQL_HOST'
              value: mysqlServer.properties.fullyQualifiedDomainName
            }
            {
              name: 'MYSQL_ROOT_PASS'
              secretRef: 'mysql-password'
            }
            {
              name: 'MYSQL_USER'
              value: mysqlAdminLogin
            }
            {
              name: 'MYSQL_PASS'
              secretRef: 'mysql-password'
            }
            {
              name: 'MYSQL_DATABASE'
              value: mysqlDatabaseName
            }
            {
              name: 'OE_USER'
              value: 'admin'
            }
            {
              name: 'OE_PASS'
              value: 'admin123'
            }
            {
              name: 'EASY_DEV_MODE'
              value: environment == 'dev' ? 'yes' : 'no'
            }
            {
              name: 'EASY_DEV_MODE_NEW'
              value: environment == 'dev' ? 'yes' : 'no'
            }
            {
              name: 'DEVELOPER_TOOLS'
              value: environment == 'dev' ? 'yes' : 'no'
            }
            {
              name: 'XDEBUG_ON'
              value: environment == 'dev' ? '1' : '0'
            }
            {
              name: 'APPLICATIONINSIGHTS_CONNECTION_STRING'
              value: appInsights.properties.ConnectionString
            }
          ]
          probes: [
            {
              type: 'Liveness'
              httpGet: {
                path: '/'
                port: 80
                scheme: 'HTTP'
              }
              initialDelaySeconds: 60
              periodSeconds: 30
              timeoutSeconds: 10
              failureThreshold: 3
            }
            {
              type: 'Readiness'
              httpGet: {
                path: '/'
                port: 80
                scheme: 'HTTP'
              }
              initialDelaySeconds: 30
              periodSeconds: 10
              timeoutSeconds: 5
              failureThreshold: 3
            }
          ]
        }
      ]
      scale: {
        minReplicas: 1
        maxReplicas: environment == 'prod' ? 10 : 3
        rules: [
          {
            name: 'http-scaling'
            http: {
              metadata: {
                concurrentRequests: '50'
              }
            }
          }
          {
            name: 'cpu-scaling'
            custom: {
              type: 'cpu'
              metadata: {
                type: 'Utilization'
                value: '70'
              }
            }
          }
        ]
      }
    }
  }
  tags: {
    Environment: environment
    Application: 'OpenEMR'
  }

  dependsOn: [
    mysqlDatabase
  ]
}

// Outputs
@description('Container App FQDN')
output containerAppFqdn string = openEmrContainerApp.properties.configuration.ingress.fqdn

@description('MySQL Server FQDN')
output mysqlServerFqdn string = mysqlServer.properties.fullyQualifiedDomainName

@description('Application Insights Connection String')
output appInsightsConnectionString string = appInsights.properties.ConnectionString

@description('Log Analytics Workspace ID')
output logAnalyticsWorkspaceId string = logAnalytics.id

@description('OpenEMR Application URL')
output openEmrUrl string = 'https://${openEmrContainerApp.properties.configuration.ingress.fqdn}'

@description('Resource Group Name')
output resourceGroupName string = resourceGroup().name

@description('Container Apps Environment Name')
output containerAppEnvironmentName string = containerAppEnv.name

@description('MySQL Database Name')
output mysqlDatabaseName string = mysqlDatabase.name
