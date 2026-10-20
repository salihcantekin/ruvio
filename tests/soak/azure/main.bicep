// The cluster of one soak run: three data-node VMs (one per zone) and a controller VM, in
// their own resource group. Delete the resource group after the run; nothing here is meant to
// outlive it. The storage account for status files and the budget alert are in storage.bicep
// and budget.bicep, deployed once.
//
// az group create -n rg-ruvio-soak -l uksouth
// az deployment group create -g rg-ruvio-soak -f tests/soak/azure/main.bicep \
//   -p sshPublicKey="$(cat ~/.ssh/ruvio_azure.pub)" runKey="$(cat run-key.pub)" \
//      allowedSshCidr="$(curl -s https://ifconfig.me)/32" autoShutdownTimeUtc=0330

@description('Region. uksouth offers both VM sizes in zones 1, 2 and 3.')
param location string = resourceGroup().location

@description('Name prefix of every resource.')
param prefix string = 'ruvio-soak'

@description('Admin user on every VM (passwordless sudo, SSH key only).')
param adminUsername string = 'soak'

@description('Public key of the operator; it reaches the controller only.')
param sshPublicKey string

@description('Public key of this run, generated per run; the controller uses it to reach the data nodes.')
param runKey string

@description('The only address allowed to SSH to the controller, as a /32.')
param allowedSshCidr string

@description('Size of the three data-node VMs (each runs a coordinator and one member of each group).')
param nodeSize string = 'Standard_D4ads_v6'

@description('Size of the controller VM (load, checker, fault injection).')
param controllerSize string = 'Standard_D4as_v6'

@description('Data disk of each data node, mounted on /var/lib/ruvio.')
param dataDiskSizeGb int = 128

@allowed(['Premium_LRS', 'StandardSSD_LRS'])
param dataDiskSku string = 'Premium_LRS'

@description('Daily UTC time (HHmm) at which every VM is deallocated: a dead-man switch for a forgotten run. Set it to the planned end of the run plus a margin.')
param autoShutdownTimeUtc string

@description('Tag values; ttl-hours is informational, the auto-shutdown enforces it.')
param runId string = utcNow('yyyyMMdd-HHmm')
param ttlHours int = 8

var tags = {
  purpose: 'ruvio-soak'
  'run-id': runId
  'ttl-hours': string(ttlHours)
}
var subnetPrefix = '10.42.1.0/24'
var controllerIp = '10.42.1.10'
var nodeIps = ['10.42.1.11', '10.42.1.12', '10.42.1.13']
var image = {
  publisher: 'Canonical'
  offer: 'ubuntu-24_04-lts'
  sku: 'server'
  version: 'latest'
}

resource nsg 'Microsoft.Network/networkSecurityGroups@2024-05-01' = {
  name: '${prefix}-nsg'
  location: location
  tags: tags
  properties: {
    securityRules: [
      {
        name: 'ssh-operator-to-controller'
        properties: {
          priority: 100
          direction: 'Inbound'
          access: 'Allow'
          protocol: 'Tcp'
          sourceAddressPrefix: allowedSshCidr
          sourcePortRange: '*'
          destinationAddressPrefix: controllerIp
          destinationPortRange: '22'
        }
      }
      {
        // Ruvio, replication and coordinator ports stay inside the subnet.
        name: 'subnet-internal'
        properties: {
          priority: 200
          direction: 'Inbound'
          access: 'Allow'
          protocol: '*'
          sourceAddressPrefix: subnetPrefix
          sourcePortRange: '*'
          destinationAddressPrefix: subnetPrefix
          destinationPortRange: '*'
        }
      }
      {
        name: 'deny-other-inbound'
        properties: {
          priority: 4000
          direction: 'Inbound'
          access: 'Deny'
          protocol: '*'
          sourceAddressPrefix: '*'
          sourcePortRange: '*'
          destinationAddressPrefix: '*'
          destinationPortRange: '*'
        }
      }
    ]
  }
}

resource vnet 'Microsoft.Network/virtualNetworks@2024-05-01' = {
  name: '${prefix}-vnet'
  location: location
  tags: tags
  properties: {
    addressSpace: { addressPrefixes: ['10.42.0.0/16'] }
    subnets: [
      {
        name: 'cluster'
        properties: {
          addressPrefix: subnetPrefix
          networkSecurityGroup: { id: nsg.id }
        }
      }
    ]
  }
}

resource controllerPip 'Microsoft.Network/publicIPAddresses@2024-05-01' = {
  name: '${prefix}-ctl-ip'
  location: location
  tags: tags
  sku: { name: 'Standard' }
  zones: ['1']
  properties: {
    publicIPAllocationMethod: 'Static'
  }
}

resource controllerNic 'Microsoft.Network/networkInterfaces@2024-05-01' = {
  name: '${prefix}-ctl-nic'
  location: location
  tags: tags
  properties: {
    enableAcceleratedNetworking: true
    ipConfigurations: [
      {
        name: 'ipconfig'
        properties: {
          subnet: { id: vnet.properties.subnets[0].id }
          privateIPAllocationMethod: 'Static'
          privateIPAddress: controllerIp
          publicIPAddress: { id: controllerPip.id }
        }
      }
    ]
  }
}

resource nodeNics 'Microsoft.Network/networkInterfaces@2024-05-01' = [for (ip, i) in nodeIps: {
  name: '${prefix}-node${i + 1}-nic'
  location: location
  tags: tags
  properties: {
    enableAcceleratedNetworking: true
    ipConfigurations: [
      {
        name: 'ipconfig'
        properties: {
          subnet: { id: vnet.properties.subnets[0].id }
          privateIPAllocationMethod: 'Static'
          privateIPAddress: ip
        }
      }
    ]
  }
}]

var linuxConfiguration = {
  disablePasswordAuthentication: true
  ssh: {
    publicKeys: [
      {
        path: '/home/${adminUsername}/.ssh/authorized_keys'
        keyData: sshPublicKey
      }
      {
        path: '/home/${adminUsername}/.ssh/authorized_keys'
        keyData: runKey
      }
    ]
  }
}

resource controller 'Microsoft.Compute/virtualMachines@2024-07-01' = {
  name: '${prefix}-ctl'
  location: location
  tags: tags
  zones: ['1']
  properties: {
    hardwareProfile: { vmSize: controllerSize }
    osProfile: {
      computerName: 'ctl'
      adminUsername: adminUsername
      linuxConfiguration: linuxConfiguration
      customData: loadFileAsBase64('cloud-init/controller.yaml')
    }
    storageProfile: {
      imageReference: image
      osDisk: {
        createOption: 'FromImage'
        deleteOption: 'Delete'
        diskSizeGB: 64
        managedDisk: { storageAccountType: 'Premium_LRS' }
      }
    }
    networkProfile: {
      networkInterfaces: [{ id: controllerNic.id, properties: { deleteOption: 'Delete' } }]
    }
  }
}

resource nodes 'Microsoft.Compute/virtualMachines@2024-07-01' = [for (ip, i) in nodeIps: {
  name: '${prefix}-node${i + 1}'
  location: location
  tags: tags
  zones: [string(i + 1)]
  properties: {
    hardwareProfile: { vmSize: nodeSize }
    osProfile: {
      computerName: 'node${i + 1}'
      adminUsername: adminUsername
      linuxConfiguration: linuxConfiguration
      customData: loadFileAsBase64('cloud-init/node.yaml')
    }
    storageProfile: {
      imageReference: image
      osDisk: {
        createOption: 'FromImage'
        deleteOption: 'Delete'
        diskSizeGB: 64
        managedDisk: { storageAccountType: 'Premium_LRS' }
      }
      dataDisks: [
        {
          lun: 0
          createOption: 'Empty'
          deleteOption: 'Delete'
          diskSizeGB: dataDiskSizeGb
          caching: 'None'
          managedDisk: { storageAccountType: dataDiskSku }
        }
      ]
    }
    networkProfile: {
      networkInterfaces: [{ id: nodeNics[i].id, properties: { deleteOption: 'Delete' } }]
    }
  }
}]

// Auto-shutdown deallocates the VM, which stops compute billing (a VM stopped from inside
// the OS keeps billing).
resource controllerShutdown 'Microsoft.DevTestLab/schedules@2018-09-15' = {
  name: 'shutdown-computevm-${controller.name}'
  location: location
  tags: tags
  properties: {
    status: 'Enabled'
    taskType: 'ComputeVmShutdownTask'
    dailyRecurrence: { time: autoShutdownTimeUtc }
    timeZoneId: 'UTC'
    targetResourceId: controller.id
    notificationSettings: { status: 'Disabled' }
  }
}

resource nodeShutdown 'Microsoft.DevTestLab/schedules@2018-09-15' = [for (ip, i) in nodeIps: {
  name: 'shutdown-computevm-${nodes[i].name}'
  location: location
  tags: tags
  properties: {
    status: 'Enabled'
    taskType: 'ComputeVmShutdownTask'
    dailyRecurrence: { time: autoShutdownTimeUtc }
    timeZoneId: 'UTC'
    targetResourceId: nodes[i].id
    notificationSettings: { status: 'Disabled' }
  }
}]

output controllerPublicIp string = controllerPip.properties.ipAddress
output nodePrivateIps array = nodeIps
output adminUsername string = adminUsername
