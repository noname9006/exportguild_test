// config.js - Configuration settings for the Discord guild export bot
const fs = require('fs');
const path = require('path');

// Configuration object
const config = {
  // List of channel IDs to exclude from export
  excludedChannels: [
    '1162761808541659208',
    '1162761859619885076',
    '1162761889537863760',
    '1162761923872432308',
    '1162761953983340614',
    '1317824160843432056',
    '1322505396778172426',
    '1317881540176248904',
    '1338452316109668393',
    '1329766199248031776',
    '1142961066163327089',
    '1151246155141890098',
    '937915189435703329',
    '1142972007080808468'
  ],
  
  // Export threshold - how many messages to process before auto-save
  exportThreshold: 1000,
  
  // Memory limit in MB - default to 500MB
  memoryLimitMB: 500,
  
  // Memory check interval in milliseconds (20 seconds)
  memoryCheckInterval: 1200000,
  
  // Auto-save interval in milliseconds (90 seconds)
  autoSaveInterval: 90000,
  
  // Message database timeout in milliseconds (1 hour)
  messageDbTimeout: 3600000,
  
  // WAL entry retention time in milliseconds (7 days)
  walRetentionTime: 604800000,
  
  // Database batch insert size - how many messages to insert at once during export
  dbBatchSize: 1000,
  
  // Monitor batch insert size - how many messages to insert at once during monitoring
  monitorBatchSize: 5,
  
  // Status update interval in milliseconds 
  statusUpdateInterval: 20000,
  
  // How often to check the WAL for aged entries (in milliseconds)
  walCheckInterval: 6100000,
  
   // Cron schedule for automatic vacuum operations (default: every day at 2 AM)
  vacuumCronSchedule: "0 2 * * *",
  
  // Whether automatic vacuum is enabled
  autoVacuumEnabled: true,
  
    // Channel where monitoring notifications will be sent (set to empty string to disable)
  channelMonitorLogChannel: "", 
  
  // Whether automatic channel monitoring is enabled
  channelMonitoringEnabled: true,
  
  // Helper function to get environment variables or use defaults from this config
  getConfig: function(key, envName) {
    // If environment variable exists, use it, otherwise use config value
    if (process.env[envName] !== undefined) {
      // Handle number conversion for numeric values
      if (typeof this[key] === 'number') {
        return parseInt(process.env[envName]);
      }
      // Handle array conversion for channel lists
      else if (Array.isArray(this[key]) && envName === 'EX_CHANNELS') {
        return process.env[envName].split(',').map(id => id.trim());
      }
      return process.env[envName];
    }
    // Return the default config value
    return this[key];
  },
  
  // Add a channel to the excluded channels list
  addExcludedChannel: function(channelId) {
    // Make sure we have a clean channel ID (no spaces, only the ID numbers)
    channelId = channelId.trim();
    
    // Extract the ID if a URL or mention was provided
    if (channelId.includes('/')) {
      // Handle URL format (extract the last part of the URL)
      channelId = channelId.split('/').pop();
    } else if (channelId.startsWith('<#') && channelId.endsWith('>')) {
      // Handle channel mention format (<#123456789>)
      channelId = channelId.substring(2, channelId.length - 1);
    }
    
    // Check if the channel is already in the list
    if (!this.excludedChannels.includes(channelId)) {
      this.excludedChannels.push(channelId);
      this.saveConfig();
      return true;
    }
    return false;
  },
  
  // Remove a channel from the excluded channels list
  removeExcludedChannel: function(channelId) {
    // Clean up the channel ID input
    channelId = channelId.trim();
    
    // Extract the ID if a URL or mention was provided
    if (channelId.includes('/')) {
      // Handle URL format
      channelId = channelId.split('/').pop();
    } else if (channelId.startsWith('<#') && channelId.endsWith('>')) {
      // Handle channel mention format
      channelId = channelId.substring(2, channelId.length - 1);
    }
    
    // Find the index of the channel in the array
    const index = this.excludedChannels.indexOf(channelId);
    
    // Remove the channel if found
    if (index !== -1) {
      this.excludedChannels.splice(index, 1);
      this.saveConfig();
      return true;
    }
    return false;
  },
  
  // Save the current configuration to the config.js file
  saveConfig: function() {
    try {
      // Get the path to this file
      const configPath = path.resolve(__dirname, 'config.js');
      
      // Create a formatted representation of the excluded channels array
      const channelsStr = this.excludedChannels
        .map(id => `    '${id}'`)
        .join(',\n');
      
      // Create the new file content
      const fileContent = `// config.js - Configuration settings for the Discord guild export bot
const fs = require('fs');
const path = require('path');

// Configuration object
const config = {
  // List of channel IDs to exclude from export
  excludedChannels: [
${channelsStr}
  ],
  
  // Export threshold - how many messages to process before auto-save
  exportThreshold: ${this.exportThreshold},
  
  // Memory limit in MB - default to 500MB
  memoryLimitMB: ${this.memoryLimitMB},
  
  // Memory check interval in milliseconds (20 seconds)
  memoryCheckInterval: ${this.memoryCheckInterval},
  
  // Auto-save interval in milliseconds (90 seconds)
  autoSaveInterval: ${this.autoSaveInterval},
  
  // Message database timeout in milliseconds (1 hour)
  messageDbTimeout: ${this.messageDbTimeout},
  
  // WAL entry retention time in milliseconds (7 days)
  walRetentionTime: ${this.walRetentionTime},
  
  // Database batch insert size - how many messages to insert at once during export
  dbBatchSize: ${this.dbBatchSize},
  
  // Monitor batch insert size - how many messages to insert at once during monitoring
  monitorBatchSize: ${this.monitorBatchSize},
  
  // Status update interval in milliseconds
  statusUpdateInterval: ${this.statusUpdateInterval},
  
  // How often to check the WAL for aged entries (in milliseconds)
  walCheckInterval: ${this.walCheckInterval},
  
  // Helper function to get environment variables or use defaults from this config
  getConfig: ${this.getConfig.toString()},
  
  // Add a channel to the excluded channels list
  addExcludedChannel: ${this.addExcludedChannel.toString()},
  
  // Remove a channel from the excluded channels list
  removeExcludedChannel: ${this.removeExcludedChannel.toString()},
  
  // Save the current configuration to the config.js file
  saveConfig: ${this.saveConfig.toString()}
};

module.exports = config;`;
      
      // Write the file
      fs.writeFileSync(configPath, fileContent, 'utf8');
      return true;
    } catch (error) {
      console.error('Error saving config:', error);
      return false;
    }
  }
};

module.exports = config;