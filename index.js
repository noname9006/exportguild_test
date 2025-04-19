// index.js - Main bot entry point
require('dotenv').config();
const fs = require('fs');
const path = require('path');
const { 
  Client, 
  GatewayIntentBits, 
  Partials,
  EmbedBuilder,
  PermissionFlagsBits
} = require('discord.js');

// Import modules
const exportGuild = require('./exportguild');
const processData = require('./processData');
const config = require('./config');
const channelList = require('./channelList'); // Import the channelList module
const monitor = require('./monitor'); // Import the monitor module

// Set up the Discord client with necessary intents to read messages
const client = new Client({
  intents: [
    GatewayIntentBits.Guilds,
    GatewayIntentBits.GuildMessages,
    GatewayIntentBits.MessageContent,
    GatewayIntentBits.GuildMembers
  ],
  partials: [
    Partials.Channel,
    Partials.Message,
    Partials.ThreadMember
  ]
});

// Track active operations to prevent multiple operations in the same guild
const activeOperations = new Set();

client.once('ready', async () => {
  console.log(`Bot is ready! Logged in as ${client.user.tag}`);
  
  // Check if database exists and initialize
  const dbExists = monitor.checkDatabaseExists();
  console.log(`Database exists: ${dbExists}`);
  
  try {
    await monitor.initializeDatabase();
    console.log('Database initialized');
    
    // Start processing the message cache
    monitor.processMessageCache();
    
    // If database existed, load channels that were being fetched
    if (dbExists) {
      const fetchedChannels = await monitor.getFetchedChannels();
      console.log(`Found ${fetchedChannels.length} channels that have been fetched or are being fetched`);
      
      // Pre-populate the fetching sets for monitoring
      for (const channel of fetchedChannels) {
        if (channel.fetchCompleted === 1) {
          console.log(`Channel ${channel.id} (${channel.name}) fetching is complete`);
        } else {
          console.log(`Channel ${channel.id} (${channel.name}) fetching is in progress`);
        }
      }
    }
  } catch (error) {
    console.error('Error initializing database:', error);
  }
});

// Function to handle excluded channels commands
async function handleExcludedChannelsCommands(message, args) {
  // Check if user has administrator permissions
  if (!message.member.permissions.has(PermissionFlagsBits.Administrator)) {
    return message.reply('You need administrator permissions to manage excluded channels.');
  }

  const subCommand = args[1]?.toLowerCase();

  // !ex list command
  if (!subCommand || subCommand === 'list') {
    const embed = new EmbedBuilder()
      .setTitle('Excluded Channels')
      .setDescription('These channels are excluded from the export process:')
      .setColor('#0099ff')
      .setTimestamp()
      .setFooter({ 
        text: `Requested by ${message.author.username}`,
        iconURL: message.author.displayAvatarURL() 
      });

    if (config.excludedChannels.length === 0) {
      embed.addFields({ name: 'No channels excluded', value: 'All channels will be exported.' });
    } else {
      // Create a field for each excluded channel
      let channelList = '';
      
      for (const channelId of config.excludedChannels) {
        // Try to fetch the channel to get its name
        try {
          const channel = await message.guild.channels.fetch(channelId);
          if (channel) {
            channelList += `• <#${channelId}> (${channel.name})\n`;
          } else {
            channelList += `• Channel ID: ${channelId} (not found in server)\n`;
          }
        } catch (error) {
          // Channel might not exist anymore
          channelList += `• Channel ID: ${channelId} (not accessible)\n`;
        }
      }
      
      embed.addFields({ name: `${config.excludedChannels.length} Excluded Channels`, value: channelList || 'Error retrieving channels' });
    }

    return message.channel.send({ embeds: [embed] });
  }
  
  // !ex add command
  else if (subCommand === 'add') {
    if (args.length < 3) {
      return message.reply('Please provide a channel ID, URL, or mention to add it to the exclusion list.');
    }

    // Get all arguments after "add" as potential channel references
    const channelReferences = args.slice(2);
    const addedChannels = [];
    const failedChannels = [];

    for (const channelRef of channelReferences) {
      // Try to resolve the channel reference
      let channelId = channelRef.trim();
      
      // Handle channel mentions
      if (channelRef.startsWith('<#') && channelRef.endsWith('>')) {
        channelId = channelRef.substring(2, channelRef.length - 1);
      }
      // Handle URLs
      else if (channelRef.includes('/channels/')) {
        const parts = channelRef.split('/');
        channelId = parts[parts.length - 1];
      }

      // Try to fetch the channel to validate it exists
      try {
        const channel = await message.guild.channels.fetch(channelId);
        if (channel) {
          // Valid channel, add to exclusion list
          if (config.addExcludedChannel(channelId)) {
            addedChannels.push(`<#${channelId}> (${channel.name})`);
          } else {
            failedChannels.push(`<#${channelId}> - already in the exclusion list`);
          }
        } else {
          failedChannels.push(`${channelRef} - channel not found`);
        }
      } catch (error) {
        // Could not fetch channel or invalid ID
        failedChannels.push(`${channelRef} - ${error.message}`);
      }
    }

    // Create response message
    let response = '';
    if (addedChannels.length > 0) {
      response += `✅ Added ${addedChannels.length} channel(s) to the exclusion list:\n`;
      response += addedChannels.map(ch => `• ${ch}`).join('\n');
    }
    if (failedChannels.length > 0) {
      if (response) response += '\n\n';
      response += `❌ Failed to add ${failedChannels.length} channel(s):\n`;
      response += failedChannels.map(ch => `• ${ch}`).join('\n');
    }

    return message.channel.send(response || 'No channels were processed.');
  }
  
  // !ex remove command
  else if (subCommand === 'remove') {
    if (args.length < 3) {
      return message.reply('Please provide a channel ID, URL, or mention to remove it from the exclusion list.');
    }

    // Get all arguments after "remove" as potential channel references
    const channelReferences = args.slice(2);
    const removedChannels = [];
    const failedChannels = [];

    for (const channelRef of channelReferences) {
      // Try to resolve the channel reference
      let channelId = channelRef.trim();
      
      // Handle channel mentions
      if (channelRef.startsWith('<#') && channelRef.endsWith('>')) {
        channelId = channelRef.substring(2, channelRef.length - 1);
      }
      // Handle URLs
      else if (channelRef.includes('/channels/')) {
        const parts = channelRef.split('/');
        channelId = parts[parts.length - 1];
      }

      // Try to fetch the channel to validate it (if possible)
      try {
        const channel = await message.guild.channels.fetch(channelId);
        if (channel) {
          // Valid channel, remove from exclusion list
          if (config.removeExcludedChannel(channelId)) {
            removedChannels.push(`<#${channelId}> (${channel.name})`);
          } else {
            failedChannels.push(`<#${channelId}> - not in the exclusion list`);
          }
        } else {
          // Channel not found in the server but try to remove it anyway
          if (config.removeExcludedChannel(channelId)) {
            removedChannels.push(`Channel ID: ${channelId} (not found in server)`);
          } else {
            failedChannels.push(`${channelRef} - not in the exclusion list`);
          }
        }
      } catch (error) {
        // Could not fetch channel, but still try to remove by ID
        if (config.removeExcludedChannel(channelId)) {
          removedChannels.push(`Channel ID: ${channelId} (not accessible)`);
        } else {
          failedChannels.push(`${channelRef} - ${error.message}`);
        }
      }
    }

    // Create response message
    let response = '';
    if (removedChannels.length > 0) {
      response += `✅ Removed ${removedChannels.length} channel(s) from the exclusion list:\n`;
      response += removedChannels.map(ch => `• ${ch}`).join('\n');
    }
    if (failedChannels.length > 0) {
      if (response) response += '\n\n';
      response += `❌ Failed to remove ${failedChannels.length} channel(s):\n`;
      response += failedChannels.map(ch => `• ${ch}`).join('\n');
    }

    return message.channel.send(response || 'No channels were processed.');
  }
  
  // Unknown subcommand
  else {
    return message.reply('Unknown subcommand. Available commands: `!ex list`, `!ex add <channel>`, `!ex remove <channel>`');
  }
}

// Helper function for formatted current date and time (UTC)
function getFormattedDateTime() {
  const now = new Date();
  return `${now.getUTCFullYear()}-${String(now.getUTCMonth() + 1).padStart(2, '0')}-${String(now.getUTCDate()).padStart(2, '0')} ${String(now.getUTCHours()).padStart(2, '0')}:${String(now.getUTCMinutes()).padStart(2, '0')}:${String(now.getUTCSeconds()).padStart(2, '0')}`;
}

// Command handler
client.on('messageCreate', async (message) => {
  // First check if we should be monitoring this message's channel
  const shouldMonitor = monitor.shouldMonitorChannel(message.channelId);
  
  // If this is a message to be monitored (not from a bot, in a channel we're monitoring)
  if (shouldMonitor && !message.author.bot) {
    console.log(`Monitoring message ${message.id} in channel ${message.channelId}`);
    
    // Add to message cache for later verification and storage
    monitor.addMessageToCache(message);
  }
  
  // Ignore messages from bots for commands
  if (message.author.bot) return;

  const args = message.content.trim().split(/\s+/);
  const command = args[0].toLowerCase();

  // Handle excluded channels commands
  if (command === '!ex') {
    await handleExcludedChannelsCommands(message, args);
    return;
  }
  
  // Handle channellist command
  else if (command === '!channellist') {
    // Check if user has administrator permissions
    if (!message.member.permissions.has(PermissionFlagsBits.Administrator)) {
      return message.reply('You need administrator permissions to use the channel list command.');
    }
    
    // Check if an operation is already running for this guild
    if (activeOperations.has(message.guildId)) {
      return message.reply('An operation is already running for this guild!');
    }
    
    // Set guild as being processed
    activeOperations.add(message.guildId);
    
    // Log the command execution with timestamp and user info
    const timestamp = getFormattedDateTime();
    console.log(`[${timestamp}] Command: !channellist executed by ${message.author.tag} (${message.author.id}) in guild ${message.guild.name} (${message.guild.id})`);
    
    try {
      // Call the handleChannelListCommand from the channelList module
      await channelList.handleChannelListCommand(message);
      
      // Log successful completion
      console.log(`[${getFormattedDateTime()}] Completed: !channellist for ${message.guild.name} (${message.guild.id})`);
    } catch (error) {
      console.error(`[${getFormattedDateTime()}] Error: !channellist command failed:`, error);
      message.channel.send(`Error generating channel list: ${error.message}`);
    } finally {
      // Remove guild from active operations when done (even if there was an error)
      activeOperations.delete(message.guildId);
    }
    
    return;
  }
  
  // Existing commands for exportguild
  else if (command === '!exportguild') {
    // Check if an operation is already running for this guild
    if (activeOperations.has(message.guildId)) {
      return message.reply('An operation is already running for this guild!');
    }
    
    // Set guild as being processed
    activeOperations.add(message.guildId);

    try {
      const subCommand = args[1]?.toLowerCase();
      
      if (!subCommand || subCommand === 'export') {
        // Export guild data
        await exportGuild.handleExportGuild(message, client);
        
        // After export is complete, check for duplicates in the database
        try {
          const duplicates = await monitor.checkForDuplicates();
          console.log(`Database duplicate check complete. Found ${duplicates} duplicate message IDs.`);
          if (duplicates > 0) {
            await message.channel.send(`✅ Export completed! Note: Found and removed ${duplicates} duplicate message entries in the database.`);
          }
        } catch (dbError) {
          console.error('Error checking for duplicates:', dbError);
        }
      } else if (subCommand === 'process') {
        // Process NDJSON data
        await processData.processNDJSON(message);
      } else {
        message.reply('Unknown subcommand. Available commands: `!exportguild` or `!exportguild process`');
      }
    } catch (error) {
      console.error('Critical error:', error);
      message.channel.send(`Critical error during operation: ${error.message}`);
    } finally {
      // Remove guild from active operations when done (even if there was an error)
      activeOperations.delete(message.guildId);
    }
  }
});

// Login to Discord
console.log('Starting Discord bot...');
console.log(`Current Date and Time (UTC): 2025-04-19 11:44:58`);
console.log(`Current User's Login: noname9006`);
client.login(process.env.DISCORD_TOKEN).catch(error => {
  console.error('Failed to login:', error);
  process.exit(1);
});