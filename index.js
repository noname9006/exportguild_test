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
const channelList = require('./channelList');
const monitor = require('./monitor');
const vacuum = require('./vacuum');
const walManager = require('./wal-manager');
const autoVacuum = require('./vacuum-auto');
const channelMonitor = require('./channel-monitor');
const memberTracker = require('./member-tracker');


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
// Track guilds that have databases initialized
const initializedGuilds = new Set();

client.once('ready', async () => {
  console.log(`Bot is ready! Logged in as ${client.user.tag}`);
  
  try {
    // Make sure monitoring is not started multiple times
    if (!global.monitoringActive) {
      global.monitoringActive = true;
      monitor.processMessageCache();
      console.log('Message cache processing started globally');
    }
    
    // For each guild the bot is connected to, check if a database exists
    for (const [guildId, guild] of client.guilds.cache) {
      try {
        // This will check if a database exists without creating one
        const dbExists = monitor.checkDatabaseExists(guild);
        if (dbExists) {
          console.log(`Found existing database for guild ${guild.name} (${guild.id})`);
          // Initialize database with existing file
          await monitor.initializeDatabase(guild);
          
          // Get the database instance from monitor instead of using global db
          const db = monitor.getDatabase();
          
          // Initialize WAL manager with the database from monitor
          if (db) {
            await walManager.initialize(client, db);
            
            // Initialize member tracking tables in the database
            await memberTracker.initializeMemberDatabase(db);
            
            // Mark guild as initialized
            initializedGuilds.add(guildId);
            console.log(`Database initialized for guild ${guild.name} (${guild.id}), monitoring active`);
          } else {
            console.error(`Database not available for guild ${guild.name} after initialization`);
          }
        } else {
          console.log(`No database found for guild ${guild.name} (${guild.id}). Will create one when !exportguild is used.`);
        }
      } catch (error) {
        console.error(`Error checking database for guild ${guild.name} (${guild.id}):`, error);
      }
    }
	  
    // Initialize auto-vacuum schedule
    autoVacuum.initializeAutoVacuum(client, {
      // You can add a log channel ID if you want vacuum notifications in a specific channel
      // logChannelId: 'YOUR_LOG_CHANNEL_ID' 
    });
	
    // Initialize channel monitoring
    if (config.getConfig('channelMonitoringEnabled', 'CHANNEL_MONITORING_ENABLED')) {
      channelMonitor.initializeChannelMonitoring(client);
      console.log('Channel monitoring initialized');
    }
    
    // Initialize member monitoring
    if (config.getConfig('memberTrackingEnabled', 'MEMBER_TRACKING_ENABLED')) {
      memberTracker.initializeMemberTracking(client);
      console.log('Member tracking initialized');
    }
    
  } catch (error) {
    console.error('Error during startup:', error);
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
  // Ignore messages from bots for both commands and monitoring
  if (message.author.bot) return;
  
  const guildId = message.guildId;
  
  // First check if this guild has an initialized database
  if (initializedGuilds.has(guildId)) {
    // Check if we should be monitoring this message's channel
    const shouldMonitor = monitor.shouldMonitorChannel(message.channelId);
    
    // If this is a message to be monitored
    if (shouldMonitor) {
      console.log(`Monitoring message ${message.id} in channel ${message.channelId}`);
      
      // Add message to WAL manager
      try {
        await walManager.addMessage(message);
      } catch (error) {
        console.error(`Error adding message ${message.id} to WAL:`, error);
      }
    }
  }

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
        // Initialize the database with this guild info if not already initialized
        if (!initializedGuilds.has(message.guildId)) {
          try {
            await monitor.initializeDatabase(message.guild);
            
            // Get the database from monitor and initialize WAL manager
            const db = monitor.getDatabase();
            if (db) {
              await walManager.initialize(client, db);
              
              // Initialize member tracking tables in the database
              await memberTracker.initializeMemberDatabase(db);
            }
            
            initializedGuilds.add(message.guildId);
            console.log(`Database initialized for guild ${message.guild.name} (${message.guild.id})`);
            console.log(`Using database: ${monitor.getCurrentDatabasePath()}`);
          } catch (dbError) {
            console.error('Error initializing database:', dbError);
          }
        }
        
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
  
  else if (command === '!vacuum') {
    // Check if an operation is already running for this guild
    if (activeOperations.has(message.guildId)) {
      return message.reply('An operation is already running for this guild!');
    }
    
    // Set guild as being processed
    activeOperations.add(message.guildId);

    try {
      // Log the command execution with timestamp and user info
      const timestamp = getFormattedDateTime();
      console.log(`[${timestamp}] Command: !vacuum executed by ${message.author.tag} (${message.author.id}) in guild ${message.guild.name} (${message.guild.id})`);
      
      // Run the vacuum command
      await vacuum.handleVacuumCommand(message, monitor);
      
      // Log successful completion
      console.log(`[${getFormattedDateTime()}] Completed: !vacuum for ${message.guild.name} (${message.guild.id})`);
    } catch (error) {
      console.error(`[${getFormattedDateTime()}] Error: !vacuum command failed:`, error);
      message.channel.send(`Error vacuuming database: ${error.message}`);
    } finally {
      // Remove guild from active operations when done (even if there was an error)
      activeOperations.delete(message.guildId);
    }
    
    return;
  }
  
  else if (command === '!memberstats') {
    // Check if user has administrator permissions
    if (!message.member.permissions.has(PermissionFlagsBits.Administrator)) {
      return message.reply('You need administrator permissions to use this command.');
    }
    
    // Check if an operation is already running for this guild
    if (activeOperations.has(message.guildId)) {
      return message.reply('An operation is already running for this guild!');
    }
    
    // Set guild as being processed
    activeOperations.add(message.guildId);
    
    // Log the command execution with timestamp and user info
    const timestamp = getFormattedDateTime();
    console.log(`[${timestamp}] Command: !memberstats executed by ${message.author.tag} (${message.author.id}) in guild ${message.guild.name} (${message.guild.id})`);
    
    try {
      const statusMessage = await message.channel.send('Generating member statistics...');
      
      // Get the database
      const db = monitor.getDatabase();
      if (!db) {
        throw new Error('No database available. Please run !exportguild first to set up the database.');
      }
      
      // Run the queries
      db.all(`SELECT COUNT(*) as totalMembers FROM guild_members WHERE leftGuild = 0`, [], async (err, totalResult) => {
        if (err) throw err;
        
        db.all(`SELECT COUNT(*) as botCount FROM guild_members WHERE bot = 1 AND leftGuild = 0`, [], async (err, botResult) => {
          if (err) throw err;
          
          db.all(`SELECT roleName, COUNT(memberId) as memberCount FROM member_roles GROUP BY roleId ORDER BY memberCount DESC LIMIT 10`, [], async (err, roleResult) => {
            if (err) throw err;
            
            db.all(`SELECT username, displayName, COUNT(roleId) as roleCount FROM guild_members 
                    JOIN member_roles ON guild_members.id = member_roles.memberId 
                    WHERE leftGuild = 0 
                    GROUP BY memberId ORDER BY roleCount DESC LIMIT 10`, [], async (err, memberResult) => {
              if (err) throw err;
              
              // Format the response
              const totalMembers = totalResult[0].totalMembers;
              const botCount = botResult[0].botCount;
              const humanCount = totalMembers - botCount;
              
              let response = `# Member Statistics for ${message.guild.name}\n\n`;
              response += `**Total Members:** ${totalMembers}\n`;
              response += `**Human Members:** ${humanCount}\n`;
              response += `**Bot Members:** ${botCount}\n\n`;
              
              response += `## Top 10 Roles by Member Count\n`;
              if (roleResult.length === 0) {
                response += "*No role data available*\n";
              } else {
                roleResult.forEach(role => {
                  response += `- **${role.roleName}**: ${role.memberCount} members\n`;
                });
              }
              
              response += `\n## Top 10 Members by Role Count\n`;
              if (memberResult.length === 0) {
                response += "*No member role data available*\n";
              } else {
                memberResult.forEach(member => {
                  response += `- **${member.displayName}** (${member.username}): ${member.roleCount} roles\n`;
                });
              }

              db.all(`SELECT COUNT(*) as totalRoles FROM guild_roles WHERE deleted = 0`, [], async (err, roleCountResult) => {
  if (err) throw err;

  db.all(`SELECT name, color, position, mentionable, hoist FROM guild_roles WHERE deleted = 0 ORDER BY position DESC LIMIT 15`, [], async (err, roleDetailResult) => {
    if (err) throw err;
    
    // Add this to your response:
    response += `\n## Role Statistics\n`;
    response += `**Total Roles:** ${roleCountResult[0].totalRoles}\n\n`;
    
    if (roleDetailResult.length > 0) {
      response += `### Top ${roleDetailResult.length} Roles (by hierarchy)\n`;
      roleDetailResult.forEach(role => {
        // Add emoji indicators for special properties
        const hoistEmoji = role.hoist ? '📌' : ''; // Hoisted/displayed separately
        const mentionEmoji = role.mentionable ? '🔔' : ''; // Mentionable
        
        response += `- ${hoistEmoji}${mentionEmoji} **${role.name}** (Position: ${role.position})\n`;
      });
    } else {
      response += "*No role data available*\n";
    }
    
    // Continue with updating the status message
  });
});
			  
              // Add timestamp to the report
              response += `\n\n*Report generated: ${getFormattedDateTime()} UTC*`;
              
              // Update the status message with the statistics
              await statusMessage.edit(response);
              
              // Log successful completion
              console.log(`[${getFormattedDateTime()}] Completed: !memberstats for ${message.guild.name} (${message.guild.id})`);
            });
          });
        });
      });
    } catch (error) {
      console.error(`[${getFormattedDateTime()}] Error: !memberstats command failed:`, error);
      message.channel.send(`Error generating member statistics: ${error.message}`);
    } finally {
      // Remove guild from active operations when done
      activeOperations.delete(message.guildId);
    }
  }
});

// Login to Discord
console.log('Starting Discord bot...');
console.log(`Current Date and Time (UTC): ${getFormattedDateTime()}`);
console.log(`Current User's Login: noname9006`);
client.login(process.env.DISCORD_TOKEN).catch(error => {
  console.error('Failed to login:', error);
  process.exit(1);
});