// channel-monitor.js - Module to monitor channel creation and deletion events
const { ChannelType } = require('discord.js');
const config = require('./config');
const monitor = require('./monitor');

/**
 * Initialize channel monitoring
 * @param {Client} client - Discord client
 */
function initializeChannelMonitoring(client) {
  console.log(`[${getFormattedDateTime()}] Channel monitoring initialized`);

  // Listen for channel creation events
  client.on('channelCreate', async (channel) => {
    try {
      // Only process channels in guilds (ignore DMs)
      if (!channel.guild) return;

      // Only process text-based channels
      if (!isTextBasedChannel(channel)) {
        console.log(`[${getFormattedDateTime()}] Ignoring new non-text channel: ${channel.name} (${channel.id})`);
        return;
      }

      // Check if channel is excluded
      if (config.excludedChannels.includes(channel.id)) {
        console.log(`[${getFormattedDateTime()}] Ignoring excluded channel: ${channel.name} (${channel.id})`);
        return;
      }

      // Check if bot has access to channel
      if (!canAccessChannel(channel)) {
        console.log(`[${getFormattedDateTime()}] Cannot access new channel: ${channel.name} (${channel.id})`);
        return;
      }

      console.log(`[${getFormattedDateTime()}] New channel detected: ${channel.name} (${channel.id})`);

      // Only process if a database is initialized for this guild
      const dbExists = monitor.checkDatabaseExists(channel.guild);
      if (!dbExists) {
        console.log(`[${getFormattedDateTime()}] Skipping new channel tracking - no database for guild ${channel.guild.name}`);
        return;
      }

      // Add channel to monitoring
      await monitor.markChannelFetchingStarted(channel.id, channel.name);
      await monitor.markChannelFetchingCompleted(channel.id);
      console.log(`[${getFormattedDateTime()}] Added new channel to monitoring: ${channel.name} (${channel.id})`);

      // Optionally send a notification to a designated log channel
      await sendNotification(client, channel, 'added');

    } catch (error) {
      console.error(`[${getFormattedDateTime()}] Error processing new channel:`, error);
    }
  });

  // Listen for channel deletion events
client.on('channelDelete', async (channel) => {
  try {
    // Only process channels in guilds (ignore DMs)
    if (!channel.guild) return;

    console.log(`[${getFormattedDateTime()}] Channel deleted: ${channel.name} (${channel.id})`);
    
    // Mark the channel as deleted in the database
    await markChannelAsDeleted(channel.id, channel.name);
    
    // Optionally send a notification to a designated log channel
    await sendNotification(client, channel, 'deleted');
    
  } catch (error) {
    console.error(`[${getFormattedDateTime()}] Error processing deleted channel:`, error);
  }
});

// Don't forget to export the function
module.exports = {
  initializeChannelMonitoring,
  markChannelAsDeleted
};

  // Listen for thread creation events
  client.on('threadCreate', async (thread) => {
    try {
      // Check if thread is excluded
      if (config.excludedChannels.includes(thread.id)) {
        console.log(`[${getFormattedDateTime()}] Ignoring excluded thread: ${thread.name} (${thread.id})`);
        return;
      }

      // Check if bot has access to thread
      if (!canAccessChannel(thread)) {
        console.log(`[${getFormattedDateTime()}] Cannot access new thread: ${thread.name} (${thread.id})`);
        return;
      }

      console.log(`[${getFormattedDateTime()}] New thread detected: ${thread.name} (${thread.id})`);

      // Only process if a database is initialized for this guild
      const dbExists = monitor.checkDatabaseExists(thread.guild);
      if (!dbExists) {
        console.log(`[${getFormattedDateTime()}] Skipping new thread tracking - no database for guild ${thread.guild.name}`);
        return;
      }

      // Add thread to monitoring
      await monitor.markChannelFetchingStarted(thread.id, thread.name);
      await monitor.markChannelFetchingCompleted(thread.id);
      console.log(`[${getFormattedDateTime()}] Added new thread to monitoring: ${thread.name} (${thread.id})`);

      // Optionally send a notification to a designated log channel
      await sendNotification(client, thread, 'added', true);

    } catch (error) {
      console.error(`[${getFormattedDateTime()}] Error processing new thread:`, error);
    }
  });
}

async function markChannelAsDeleted(channelId, channelName) {
  try {
    const db = monitor.getDatabase();
    if (!db) {
      console.log(`No database available to mark channel ${channelName} (${channelId}) as deleted`);
      return false;
    }
    
    return new Promise((resolve, reject) => {
      // Update the channel record to mark it as deleted
      const sql = `
        UPDATE channels 
        SET deleted = 1, deletedAt = ? 
        WHERE id = ?
      `;
      
      db.run(sql, [Date.now(), channelId], function(err) {
        if (err) {
          console.error(`Error marking channel ${channelId} as deleted:`, err);
          reject(err);
          return;
        }
        
        if (this.changes > 0) {
          console.log(`Marked channel ${channelName} (${channelId}) as deleted in database`);
          resolve(true);
        } else {
          console.log(`Channel ${channelId} not found in database or already marked as deleted`);
          resolve(false);
        }
      });
    });
  } catch (error) {
    console.error(`Error in markChannelAsDeleted for ${channelId}:`, error);
    return false;
  }
}

/**
 * Check if a channel is text-based and suitable for monitoring
 * @param {Channel} channel - The Discord channel object
 * @returns {boolean} Whether channel is text-based
 */
function isTextBasedChannel(channel) {
  return channel.type === ChannelType.GuildText || 
         channel.type === ChannelType.GuildForum ||
         channel.type === ChannelType.PublicThread ||
         channel.type === ChannelType.PrivateThread;
}

/**
 * Check if the bot can access the channel
 * @param {Channel} channel - The Discord channel object
 * @returns {boolean} Whether bot can access channel
 */
function canAccessChannel(channel) {
  // Skip if channel isn't viewable
  if (!channel.viewable) return false;

  // Check for message history permission
  const permissions = channel.permissionsFor(channel.guild.members.me);
  if (!permissions || !permissions.has('ReadMessageHistory')) return false;

  return true;
}

/**
 * Send notification about channel changes to a log channel
 * @param {Client} client - Discord client
 * @param {Channel} channel - The affected channel
 * @param {string} action - The action performed (added/deleted)
 * @param {boolean} isThread - Whether the channel is a thread
 */
async function sendNotification(client, channel, action, isThread = false) {
  // Check if log channel is configured
  const logChannelId = config.getConfig('channelMonitorLogChannel', 'CHANNEL_MONITOR_LOG');
  if (!logChannelId) return;

  try {
    const logChannel = await client.channels.fetch(logChannelId);
    if (!logChannel) return;

    const channelType = isThread ? 'Thread' : 'Channel';
    const emoji = action === 'added' ? '➕' : '🗑️';
    const color = action === 'added' ? '#00FF00' : '#FF0000';
    
    let parentInfo = '';
    if (isThread && channel.parent) {
      parentInfo = `\n• Parent Channel: <#${channel.parentId}> (${channel.parent.name})`;
    } else if (channel.parent) {
      parentInfo = `\n• Category: ${channel.parent.name}`;
    }

    await logChannel.send({
      embeds: [{
        title: `${emoji} ${channelType} ${action === 'added' ? 'Created' : 'Deleted'}`,
        description: `${channelType} has been ${action} and ${action === 'added' ? 'added to' : 'removed from'} monitoring.`,
        color: color,
        fields: [
          { name: 'Name', value: channel.name, inline: true },
          { name: 'ID', value: channel.id, inline: true },
          { name: 'Guild', value: channel.guild.name, inline: true }
        ],
        footer: { text: `Time: ${getFormattedDateTime()}` }
      }]
    });
  } catch (error) {
    console.error(`[${getFormattedDateTime()}] Error sending notification:`, error);
  }
}

/**
 * Format date and time for logs (UTC, YYYY-MM-DD HH:MM:SS format)
 * @returns {string} Formatted date time string
 */
function getFormattedDateTime() {
  const now = new Date();
  return `${now.getUTCFullYear()}-${String(now.getUTCMonth() + 1).padStart(2, '0')}-${String(now.getUTCDate()).padStart(2, '0')} ${String(now.getUTCHours()).padStart(2, '0')}:${String(now.getUTCMinutes()).padStart(2, '0')}:${String(now.getUTCSeconds()).padStart(2, '0')}`;
}

module.exports = {
  initializeChannelMonitoring
};