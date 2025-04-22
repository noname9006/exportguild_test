// wal-manager.js - Write-Ahead Log manager for Discord messages

const config = require('./config');
const sqlite3 = require('sqlite3').verbose();

// Global variables
let db = null;
let client = null;
let walCheckInterval = null;

/**
 * Initialize the WAL manager
 * @param {Discord.Client} discordClient - The Discord client instance
 * @param {sqlite3.Database} database - The SQLite database connection
 */
async function initialize(discordClient, database) {
  // Store references
  db = database;
  client = discordClient;
  
  // Create the WAL table if it doesn't exist
  await new Promise((resolve, reject) => {
    db.run(`
      CREATE TABLE IF NOT EXISTS message_wal (
          id TEXT PRIMARY KEY,
          content TEXT,
          authorId TEXT,
          authorUsername TEXT,
          authorBot INTEGER,
          timestamp INTEGER,
          createdAt TEXT,
          channelId TEXT,
          attachmentsJson TEXT,
          embedsJson TEXT,
          reactionsJson TEXT,
		  sticker_items TEXT
      )
    `, (err) => {
      if (err) {
        console.error('Error creating WAL table:', err);
        reject(err);
      } else {
        console.log('WAL table ready');
        resolve();
      }
    });
  });
  
  // Start periodic checking of WAL entries
  startWalChecking();
  
  return true;
}

/**
 * Start periodic checking of WAL entries
 */
function startWalChecking() {
  // Clear any existing interval
  if (walCheckInterval) {
    clearInterval(walCheckInterval);
  }
  
  // Get the check interval from config with default fallback
  const checkInterval = config.walCheckInterval || 60000;
  
  // Start new interval
  walCheckInterval = setInterval(() => {
    checkWalEntries().catch(err => {
      console.error('Error checking WAL entries:', err);
    });
  }, checkInterval);
  
  console.log(`WAL checker started with interval: ${checkInterval}ms`);
}

/**
 * Add a Discord message to the WAL
 * @param {Discord.Message} message - The Discord message to add
 */
async function addMessage(message) {
  // Return early if there's no database connection
  if (!db) {
    console.error('Cannot add message to WAL: No database connection');
    return false;
  }
  
  try {
    // Extract relevant data from the message
    const messageData = {
      message_id: message.id,
      channel_id: message.channelId,
      guild_id: message.guildId,
      content: message.content || '',
      author_id: message.author?.id || '',
      timestamp: message.createdTimestamp, // Using message's timestamp
      username: message.author?.username || '',
      global_name: message.author?.globalName || message.author?.username || '',
      avatar: message.author?.displayAvatarURL() || '',
      bot: message.author?.bot ? 1 : 0,
      attachments: JSON.stringify(Array.from(message.attachments.values())),
      embeds: JSON.stringify(message.embeds),
      reactions: JSON.stringify(Array.from(message.reactions.cache.values())),
      reference_message_id: message.reference?.messageId || null,
      reference_channel_id: message.reference?.channelId || null,
      thread_id: message.channel?.isThread() ? message.channel.id : null,
      sticker_items: JSON.stringify(Array.from(message.stickers?.values() || [])),
      processed: 0
    };

    // Insert into WAL table
    await new Promise((resolve, reject) => {
      db.run(`
        INSERT OR IGNORE INTO message_wal (
          message_id, channel_id, guild_id, content, author_id,
          timestamp, username, global_name, avatar, bot,
          attachments, embeds, reactions,
          reference_message_id, reference_channel_id, thread_id, sticker_items, processed
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      `, [
        messageData.message_id, messageData.channel_id, messageData.guild_id,
        messageData.content, messageData.author_id,
        messageData.timestamp, messageData.username, messageData.global_name,
        messageData.avatar, messageData.bot,
        messageData.attachments, messageData.embeds, messageData.reactions,
        messageData.reference_message_id, messageData.reference_channel_id,
        messageData.thread_id, messageData.sticker_items, messageData.processed
      ], function(err) {
        if (err) {
          console.error('Error adding message to WAL:', err);
          reject(err);
        } else {
          console.log(`Added message ${messageData.message_id} to WAL queue`);
          resolve(this.lastID);
        }
      });
    });
    
    return true;
  } catch (error) {
    console.error('Error processing message for WAL:', error);
    return false;
  }
}

/**
 * Check WAL entries for messages ready to be processed
 */
async function checkWalEntries() {
  // Return early if there's no database connection
  if (!db) {
    console.error('Cannot check WAL entries: No database connection');
    return false;
  }
  
  try {
    // Get message database timeout from config with default fallback
    const messageDbTimeout = config.messageDbTimeout || 3600000;
    
    // Calculate cutoff time for message timestamp
    // We'll process messages that are older than the timeout
    const ageThreshold = Date.now() - messageDbTimeout;
    
    // Get entries ready to be processed (using message timestamp)
    const entriesToProcess = await new Promise((resolve, reject) => {
      db.all(`SELECT * FROM message_wal WHERE timestamp <= ? AND processed = 0`, [ageThreshold], (err, rows) => {
        if (err) {
          console.error('Error getting WAL entries:', err);
          reject(err);
        } else {
          resolve(rows || []);
        }
      });
    });
    
    if (entriesToProcess.length === 0) {
      // No entries to process
      return true;
    }
    
    console.log(`Found ${entriesToProcess.length} WAL entries ready to be processed`);
    
    // Process each entry
    for (const entry of entriesToProcess) {
      try {
        // Mark as being processed
        await new Promise((resolve, reject) => {
          db.run(`UPDATE message_wal SET processed = 1 WHERE message_id = ?`, [entry.message_id], (err) => {
            if (err) reject(err);
            else resolve();
          });
        });
        
        // Check if the message exists in the messages table
        const exists = await new Promise((resolve, reject) => {
          db.get(`SELECT 1 FROM messages WHERE message_id = ?`, [entry.message_id], (err, row) => {
            if (err) reject(err);
            else resolve(!!row);
          });
        });
        
        if (exists) {
          console.log(`Message ${entry.message_id} already exists in database, skipping`);
          // Delete from WAL since it's already in the main table
          await new Promise((resolve, reject) => {
            db.run(`DELETE FROM message_wal WHERE message_id = ?`, [entry.message_id], (err) => {
              if (err) reject(err);
              else resolve();
            });
          });
          continue;
        }
        
        // Insert into messages table
        await new Promise((resolve, reject) => {
          db.run(`
            INSERT INTO messages (
              message_id, channel_id, guild_id, content, author_id,
              timestamp, username, global_name, avatar, bot,
              attachments, embeds, reactions,
              reference_message_id, reference_channel_id, thread_id, sticker_items
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
          `, [
            entry.message_id, entry.channel_id, entry.guild_id,
            entry.content, entry.author_id,
            entry.timestamp, entry.username, entry.global_name,
            entry.avatar, entry.bot,
            entry.attachments, entry.embeds, entry.reactions,
            entry.reference_message_id, entry.reference_channel_id,
            entry.thread_id, entry.sticker_items
          ], function(err) {
            if (err) {
              console.error(`Error inserting message ${entry.message_id} from WAL to main table:`, err);
              reject(err);
            } else {
              console.log(`Successfully moved message ${entry.message_id} from WAL to main table`);
              resolve(this.lastID);
            }
          });
        });
        
        // Delete from WAL after successful insert
        await new Promise((resolve, reject) => {
          db.run(`DELETE FROM message_wal WHERE message_id = ?`, [entry.message_id], (err) => {
            if (err) reject(err);
            else resolve();
          });
        });
        
      } catch (err) {
        console.error(`Error processing WAL entry for message ${entry.message_id}:`, err);
        
        // Reset processed flag so it can be retried
        await new Promise((resolve) => {
          db.run(`UPDATE message_wal SET processed = 0 WHERE message_id = ?`, [entry.message_id], () => {
            resolve();
          });
        }).catch(() => {});
      }
    }
    
    return true;
    
  } catch (error) {
    console.error('Error checking WAL entries:', error);
    return false;
  }
}

/**
 * Get statistics about the current WAL state
 */
async function getWalStats() {
  // Return early if there's no database connection
  if (!db) {
    console.error('Cannot get WAL stats: No database connection');
    return null;
  }
  
  try {
    // Get total entries count
    const totalCount = await new Promise((resolve, reject) => {
      db.get(`SELECT COUNT(*) as count FROM message_wal`, (err, row) => {
        if (err) reject(err);
        else resolve(row?.count || 0);
      });
    });
    
    // Get oldest message timestamp
    const oldest = await new Promise((resolve, reject) => {
      db.get(`SELECT MIN(timestamp) as oldest FROM message_wal`, (err, row) => {
        if (err) reject(err);
        else resolve(row?.oldest || null);
      });
    });
    
    // Get newest message timestamp
    const newest = await new Promise((resolve, reject) => {
      db.get(`SELECT MAX(timestamp) as newest FROM message_wal`, (err, row) => {
        if (err) reject(err);
        else resolve(row?.newest || null);
      });
    });
    
    // Get count of entries ready to be processed
    const messageDbTimeout = config.messageDbTimeout || 3600000;
    const ageThreshold = Date.now() - messageDbTimeout;
    
    const readyCount = await new Promise((resolve, reject) => {
      db.get(`SELECT COUNT(*) as count FROM message_wal WHERE timestamp <= ? AND processed = 0`, [ageThreshold], (err, row) => {
        if (err) reject(err);
        else resolve(row?.count || 0);
      });
    });
    
    // Calculate age statistics if we have entries
    let oldestAge = null;
    let newestAge = null;
    
    if (oldest) {
      oldestAge = Date.now() - oldest;
    }
    
    if (newest) {
      newestAge = Date.now() - newest;
    }
    
    return {
      totalEntries: totalCount,
      readyToProcess: readyCount,
      oldestTimestamp: oldest ? new Date(oldest).toISOString() : null,
      newestTimestamp: newest ? new Date(newest).toISOString() : null,
      oldestAgeMs: oldestAge,
      newestAgeMs: newestAge,
      oldestAgeFormatted: oldestAge ? formatDuration(oldestAge) : null,
      newestAgeFormatted: newestAge ? formatDuration(newestAge) : null
    };
    
  } catch (error) {
    console.error('Error getting WAL stats:', error);
    return null;
  }
}

/**
 * Format a duration in milliseconds to a human-readable string
 * @param {number} ms - Duration in milliseconds
 * @returns {string} Formatted duration
 */
function formatDuration(ms) {
  const seconds = Math.floor(ms / 1000);
  const minutes = Math.floor(seconds / 60);
  const hours = Math.floor(minutes / 60);
  const days = Math.floor(hours / 24);
  
  if (days > 0) {
    return `${days}d ${hours % 24}h ${minutes % 60}m ${seconds % 60}s`;
  } else if (hours > 0) {
    return `${hours}h ${minutes % 60}m ${seconds % 60}s`;
  } else if (minutes > 0) {
    return `${minutes}m ${seconds % 60}s`;
  } else {
    return `${seconds}s`;
  }
}

/**
 * Clean up resources on shutdown
 */
function shutdown() {
  if (walCheckInterval) {
    clearInterval(walCheckInterval);
    walCheckInterval = null;
  }
  
  // Database connection will be closed by the main application
}

// Export functions
module.exports = {
  initialize,
  addMessage,
  checkWalEntries,
  getWalStats,
  shutdown
};