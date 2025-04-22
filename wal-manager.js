// wal-manager.js - Write-Ahead Log manager for Discord messages
const config = require('./config');
const sqlite3 = require('sqlite3').verbose();
const { extractMessageMetadata } = require('./monitor');

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
  
  // Create the WAL table if it doesn't exist - using the same structure as the messages table
  // plus the processed flag
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
          sticker_items TEXT,
          processed INTEGER DEFAULT 0
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
    
    // Also clean up old WAL entries
    cleanupOldWalEntries().catch(err => {
      console.error('Error cleaning up old WAL entries:', err);
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
    // Use the same message extraction logic from monitor.js for consistency
    const messageData = extractMessageMetadata(message);
    
    // Convert objects to JSON strings
    const attachmentsJson = JSON.stringify(messageData.attachments);
    const embedsJson = JSON.stringify(messageData.embeds);
    const reactionsJson = JSON.stringify(messageData.reactions);
    
    // Insert into WAL table using consistent column names with the messages table
    await new Promise((resolve, reject) => {
      db.run(`
        INSERT OR IGNORE INTO message_wal (
          id, content, authorId, authorUsername, authorBot, 
          timestamp, createdAt, channelId, 
          attachmentsJson, embedsJson, reactionsJson, processed
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      `, [
        messageData.id, 
        messageData.content, 
        messageData.authorId, 
        messageData.authorUsername, 
        messageData.authorBot ? 1 : 0, 
        messageData.timestamp, 
        messageData.createdAt, 
        messageData.channelId,
        attachmentsJson,
        embedsJson,
        reactionsJson,
        0 // processed flag initially set to 0
      ], function(err) {
        if (err) {
          console.error('Error adding message to WAL:', err);
          reject(err);
        } else {
          console.log(`Added message ${messageData.id} to WAL queue`);
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
    
    // Log current time for reference
    const now = new Date();
    const currentTime = `${now.getUTCFullYear()}-${String(now.getUTCMonth() + 1).padStart(2, '0')}-${String(now.getUTCDate()).padStart(2, '0')} ${String(now.getUTCHours()).padStart(2, '0')}:${String(now.getUTCMinutes()).padStart(2, '0')}:${String(now.getUTCSeconds()).padStart(2, '0')}`;
    console.log(`WAL check at ${currentTime} (UTC), age threshold: ${new Date(ageThreshold).toISOString()}`);
    
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
        // Mark as processed first, regardless of outcome
        await new Promise((resolve, reject) => {
          db.run(`UPDATE message_wal SET processed = 1 WHERE id = ?`, [entry.id], (err) => {
            if (err) reject(err);
            else resolve();
          });
        });
        
        // Check if the message already exists in the database
        const existsInDb = await new Promise((resolve, reject) => {
          db.get(`SELECT 1 FROM messages WHERE id = ?`, [entry.id], (err, row) => {
            if (err) reject(err);
            else resolve(!!row);
          });
        });
        
        if (existsInDb) {
          console.log(`Message ${entry.id} already exists in database, skipping`);
          // We don't delete from WAL here - will be handled by cleanup logic
          continue;
        }
        
        // Try to verify that the message still exists on Discord
        let messageExists = false;
        try {
          // Get the channel from Discord
          const channel = await client.channels.fetch(entry.channelId);
          if (channel) {
            // Try to fetch the message
            const message = await channel.messages.fetch(entry.id);
            // If we get here, the message exists
            messageExists = !!message;
          }
        } catch (discordErr) {
          console.log(`Message ${entry.id} no longer exists on Discord: ${discordErr.message}`);
          // Message doesn't exist (or we can't fetch it)
          messageExists = false;
        }
        
        // Only insert into the messages table if the message still exists
        if (messageExists) {
          // Insert into messages table
          await new Promise((resolve, reject) => {
            db.run(`
              INSERT INTO messages (
                id, content, authorId, authorUsername, authorBot,
                timestamp, createdAt, channelId,
                attachmentsJson, embedsJson, reactionsJson, sticker_items
              ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            `, [
              entry.id, 
              entry.content, 
              entry.authorId, 
              entry.authorUsername, 
              entry.authorBot,
              entry.timestamp, 
              entry.createdAt, 
              entry.channelId,
              entry.attachmentsJson, 
              entry.embedsJson, 
              entry.reactionsJson,
              entry.sticker_items
            ], function(err) {
              if (err) {
                console.error(`Error inserting message ${entry.id} from WAL to main table:`, err);
                reject(err);
              } else {
                console.log(`Successfully moved message ${entry.id} from WAL to main table`);
                resolve(this.lastID);
              }
            });
          });
        } else {
          console.log(`Message ${entry.id} no longer exists, skipping database insertion`);
        }
        
        // Note: We don't delete the WAL entry here - it will be cleaned up by the retention policy
        
      } catch (err) {
        console.error(`Error processing WAL entry for message ${entry.id}:`, err);
        
        // Reset processed flag so it can be retried
        await new Promise((resolve) => {
          db.run(`UPDATE message_wal SET processed = 0 WHERE id = ?`, [entry.id], () => {
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
 * Clean up old WAL entries based on retention time
 */
async function cleanupOldWalEntries() {
  // Return early if there's no database connection
  if (!db) {
    console.error('Cannot clean up WAL entries: No database connection');
    return false;
  }
  
  try {
    // Get the retention time from config or use default (7 days)
    const walRetentionTime = config.walRetentionTime || 604800000; // 7 days in ms
    
    // Calculate cutoff timestamp - use the message's original timestamp for retention
    const cutoffTime = Date.now() - walRetentionTime;
    
    console.log(`Cleaning up WAL entries older than ${new Date(cutoffTime).toISOString()}`);
    
    // First, count how many entries will be deleted
    const countToDelete = await new Promise((resolve, reject) => {
      db.get(`SELECT COUNT(*) as count FROM message_wal WHERE timestamp < ?`, [cutoffTime], (err, row) => {
        if (err) reject(err);
        else resolve(row?.count || 0);
      });
    });
    
    if (countToDelete === 0) {
      console.log('No old WAL entries to clean up');
      return true;
    }
    
    // Delete entries older than the retention period
    await new Promise((resolve, reject) => {
      db.run(`DELETE FROM message_wal WHERE timestamp < ?`, [cutoffTime], function(err) {
        if (err) {
          console.error('Error deleting old WAL entries:', err);
          reject(err);
        } else {
          console.log(`Deleted ${this.changes} old WAL entries`);
          resolve(this.changes);
        }
      });
    });
    
    return true;
  } catch (error) {
    console.error('Error cleaning up old WAL entries:', error);
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
    
    // Get count of processed entries
    const processedCount = await new Promise((resolve, reject) => {
      db.get(`SELECT COUNT(*) as count FROM message_wal WHERE processed = 1`, (err, row) => {
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
      processedEntries: processedCount,
      oldestTimestamp: oldest ? new Date(oldest).toISOString() : null,
      newestTimestamp: newest ? new Date(newest).toISOString() : null,
      oldestAgeMs: oldestAge,
      newestAgeMs: newestAge,
      oldestAgeFormatted: oldestAge ? formatDuration(oldestAge) : null,
      newestAgeFormatted: newestAge ? formatDuration(newestAge) : null,
      retentionTimeMs: config.walRetentionTime || 604800000,
      retentionTimeFormatted: formatDuration(config.walRetentionTime || 604800000)
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
  cleanupOldWalEntries,
  getWalStats,
  shutdown
};