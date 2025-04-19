// monitor.js - Database operations and message monitoring functionality
const fs = require('fs');
const path = require('path');
const sqlite3 = require('sqlite3').verbose();
const config = require('./config');

// Database connection
let db = null;
let dbInitialized = false;
let fetchingInProgress = new Set(); // Set of channel IDs being fetched
let fetchingComplete = new Set(); // Set of channel IDs that completed fetching

// Message cache with timestamps
const messageCache = new Map(); // Map of messageId -> { message, timestamp }

// Check if database exists on startup
function checkDatabaseExists() {
  const dbPath = path.join(process.cwd(), 'exportguild.db');
  return fs.existsSync(dbPath);
}

// Initialize database
function initializeDatabase() {
  return new Promise((resolve, reject) => {
    const dbPath = path.join(process.cwd(), 'exportguild.db');
    const dbExists = fs.existsSync(dbPath);
    
    // Connect to SQLite database (creates file if it doesn't exist)
    db = new sqlite3.Database(dbPath, (err) => {
      if (err) {
        console.error('Error connecting to database:', err);
        reject(err);
        return;
      }
      
      console.log(`Connected to database at ${dbPath}`);
      
      // If database already existed, we're done
      if (dbExists) {
        console.log('Using existing database');
        dbInitialized = true;
        resolve(true);
        return;
      }
      
      // Create messages table
      db.run(`
        CREATE TABLE IF NOT EXISTS messages (
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
          reactionsJson TEXT
        )
      `, (err) => {
        if (err) {
          console.error('Error creating messages table:', err);
          reject(err);
          return;
        }
        
        // Create channels table to track which channels have been fetched
        db.run(`
          CREATE TABLE IF NOT EXISTS channels (
            id TEXT PRIMARY KEY,
            name TEXT,
            fetchStarted INTEGER DEFAULT 0,
            fetchCompleted INTEGER DEFAULT 0,
            lastMessageId TEXT,
            lastFetchTimestamp INTEGER
          )
        `, (err) => {
          if (err) {
            console.error('Error creating channels table:', err);
            reject(err);
            return;
          }
          
          console.log('Database initialized successfully');
          dbInitialized = true;
          resolve(true);
        });
      });
    });
  });
}

// Add message to cache with current timestamp
function addMessageToCache(message) {
  const messageData = {
    message,
    timestamp: Date.now()
  };
  
  messageCache.set(message.id, messageData);
}

// Process message cache periodically
async function processMessageCache() {
  const now = Date.now();
  const messageDbTimeout = config.getConfig('messageDbTimeout', 'MESSAGE_DB_TIMEOUT') || 3600000; // Default 1 hour
  
  for (const [messageId, data] of messageCache.entries()) {
    // Check if cache timeout has passed
    if (now - data.timestamp >= messageDbTimeout) {
      try {
        // Try to verify the message still exists
        const guild = data.message.guild;
        const channelId = data.message.channelId;
        
        // Check if guild is available
        if (!guild) {
          console.log(`Guild not available for message ${messageId}, removing from cache`);
          messageCache.delete(messageId);
          continue;
        }
        
        // Try to fetch the channel
        try {
          const channel = await guild.channels.fetch(channelId);
          
          // If channel doesn't exist, remove from cache
          if (!channel) {
            console.log(`Channel ${channelId} no longer exists, removing message ${messageId} from cache`);
            messageCache.delete(messageId);
            continue;
          }
          
          // Try to fetch the message
          try {
            await channel.messages.fetch(messageId);
            
            // If the message exists, store it in the database
            await storeMessageInDb(data.message);
            console.log(`Verified and stored message ${messageId} in database`);
            
          } catch (msgError) {
            console.log(`Message ${messageId} no longer exists, removing from cache`);
          }
        } catch (channelError) {
          console.log(`Error fetching channel ${channelId}: ${channelError}`);
        }
        
        // Remove from cache regardless of outcome
        messageCache.delete(messageId);
        
      } catch (error) {
        console.error(`Error processing cached message ${messageId}:`, error);
        // Remove problematic message from cache
        messageCache.delete(messageId);
      }
    }
  }
  
  // Schedule next processing
  setTimeout(processMessageCache, 60000); // Check every minute
}

// Extract message metadata similar to exportguild.js
function extractMessageMetadata(message) {
  return {
    id: message.id,
    content: message.content,
    authorId: message.author.id,
    authorUsername: message.author.username,
    authorBot: message.author.bot,
    timestamp: message.createdTimestamp,
    createdAt: new Date(message.createdTimestamp).toISOString(),
    channelId: message.channelId,
    attachments: Array.from(message.attachments.values()).map(att => ({
      id: att.id,
      url: att.url,
      filename: att.name,
      size: att.size
    })),
    embeds: message.embeds.map(embed => ({
      type: embed.type,
      title: embed.title || null
    })),
    reactions: Array.from(message.reactions.cache.values()).map(reaction => ({
      emoji: reaction.emoji.name,
      count: reaction.count
    }))
  };
}

// Store message in database
async function storeMessageInDb(message) {
  return new Promise((resolve, reject) => {
    // Get message metadata
    const messageData = extractMessageMetadata(message);
    
    // Convert objects to JSON strings
    const attachmentsJson = JSON.stringify(messageData.attachments);
    const embedsJson = JSON.stringify(messageData.embeds);
    const reactionsJson = JSON.stringify(messageData.reactions);
    
    // Insert message into database
    const sql = `
      INSERT OR REPLACE INTO messages 
      (id, content, authorId, authorUsername, authorBot, timestamp, createdAt, channelId, attachmentsJson, embedsJson, reactionsJson) 
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `;
    
    db.run(sql, [
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
      reactionsJson
    ], function(err) {
      if (err) {
        console.error('Error storing message in database:', err);
        reject(err);
        return;
      }
      
      resolve(this.changes);
    });
  });
}

// Mark channel as fetching started
function markChannelFetchingStarted(channelId, channelName) {
  return new Promise((resolve, reject) => {
    fetchingInProgress.add(channelId);
    
    const sql = `
      INSERT OR REPLACE INTO channels 
      (id, name, fetchStarted, fetchCompleted, lastFetchTimestamp) 
      VALUES (?, ?, ?, ?, ?)
    `;
    
    db.run(sql, [
      channelId,
      channelName,
      1, // fetch started
      0, // fetch not completed
      Date.now()
    ], function(err) {
      if (err) {
        console.error(`Error marking channel ${channelId} as fetching started:`, err);
        reject(err);
        return;
      }
      
      resolve(true);
    });
  });
}

// Mark channel as fetching completed
function markChannelFetchingCompleted(channelId, lastMessageId = null) {
  return new Promise((resolve, reject) => {
    fetchingInProgress.delete(channelId);
    fetchingComplete.add(channelId);
    
    const sql = `
      UPDATE channels 
      SET fetchCompleted = 1, lastMessageId = ?, lastFetchTimestamp = ?
      WHERE id = ?
    `;
    
    db.run(sql, [
      lastMessageId,
      Date.now(),
      channelId
    ], function(err) {
      if (err) {
        console.error(`Error marking channel ${channelId} as fetching completed:`, err);
        reject(err);
        return;
      }
      
      resolve(true);
    });
  });
}

// Check for duplicate messages in the database
function checkForDuplicates() {
  return new Promise((resolve, reject) => {
    const sql = `
      SELECT id, COUNT(*) as count
      FROM messages
      GROUP BY id
      HAVING COUNT(*) > 1
    `;
    
    db.all(sql, [], (err, rows) => {
      if (err) {
        console.error('Error checking for duplicate messages:', err);
        reject(err);
        return;
      }
      
      if (rows.length > 0) {
        console.log(`Found ${rows.length} duplicate message IDs in the database`);
        
        // Remove duplicates, keeping only one copy of each message
        const duplicateIds = rows.map(row => row.id);
        
        for (const id of duplicateIds) {
          const deleteSql = `
            DELETE FROM messages 
            WHERE id = ? 
            AND rowid NOT IN (
              SELECT MIN(rowid) 
              FROM messages 
              WHERE id = ?
            )
          `;
          
          db.run(deleteSql, [id, id], function(err) {
            if (err) {
              console.error(`Error removing duplicate message ${id}:`, err);
            } else {
              console.log(`Removed ${this.changes} duplicates for message ID ${id}`);
            }
          });
        }
      } else {
        console.log('No duplicate message IDs found in the database');
      }
      
      resolve(rows.length);
    });
  });
}

// Get channels that have been fetched
async function getFetchedChannels() {
  return new Promise((resolve, reject) => {
    const sql = `
      SELECT id, name, fetchStarted, fetchCompleted, lastMessageId, lastFetchTimestamp
      FROM channels
      WHERE fetchStarted = 1
    `;
    
    db.all(sql, [], (err, rows) => {
      if (err) {
        console.error('Error getting fetched channels:', err);
        reject(err);
        return;
      }
      
      resolve(rows);
    });
  });
}

// Should monitor channel?
function shouldMonitorChannel(channelId) {
  // Don't monitor excluded channels
  if (config.excludedChannels.includes(channelId)) {
    return false;
  }
  
  // If we have a database, we can monitor if we've fetched or are in process of fetching
  if (dbInitialized) {
    return fetchingInProgress.has(channelId) || fetchingComplete.has(channelId);
  }
  
  // If no database, don't monitor (waiting for fetching to begin)
  return false;
}

// Export functions
module.exports = {
  checkDatabaseExists,
  initializeDatabase,
  addMessageToCache,
  processMessageCache,
  storeMessageInDb,
  markChannelFetchingStarted,
  markChannelFetchingCompleted,
  checkForDuplicates,
  getFetchedChannels,
  shouldMonitorChannel,
  extractMessageMetadata
};