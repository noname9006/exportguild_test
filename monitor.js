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
let currentDbPath = null; // Store the current database path

// Message cache with timestamps
const messageCache = new Map(); // Map of messageId -> { message, timestamp }

/**
 * Generates a database filename with guild name, ID and creation date
 * @param {Object} guild - The Discord guild object
 * @returns {String} - The generated database filename
 */
function generateDbFilename(guild) {
  if (!guild) {
    return 'exportguild.db'; // Default filename if no guild provided
  }
  
  // Sanitize guild name for filename usage
  const sanitizedGuildName = guild.name.replace(/[^a-z0-9]/gi, '-').toLowerCase();
  
  // Get current date in YYYY-MM-DD_HH-MM-SS format
  const now = new Date();
  const dateStr = `${now.getUTCFullYear()}-${String(now.getUTCMonth() + 1).padStart(2, '0')}-${String(now.getUTCDate()).padStart(2, '0')}`;
  const timeStr = `${String(now.getUTCHours()).padStart(2, '0')}-${String(now.getUTCMinutes()).padStart(2, '0')}-${String(now.getUTCSeconds()).padStart(2, '0')}`;
  
  // Create filename in format: guildname_guild_id_date_time_created.db
  return `${sanitizedGuildName}_${guild.id}_${dateStr}_${timeStr}.db`;
}

// Check if database exists on startup
function checkDatabaseExists(guild = null) {
  if (!guild) {
    // Check for default database for backward compatibility
    const defaultPath = path.join(process.cwd(), 'exportguild.db');
    currentDbPath = defaultPath; // Set the default path
    return fs.existsSync(defaultPath);
  } else {
    // Look for guild-specific database files
    const files = fs.readdirSync(process.cwd())
      .filter(file => file.endsWith('.db') && file.includes(`_${guild.id}_`))
      .map(file => ({
        name: file,
        path: path.join(process.cwd(), file),
        mtime: fs.statSync(path.join(process.cwd(), file)).mtime
      }))
      .sort((a, b) => b.mtime - a.mtime); // Sort by modification time, newest first
      
    if (files.length === 0) {
      // No database found for this guild
      // Don't set currentDbPath yet since we don't have a file
      return false;
    }
    
    // Use the most recent database file
    console.log(`Found ${files.length} database files for guild ${guild.name}, using most recent: ${files[0].name}`);
    currentDbPath = files[0].path;
    return true;
  }
}

// Initialize database
// Initialize database
function initializeDatabase(guild = null) {
  return new Promise((resolve, reject) => {
    // We should always require a guild parameter now
    if (!guild) {
      reject(new Error('Guild parameter is required for database initialization'));
      return;
    }
    
    // Determine database path based on guild info
    const dbExists = checkDatabaseExists(guild);
    
    // If no database exists, create a new one with the proper naming format
    if (!dbExists) {
      currentDbPath = path.join(process.cwd(), generateDbFilename(guild));
      console.log(`Creating new database at: ${currentDbPath}`);
    }
    
    // At this point currentDbPath should be set
    if (!currentDbPath) {
      reject(new Error('Database path not set'));
      return;
    }
    
    console.log(`Initializing database at: ${currentDbPath}`);
    
    // Connect to SQLite database (creates file if it doesn't exist)
    db = new sqlite3.Database(currentDbPath, (err) => {
      if (err) {
        console.error('Error connecting to database:', err);
        reject(err);
        return;
      }
      
      console.log(`Connected to database at ${currentDbPath}`);
      
      // If database already existed, we're done
      if (dbExists) {
        console.log('Using existing database');
        dbInitialized = true;
        
        // Load previously fetched channels after database initialization
        loadFetchedChannelsState().then(() => {
          // Start message monitoring if not already running
          if (!global.monitoringActive) {
            global.monitoringActive = true;
            console.log(`Monitoring activated for existing database: ${currentDbPath}`);
            processMessageCache();
          } else {
            console.log(`Monitoring already active, continuing with existing process`);
          }
          resolve(true);
        }).catch(err => {
          console.error('Error loading channel states:', err);
          // Still try to activate monitoring even if loading states fails
          if (!global.monitoringActive) {
            global.monitoringActive = true;
            console.log(`Monitoring activated despite channel state loading error`);
            processMessageCache();
          }
          resolve(true); // Still resolve even if loading channel states fails
        });
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
          reactionsJson TEXT,
          sticker_items TEXT
        )
      `, (err) => {
        if (err) {
          console.error('Error creating messages table:', err);
          reject(err);
          return;
        }
        
        // Create channels table to track which channels have been fetched
        // Removed fetchCompleted and lastFetchTimestamp fields
        db.run(`
  CREATE TABLE IF NOT EXISTS channels (
    id TEXT PRIMARY KEY,
    name TEXT,
    fetchStarted INTEGER DEFAULT 0,
    lastMessageId TEXT,
    deleted INTEGER DEFAULT 0,
    deletedAt INTEGER
  )
`, (err) => {
  if (err) {
    console.error('Error creating channels table:', err);
    reject(err);
    return;
  }
          
          // Add metadata table for guild information
          db.run(`
            CREATE TABLE IF NOT EXISTS guild_metadata (
              key TEXT PRIMARY KEY,
              value TEXT
            )
          `, (err) => {
            if (err) {
              console.error('Error creating guild_metadata table:', err);
              reject(err);
              return;
            }
            
            // Store guild info in metadata
            const timestamp = new Date().toISOString();
            const metadataEntries = [
              { key: 'guild_id', value: guild.id },
              { key: 'guild_name', value: guild.name },
              { key: 'creation_date', value: timestamp }
            ];
            
            // Insert all metadata entries
            const insertPromises = metadataEntries.map(entry => {
              return new Promise((resolve, reject) => {
                db.run(
                  'INSERT INTO guild_metadata (key, value) VALUES (?, ?)',
                  [entry.key, entry.value],
                  function(err) {
                    if (err) {
                      console.error(`Error storing metadata ${entry.key}:`, err);
                      reject(err);
                    } else {
                      resolve();
                    }
                  }
                );
              });
            });
            
            // Wait for all inserts to complete
            Promise.all(insertPromises)
              .then(() => {
                console.log('Guild metadata stored in database');
                dbInitialized = true;
                
                // Load previously fetched channels after database initialization
                loadFetchedChannelsState().then(() => {
                  // Start message monitoring for new database
                  if (!global.monitoringActive) {
                    global.monitoringActive = true;
                    console.log(`Monitoring activated for new database: ${currentDbPath}`);
                    processMessageCache();
                  }
                  resolve(true);
                }).catch(err => {
                  console.error('Error loading channel states:', err);
                  // Still try to activate monitoring even if loading states fails
                  if (!global.monitoringActive) {
                    global.monitoringActive = true;
                    console.log(`Monitoring activated for new database despite channel state error`);
                    processMessageCache();
                  }
                  resolve(true); // Still resolve even if loading channel states fails
                });
              })
              .catch(err => {
                console.error('Error storing guild metadata:', err);
                // Still mark as initialized even if metadata failed
                dbInitialized = true;
                
                // Load previously fetched channels even if metadata failed
                loadFetchedChannelsState().then(() => {
                  // Start message monitoring even if metadata failed
                  if (!global.monitoringActive) {
                    global.monitoringActive = true;
                    console.log(`Monitoring activated despite metadata error`);
                    processMessageCache();
                  }
                  resolve(true);
                }).catch(err => {
                  console.error('Error loading channel states:', err);
                  // Still try to activate monitoring as last resort
                  if (!global.monitoringActive) {
                    global.monitoringActive = true;
                    console.log(`Monitoring activated as last resort despite errors`);
                    processMessageCache();
                  }
                  resolve(true); // Still resolve even if loading channel states fails
                });
              });
          });
        });
      });
    });
  });
}

// Get current database path
function getCurrentDatabasePath() {
  return currentDbPath || path.join(process.cwd(), 'exportguild.db');
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
  // If database isn't initialized, don't process messages
  if (!dbInitialized || !db) {
    console.log('Database not initialized. Will retry message cache processing in 60 seconds.');
    setTimeout(processMessageCache, 60000); // Check again in a minute
    return;
  }

  const now = Date.now();
  const messageDbTimeout = config.getConfig('messageDbTimeout', 'MESSAGE_DB_TIMEOUT') || 3600000; // Default 1 hour
  const monitorBatchSize = config.getConfig('monitorBatchSize', 'MONITOR_BATCH_SIZE') || 10; // Use dedicated monitor batch size
  
  // For batch database operations
  let messageBatch = [];
  let processedCount = 0;
  
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
            const fetchedMessage = await channel.messages.fetch(messageId);
            
            // If the message exists, add it to the batch
            messageBatch.push(fetchedMessage);
            processedCount++;
            
            // If we've reached the batch size, process the batch
            if (messageBatch.length >= monitorBatchSize) {
              try {
                await storeMessagesInDbBatch(messageBatch);
                console.log(`Stored batch of ${messageBatch.length} messages in database from cache processing`);
                messageBatch = []; // Clear batch after successful insert
              } catch (batchError) {
                console.error('Error inserting message batch into database:', batchError);
                
                // If batch fails, try individual inserts
                console.log('Falling back to individual message processing...');
                for (const msg of messageBatch) {
                  try {
                    await storeMessageInDb(msg);
                    console.log(`Stored individual message ${msg.id} in database`);
                  } catch (singleError) {
                    console.error(`Error storing individual message ${msg.id}:`, singleError);
                  }
                }
                messageBatch = []; // Clear batch after individual processing
              }
            }
            
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
  
  // Process any remaining messages in the batch
  if (messageBatch.length > 0) {
    try {
      await storeMessagesInDbBatch(messageBatch);
      console.log(`Stored final batch of ${messageBatch.length} messages in database from cache processing`);
    } catch (batchError) {
      console.error('Error inserting final message batch into database:', batchError);
      
      // If batch fails, try individual inserts
      console.log('Falling back to individual message processing for final batch...');
      for (const msg of messageBatch) {
        try {
          await storeMessageInDb(msg);
          console.log(`Stored individual message ${msg.id} in database`);
        } catch (singleError) {
          console.error(`Error storing individual message ${msg.id}:`, singleError);
        }
      }
    }
  }
  
  if (processedCount > 0) {
    console.log(`Cache processing completed: ${processedCount} messages processed and stored in database`);
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
    if (!db) {
      reject(new Error("Database not initialized"));
      return;
    }
    
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

// Store multiple messages in database as a batch
async function storeMessagesInDbBatch(messages) {
  return new Promise((resolve, reject) => {
    if (!db) {
      reject(new Error("Database not initialized"));
      return;
    }
    
    if (!messages || messages.length === 0) {
      resolve(0);
      return;
    }
    
    // Prepare batch data
    const messageValues = [];
    const placeholders = [];
    
    // For each message, add values and build placeholders
    for (const message of messages) {
      // Get message metadata
      const messageData = extractMessageMetadata(message);
      
      // Convert objects to JSON strings
      const attachmentsJson = JSON.stringify(messageData.attachments);
      const embedsJson = JSON.stringify(messageData.embeds);
      const reactionsJson = JSON.stringify(messageData.reactions);
      
      // Add values
      messageValues.push(
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
      );
      
      // Add placeholder for this message
      placeholders.push('(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)');
    }
    
    // Build the SQL statement for batch insert
    const sql = `
      INSERT OR REPLACE INTO messages 
      (id, content, authorId, authorUsername, authorBot, timestamp, createdAt, channelId, attachmentsJson, embedsJson, reactionsJson) 
      VALUES ${placeholders.join(', ')}
    `;
    
    // Execute batch insert
    db.run(sql, messageValues, function(err) {
      if (err) {
        console.error('Error batch storing messages in database:', err);
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
    if (!db) {
      reject(new Error("Database not initialized"));
      return;
    }
    
    fetchingInProgress.add(channelId);
    
    // First, check if we already have a lastMessageId for this channel
    const checkSql = `
      SELECT lastMessageId
      FROM channels
      WHERE id = ?
    `;
    
    db.get(checkSql, [channelId], (err, row) => {
      if (err) {
        console.error(`Error checking existing channel data for ${channelId}:`, err);
        reject(err);
        return;
      }
      
      // Get any existing lastMessageId
      const existingLastMessageId = row ? row.lastMessageId : null;
      
      // Now update the channel but preserve lastMessageId if it exists
      const updateSql = existingLastMessageId
        ? `
          INSERT OR REPLACE INTO channels 
          (id, name, fetchStarted, lastMessageId) 
          VALUES (?, ?, ?, ?)
        `
        : `
          INSERT OR REPLACE INTO channels 
          (id, name, fetchStarted) 
          VALUES (?, ?, ?)
        `;
      
      const params = existingLastMessageId
        ? [channelId, channelName, 1, existingLastMessageId]
        : [channelId, channelName, 1];
      
      db.run(updateSql, params, function(err) {
        if (err) {
          console.error(`Error marking channel ${channelId} as fetching started:`, err);
          reject(err);
          return;
        }
        
        console.log(`Marked channel ${channelName} (${channelId}) as fetching started${existingLastMessageId ? ` with existing lastMessageId: ${existingLastMessageId}` : ''}`);
        resolve(true);
      });
    });
  });
}

// Mark channel as fetching completed
function markChannelFetchingCompleted(channelId, lastMessageId = null) {
  return new Promise((resolve, reject) => {
    if (!db) {
      reject(new Error("Database not initialized"));
      return;
    }
    
    fetchingInProgress.delete(channelId);
    fetchingComplete.add(channelId);
    
    // Update SQL to set fetchStarted to 1 and lastMessageId
    const sql = `
      UPDATE channels 
      SET fetchStarted = 1, lastMessageId = ?
      WHERE id = ?
    `;
    
    db.run(sql, [
      lastMessageId,
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
    if (!db) {
      reject(new Error("Database not initialized"));
      return;
    }
    
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
    if (!db) {
      console.log("Database not initialized, returning empty array");
      resolve([]);
      return;
    }
    
    // Update SQL removing fetchCompleted and lastFetchTimestamp fields
    const sql = `
      SELECT id, name, fetchStarted, lastMessageId
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

// Add this function to monitor.js
async function loadFetchedChannelsState() {
  try {
    if (!db || !dbInitialized) {
      console.log('Cannot load fetched channels: database not initialized');
      return;
    }

    console.log('Loading previously fetched channels from database...');
    
    // Query the database for channels that have been fetched
    const channels = await getFetchedChannels();
    
    // Populate the fetchingComplete set from the database
    for (const channel of channels) {
      console.log(`Adding channel to monitoring: ${channel.name} (${channel.id})`);
      // Since fetchCompleted is removed, all channels with fetchStarted=1 
      // are considered complete
      fetchingComplete.add(channel.id);
    }
    
    console.log(`Loaded ${fetchingComplete.size} channels for monitoring`);
  } catch (error) {
    console.error('Error loading fetched channels state:', error);
  }
}

function shouldMonitorChannel(channelId) {
  // Don't monitor excluded channels
  if (config.excludedChannels.includes(channelId)) {
    return false;
  }
  
  // If we have a database, we can monitor if we've fetched or are in process of fetching
  if (dbInitialized) {
    const shouldMonitor = fetchingInProgress.has(channelId) || fetchingComplete.has(channelId);
    if (shouldMonitor) {
      console.log(`Monitoring active for channel ${channelId}`);
    }
    return shouldMonitor;
  }
  
  // If no database, don't monitor (waiting for fetching to begin)
  console.log(`Not monitoring channel ${channelId} - database not initialized`);
  return false;
}

async function storeGuildMetadata(key, value) {
  return new Promise((resolve, reject) => {
    if (!db) {
      reject(new Error("Database not initialized"));
      return;
    }
    
    try {
      // Use INSERT OR REPLACE instead of just INSERT to handle duplicate keys
      const stmt = db.prepare('INSERT OR REPLACE INTO guild_metadata (key, value) VALUES (?, ?)');
      
      stmt.run(key, value, function(err) {
        if (err) {
          console.error(`Error storing metadata ${key}:`, err);
          reject(err);
          return;
        }
        
        console.log(`Stored guild metadata: ${key} = ${value}`);
        stmt.finalize();
        resolve();
      });
    } catch (error) {
      console.error(`Error in storeGuildMetadata for ${key}:`, error);
      reject(error);
    }
  });
}

function getDatabase() {
  return db; // Return the database connection object
}

// Export functions
module.exports = {
  checkDatabaseExists,
  initializeDatabase,
  addMessageToCache,
  processMessageCache,
  storeMessageInDb,
  storeMessagesInDbBatch,
  storeGuildMetadata,
  markChannelFetchingStarted,
  markChannelFetchingCompleted,
  checkForDuplicates,
  getFetchedChannels,
  loadFetchedChannelsState,
  shouldMonitorChannel,
  extractMessageMetadata,
  generateDbFilename,
  getCurrentDatabasePath,
  getDatabase
};