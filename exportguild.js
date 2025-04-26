const path = require('path');
const { 
  PermissionFlagsBits,
  ChannelType
} = require('discord.js');
const config = require('./config');
const monitor = require('./monitor');
const memberTracker = require('./member-tracker');

// Parse excluded channels from environment variable or config
const excludedChannelsArray = config.getConfig('excludedChannels', 'EX_CHANNELS');
const excludedChannels = new Set(excludedChannelsArray);

// Database batch size - how many messages to insert at once
const DB_BATCH_SIZE = config.getConfig('dbBatchSize', 'DB_BATCH_SIZE') || 100;

// Memory limit in MB
const MEMORY_LIMIT_MB = config.getConfig('memoryLimitMB', 'MEMORY_LIMIT_MB');
// Convert to bytes for easier comparison with process.memoryUsage()
const MEMORY_LIMIT_BYTES = MEMORY_LIMIT_MB * 1024 * 1024;
// Memory scale factor - use only this percentage of the configured limit as effective limit
const MEMORY_SCALE_FACTOR = 0.85;

// Memory check frequency in milliseconds
const MEMORY_CHECK_INTERVAL = config.getConfig('memoryCheckInterval', 'MEMORY_CHECK_INTERVAL');

// Status update interval in milliseconds
const STATUS_UPDATE_INTERVAL = config.getConfig('statusUpdateInterval', 'STATUS_UPDATE_INTERVAL') || 5000;

// Maximum concurrent API requests
const MAX_CONCURRENT_REQUESTS = 15;

// Function to check current memory usage and return details
function checkMemoryUsage() {
  const memoryUsage = process.memoryUsage();
  const heapUsed = memoryUsage.heapUsed;
  const rss = memoryUsage.rss; // Resident Set Size - total memory allocated
  
  const heapUsedMB = Math.round(heapUsed / 1024 / 1024 * 100) / 100;
  const rssMB = Math.round(rss / 1024 / 1024 * 100) / 100;
  
  // Calculate effective limit
  const effectiveLimit = MEMORY_LIMIT_BYTES * MEMORY_SCALE_FACTOR;
  const effectiveLimitMB = Math.round(effectiveLimit / 1024 / 1024 * 100) / 100;
  
  return {
    heapUsed,
    rss,
    heapUsedMB,
    rssMB,
    isAboveLimit: rss > effectiveLimit,
    percentOfLimit: Math.round((rss / MEMORY_LIMIT_BYTES) * 100),
    effectiveLimitMB
  };
}

// Log memory usage
function logMemoryUsage(prefix = '') {
  const memory = checkMemoryUsage();
  console.log(`${prefix} Memory usage: ${memory.rssMB} MB / ${MEMORY_LIMIT_MB} MB (${memory.percentOfLimit}% of limit), Heap: ${memory.heapUsedMB} MB`);
  return memory;
}

// Function for aggressive memory cleanup
async function forceMemoryRelease() {
  console.log('Forcing aggressive memory cleanup...');
  
  // Run garbage collection multiple times if available
  if (global.gc) {
    for (let i = 0; i < 3; i++) {
      console.log(`Forcing garbage collection pass ${i+1}...`);
      global.gc();
      // Small delay between GC calls
      await new Promise(resolve => setTimeout(resolve, 100));
    }
  }
  
  // Attempt to force memory compaction in newer Node versions
  if (process.versions.node.split('.')[0] >= 12) {
    try {
      console.log('Attempting to compact heap memory...');
      if (typeof v8 !== 'undefined' && v8.getHeapStatistics && v8.writeHeapSnapshot) {
        const v8 = require('v8');
        const heapBefore = v8.getHeapStatistics().total_heap_size;
        v8.writeHeapSnapshot(); // This can help compact memory in some cases
        const heapAfter = v8.getHeapStatistics().total_heap_size;
        console.log(`Heap size change: ${(heapBefore - heapAfter) / 1024 / 1024} MB`);
      }
    } catch (e) {
      console.error('Error during heap compaction:', e);
    }
  }
  
  // Run another GC pass after compaction
  if (global.gc) {
    global.gc();
  }
}

// Function for performing memory cleanup
async function performMemoryCleanup(exportState) {
  if (exportState.saveInProgress) return;
  
  exportState.saveInProgress = true;
  
  try {
    console.log('Performing memory cleanup...');
    
    // Clear any references to large objects
    global._lastMemoryReport = null; // Clear any references we might have created
    
    // Force garbage collection with enhanced approach
    await forceMemoryRelease();
    
    // Log memory after cleanup
    logMemoryUsage('After cleanup');
    
    // If memory is still too high after cleanup, pause operations briefly
    const memoryAfter = checkMemoryUsage();
    if (memoryAfter.isAboveLimit) {
      console.log('Memory still above limit after cleanup. Pausing operations for 2 seconds...');
      // This pause can help the system actually release memory
      await new Promise(resolve => setTimeout(resolve, 2000));
      
      // One more GC attempt after the pause
      if (global.gc) global.gc();
      
      logMemoryUsage('After pause');
    }
  } catch (error) {
    console.error('Error during memory cleanup:', error);
  } finally {
    exportState.saveInProgress = false;
  }
}

// Function to check memory and handle if above limit
async function checkAndHandleMemoryUsage(exportState, trigger = 'MANUAL') {
  exportState.memoryCheckCount++;
  
  // Check memory usage
  const memory = logMemoryUsage(`Memory check #${exportState.memoryCheckCount} (${trigger})`);
  
  // If above limit and not currently saving, trigger memory cleanup
  if (memory.isAboveLimit && !exportState.saveInProgress) {
    console.log(`🚨 Memory usage above limit (${memory.rssMB}MB / ${memory.effectiveLimitMB}MB). Triggering cleanup...`);
    exportState.memoryTriggeredSaves++;
    await performMemoryCleanup(exportState);
    return true;
  }
  return false;
}

async function fetchVisibleChannels(guild) {
  // Get only visible text-based channels
  const visibleChannels = [];
  
  // Log guild info
  console.log(`Guild: ${guild.name} (${guild.id})`);
  console.log(`Total channels in guild: ${guild.channels.cache.size}`);
  
  // Get text channels that the bot can actually see and read messages in
  const textChannels = guild.channels.cache
    .filter(channel => {
      const isTextChannel = channel.type === ChannelType.GuildText || channel.type === ChannelType.GuildForum;
      const notExcluded = !excludedChannels.has(channel.id);
      const isViewable = channel.viewable;
      const canReadHistory = channel.permissionsFor(guild.members.me)?.has(PermissionFlagsBits.ReadMessageHistory) ?? false;
      
      return isTextChannel && notExcluded && isViewable && canReadHistory;
    })
    .map(channel => ({
      channel,
      isThread: false,
      parentId: null
    }));
  
  // Sort channels by ID (numerically smaller first)
  textChannels.sort((a, b) => {
    // Convert IDs to BigInt for proper numeric comparison
    const idA = BigInt(a.channel.id);
    const idB = BigInt(b.channel.id);
    return idA < idB ? -1 : idA > idB ? 1 : 0;
  });
  
  visibleChannels.push(...textChannels);
  console.log(`Found ${textChannels.length} text channels to process (sorted by channel ID)`);
  
  // Get threads in batches to avoid rate limiting
  const threadChannels = [];
  for (const channelObj of textChannels) {
    const channel = channelObj.channel;
    if (!channel.threads) {
      console.log(`Channel ${channel.name} (${channel.id}) doesn't have threads property, skipping thread processing`);
      continue;
    }
    
    console.log(`Fetching threads for channel: ${channel.name} (${channel.id})`);
    
    try {
      // Get active threads
      let activeThreads;
      try {
        activeThreads = await channel.threads.fetchActive();
        console.log(`Found ${activeThreads.threads.size} active threads in ${channel.name}`);
      } catch (e) {
        console.error(`Error fetching active threads for ${channel.name}:`, e);
        activeThreads = { threads: new Map() };
      }
      
      // Get archived threads
      let archivedThreads;
      try {
        archivedThreads = await channel.threads.fetchArchived();
        console.log(`Found ${archivedThreads.threads.size} archived threads in ${channel.name}`);
      } catch (e) {
        console.error(`Error fetching archived threads for ${channel.name}:`, e);
        archivedThreads = { threads: new Map() };
      }
      
      // Add visible threads
      for (const thread of [...activeThreads.threads.values(), ...archivedThreads.threads.values()]) {
        try {
          if (!excludedChannels.has(thread.id) && 
              thread.viewable && 
              thread.permissionsFor(guild.members.me).has(PermissionFlagsBits.ReadMessageHistory)) {
            threadChannels.push({
              channel: thread,
              isThread: true,
              parentId: channel.id,
              parentName: channel.name
            });
            console.log(`Added thread: ${thread.name} (${thread.id}) from parent ${channel.name}`);
          } else {
            console.log(`Skipping thread ${thread.name} (${thread.id}) due to permissions or exclusion`);
          }
        } catch (threadError) {
          console.error(`Error processing individual thread ${thread.id}:`, threadError);
        }
      }
    } catch (error) {
      console.error(`Error processing threads for channel ${channel.name}:`, error);
    }
  }
  
  console.log(`Found ${threadChannels.length} thread channels before sorting`);
  
  // Sort thread channels by ID as well if there are any
  if (threadChannels.length > 0) {
    threadChannels.sort((a, b) => {
      // Use try-catch to handle any potential errors with ID parsing
      try {
        const idA = BigInt(a.channel.id);
        const idB = BigInt(b.channel.id);
        return idA < idB ? -1 : idA > idB ? 1 : 0;
      } catch (err) {
        console.error('Error sorting thread channels:', err);
        return 0; // Keep original order if error
      }
    });
    visibleChannels.push(...threadChannels);
  }
  
  console.log(`Total of ${threadChannels.length} thread channels added to processing queue`);
  
  // Final summary
  const regularCount = visibleChannels.filter(ch => !ch.isThread).length;
  const threadCount = visibleChannels.filter(ch => ch.isThread).length;
  console.log(`Processing queue contains ${regularCount} regular channels and ${threadCount} thread channels`);
  
  return visibleChannels;
}

async function processChannelsInParallel(channels, exportState, statusMessage, guild) {
  // Create a queue for processing channels with controlled concurrency
  let currentIndex = 0;
  
  // Process function that takes from the queue
  const processNext = async () => {
    if (currentIndex >= channels.length) return;
    
    const channelIndex = currentIndex++;
    const channelObj = channels[channelIndex];
    const channel = channelObj.channel;
    
    exportState.runningTasksCount++;
    exportState.currentChannel = channel;
    exportState.currentChannelIndex = channelIndex + 1;
    exportState.messagesInCurrentChannel = 0;
    
    // Store the channel in the active channels list
    exportState.activeChannels.set(channel.id, channel.name);
    
    console.log(`Processing channel ${channelIndex + 1}/${channels.length}: ${channel.name} (${channel.id})`);
    
    try {
      await fetchMessagesFromChannel(channel, exportState, statusMessage, guild);
    } catch (error) {
      console.error(`Error processing channel ${channel.name}:`, error);
    } finally {
      exportState.runningTasksCount--;
      exportState.processedChannels++;
      // Remove channel from active channels list
      exportState.activeChannels.delete(channel.id);
      // Always process next to ensure we continue even after errors
      processNext();
    }
  };
  
  // Start initial batch of tasks
  const initialBatch = Math.min(MAX_CONCURRENT_REQUESTS, channels.length);
  const initialPromises = [];
  
  for (let i = 0; i < initialBatch; i++) {
    initialPromises.push(processNext());
  }
  
  // Wait for all channels to complete processing
  await Promise.all(initialPromises);
  
  // Wait until all concurrent tasks are done
  while (exportState.runningTasksCount > 0) {
    await new Promise(resolve => setTimeout(resolve, 100));
  }
}

// Helper function to get channel info from the database with proper sanitization
async function getChannelInfo(channelId) {
  return new Promise((resolve, reject) => {
    const db = monitor.getDatabase();
    if (!db) {
      console.log("Database not initialized, returning null");
      resolve(null);
      return;
    }
    
    const sql = `
      SELECT id, name, fetchStarted, lastMessageId
      FROM channels
      WHERE id = ?
    `;
    
    db.get(sql, [channelId], (err, row) => {
      if (err) {
        console.error('Error getting channel info:', err);
        reject(err);
        return;
      }
      
      resolve(row || null);
    });
  });
}

// fetchMessagesFromChannel function in exportguild.js - CORRECTED VERSION
async function fetchMessagesFromChannel(channel, exportState, statusMessage, guild) {
  if (!channel.isTextBased()) {
    console.log(`Skipping non-text channel: ${channel.name}`);
    return;
  }
  
  // IMPORTANT: Initialize lastStoredMessageId here first before using it
  let lastStoredMessageId = null;
  
  try {
    // Get channel info from database before marking as fetching started
    const channelInfo = await getChannelInfo(channel.id);
    if (channelInfo && channelInfo.lastMessageId) {
      lastStoredMessageId = channelInfo.lastMessageId;
      console.log(`Found existing messages for channel ${channel.name} (${channel.id}), last message ID: ${lastStoredMessageId}`);
    }
  } catch (error) {
    console.error(`Error checking for existing messages in channel ${channel.name}:`, error);
  }
  
  // Now mark the channel as fetching started AFTER we've retrieved the lastStoredMessageId
  await monitor.markChannelFetchingStarted(channel.id, channel.name);
  console.log(`Marked channel ${channel.name} (${channel.id}) for monitoring`);
  
  let lastMessageId = null;
  let keepFetching = true;
  let fetchCount = 0;
  
  // For batch database operations
  let messageBatch = [];
  
  // For measuring batch fetch speed
  let lastBatchStartTime = Date.now();
  let lastBatchSize = 0;
  let currentBatchSpeed = 0;
  
  console.log(`Starting to fetch messages from channel: ${channel.name} (${channel.id})`);
  console.log(`Last stored message ID: ${lastStoredMessageId || 'None'}`);

  // Debug information to help diagnose issues
  console.log(`DEBUG: Starting message fetch for ${channel.name}`);
  console.log(`DEBUG: lastStoredMessageId = ${lastStoredMessageId}`);
  console.log(`DEBUG: lastStoredMessageId type = ${typeof lastStoredMessageId}`);
  console.log(`DEBUG: lastStoredMessageId length = ${lastStoredMessageId ? lastStoredMessageId.length : 'N/A'}`);

  // Test conversion to BigInt to check if it's valid
  try {
    if (lastStoredMessageId) {
      const testBigInt = BigInt(lastStoredMessageId);
      console.log(`DEBUG: lastStoredMessageId converts to BigInt successfully: ${testBigInt}`);
    } else {
      console.log(`DEBUG: Cannot convert lastStoredMessageId to BigInt - value is null or undefined`);
    }
  } catch (err) {
    console.error(`DEBUG: Error converting lastStoredMessageId to BigInt: ${err.message}`);
    // If we can't convert to BigInt, clear it to use standard fetch method
    lastStoredMessageId = null;
  }

  // If we have a lastStoredMessageId, fetch only messages AFTER that ID using pagination
  if (lastStoredMessageId) {
    try {
      console.log(`Using pagination to fetch messages after ID: ${lastStoredMessageId} in ${channel.name}`);
      
      // Start with an empty array to collect all new messages
      let newMessages = [];
      
      // We'll fetch in reverse chronological order (newest first)
      let currentAfter = lastStoredMessageId;
      let hasMoreMessages = true;
      let batchCount = 0;
      
      // Track the highest message ID (newest message)
      let highestMessageId = null;
      
      // Keep fetching batches of messages until we've got them all
      while (hasMoreMessages) {
        batchCount++;
        console.log(`Fetching batch ${batchCount} of newer messages with after=${currentAfter} in ${channel.name}`);
        
        // Use the after parameter to get messages newer than our reference point
        const messages = await channel.messages.fetch({ 
          limit: 100,
          after: currentAfter
        });
        
        console.log(`Fetched ${messages.size} messages after ${currentAfter} in ${channel.name}`);
        
        // If no messages, we're done
        if (messages.size === 0) {
          hasMoreMessages = false;
          console.log(`No more new messages in ${channel.name} after batch ${batchCount}`);
          continue;
        }
        
        // For the first batch, the newest message will be the first one in the collection
        // (Discord returns newest-first when using "after")
        if (batchCount === 1 && messages.size > 0) {
          // Get the first message from the collection (newest one)
          const newestMessage = messages.first();
          highestMessageId = newestMessage.id;
          console.log(`Identified newest message ID: ${highestMessageId} from first batch`);
        } else if (messages.size > 0) {
          // Check if any message in this batch is newer than our current highest
          const currentBatchNewest = messages.first().id;
          
          // Compare as BigInt to ensure proper numerical comparison
          if (!highestMessageId || BigInt(currentBatchNewest) > BigInt(highestMessageId)) {
            highestMessageId = currentBatchNewest;
            console.log(`Updated highest message ID to: ${highestMessageId} from batch ${batchCount}`);
          }
        }
        
        // Discord returns newest-first when using "after", but we want oldest-first for processing
        // So add them in the right order
        const messagesArray = Array.from(messages.values());
        
        // Sort by ID ascending (oldest first)
        messagesArray.sort((a, b) => {
          const aId = BigInt(a.id);
          const bId = BigInt(b.id);
          return aId < bId ? -1 : aId > bId ? 1 : 0;
        });
        
        // Add these messages to our collection
        newMessages = [...newMessages, ...messagesArray];
        console.log(`Added ${messagesArray.length} messages, total now: ${newMessages.length}`);
        
        // Update the reference point to get the next batch
        // We need the highest ID (newest message) from this batch for the next "after" query
        const batchHighestId = messagesArray.reduce((max, msg) => {
          return BigInt(msg.id) > BigInt(max) ? msg.id : max;
        }, messagesArray[0].id);
        
        currentAfter = batchHighestId;
        console.log(`Updated currentAfter to ${currentAfter} for next batch`);
        
        // Discord pagination with "after" gives us newest messages first in each batch
        // If we got less than 100, we've reached the end
        if (messages.size < 100) {
          hasMoreMessages = false;
          console.log(`Reached newest messages for ${channel.name} after ${batchCount} batches`);
        }

        // Check memory usage occasionally
        if (batchCount % 5 === 0) {
          const memoryExceeded = await checkAndHandleMemoryUsage(exportState, 'PAGINATION_FETCH');
          if (memoryExceeded) {
            console.log(`Memory limit reached during pagination. Will continue but may need cleanup.`);
          }
        }
      }
      
      // Final count of all new messages
      console.log(`Total new messages found in ${channel.name}: ${newMessages.length}`);
      
      // If no new messages, we're done with this channel
      if (newMessages.length === 0) {
        console.log(`No new messages in ${channel.name} since last fetch`);
        // Preserve the existing lastMessageId since nothing has changed
        await monitor.markChannelFetchingCompleted(channel.id, lastStoredMessageId);
        console.log(`Completed monitoring setup for channel: ${channel.name} (${channel.id}) - no changes`);
        return;
      }
      
      // Use the highest message ID we tracked during fetching
      // Make sure it's clean
      if (highestMessageId) {
        // Make sure it's a clean string with no spaces
        highestMessageId = String(highestMessageId).replace(/\s+/g, '');
      }
      
      // Filter out bot messages
      const nonBotMessages = newMessages.filter(message => !message.author.bot);
      exportState.messageDroppedCount += (newMessages.length - nonBotMessages.length);
      exportState.messagesTotalProcessed += newMessages.length;
      
      console.log(`Found ${nonBotMessages.length} new non-bot messages in ${channel.name}`);
      
      // Process and store these messages in batches
      const DB_BATCH_SIZE = exportState.dbBatchSize;
      for (let i = 0; i < nonBotMessages.length; i += DB_BATCH_SIZE) {
        const batch = nonBotMessages.slice(i, i + DB_BATCH_SIZE);
        try {
          console.log(`Storing batch of ${batch.length} messages from ${channel.name}`);
          await monitor.storeMessagesInDbBatch(batch);
          exportState.messagesStoredInDb += batch.length;
          exportState.messagesInCurrentChannel += batch.length;
          exportState.processedMessages += batch.length;
        } catch (dbError) {
          console.error(`Error storing messages batch from ${channel.name}:`, dbError);
          exportState.dbErrors++;
          
          // Try individual storage if batch fails
          for (const msg of batch) {
            try {
              await monitor.storeMessageInDb(msg);
              exportState.messagesStoredInDb++;
              exportState.messagesInCurrentChannel++;
              exportState.processedMessages++;
            } catch (singleError) {
              console.error(`Error storing message ${msg.id}:`, singleError);
              exportState.dbErrors++;
            }
          }
        }
        
        // Update status message
        const currentTime = Date.now();
        if (currentTime - exportState.lastStatusUpdateTime > STATUS_UPDATE_INTERVAL) {
          exportState.lastStatusUpdateTime = currentTime;
          updateStatusMessage(statusMessage, exportState, guild);
        }
      }
      
      // Update the channel with the new lastMessageId
      console.log(`Setting new lastMessageId to ${highestMessageId} for channel ${channel.name} (old ID: ${lastStoredMessageId})`);
      await monitor.markChannelFetchingCompleted(channel.id, highestMessageId);
      console.log(`Completed monitoring setup for channel: ${channel.name} (${channel.id}) with updated lastMessageId`);
      return;
      
    } catch (error) {
      console.error(`Error during "after" pagination in ${channel.name}:`, error);
      console.log(`Falling back to standard fetch method for ${channel.name}`);
      // If the pagination approach fails, fall back to the standard method
    }
  }
  
  // Standard fetching logic (original code) follows for channels without lastStoredMessageId
  // or if pagination for newer messages failed
  console.log(`Using standard fetch method for ${channel.name} - either no lastStoredMessageId or pagination failed`);
  
  // For the standard fetch method, we need to track the newest message ID
  // This will be different from lastMessageId which is used for pagination
  let newestMessageId = null;
  
  while (keepFetching) {
    try {
      // Check memory usage every 5 fetch operations
      if (fetchCount % 5 === 0) {
        const memoryExceeded = await checkAndHandleMemoryUsage(exportState, 'FETCH_CYCLE');
        if (memoryExceeded) {
          console.log(`Memory limit reached during channel processing.`);
        }
      }
      
      // Start timing for this batch
      lastBatchStartTime = Date.now();
      
      // Fetch messages - use optimal batch size
      const options = { limit: 100 }; // Max allowed by Discord API
      if (lastMessageId) {
        options.before = lastMessageId;
      }
      
      fetchCount++;
      console.log(`Fetching batch ${fetchCount} from ${channel.name}, options:`, options);
      
      const messages = await channel.messages.fetch(options);
      console.log(`Fetched ${messages.size} messages from ${channel.name}`);
      
      // Calculate the speed for this batch
      const batchEndTime = Date.now();
      const batchDuration = (batchEndTime - lastBatchStartTime) / 1000;
      if (batchDuration > 0 && messages.size > 0) {
        currentBatchSpeed = (messages.size / batchDuration).toFixed(2);
        lastBatchSize = messages.size;
        // Store current batch speed for this channel
        exportState.channelBatchSpeed.set(channel.id, currentBatchSpeed);
      }
      
      if (messages.size === 0) {
        console.log(`No more messages in ${channel.name}`);
        keepFetching = false;
        
        // Flush any remaining messages in the batch
        if (messageBatch.length > 0) {
          try {
            console.log(`Inserting final batch of ${messageBatch.length} messages into database`);
            await monitor.storeMessagesInDbBatch(messageBatch);
            exportState.messagesStoredInDb += messageBatch.length;
            messageBatch = [];
          } catch (dbError) {
            console.error('Error inserting final message batch into database:', dbError);
            exportState.dbErrors++;
          }
        }
        
        continue;
      }
      
      // If we're on the first batch, find the newest message
      // With standard fetch without 'before' parameter, the newest message is first
      if (fetchCount === 1 && !options.before) {
        newestMessageId = messages.first().id;
        console.log(`First batch has newest message ID: ${newestMessageId}`);
      }
      
      // Save the last message ID for pagination
      lastMessageId = messages.last().id;
      
      // Check if we've reached previously fetched messages
      if (lastStoredMessageId && lastMessageId) {
        try {
          console.log(`Comparing message IDs in ${channel.name}:`);
          console.log(`  Current batch last message ID: "${lastMessageId}" (length: ${lastMessageId.length})`);
          console.log(`  Stored last message ID: "${lastStoredMessageId}" (length: ${lastStoredMessageId.length})`);
          
          const currentIdBigInt = BigInt(lastMessageId);
          const storedIdBigInt = BigInt(lastStoredMessageId);
          const compareResult = currentIdBigInt <= storedIdBigInt;
          
          console.log(`  Comparison result (current <= stored): ${compareResult}`);
          
          if (compareResult) {
            console.log(`Reached previously fetched messages in ${channel.name}, stopping fetch`);
            keepFetching = false;
            
            // Process only messages that are newer than lastStoredMessageId
            const newMessages = Array.from(messages.values())
              .filter(msg => {
                try {
                  return BigInt(msg.id) > storedIdBigInt;
                } catch (err) {
                  console.error(`Error comparing message ID ${msg.id}:`, err);
                  return false; // Skip on error
                }
              });
            
            console.log(`Found ${newMessages.length} new messages since last fetch`);
            
            // Continue with only the new messages
            const nonBotMessages = newMessages.filter(message => !message.author.bot);
            exportState.messageDroppedCount += (newMessages.length - nonBotMessages.length);
            exportState.messagesTotalProcessed += newMessages.length;
            
            // Process each non-bot message
            for (const message of nonBotMessages) {
              messageBatch.push(message);
              exportState.messagesInCurrentChannel++;
              exportState.processedMessages++;
            }
            
            // Flush any remaining messages in the batch
            if (messageBatch.length > 0) {
              try {
                console.log(`Inserting final batch of ${messageBatch.length} new messages into database`);
                await monitor.storeMessagesInDbBatch(messageBatch);
                exportState.messagesStoredInDb += messageBatch.length;
                messageBatch = [];
              } catch (dbError) {
                console.error('Error inserting message batch into database:', dbError);
                exportState.dbErrors++;
              }
            }
            
            continue;
          }
        } catch (idError) {
          console.error(`Error comparing message IDs in ${channel.name}:`, idError);
          console.error(`lastMessageId: "${lastMessageId}", lastStoredMessageId: "${lastStoredMessageId}"`);
          // Continue fetching on error
        }
      }
      
      // Track all messages encountered
      exportState.messagesTotalProcessed += messages.size;
      
      // Filter out bot messages
      const nonBotMessages = Array.from(messages.values())
        .filter(message => !message.author.bot);
      
      // Track dropped (bot) messages
      exportState.messageDroppedCount += (messages.size - nonBotMessages.length);
      
      console.log(`Found ${nonBotMessages.length} non-bot messages in batch`);
      
      // Process each non-bot message
      for (const message of nonBotMessages) {
        // Add to database batch
        messageBatch.push(message);
        exportState.messagesInCurrentChannel++;
        
        // If batch reaches the configured size, store in database
        if (messageBatch.length >= DB_BATCH_SIZE) {
          try {
            console.log(`Inserting batch of ${messageBatch.length} messages into database`);
            await monitor.storeMessagesInDbBatch(messageBatch);
            exportState.messagesStoredInDb += messageBatch.length;
            messageBatch = []; // Clear the batch after successful insert
          } catch (dbError) {
            console.error('Error inserting message batch into database:', dbError);
            exportState.dbErrors++;
            
            // If batch insert fails, try inserting messages individually
            console.log('Attempting to insert messages individually...');
            for (const batchMessage of messageBatch) {
              try {
                await monitor.storeMessageInDb(batchMessage);
                exportState.messagesStoredInDb++;
              } catch (singleError) {
                console.error(`Error inserting individual message ${batchMessage.id}:`, singleError);
                exportState.dbErrors++;
              }
            }
            messageBatch = []; // Clear the batch after attempting individual inserts
          }
        }
        
        // Update processed counter for status display
        exportState.processedMessages++;
      }
      
      // Update status based on configured interval
      const currentTime = Date.now();
      if (currentTime - exportState.lastStatusUpdateTime > STATUS_UPDATE_INTERVAL) {
        exportState.lastStatusUpdateTime = currentTime;
        updateStatusMessage(statusMessage, exportState, guild);
      }
      
      // If we got fewer messages than requested, we've reached the end
      if (messages.size < 100) {
        console.log(`Reached end of messages for ${channel.name}`);
        keepFetching = false;
        
        // Flush any remaining messages in the batch
        if (messageBatch.length > 0) {
          try {
            console.log(`Inserting final batch of ${messageBatch.length} messages into database`);
            await monitor.storeMessagesInDbBatch(messageBatch);
            exportState.messagesStoredInDb += messageBatch.length;
            messageBatch = [];
          } catch (dbError) {
            console.error('Error inserting final message batch into database:', dbError);
            exportState.dbErrors++;
            
            // If batch insert fails, try inserting messages individually
            console.log('Attempting to insert remaining messages individually...');
            for (const batchMessage of messageBatch) {
              try {
                await monitor.storeMessageInDb(batchMessage);
                exportState.messagesStoredInDb++;
              } catch (singleError) {
                console.error(`Error inserting individual message ${batchMessage.id}:`, singleError);
                exportState.dbErrors++;
              }
            }
            messageBatch = []; // Clear the batch after attempting individual inserts
          }
        }
      }
      
    } catch (error) {
      if (error.code === 10008 || error.code === 50001) {
        // Message or channel not found, skip
        console.log(`Skipping channel ${channel.name}: ${error.message}`);
        keepFetching = false;
      }
      else if (error.httpStatus === 429 || error.code === 'RateLimitedError') {
        exportState.rateLimitHits++;
        // Use the retry_after value from the error or default to 1 second
        const retryAfter = error.retry_after || error.timeout || 1000;
        console.log(`Rate limited in ${channel.name}, waiting ${retryAfter}ms`);
        await new Promise(resolve => setTimeout(resolve, retryAfter));
      } else {
        console.error(`Error fetching messages from ${channel.name}:`, error);
        keepFetching = false;
        
        // If there's an error and we still have messages in the batch, try to save them
        if (messageBatch.length > 0) {
          try {
            console.log(`Attempting to insert ${messageBatch.length} messages into database after error`);
            await monitor.storeMessagesInDbBatch(messageBatch);
            exportState.messagesStoredInDb += messageBatch.length;
          } catch (dbError) {
            console.error('Error inserting message batch into database after fetch error:', dbError);
            exportState.dbErrors++;
            
            // Try inserting individually
            for (const batchMessage of messageBatch) {
              try {
                await monitor.storeMessageInDb(batchMessage);
                exportState.messagesStoredInDb++;
              } catch (singleError) {
                console.error(`Error inserting individual message ${batchMessage.id}:`, singleError);
                exportState.dbErrors++;
              }
            }
          }
          messageBatch = [];
        }
      }
    }
  }
  
  // IMPORTANT: For standard fetching, we want to store either:
  // 1. The newest message ID if we found one
  // 2. The current lastMessageId we used for pagination
  // 3. The previously stored ID as fallback
  const idToStore = newestMessageId || lastMessageId || lastStoredMessageId;
  
  // Clean the ID to ensure no spaces
  const cleanIdToStore = idToStore ? String(idToStore).replace(/\s+/g, '') : null;
  
  console.log(`Standard fetch completed. Saving lastMessageId: ${cleanIdToStore}`);
  await monitor.markChannelFetchingCompleted(channel.id, cleanIdToStore);
  console.log(`Completed monitoring setup for channel: ${channel.name} (${channel.id})`);
}

// Extract message metadata - this function is still needed for monitor.js
function extractMessageMetadata(message) {
  // Format with all relevant message data
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

// Format the date exactly as requested 
function formatDateToUTC() {
  const now = new Date();
  return `${now.getUTCFullYear()}-${String(now.getUTCMonth() + 1).padStart(2, '0')}-${String(now.getUTCDate()).padStart(2, '0')} ${String(now.getUTCHours()).padStart(2, '0')}:${String(now.getUTCMinutes()).padStart(2, '0')}:${String(now.getUTCSeconds()).padStart(2, '0')}`;
}

// Status update with proper formatting
let statusUpdateTimeout = null;
let lastStatusUpdate = {
  time: 0,
  processedMessages: 0
};

async function updateStatusMessage(statusMessage, exportState, guild, isFinal = false) {
  // Skip updates that are too frequent unless final
  if (!isFinal && statusUpdateTimeout) return;
  
  // Set update throttling (using configured interval)
  if (!isFinal) {
    statusUpdateTimeout = setTimeout(() => {
      statusUpdateTimeout = null;
    }, STATUS_UPDATE_INTERVAL);
  }
  
  // Get memory usage for status message
  const memory = checkMemoryUsage();
  
  const currentTime = Date.now();
  const elapsedTime = currentTime - exportState.startTime;
  
  // Calculate time components
  const hours = Math.floor(elapsedTime / (1000 * 60 * 60));
  const minutes = Math.floor((elapsedTime % (1000 * 60 * 60)) / (1000 * 60));
  const seconds = Math.floor((elapsedTime % (1000 * 60)) / 1000);
  
  // Calculate average processing speed over the entire runtime
  const avgMessagesPerSecond = elapsedTime > 0 ? 
    (exportState.processedMessages / (elapsedTime / 1000)).toFixed(2) : 
    "0.00";
  
// Calculate current processing speed based on differences between status updates
  let currentSpeed = "0.00";
  if (lastStatusUpdate.time > 0) {
    const timeDiff = (currentTime - lastStatusUpdate.time) / 1000; // Convert to seconds
    const messageDiff = exportState.processedMessages - lastStatusUpdate.processedMessages;
    
    if (timeDiff > 0) {
      currentSpeed = (messageDiff / timeDiff).toFixed(2);
    }
  }
  
    // Update the last status values for next calculation
  lastStatusUpdate = {
    time: currentTime,
    processedMessages: exportState.processedMessages
  };
  
  // Current date in the exact format from your example
  const nowFormatted = formatDateToUTC();

  // Calculate export number based on memory-triggered saves
  const exportNumber = exportState.memoryTriggeredSaves + 1;
  
  // Build status message
  let status = `Guild Database Import Status (#${exportNumber})\n`;
  
  if (isFinal) {
    status += `✅ Import completed! ${exportState.processedMessages.toLocaleString()} non-bot messages saved to database\n`;
        
    if (exportState.dbErrors > 0) {
      status += `⚠️ Database errors encountered: ${exportState.dbErrors}\n`;
    }
    
    // Add database name
    status += `💾 Database file: ${monitor.getCurrentDatabaseFilename()}\n`;
  } else if (exportState.activeChannels.size > 0) {
    // Get all active channel names
    const channelNames = Array.from(exportState.activeChannels.values());
    status += `🔄 Processing ${exportState.activeChannels.size} channel(s): ${channelNames.join(', ')}\n`;
  } else {
    status += `🔄 Initializing database import...\n`;
  }
  
  status += `📊 Processed ${exportState.processedMessages.toLocaleString()} non-bot messages from ${guild.name}\n`;
    status += `⚡ Processing speed: ${currentSpeed} msg/sec (${avgMessagesPerSecond} average)\n`;
    status += `📈 Progress: ${exportState.processedChannels}/${exportState.totalChannels} channels (${Math.round(exportState.processedChannels / exportState.totalChannels * 100)}%)\n`;
  
  status += `🚦 Rate limit hits: ${exportState.rateLimitHits}\n`;
  // Add memory usage info

  status += `⏱️ Time elapsed: ${hours}h ${minutes}m ${seconds}s\n`;
  status += `⏰ Last update: ${nowFormatted}`;
  
  try {
    await statusMessage.edit(status);
    console.log(`Updated status message`);
  } catch (error) {
    console.error('Error updating status message:', error);
  }
}

async function handleExportGuild(message, client) {
  const guild = message.guild;
  
  console.log(`Starting database import for guild: ${guild.name} (${guild.id})`);
  logMemoryUsage('Initial');
  
  // Verify the user has administrator permissions
  if (!message.member.permissions.has(PermissionFlagsBits.Administrator)) {
    return message.reply('You need administrator permissions to use this command.');
  }
  
  // Create status message
  const statusMessage = await message.channel.send(
    `Guild Database Import Status (#1)\n` +
    `🔄 Initializing database import...`
  );

  // Check if database needs initialization without relying on initializedGuilds
  try {
    // Check if database is already initialized by getting the database instance
    const currentDb = monitor.getDatabase();
    if (!currentDb) {
      console.log(`Database not yet initialized for guild ${guild.name}, initializing now`);
      await monitor.initializeDatabase(guild);
      
      // Get the database from monitor and initialize WAL manager
      const db = monitor.getDatabase();
      if (db) {
        await walManager.initialize(client, db);
      }
      
      console.log(`Database initialized for guild ${guild.name} (${guild.id})`);
      console.log(`Using database: ${monitor.getCurrentDatabasePath()}`);
    }
  } catch (dbError) {
    console.error('Error initializing database:', dbError);
  }
  
  console.log(`Using database at ${monitor.getCurrentDatabasePath()}`);
  
  // Initialize export state
  const exportState = {
    startTime: Date.now(),
    processedMessages: 0,
    messagesTotalProcessed: 0,
    messagesStoredInDb: 0,
    messageDroppedCount: 0,
    dbErrors: 0,
    totalChannels: 0,
    processedChannels: 0,
    currentChannelIndex: 0,
    currentChannel: null,
    messagesInCurrentChannel: 0,
    rateLimitHits: 0,
    lastStatusUpdateTime: Date.now(),
    lastAutoSaveTime: Date.now(),
    memoryCheckCount: 0,
    memoryTriggeredSaves: 0,
    runningTasksCount: 0,
    saveInProgress: false,
    memoryLimit: MEMORY_LIMIT_BYTES,
    dbBatchSize: DB_BATCH_SIZE,
    activeChannels: new Map(), // Map to track active channels (id -> name)
    channelBatchSpeed: new Map() // Map to track current batch speed for each channel
  };
  
  // Update the status message initially
  await updateStatusMessage(statusMessage, exportState, guild);
  
  // Set up memory check timer
  const memoryCheckTimer = setInterval(() => {
    checkAndHandleMemoryUsage(exportState, 'TIMER_CHECK');
  }, MEMORY_CHECK_INTERVAL);
  
  // Set up status update timer
  const statusUpdateTimer = setInterval(async () => {
    await updateStatusMessage(statusMessage, exportState, guild);
  }, STATUS_UPDATE_INTERVAL);
  
  try {
    // Get all channels in the guild that are actually visible
    const allChannels = await fetchVisibleChannels(guild);
    exportState.totalChannels = allChannels.length;
    
    console.log(`Found ${allChannels.length} visible channels to process`);
    
    // Store channel metadata in database
    for (const channelObj of allChannels) {
  const channel = channelObj.channel;
  try {
    // Get any existing channel info before updating
    const channelInfo = await getChannelInfo(channel.id);
    
    // If the channel exists and has a lastMessageId, use markChannelFetchingStarted
    // which will preserve the lastMessageId
    await monitor.markChannelFetchingStarted(channel.id, channel.name);
    
    console.log(`Updated channel info for ${channel.name} (${channel.id})${
      channelInfo && channelInfo.lastMessageId ? ` with existing lastMessageId: ${channelInfo.lastMessageId}` : ''
    }`);
  } catch (error) {
    console.error(`Error updating channel info for ${channel.name} (${channel.id}):`, error);
  }
}
    
    // Process channels in parallel with controlled concurrency
    await processChannelsInParallel(allChannels, exportState, statusMessage, guild);
    
    // Check for duplicates in the database after export is complete
    const duplicates = await monitor.checkForDuplicates();
    console.log(`Database duplicate check complete. Found ${duplicates} duplicate message IDs.`);
    
    // Store final metadata in the database
    try {
      await monitor.storeGuildMetadata('import_completed_at', new Date().toISOString());
      await monitor.storeGuildMetadata('total_messages_processed', exportState.messagesTotalProcessed.toString());
      await monitor.storeGuildMetadata('total_non_bot_messages', exportState.processedMessages.toString());
      await monitor.storeGuildMetadata('messages_stored_in_db', exportState.messagesStoredInDb.toString());
      await monitor.storeGuildMetadata('bot_messages_filtered', exportState.messageDroppedCount.toString());
      await monitor.storeGuildMetadata('database_errors', exportState.dbErrors.toString());
      await monitor.storeGuildMetadata('export_duration_seconds', Math.floor((Date.now() - exportState.startTime) / 1000).toString());
      await monitor.storeGuildMetadata('channels_processed', exportState.totalChannels.toString());
      await monitor.storeGuildMetadata('rate_limit_hits', exportState.rateLimitHits.toString());
    } catch (metadataError) {
      console.error('Error storing final metadata:', metadataError);
    }
    
// Clear timers before the final status update
clearInterval(statusUpdateTimer);
// Then do the final status update
await updateStatusMessage(statusMessage, exportState, guild, true);
	
	// Fetch and store member data
try {
  console.log('Starting member data export...');
  
  const memberStatusMessage = await message.channel.send(
    `Member Database Import Status\n` +
    `🔄 Initializing member data import...`
  );
  
  const memberResult = await memberTracker.fetchAndStoreMembersForGuild(guild, memberStatusMessage);
  
  if (memberResult.success) {
    console.log(`Successfully stored member data: ${memberResult.memberCount} members with ${memberResult.roleCount} roles`);
    
    // Store metadata about member export
    await monitor.storeGuildMetadata('members_exported', memberResult.memberCount.toString());
    await monitor.storeGuildMetadata('member_roles_exported', memberResult.roleCount.toString());
    await monitor.storeGuildMetadata('member_export_completed_at', new Date().toISOString());
  } else {
    console.error('Error during member export:', memberResult.error);
    await monitor.storeGuildMetadata('member_export_error', memberResult.error);
  }
} catch (memberError) {
  console.error('Error during member data export:', memberError);
  await monitor.storeGuildMetadata('member_export_error', memberError.message);
}
    
    // IMPORTANT: Ensure monitoring is active after export completes
    console.log(`Export completed. Ensuring monitoring is active for guild ${guild.name}`);
    
    // Reload channel states to ensure monitoring picks up newly fetched channels
    try {
      await monitor.loadFetchedChannelsState();
      console.log(`Reloaded fetched channel states after export for monitoring`);
    } catch (channelStateError) {
      console.error('Error reloading channel states after export:', channelStateError);
    }
    
    // Make sure monitoring is running
    if (!global.monitoringActive) {
      global.monitoringActive = true;
      monitor.processMessageCache();
      console.log(`Started monitoring process after export completion`);
    } else {
      console.log(`Monitoring already active, continuing with existing process`);
    }
    
    console.log(`Database import completed successfully for guild: ${guild.name} (${guild.id})`);
    logMemoryUsage('Final');
  } catch (error) {
    console.error('Error during database import:', error);
    
    try {
      // Store error information in metadata
      await monitor.storeGuildMetadata('import_error', error.message);
      await monitor.storeGuildMetadata('import_error_time', new Date().toISOString());
    } catch (e) {
      console.error('Error saving error metadata:', e);
    }
    
    await statusMessage.edit(`Error occurred during database import: ${error.message}`);
    
    // Even after error, try to ensure monitoring is active
    try {
      if (!global.monitoringActive) {
        global.monitoringActive = true;
        monitor.processMessageCache();
        console.log(`Started monitoring process despite export error`);
      }
    } catch (monitorError) {
      console.error('Failed to start monitoring after export error:', monitorError);
    }
  } finally {
    // Clear timers
    clearInterval(memoryCheckTimer);
    clearInterval(statusUpdateTimer);
  }
}

// Export functions
module.exports = {
  handleExportGuild,
  extractMessageMetadata // Still needed for other modules
};