// exportguild.js - Functions for exporting Discord guild data
const path = require('path');
const { 
  PermissionFlagsBits,
  ChannelType
} = require('discord.js');
const config = require('./config');
const monitor = require('./monitor');

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
const MAX_CONCURRENT_REQUESTS = 10;

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
  
  visibleChannels.push(...textChannels);
  console.log(`Found ${textChannels.length} text channels to process`);
  
  // Get threads in batches to avoid rate limiting
  const threadChannels = [];
  for (const channelObj of textChannels) {
    const channel = channelObj.channel;
    if (!channel.threads) continue;
    
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
        if (!excludedChannels.has(thread.id) && 
            thread.viewable && 
            thread.permissionsFor(guild.members.me).has(PermissionFlagsBits.ReadMessageHistory)) {
          threadChannels.push({
            channel: thread,
            isThread: true,
            parentId: channel.id,
            parentName: channel.name
          });
        }
      }
    } catch (error) {
      console.error(`Error processing threads for channel ${channel.name}:`, error);
    }
  }
  
  visibleChannels.push(...threadChannels);
  console.log(`Found ${threadChannels.length} thread channels to process`);
  
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

async function fetchMessagesFromChannel(channel, exportState, statusMessage, guild) {
  if (!channel.isTextBased()) {
    console.log(`Skipping non-text channel: ${channel.name}`);
    return;
  }
  
  // Mark channel as fetching started in database
  await monitor.markChannelFetchingStarted(channel.id, channel.name);
  
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
      
      // Save the last message ID for pagination
      lastMessageId = messages.last().id;
      
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
  
  // Mark channel as fetching completed in database with last message ID
  await monitor.markChannelFetchingCompleted(channel.id, lastMessageId);
  
  console.log(`Completed processing channel: ${channel.name} (${channel.id}), processed ${exportState.messagesInCurrentChannel} messages`);
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
  
  // Calculate average processing speed
  const avgMessagesPerSecond = elapsedTime > 0 ? 
    (exportState.processedMessages / (elapsedTime / 1000)).toFixed(2) : 
    "0.00";
  
  // Calculate current processing speed (average of all active channels)
  let currentSpeed = "0.00";
  if (exportState.channelBatchSpeed.size > 0) {
    let speedSum = 0;
    for (const speed of exportState.channelBatchSpeed.values()) {
      speedSum += parseFloat(speed);
    }
    currentSpeed = (speedSum / exportState.channelBatchSpeed.size).toFixed(2);
  }
  
  // Current date in the exact format from your example
  const nowFormatted = formatDateToUTC();

  // Calculate export number based on memory-triggered saves
  const exportNumber = exportState.memoryTriggeredSaves + 1;
  
  // Build status message
  let status = `Guild Database Import Status (#${exportNumber})\n`;
  
  if (isFinal) {
    status += `✅ Import completed! ${exportState.processedMessages.toLocaleString()} non-bot messages saved to database\n`;
    status += `📝 Messages stored in database: ${exportState.messagesStoredInDb.toLocaleString()}\n`;
    status += `🤖 Bot messages skipped: ${exportState.messageDroppedCount.toLocaleString()}\n`;
    
    if (exportState.dbErrors > 0) {
      status += `⚠️ Database errors encountered: ${exportState.dbErrors}\n`;
    }
    
    // Add database name
    status += `💾 Database file: ${monitor.getCurrentDatabasePath()}\n`;
  } else if (exportState.activeChannels.size > 0) {
    // Get all active channel names
    const channelNames = Array.from(exportState.activeChannels.values());
    status += `🔄 Processing ${exportState.activeChannels.size} channel(s): ${channelNames.join(', ')}\n`;
  } else {
    status += `🔄 Initializing database import...\n`;
  }
  
  status += `📊 Processed ${exportState.processedMessages.toLocaleString()} non-bot messages from ${guild.name}\n`;
  status += `⏱️ Time elapsed: ${hours}h ${minutes}m ${seconds}s\n`;
  status += `⚡ Processing speed: ${currentSpeed} messages/second (${avgMessagesPerSecond} average)\n`;
    status += `📈 Progress: ${exportState.processedChannels}/${exportState.totalChannels} channels (${Math.round(exportState.processedChannels / exportState.totalChannels * 100)}%)\n`;
  
  status += `🚦 Rate limit hits: ${exportState.rateLimitHits}\n`;
  // Add memory usage info
  status += `💾 Memory: ${memory.rssMB}MB / ${MEMORY_LIMIT_MB}MB (${memory.percentOfLimit}%)\n`;
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

  // Note: The database should already be initialized in index.js before this function is called
  // We don't need to initialize it again here
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
      await monitor.markChannelFetchingStarted(channel.id, channel.name);
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
    
    // Final status update
    await updateStatusMessage(statusMessage, exportState, guild, true);
    
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