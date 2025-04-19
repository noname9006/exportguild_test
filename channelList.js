// channelList.js - Functionality to generate a hierarchical list of channels and threads
const { ChannelType } = require('discord.js');

/**
 * Generate a tree-like structure of all channels and threads in a guild
 * @param {Guild} guild - The Discord guild to generate the channel list for
 * @returns {string[]} Array of formatted channel list segments for multiple messages
 */
async function generateChannelList(guild) {
  // Create Map to store parent channels and their associated threads
  const channelMap = new Map();
  const topLevelChannels = [];
  const forumChannels = new Map(); // Special map for forum posts
  
  // Get all channels
  await guild.channels.fetch();
  
  // First pass: identify regular channels and forum channels
  for (const [id, channel] of guild.channels.cache) {
    // Skip categories (we'll handle them separately)
    if (channel.type === 4) continue; // 4 is GuildCategory
    
    // Handle forum channels specially (type 15 is forum channel)
    if (channel.type === 15) {
      forumChannels.set(channel.id, {
        id: channel.id,
        name: channel.name,
        position: channel.position || 0,
        parentId: channel.parentId,
        posts: []  // Will fill with forum posts
      });
      continue;
    }
    
    // Skip thread channels (we'll handle them in second pass)
    // Thread types: 10=NewsThread, 11=PublicThread, 12=PrivateThread
    if ([10, 11, 12].includes(channel.type)) continue; 
    
    // Add all non-category, non-thread, non-forum channels
    topLevelChannels.push({
      id: channel.id,
      name: channel.name,
      position: channel.position || 0,
      parentId: channel.parentId,
      type: channel.type
    });
    
    // Initialize array for threads
    channelMap.set(channel.id, []);
  }
  
  // Sort top level channels by position
  topLevelChannels.sort((a, b) => a.position - b.position);
  
  // Second pass: fetch threads for all channels
  try {
    // For each text channel, fetch both active and archived threads
    for (const channel of [...topLevelChannels]) {
      try {
        const fetchedChannel = await guild.channels.fetch(channel.id);
        if (!fetchedChannel) continue;
        
        // Skip channels that don't support threads
        if (!fetchedChannel.threads) continue;
        
        // Try to fetch active threads - handle the case where result isn't iterable
        try {
          const threadManager = fetchedChannel.threads;
          if (threadManager && typeof threadManager.fetch === 'function') {
            const threads = await threadManager.fetch();
            
            if (threads && threads.size && threads.forEach) {
              threads.forEach((thread) => {
                const threadData = {
                  id: thread.id,
                  name: thread.name,
                  locked: thread.locked || false,
                  archived: thread.archived || false,
                  createdTimestamp: thread.createdTimestamp
                };
                
                if (channelMap.has(channel.id)) {
                  channelMap.get(channel.id).push(threadData);
                }
              });
            }
          }
        } catch (threadError) {
          console.error(`Error fetching active threads for ${channel.name}:`, threadError.message);
        }
        
        // Try to fetch archived threads with proper error handling
        try {
          const threadManager = fetchedChannel.threads;
          if (threadManager && typeof threadManager.fetchArchived === 'function') {
            const archivedThreads = await threadManager.fetchArchived();
            
            if (archivedThreads && archivedThreads.threads && archivedThreads.threads.size && archivedThreads.threads.forEach) {
              archivedThreads.threads.forEach((thread) => {
                // Check for duplicates
                const isDuplicate = channelMap.get(channel.id).some(t => t.id === thread.id);
                if (!isDuplicate) {
                  const threadData = {
                    id: thread.id,
                    name: thread.name,
                    locked: thread.locked || false,
                    archived: thread.archived || true,
                    createdTimestamp: thread.createdTimestamp
                  };
                  
                  if (channelMap.has(channel.id)) {
                    channelMap.get(channel.id).push(threadData);
                  }
                }
              });
            }
          }
        } catch (archiveError) {
          console.error(`Error fetching archived threads for ${channel.name}:`, archiveError.message);
        }
      } catch (channelError) {
        console.error(`Error processing threads for channel ${channel.id}:`, channelError.message);
      }
    }
    
    // Handle forum channels separately
    for (const forum of forumChannels.values()) {
      try {
        const fetchedChannel = await guild.channels.fetch(forum.id);
        if (!fetchedChannel) continue;
        
        // Get active posts (which are threads in a forum)
        try {
          const threadManager = fetchedChannel.threads;
          if (threadManager && typeof threadManager.fetch === 'function') {
            const threads = await threadManager.fetch();
            
            if (threads && threads.size && threads.forEach) {
              threads.forEach((thread) => {
                forum.posts.push({
                  id: thread.id,
                  name: thread.name,
                  locked: thread.locked || false,
                  archived: thread.archived || false,
                  createdTimestamp: thread.createdTimestamp
                });
              });
            }
          }
        } catch (threadError) {
          console.error(`Error fetching active posts for forum ${forum.name}:`, threadError.message);
        }
        
        // Get archived posts
        try {
          const threadManager = fetchedChannel.threads;
          if (threadManager && typeof threadManager.fetchArchived === 'function') {
            const archivedThreads = await threadManager.fetchArchived();
            
            if (archivedThreads && archivedThreads.threads && archivedThreads.threads.size && archivedThreads.threads.forEach) {
              archivedThreads.threads.forEach((thread) => {
                // Check for duplicates
                const isDuplicate = forum.posts.some(p => p.id === thread.id);
                if (!isDuplicate) {
                  forum.posts.push({
                    id: thread.id,
                    name: thread.name,
                    locked: thread.locked || false,
                    archived: true,
                    createdTimestamp: thread.createdTimestamp
                  });
                }
              });
            }
          }
        } catch (archiveError) {
          console.error(`Error fetching archived posts for forum ${forum.name}:`, archiveError.message);
        }
      } catch (forumError) {
        console.error(`Error processing forum ${forum.id}:`, forumError.message);
      }
    }
  } catch (generalError) {
    console.error('General error fetching threads:', generalError);
  }
  
  // Sort threads and forum posts by creation time (newest first)
  for (const [channelId, threads] of channelMap) {
    threads.sort((a, b) => (b.createdTimestamp || 0) - (a.createdTimestamp || 0));
  }
  
  for (const forum of forumChannels.values()) {
    forum.posts.sort((a, b) => (b.createdTimestamp || 0) - (a.createdTimestamp || 0));
  }
  
  // Sort categories to be displayed first
  const categories = Array.from(guild.channels.cache.values())
    .filter(ch => ch.type === 4) // 4 is the value for GuildCategory
    .sort((a, b) => (a.position || 0) - (b.position || 0));
  
  // Array to hold message segments
  const messageSegments = [];
  
  // Start with header segment
  let currentSegment = `# Channel List for ${guild.name}\n\n`;
  
  // Add current timestamp in required format
  const now = new Date();
  const timestamp = `${now.getUTCFullYear()}-${String(now.getUTCMonth() + 1).padStart(2, '0')}-${String(now.getUTCDate()).padStart(2, '0')} ${String(now.getUTCHours()).padStart(2, '0')}:${String(now.getUTCMinutes()).padStart(2, '0')}:${String(now.getUTCSeconds()).padStart(2, '0')}`;
  currentSegment += `Generated: ${timestamp} (UTC)\n`;
  currentSegment += `Generated by: noname9006\n\n`;
  
  // Function to add segment to array if it's getting too long
  const checkAndAppendSegment = (newContent) => {
    if (currentSegment.length + newContent.length > 1900) { // Using 1900 to leave margin for Discord's limit of 2000
      messageSegments.push(currentSegment);
      currentSegment = `# Channel List (continued)\n\n`;
    }
    currentSegment += newContent;
  };
  
  // Process each category
  for (const category of categories) {
    // Add category header
    const categoryHeader = `## 📁 ${category.name}\n`;
    checkAndAppendSegment(categoryHeader);
    
    // Get channels and forums in this category
    const channelsInCategory = topLevelChannels
      .filter(ch => ch.parentId === category.id)
      .sort((a, b) => a.position - b.position);
    
    const forumsInCategory = Array.from(forumChannels.values())
      .filter(ch => ch.parentId === category.id)
      .sort((a, b) => a.position - b.position);
    
    if (channelsInCategory.length === 0 && forumsInCategory.length === 0) {
      checkAndAppendSegment(`*No channels in this category*\n\n`);
      continue;
    }
    
    // Process each regular channel in category
    for (const channel of channelsInCategory) {
      const channelLine = `- 📝 <#${channel.id}> - \`${channel.name}\`\n`;
      checkAndAppendSegment(channelLine);
      
      // Get threads for this channel
      const threads = channelMap.get(channel.id) || [];
      
      // Add threads with indentation
      for (const thread of threads) {
        let threadEmoji = thread.archived ? '📁' : (thread.locked ? '🔒' : '💬');
        const threadLine = `  - ${threadEmoji} <#${thread.id}> - \`${thread.name}\`${thread.archived ? ' (archived)' : ''}${thread.locked ? ' (locked)' : ''}\n`;
        checkAndAppendSegment(threadLine);
      }
    }
    
    // Process each forum channel in category
    for (const forum of forumsInCategory) {
      const forumLine = `- 📊 <#${forum.id}> - \`${forum.name}\` (Forum)\n`;
      checkAndAppendSegment(forumLine);
      
      // Add forum posts with indentation
      for (const post of forum.posts) {
        let postEmoji = post.archived ? '📁' : (post.locked ? '🔒' : '📌');
        const postLine = `  - ${postEmoji} <#${post.id}> - \`${post.name}\`${post.archived ? ' (archived)' : ''}${post.locked ? ' (locked)' : ''}\n`;
        checkAndAppendSegment(postLine);
      }
    }
    
    checkAndAppendSegment(`\n`); // Add spacing between categories
  }
  
  // Add channels with no category (appear at the bottom)
  const channelsWithoutCategory = topLevelChannels.filter(ch => !ch.parentId);
  const forumsWithoutCategory = Array.from(forumChannels.values()).filter(ch => !ch.parentId);
  
  if (channelsWithoutCategory.length > 0 || forumsWithoutCategory.length > 0) {
    const noCategoryHeader = `## ⚪ No Category\n`;
    checkAndAppendSegment(noCategoryHeader);
    
    // Process each channel with no category
    for (const channel of channelsWithoutCategory) {
      const channelLine = `- 📝 <#${channel.id}> - \`${channel.name}\`\n`;
      checkAndAppendSegment(channelLine);
      
      // Get threads for this channel
      const threads = channelMap.get(channel.id) || [];
      
      // Add threads with indentation
      for (const thread of threads) {
        let threadEmoji = thread.archived ? '📁' : (thread.locked ? '🔒' : '💬');
        const threadLine = `  - ${threadEmoji} <#${thread.id}> - \`${thread.name}\`${thread.archived ? ' (archived)' : ''}${thread.locked ? ' (locked)' : ''}\n`;
        checkAndAppendSegment(threadLine);
      }
    }
    
    // Process each forum with no category
    for (const forum of forumsWithoutCategory) {
      const forumLine = `- 📊 <#${forum.id}> - \`${forum.name}\` (Forum)\n`;
      checkAndAppendSegment(forumLine);
      
      // Add forum posts with indentation
      for (const post of forum.posts) {
        let postEmoji = post.archived ? '📁' : (post.locked ? '🔒' : '📌');
        const postLine = `  - ${postEmoji} <#${post.id}> - \`${post.name}\`${post.archived ? ' (archived)' : ''}${post.locked ? ' (locked)' : ''}\n`;
        checkAndAppendSegment(postLine);
      }
    }
  }
  
  // Statistics
  const textChannelCount = topLevelChannels.length;
  const forumChannelCount = forumChannels.size;
  const threadCount = Array.from(channelMap.values()).reduce((count, threads) => count + threads.length, 0);
  const forumPostCount = Array.from(forumChannels.values()).reduce((count, forum) => count + forum.posts.length, 0);
  const categoryCount = categories.length;
  
  // Fixed the statsContent string - it was missing a closing backtick
  const statsContent = `\n## 📊 Statistics\n` +
    `- Categories: ${categoryCount}\n` +
    `- Text Channels: ${textChannelCount}\n` +
    `- Threads: ${threadCount}\n` +
    `- Forum Channels: ${forumChannelCount}\n` +
    `- Forum Posts: ${forumPostCount}\n`;
  
  checkAndAppendSegment(statsContent);
  
  // Add notes about clickable links
  const noteContent = `\n> 💡 **Note**: Channel/thread names are clickable links that will take you to the channel.\n` +
                      `> 📁 = archived thread, 🔒 = locked thread, 💬 = active thread, 📊 = forum, 📌 = forum post`;
  checkAndAppendSegment(noteContent);
  
  // Add the final segment
  messageSegments.push(currentSegment);
  
  return messageSegments;
}

/**
 * Handle the !channellist command
 * @param {Message} message - The Discord message that triggered the command
 */
async function handleChannelListCommand(message) {
  try {
    const guild = message.guild;
    
    // Check user permissions - require at least manage messages or administrator permission
    if (!message.member.permissions.has('ManageMessages') && !message.member.permissions.has('Administrator')) {
      return message.reply('You need Manage Messages or Administrator permissions to use this command.');
    }
    
    // Status message
    const statusMessage = await message.channel.send(
      `Generating channel list for ${guild.name}...\nThis might take some time as I'm fetching all channels and threads.`
    );

    // Generate the channel list (returns array of message segments)
    const channelListSegments = await generateChannelList(guild);
    
    // Update the status message with the first segment
    await statusMessage.edit(channelListSegments[0]);
    
    // If there are more segments, send them as additional messages
    for (let i = 1; i < channelListSegments.length; i++) {
      await message.channel.send(channelListSegments[i]);
    }
    
  } catch (error) {
    console.error('Error generating channel list:', error);
    message.channel.send(`Error generating channel list: ${error.message}`);
    throw error; // Re-throw to allow the caller to handle it
  }
}

module.exports = {
  handleChannelListCommand
};