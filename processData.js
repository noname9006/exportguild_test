// processData.js - Contains functionality to analyze NDJSON files and create CSV reports
const fs = require('fs');
const path = require('path');
const readline = require('readline');
const { createObjectCsvWriter } = require('csv-writer');

// Find the most recent NDJSON file in the root folder
function findMostRecentNDJSON() {
  const files = fs.readdirSync(process.cwd())
    .filter(file => file.endsWith('.ndjson'))
    .map(file => ({
      name: file,
      path: path.join(process.cwd(), file),
      mtime: fs.statSync(path.join(process.cwd(), file)).mtime
    }))
    .sort((a, b) => b.mtime - a.mtime); // Sort by modification time, newest first

  if (files.length === 0) {
    throw new Error('No .ndjson files found in the current directory');
  }

  console.log(`Found ${files.length} NDJSON files, using most recent: ${files[0].name}`);
  return files[0].path;
}

// Format date to YYYY-MM-DD
function formatDate(dateString) {
  const date = new Date(dateString);
  return `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, '0')}-${String(date.getDate()).padStart(2, '0')}`;
}

// Calculate days between two dates
function daysBetween(date1, date2) {
  const oneDay = 24 * 60 * 60 * 1000; // hours*minutes*seconds*milliseconds
  const diffDays = Math.round(Math.abs((date1 - date2) / oneDay));
  return diffDays;
}

// Process NDJSON file and create CSV reports
async function processNDJSON(message) {
  try {
    const guild = message.guild;
    // Get the current date for calculating days on server
    const currentDate = new Date();
    
    const statusMessage = await message.channel.send(
      `Data Processing Status\n` +
      `🔄 Finding the most recent NDJSON file...`
    );

    // Extract command arguments
    const args = message.content.trim().split(/\s+/);
    // Check if the "--noroles" flag is present
    const skipRoles = args.includes("--noroles");

    // Find the most recent NDJSON file
    const ndjsonFilePath = findMostRecentNDJSON();
    const fileName = path.basename(ndjsonFilePath);

    // Update status message
    await statusMessage.edit(
      `Data Processing Status\n` +
      `🔄 Processing file: ${fileName}\n` +
      `🔄 Analyzing messages...`
    );

    // Read the NDJSON file line by line
    const fileStream = fs.createReadStream(ndjsonFilePath);
    const rl = readline.createInterface({
      input: fileStream,
      crlfDelay: Infinity
    });

    // Data structures to track messages by user
    const userMessages = new Map(); // userId -> array of messages
    const userMessageCounts = new Map(); // userId -> total message count
    const userDailyMessageCounts = new Map(); // userId -> Map of date -> message count
    const userSymbolCounts = new Map(); // userId -> total symbol count
    const userDailySymbols = new Map(); // userId -> Map of date -> symbol count
    const usernames = new Map(); // userId -> username
    const userActiveDays = new Map(); // userId -> Set of active days
    const allDates = new Set(); // All unique dates in the dataset
    
    // For calculating average symbols per message
    let totalMessages = 0;
    let totalSymbols = 0;

    // Process each line
    let lineCount = 0;
    for await (const line of rl) {
      lineCount++;
      if (lineCount % 10000 === 0) {
        await statusMessage.edit(
          `Data Processing Status\n` +
          `🔄 Processing file: ${fileName}\n` +
          `🔄 Analyzed ${lineCount.toLocaleString()} lines...`
        );
      }

      try {
        const data = JSON.parse(line);
        
        // Only process message records
        if (data.type === 'message') {
          const userId = data.authorId;
          const username = data.authorUsername;
          const date = formatDate(data.createdAt);
          const content = data.content || "";
          const symbolCount = content.length;
          
          // Track total messages and symbols for average calculation
          totalMessages++;
          totalSymbols += symbolCount;
          
          // Store username
          usernames.set(userId, username);
          
          // Track message count
          userMessageCounts.set(userId, (userMessageCounts.get(userId) || 0) + 1);
          
          // Track message timestamps
          if (!userMessages.has(userId)) {
            userMessages.set(userId, []);
          }
          userMessages.get(userId).push(date);
          
          // Track active days
          if (!userActiveDays.has(userId)) {
            userActiveDays.set(userId, new Set());
          }
          userActiveDays.get(userId).add(date);
          
          // Track total symbol count
          userSymbolCounts.set(userId, (userSymbolCounts.get(userId) || 0) + symbolCount);
          
          // Track daily symbol counts
          if (!userDailySymbols.has(userId)) {
            userDailySymbols.set(userId, new Map());
          }
          const dailySymbolsMap = userDailySymbols.get(userId);
          dailySymbolsMap.set(date, (dailySymbolsMap.get(date) || 0) + symbolCount);
          
          // Track daily message counts
          if (!userDailyMessageCounts.has(userId)) {
            userDailyMessageCounts.set(userId, new Map());
          }
          const dailyMsgCountMap = userDailyMessageCounts.get(userId);
          dailyMsgCountMap.set(date, (dailyMsgCountMap.get(date) || 0) + 1);
          
          // Add date to all dates set
          allDates.add(date);
        }
      } catch (error) {
        console.error(`Error processing line ${lineCount}:`, error);
      }
    }

    // Get all user IDs
    const userIds = Array.from(usernames.keys());
    const userRoles = new Map(); // userId -> highest role name
    const userDisplayNames = new Map(); // userId -> display name
    const userJoinDates = new Map(); // userId -> join date
    const userTotalDays = new Map(); // userId -> total days on server
    
    // Initialize these variables outside of any conditional blocks
    let processedCount = 0;
    const uniqueMessageUserCount = userIds.length;
    
    // Set to keep track of users currently on the server
    const currentServerUsers = new Set();

    // Handle roles based on flag
    if (skipRoles) {
      // Skip role fetching, just set placeholder
      for (const userId of userIds) {
        userRoles.set(userId, "Not Fetched");
        userDisplayNames.set(userId, "Not Fetched");
        userJoinDates.set(userId, "Unknown");
        userTotalDays.set(userId, "Unknown");
      }
      
      // No users have roles, so processedCount remains 0
      
      await statusMessage.edit(
        `Data Processing Status\n` +
        `🔄 Processing file: ${fileName}\n` +
        `✅ Analyzed ${lineCount.toLocaleString()} lines\n` +
        `ℹ️ Skipping role fetching (--noroles flag used)\n` +
        `🔄 Preparing CSV reports...`
      );
    } else {
      await statusMessage.edit(
        `Data Processing Status\n` +
        `🔄 Processing file: ${fileName}\n` +
        `✅ Analyzed ${lineCount.toLocaleString()} lines\n` +
        `🔄 Fetching member data...\n` +
        `   Tip: Use '!exportguild process --noroles' to skip role fetching`
      );

      // Start with a map of members we need to check - all users
      const membersToCheck = new Map(userIds.map(id => [id, true]));
      
      // Default all to "Not in server" initially
      for (const userId of userIds) {
        userRoles.set(userId, "Not in server");
        userDisplayNames.set(userId, usernames.get(userId) || "Unknown");
        userJoinDates.set(userId, "Not in server");
        userTotalDays.set(userId, "N/A");
      }
      
      // First check the cache
      let cachedCount = 0;
      guild.members.cache.forEach(member => {
        const userId = member.user.id;
        if (membersToCheck.has(userId)) {
          userRoles.set(userId, member.roles.highest.name || "No Role");
          userDisplayNames.set(userId, member.displayName || member.user.username);
          
          // Get join date and calculate days on server
          const joinDate = member.joinedAt;
          if (joinDate) {
            const formattedJoinDate = formatDate(joinDate);
            userJoinDates.set(userId, formattedJoinDate);
            userTotalDays.set(userId, daysBetween(currentDate, joinDate));
          } else {
            userJoinDates.set(userId, "Unknown");
            userTotalDays.set(userId, "Unknown");
          }
          
          membersToCheck.delete(userId);
          cachedCount++;
          processedCount++;
          currentServerUsers.add(userId); // Mark user as currently on server
          
          // Update status occasionally
          if (processedCount % 100 === 0 || processedCount === uniqueMessageUserCount) {
            statusMessage.edit(
              `Data Processing Status\n` +
              `🔄 Processing file: ${fileName}\n` +
              `✅ Analyzed ${lineCount.toLocaleString()} lines\n` +
              `🔄 Processing roles: ${processedCount}/${uniqueMessageUserCount} members\n` +
              `   (${cachedCount} from cache, fetching remaining...)`
            ).catch(() => {});
          }
        }
      });
      
      await statusMessage.edit(
        `Data Processing Status\n` +
        `🔄 Processing file: ${fileName}\n` +
        `✅ Analyzed ${lineCount.toLocaleString()} lines\n` +
        `🔄 Found ${cachedCount} members in cache\n` +
        `🔄 Trying to fetch ${membersToCheck.size} remaining members...`
      );
      
      // Use guild.members.list() to get all current members efficiently
      try {
        console.log("Attempting to fetch all guild members...");
        // Get first batch of members
        let members = await guild.members.list({ limit: 1000 });
        let memberCount = members.size;
        let lastId = members.last()?.id;
        
        // Process this batch
        for (const [id, member] of members) {
          if (membersToCheck.has(id)) {
            userRoles.set(id, member.roles.highest.name || "No Role");
            userDisplayNames.set(id, member.displayName || member.user.username);
            
            // Get join date and calculate days on server
            const joinDate = member.joinedAt;
            if (joinDate) {
              const formattedJoinDate = formatDate(joinDate);
              userJoinDates.set(id, formattedJoinDate);
              userTotalDays.set(id, daysBetween(currentDate, joinDate));
            } else {
              userJoinDates.set(id, "Unknown");
              userTotalDays.set(id, "Unknown");
            }
            
            membersToCheck.delete(id);
            processedCount++;
            currentServerUsers.add(id); // Mark user as currently on server
          }
        }
        
        // Update status
        await statusMessage.edit(
          `Data Processing Status\n` +
          `🔄 Processing file: ${fileName}\n` +
          `✅ Analyzed ${lineCount.toLocaleString()} lines\n` +
          `🔄 Fetching member data: ${memberCount} members fetched\n` +
          `🔄 Processing roles: ${processedCount}/${uniqueMessageUserCount} members identified`
        );
        
        // Continue fetching if there are more members
        while (lastId && members.size === 1000) {
          // Fetch next batch
          members = await guild.members.list({ limit: 1000, after: lastId });
          memberCount += members.size;
          
          if (members.size > 0) {
            lastId = members.last().id;
            
            // Process this batch
            for (const [id, member] of members) {
              if (membersToCheck.has(id)) {
                userRoles.set(id, member.roles.highest.name || "No Role");
                userDisplayNames.set(id, member.displayName || member.user.username);
                
                // Get join date and calculate days on server
                const joinDate = member.joinedAt;
                if (joinDate) {
                  const formattedJoinDate = formatDate(joinDate);
                  userJoinDates.set(id, formattedJoinDate);
                  userTotalDays.set(id, daysBetween(currentDate, joinDate));
                } else {
                  userJoinDates.set(id, "Unknown");
                  userTotalDays.set(id, "Unknown");
                }
                
                membersToCheck.delete(id);
                processedCount++;
                currentServerUsers.add(id); // Mark user as currently on server
              }
            }
            
            // Update status
            await statusMessage.edit(
              `Data Processing Status\n` +
              `🔄 Processing file: ${fileName}\n` +
              `✅ Analyzed ${lineCount.toLocaleString()} lines\n` +
              `🔄 Fetching member data: ${memberCount} members fetched\n` +
              `🔄 Processing roles: ${processedCount}/${uniqueMessageUserCount} members identified`
            );
          } else {
            break; // No more members
          }
        }
        
        console.log(`Fetched ${memberCount} total members, identified roles for ${processedCount} users`);
      } catch (error) {
        console.error("Error fetching all members:", error);
        await statusMessage.edit(
          `Data Processing Status\n` +
          `🔄 Processing file: ${fileName}\n` +
          `✅ Analyzed ${lineCount.toLocaleString()} lines\n` +
          `⚠️ Error fetching all members: ${error.message}\n` +
          `🔄 Continuing with roles for ${processedCount}/${uniqueMessageUserCount} members identified`
        );
      }
      
      // If we still have members to check and didn't get all from list()
      // Try individual fetches for a small sample of highly active members
      if (membersToCheck.size > 0) {
        await statusMessage.edit(
          `Data Processing Status\n` +
          `🔄 Processing file: ${fileName}\n` +
          `✅ Analyzed ${lineCount.toLocaleString()} lines\n` +
          `🔄 Identified ${processedCount}/${uniqueMessageUserCount} members\n` +
          `🔄 Checking top active remaining members...`
        );
        
        // Get top 20 most active users that we haven't identified yet
        const remainingActiveUsers = Array.from(membersToCheck.keys())
          .map(id => ({ id, messageCount: userMessageCounts.get(id) || 0 }))
          .sort((a, b) => b.messageCount - a.messageCount)
          .slice(0, 20); // Only check top 20
        
        // Try to fetch these users individually
        for (const {id} of remainingActiveUsers) {
          try {
            const member = await guild.members.fetch(id);
            if (member) {
              userRoles.set(id, member.roles.highest.name || "No Role");
              userDisplayNames.set(id, member.displayName || member.user.username);
              
              // Get join date and calculate days on server
              const joinDate = member.joinedAt;
              if (joinDate) {
                const formattedJoinDate = formatDate(joinDate);
                userJoinDates.set(id, formattedJoinDate);
                userTotalDays.set(id, daysBetween(currentDate, joinDate));
              } else {
                userJoinDates.set(id, "Unknown");
                userTotalDays.set(id, "Unknown");
              }
              
              membersToCheck.delete(id);
              processedCount++;
              currentServerUsers.add(id); // Mark user as currently on server
            }
          } catch (error) {
            // User likely not in server anymore - keep as "Not in server"
            console.log(`Could not fetch member ${id}: ${error.message}`);
          }
        }
      }
      
      // Calculate the percentage of identified users
      const identifiedPercentage = Math.round((processedCount / uniqueMessageUserCount) * 100);
      
      await statusMessage.edit(
        `Data Processing Status\n` +
        `🔄 Processing file: ${fileName}\n` +
        `✅ Analyzed ${lineCount.toLocaleString()} lines\n` +
        `✅ Role data complete: Identified ${processedCount}/${uniqueMessageUserCount} members (${identifiedPercentage}%)\n` +
        `🔄 Preparing CSV reports...`
      );
    }

    // Sort dates newest to oldest
    const sortedDates = Array.from(allDates).sort((a, b) => new Date(b) - new Date(a));

    // Generate CSV 1: Message counts sorted by user with daily message counts
    const messageCountRows = [];
    for (const [userId, totalMsgCount] of userMessageCounts.entries()) {
      const dailyMsgCountMap = userDailyMessageCounts.get(userId);
      const dailyData = {};
      
      // Add daily message count columns sorted newest to oldest
      for (const date of sortedDates) {
        dailyData[`count_${date}`] = dailyMsgCountMap.get(date) || 0;
      }
      
      // Get active days count
      const activeDaysCount = userActiveDays.has(userId) ? userActiveDays.get(userId).size : 0;
      
      messageCountRows.push({
        userId,
        username: usernames.get(userId),
        displayName: userDisplayNames.get(userId) || usernames.get(userId) || "Unknown",
        highestRole: userRoles.get(userId) || "Unknown",
        joinDate: userJoinDates.get(userId) || "Unknown",
        totalDaysOnServer: userTotalDays.get(userId) || "N/A",
        activeDaysCount: activeDaysCount,
        messageCount: totalMsgCount,
        ...dailyData
      });
    }
    
    // Sort by message count (highest to lowest)
    messageCountRows.sort((a, b) => b.messageCount - a.messageCount);

    // Generate CSV 2: Symbol counts sorted by user with daily symbol counts
    const symbolCountRows = [];
    for (const [userId, totalSymbols] of userSymbolCounts.entries()) {
      const dailySymbolsMap = userDailySymbols.get(userId);
      const totalMsgCount = userMessageCounts.get(userId);
      const avgSymbolsPerMsg = totalMsgCount > 0 ? Math.round(totalSymbols / totalMsgCount) : 0;
      const dailyData = {};
      
      // Add daily symbol count columns sorted newest to oldest
      for (const date of sortedDates) {
        dailyData[`symbols_${date}`] = dailySymbolsMap.get(date) || 0;
      }
      
      // Get active days count
      const activeDaysCount = userActiveDays.has(userId) ? userActiveDays.get(userId).size : 0;
      
      symbolCountRows.push({
        userId,
        username: usernames.get(userId),
        displayName: userDisplayNames.get(userId) || usernames.get(userId) || "Unknown",
        highestRole: userRoles.get(userId) || "Unknown",
        joinDate: userJoinDates.get(userId) || "Unknown",
        totalDaysOnServer: userTotalDays.get(userId) || "N/A",
        activeDaysCount: activeDaysCount,
        totalMessages: totalMsgCount,
        totalSymbols,
        avgSymbolsPerMsg,
        ...dailyData
      });
    }
    
    // Sort by total symbols (highest to lowest)
    symbolCountRows.sort((a, b) => b.totalSymbols - a.totalSymbols);

    // Create output filenames based on input file
    const baseName = path.basename(fileName, '.ndjson');
    const messageCountsFile = `${baseName}-message-counts.csv`;
    const symbolCountsFile = `${baseName}-symbol-counts.csv`;

    // Prepare CSV writers
    // For message counts - with dates sorted newest to oldest
    const messageCountHeaders = [
      { id: 'userId', title: 'User ID' },
      { id: 'username', title: 'Username' },
      { id: 'displayName', title: 'Display Name' },
      { id: 'highestRole', title: 'Highest Role' },
      { id: 'joinDate', title: 'Join Date' },
      { id: 'totalDaysOnServer', title: 'Days on Server' },
      { id: 'activeDaysCount', title: 'Active Days' },
      { id: 'messageCount', title: 'Total Messages' }
    ];
    
    // Add date columns sorted newest to oldest
    sortedDates.forEach(date => {
      messageCountHeaders.push({ id: `count_${date}`, title: date });
    });

    const messageCountWriter = createObjectCsvWriter({
      path: messageCountsFile,
      header: messageCountHeaders
    });

    // For symbol counts - with enhanced columns and dates newest to oldest
    const symbolCountHeaders = [
      { id: 'userId', title: 'User ID' },
      { id: 'username', title: 'Username' },
      { id: 'displayName', title: 'Display Name' },
      { id: 'highestRole', title: 'Highest Role' },
      { id: 'joinDate', title: 'Join Date' },
      { id: 'totalDaysOnServer', title: 'Days on Server' },
      { id: 'activeDaysCount', title: 'Active Days' },
      { id: 'totalMessages', title: 'Total Messages' },
      { id: 'totalSymbols', title: 'Total Symbols' },
      { id: 'avgSymbolsPerMsg', title: 'Avg Symbols/Message' }
    ];

    // Add date columns sorted newest to oldest
    sortedDates.forEach(date => {
      symbolCountHeaders.push({ id: `symbols_${date}`, title: date });
    });

    const symbolCountWriter = createObjectCsvWriter({
      path: symbolCountsFile,
      header: symbolCountHeaders
    });

    // Write the files
    await messageCountWriter.writeRecords(messageCountRows);
    await symbolCountWriter.writeRecords(symbolCountRows);

    // Generate role status message based on whether roles were fetched or not
    let roleStatus = "";
    if (skipRoles) {
      roleStatus = "⚠️ Role data not fetched (--noroles flag used)";
    } else {
      // Calculate percentage if we have values
      if (uniqueMessageUserCount > 0) {
        const identifiedPercentage = Math.round((processedCount / uniqueMessageUserCount) * 100);
        roleStatus = `✅ Role data included (${processedCount}/${uniqueMessageUserCount} members identified, ${identifiedPercentage}%)`;
      } else {
        roleStatus = "✅ Role data included";
      }
    }
    
    // Get total server members from the guild
    const totalServerMembers = guild.memberCount;
    
    // Calculate the requested statistics    
    // Total users who sent at least X messages
    const usersWithAtLeast1Msg = userMessageCounts.size;
    const usersWithAtLeast2Msg = Array.from(userMessageCounts.values()).filter(count => count >= 2).length;
    const usersWithAtLeast3Msg = Array.from(userMessageCounts.values()).filter(count => count >= 3).length;
    
    // Users currently on server who sent at least X messages
    const currentUsersWithAtLeast1Msg = Array.from(currentServerUsers).filter(id => 
      userMessageCounts.has(id) && userMessageCounts.get(id) >= 1
    ).length;
    
    const currentUsersWithAtLeast2Msg = Array.from(currentServerUsers).filter(id => 
      userMessageCounts.has(id) && userMessageCounts.get(id) >= 2
    ).length;
    
    const currentUsersWithAtLeast3Msg = Array.from(currentServerUsers).filter(id => 
      userMessageCounts.has(id) && userMessageCounts.get(id) >= 3
    ).length;
    
    // Calculate percentages based on total server members
    const percentUsersWithAtLeast1Msg = (usersWithAtLeast1Msg / totalServerMembers * 100).toFixed(2);
    const percentUsersWithAtLeast2Msg = (usersWithAtLeast2Msg / totalServerMembers * 100).toFixed(2);
    const percentUsersWithAtLeast3Msg = (usersWithAtLeast3Msg / totalServerMembers * 100).toFixed(2);
    
    // Calculate percentages of current server users
    const percentCurrentUsersWithAtLeast1Msg = (currentUsersWithAtLeast1Msg / totalServerMembers * 100).toFixed(2);
    const percentCurrentUsersWithAtLeast2Msg = (currentUsersWithAtLeast2Msg / totalServerMembers * 100).toFixed(2);
    const percentCurrentUsersWithAtLeast3Msg = (currentUsersWithAtLeast3Msg / totalServerMembers * 100).toFixed(2);
    
    // Calculate average symbols per message
    const avgSymbolsPerMessage = totalMessages > 0 ? (totalSymbols / totalMessages).toFixed(2) : 0;
    
    // Create statistics section
    const statisticsSection = 
      `📊 Message Statistics:\n` +
      `• ${percentUsersWithAtLeast1Msg}% of total server users have at least 1 message (${usersWithAtLeast1Msg}/${totalServerMembers})\n` +
      `• ${percentCurrentUsersWithAtLeast1Msg}% of current server users have at least 1 message (${currentUsersWithAtLeast1Msg}/${totalServerMembers})\n` +
      `• ${percentUsersWithAtLeast2Msg}% of total server users have at least 2 messages (${usersWithAtLeast2Msg}/${totalServerMembers})\n` +
      `• ${percentCurrentUsersWithAtLeast2Msg}% of current server users have at least 2 messages (${currentUsersWithAtLeast2Msg}/${totalServerMembers})\n` +
      `• ${percentUsersWithAtLeast3Msg}% of total server users have at least 3 messages (${usersWithAtLeast3Msg}/${totalServerMembers})\n` +
      `• ${percentCurrentUsersWithAtLeast3Msg}% of current server users have at least 3 messages (${currentUsersWithAtLeast3Msg}/${totalServerMembers})\n` +
      `• Average symbols per message: ${avgSymbolsPerMessage}`;

    // Update status with completion message
    await statusMessage.edit(
      `Data Processing Status\n` +
      `✅ Processing complete!\n` +
      `📄 Generated reports:\n` +
      `1. ${messageCountsFile} - Users sorted by message count with daily breakdown\n` +
      `2. ${symbolCountsFile} - Users sorted by symbol count with avg symbols and daily breakdown\n` +
      `${roleStatus}\n` +
      `📊 Processed data from ${userMessageCounts.size} users with ${lineCount.toLocaleString()} total lines\n\n` +
      `${statisticsSection}`
    );

    console.log(`Processing complete: Generated ${messageCountsFile} and ${symbolCountsFile}`);
    
  } catch (error) {
    console.error('Error during data processing:', error);
    await message.channel.send(`Error during data processing: ${error.message}`);
  }
}

module.exports = {
  processNDJSON
};