// member-tracker.js - Module to track member stats, roles, and role changes
const { PermissionFlagsBits } = require('discord.js');
const config = require('./config');
const monitor = require('./monitor');

// Initialize the database tables for member tracking
async function initializeMemberDatabase(db) {
  return new Promise((resolve, reject) => {
    if (!db) {
      reject(new Error("Database not initialized"));
      return;
    }
    
    console.log('Initializing member tracking database tables...');
    
    // Create members table to store basic member information
    db.run(`
      CREATE TABLE IF NOT EXISTS guild_members (
        id TEXT PRIMARY KEY,
        username TEXT,
        displayName TEXT,
        avatarURL TEXT,
        joinedAt TEXT,
        joinedTimestamp INTEGER,
        bot INTEGER DEFAULT 0,
        lastUpdated INTEGER,
        leftGuild INTEGER DEFAULT 0,
        leftTimestamp INTEGER
      )
    `, (err) => {
      if (err) {
        console.error('Error creating guild_members table:', err);
        reject(err);
        return;
      }
      
      // Create member_roles table to track current roles
      db.run(`
        CREATE TABLE IF NOT EXISTS member_roles (
          memberId TEXT,
          roleId TEXT,
          roleName TEXT,
          roleColor TEXT,
          rolePosition INTEGER,
          addedAt INTEGER,
          PRIMARY KEY (memberId, roleId)
        )
      `, (err) => {
        if (err) {
          console.error('Error creating member_roles table:', err);
          reject(err);
          return;
        }
        
        // Create role_history table to track role changes over time
        db.run(`
          CREATE TABLE IF NOT EXISTS role_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            memberId TEXT,
            roleId TEXT,
            roleName TEXT,
            action TEXT,
            timestamp INTEGER,
            FOREIGN KEY (memberId) REFERENCES guild_members(id)
          )
        `, (err) => {
          if (err) {
            console.error('Error creating role_history table:', err);
            reject(err);
            return;
          }
          
          // Create guild_roles table to track roles and their hierarchy
          db.run(`
            CREATE TABLE IF NOT EXISTS guild_roles (
              id TEXT PRIMARY KEY,
              name TEXT,
              color TEXT,
              position INTEGER,
              permissions TEXT,
              mentionable INTEGER,
              hoist INTEGER,
              managed INTEGER,
              createdAt TEXT,
              createdTimestamp INTEGER,
              updatedAt TEXT,
              updatedTimestamp INTEGER,
              deleted INTEGER DEFAULT 0,
              deletedAt TEXT,
              deletedTimestamp INTEGER
            )
          `, (err) => {
            if (err) {
              console.error('Error creating guild_roles table:', err);
              reject(err);
              return;
            }
            
            // Create processing_checkpoints table for efficient member fetching
            db.run(`
              CREATE TABLE IF NOT EXISTS processing_checkpoints (
                guild_id TEXT PRIMARY KEY,
                last_member_id TEXT,
                timestamp INTEGER
              )
            `, (err) => {
              if (err) {
                console.error('Error creating processing_checkpoints table:', err);
                reject(err);
                return;
              }
              
              // Create indexes for better performance
              db.run(`CREATE INDEX IF NOT EXISTS idx_member_roles_member_id ON member_roles(memberId)`, (err) => {
                if (err) {
                  console.error('Error creating member_roles index:', err);
                  // Non-fatal, continue
                }
                
                console.log('Member tracking database tables initialized successfully');
                resolve(true);
              });
            });
          });
        });
      });
    });
  });
}

// Save member to database
async function storeMemberInDb(member) {
  return new Promise((resolve, reject) => {
    const db = monitor.getDatabase();
    if (!db) {
      reject(new Error("Database not initialized"));
      return;
    }
    
    // Only process if we can get valid member data
    if (!member || !member.id) {
      reject(new Error("Invalid member object"));
      return;
    }
    
    const currentTime = Date.now();
    const joinedTimestamp = member.joinedTimestamp || null;
    const joinedAt = joinedTimestamp ? new Date(joinedTimestamp).toISOString() : null;
    
    // Store member data
    const sql = `
      INSERT OR REPLACE INTO guild_members (
        id, username, displayName, avatarURL, joinedAt, joinedTimestamp, 
        bot, lastUpdated, leftGuild
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    `;
    
    db.run(sql, [
      member.id,
      member.user.username,
      member.displayName || member.user.username,
      member.user.displayAvatarURL(),
      joinedAt,
      joinedTimestamp,
      member.user.bot ? 1 : 0,
      currentTime,
      0 // Not left guild
    ], function(err) {
      if (err) {
        console.error(`Error storing member ${member.id} in database:`, err);
        reject(err);
        return;
      }
      
      console.log(`Stored or updated member ${member.user.username} (${member.id}) in database`);
      resolve(this.changes);
    });
  });
}

// Store member roles in database
async function storeMemberRolesInDb(member) {
  return new Promise((resolve, reject) => {
    const db = monitor.getDatabase();
    if (!db) {
      reject(new Error("Database not initialized"));
      return;
    }
    
    // Only process if we have a valid member with roles
    if (!member || !member.roles || !member.roles.cache) {
      reject(new Error("Invalid member object or roles collection"));
      return;
    }
    
    const currentTime = Date.now();
    const roles = Array.from(member.roles.cache.values());
    
    // Skip @everyone role
    const filteredRoles = roles.filter(role => role.id !== member.guild.id);
    
    if (filteredRoles.length === 0) {
      console.log(`Member ${member.user.username} has no roles to store (besides @everyone)`);
      resolve(0);
      return;
    }
    
    // Remove existing roles for this member before adding current ones
    db.run(`DELETE FROM member_roles WHERE memberId = ?`, [member.id], function(err) {
      if (err) {
        console.error(`Error clearing existing roles for member ${member.id}:`, err);
        reject(err);
        return;
      }
      
      // Prepare batch insert
      const stmt = db.prepare(`
        INSERT INTO member_roles (
          memberId, roleId, roleName, roleColor, rolePosition, addedAt
        ) VALUES (?, ?, ?, ?, ?, ?)
      `);
      
      let successCount = 0;
      
      // Insert each role
      for (const role of filteredRoles) {
        stmt.run([
          member.id,
          role.id,
          role.name,
          role.hexColor,
          role.position,
          currentTime
        ], function(err) {
          if (err) {
            console.error(`Error storing role ${role.name} for member ${member.user.username}:`, err);
          } else {
            successCount++;
          }
        });
      }
      
      stmt.finalize(err => {
        if (err) {
          reject(err);
        } else {
          console.log(`Stored ${successCount} roles for member ${member.user.username} (${member.id})`);
          resolve(successCount);
        }
      });
    });
  });
}

// Store multiple members' roles in a batch transaction
async function storeMemberRolesInDbBatch(members) {
  return new Promise((resolve, reject) => {
    const db = monitor.getDatabase();
    if (!db) {
      reject(new Error("Database not initialized"));
      return;
    }
    
    const currentTime = Date.now();
    let totalRolesAdded = 0;
    
    // Start transaction
    db.serialize(() => {
      db.run('BEGIN TRANSACTION');
      
      // Prepare delete statement for reuse
      const deleteStmt = db.prepare(`DELETE FROM member_roles WHERE memberId = ?`);
      
      // Prepare insert statement for reuse
      const insertStmt = db.prepare(`
        INSERT INTO member_roles (
          memberId, roleId, roleName, roleColor, rolePosition, addedAt
        ) VALUES (?, ?, ?, ?, ?, ?)
      `);
      
      // Process each member
      for (const member of members) {
        if (!member || !member.roles || !member.roles.cache) {
          console.log(`Skipping invalid member`);
          continue;
        }
        
        const roles = Array.from(member.roles.cache.values());
        const filteredRoles = roles.filter(role => role.id !== member.guild.id);
        
        if (filteredRoles.length === 0) {
          console.log(`Member ${member.user.username} has no roles to store (besides @everyone)`);
          continue;
        }
        
        // Delete existing roles
        deleteStmt.run(member.id);
        
        // Insert each role
        for (const role of filteredRoles) {
          insertStmt.run(
            member.id,
            role.id,
            role.name,
            role.hexColor,
            role.position,
            currentTime
          );
          totalRolesAdded++;
        }
      }
      
      // Finalize statements
      deleteStmt.finalize();
      insertStmt.finalize();
      
      // Commit transaction
      db.run('COMMIT', function(err) {
        if (err) {
          console.error('Error committing transaction:', err);
          db.run('ROLLBACK');
          reject(err);
        } else {
          console.log(`Successfully stored ${totalRolesAdded} roles for ${members.length} members in batch`);
          resolve(totalRolesAdded);
        }
      });
    });
  });
}

// Add role history entry
async function addRoleHistoryEntry(memberId, roleId, roleName, action) {
  return new Promise((resolve, reject) => {
    const db = monitor.getDatabase();
    if (!db) {
      reject(new Error("Database not initialized"));
      return;
    }
    
    const currentTime = Date.now();
    
    const sql = `
      INSERT INTO role_history (
        memberId, roleId, roleName, action, timestamp
      ) VALUES (?, ?, ?, ?, ?)
    `;
    
    db.run(sql, [
      memberId,
      roleId,
      roleName,
      action, // 'added' or 'removed'
      currentTime
    ], function(err) {
      if (err) {
        console.error(`Error adding role history entry for ${memberId}:`, err);
        reject(err);
        return;
      }
      
      console.log(`Added role history entry: ${action} role ${roleName} for member ${memberId}`);
      resolve(this.lastID);
    });
  });
}

// Mark member as having left the guild
async function markMemberLeftGuild(memberId, username) {
  return new Promise((resolve, reject) => {
    const db = monitor.getDatabase();
    if (!db) {
      reject(new Error("Database not initialized"));
      return;
    }
    
    const currentTime = Date.now();
    
    const sql = `
      UPDATE guild_members 
      SET leftGuild = 1, leftTimestamp = ? 
      WHERE id = ?
    `;
    
    db.run(sql, [currentTime, memberId], function(err) {
      if (err) {
        console.error(`Error marking member ${memberId} as left:`, err);
        reject(err);
        return;
      }
      
      if (this.changes > 0) {
        console.log(`Marked member ${username} (${memberId}) as having left the guild`);
      } else {
        console.log(`Member ${memberId} not found in database or already marked as left`);
      }
      resolve(this.changes);
    });
  });
}

// Store a role in the database
async function storeRoleInDb(role) {
  return new Promise((resolve, reject) => {
    const db = monitor.getDatabase();
    if (!db) {
      reject(new Error("Database not initialized"));
      return;
    }
    
    const currentTime = Date.now();
    const currentTimeIso = new Date(currentTime).toISOString();
    const createdTimestamp = role.createdTimestamp;
    const createdAt = new Date(createdTimestamp).toISOString();
    
    const sql = `
      INSERT OR REPLACE INTO guild_roles (
        id, name, color, position, permissions, mentionable, hoist, managed,
        createdAt, createdTimestamp, updatedAt, updatedTimestamp, deleted
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `;
    
    db.run(sql, [
      role.id,
      role.name,
      role.hexColor,
      role.position,
      role.permissions.bitfield.toString(),
      role.mentionable ? 1 : 0,
      role.hoist ? 1 : 0,
      role.managed ? 1 : 0,
      createdAt,
      createdTimestamp,
      currentTimeIso,
      currentTime,
      0 // not deleted
    ], function(err) {
      if (err) {
        console.error(`Error storing role ${role.name} (${role.id}) in database:`, err);
        reject(err);
        return;
      }
      
      console.log(`Stored or updated role ${role.name} (${role.id}) in database`);
      resolve(this.changes);
    });
  });
}

// Mark a role as deleted
async function markRoleDeleted(role) {
  return new Promise((resolve, reject) => {
    const db = monitor.getDatabase();
    if (!db) {
      reject(new Error("Database not initialized"));
      return;
    }
    
    const currentTime = Date.now();
    const currentTimeIso = new Date(currentTime).toISOString();
    
    const sql = `
      UPDATE guild_roles
      SET deleted = 1, deletedAt = ?, deletedTimestamp = ?
      WHERE id = ?
    `;
    
    db.run(sql, [
      currentTimeIso,
      currentTime,
      role.id
    ], function(err) {
      if (err) {
        console.error(`Error marking role ${role.name} (${role.id}) as deleted:`, err);
        reject(err);
        return;
      }
      
      console.log(`Marked role ${role.name} (${role.id}) as deleted`);
      resolve(this.changes);
    });
  });
}

// Fetch and store all guild roles
async function fetchAndStoreGuildRoles(guild) {
  try {
    const db = monitor.getDatabase();
    if (!db) {
      console.error("Cannot fetch roles: Database not initialized");
      return { success: false, error: "Database not initialized" };
    }
    
    console.log(`Starting to fetch all roles for guild ${guild.name} (${guild.id})`);
    
    // Ensure we have fetched all roles
    await guild.roles.fetch();
    
    const roles = Array.from(guild.roles.cache.values());
    let roleCount = 0;
    
    // Process each role
    for (const role of roles) {
      try {
        // Skip @everyone role if desired
        // if (role.id === guild.id) continue;
        
        // Store the role data
        await storeRoleInDb(role);
        roleCount++;
      } catch (roleError) {
        console.error(`Error storing role ${role.name}:`, roleError);
      }
    }
    
    console.log(`Completed storing ${roleCount} roles for guild ${guild.name}`);
    
    return {
      success: true,
      roleCount
    };
  } catch (error) {
    console.error(`Error in fetchAndStoreGuildRoles:`, error);
    return {
      success: false,
      error: error.message
    };
  }
}

// Store checkpoint for efficient member processing
async function saveProcessingCheckpoint(guildId, lastProcessedMemberId) {
  const db = monitor.getDatabase();
  return new Promise((resolve, reject) => {
    db.run(
      'INSERT OR REPLACE INTO processing_checkpoints (guild_id, last_member_id, timestamp) VALUES (?, ?, ?)',
      [guildId, lastProcessedMemberId, Date.now()],
      function(err) {
        if (err) reject(err);
        else resolve(this.changes);
      }
    );
  });
}

// Get last checkpoint for resuming member processing
async function getProcessingCheckpoint(guildId) {
  const db = monitor.getDatabase();
  return new Promise((resolve, reject) => {
    db.get(
      'SELECT last_member_id FROM processing_checkpoints WHERE guild_id = ?',
      [guildId],
      (err, row) => {
        if (err) reject(err);
        else resolve(row ? row.last_member_id : null);
      }
    );
  });
}

/**
 * Memory-efficient guild member fetching for large Discord servers
 * Date: 2025-04-25
 */
async function fetchMembersInChunks(guild, statusMessage) {
  // Initialize tracking variables
  let lastId = null;
  let done = false;
  let fetchCount = 0;
  let totalFetched = 0;
  
  // Memory usage tracking
  const initialMemory = process.memoryUsage().heapUsed;
  const memoryThresholdMB = 1000; // 1GB threshold - adjust based on your environment
  const memoryThresholdBytes = memoryThresholdMB * 1024 * 1024;
  
  // Initialize database connection once outside the loop
  const db = monitor.getDatabase();
  if (!db) {
    throw new Error("Database not initialized");
  }
  
  // Prepare statements for better performance
  const memberStmt = db.prepare(`
    INSERT OR REPLACE INTO guild_members (
      id, username, displayName, avatarURL, joinedAt, joinedTimestamp, 
      bot, lastUpdated, leftGuild
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
  `);
  
  const deleteRolesStmt = db.prepare(`
    DELETE FROM member_roles WHERE memberId = ?
  `);
  
  // Change to "INSERT OR IGNORE" to prevent unique constraint errors
  const roleStmt = db.prepare(`
    INSERT OR IGNORE INTO member_roles (
      memberId, roleId, roleName, roleColor, rolePosition, addedAt
    ) VALUES (?, ?, ?, ?, ?, ?)
  `);
  
  console.log(`[${new Date().toISOString()}] Starting memory-efficient member fetch for guild ${guild.name} (${guild.id})`);
  await statusMessage.edit(`Member Database Import Status\n` +
    `🔄 Starting memory-efficient member fetch for ${guild.name}...`);
  
  // First fetch and store guild roles to ensure they're available for member role assignments
  try {
    await statusMessage.edit(`Member Database Import Status\n` +
      `🔄 Fetching roles for ${guild.name}...`);
    
    const roleResult = await fetchAndStoreGuildRoles(guild);
    await statusMessage.edit(`Member Database Import Status\n` +
      `✅ Stored ${roleResult.roleCount} roles\n` +
      `🔄 Now fetching members (this may take a while for large guilds)...`);
  } catch (roleError) {
    console.error(`[${new Date().toISOString()}] Error fetching roles:`, roleError);
  }
  
  // Create a Map for role caching to avoid repeated object creation
  const roleCache = new Map();
  
  // Track processed members to avoid duplication
  const processedMemberIds = new Set();
  
  // Fetch members in chunks and process them immediately
  while (!done) {
    try {
      // Check memory usage and perform garbage collection if available
      const currentMemory = process.memoryUsage().heapUsed;
      const memoryUsageMB = Math.round(currentMemory / 1024 / 1024);
      
      console.log(`[${new Date().toISOString()}] Memory usage: ${memoryUsageMB}MB`);
      
      if (currentMemory - initialMemory > memoryThresholdBytes) {
        console.log(`[${new Date().toISOString()}] Memory threshold reached, forcing garbage collection`);
        if (global.gc) {
          global.gc();
          await new Promise(resolve => setTimeout(resolve, 500)); // Give GC time to work
        }
      }
      
      // Build fetch options - using Discord's REST pagination
      const options = { limit: 1000 }; // Max allowed by Discord API
      if (lastId) options.after = lastId;
      
      fetchCount++;
      console.log(`[${new Date().toISOString()}] Fetching member chunk #${fetchCount} (after ID: ${lastId || 'start'})`);
      
      // Update status message periodically
      if (fetchCount % 5 === 0 || fetchCount === 1) {
        await statusMessage.edit(`Member Database Import Status\n` +
          `🔄 Fetched ${totalFetched} members so far...\n` +
          `💾 Memory usage: ${memoryUsageMB}MB\n` +
          `⏱️ Fetch operation #${fetchCount}`);
      }
      
      // Use REST API directly for better pagination control
      let response;
      
      try {
        // Use Discord.js's REST client for proper rate limit handling
        response = await guild.client.rest.get(
          `/guilds/${guild.id}/members?limit=1000${lastId ? `&after=${lastId}` : ''}`
        );
      } catch (apiError) {
        console.error(`[${new Date().toISOString()}] API Error:`, apiError);
        
        // Handle rate limiting explicitly
        if (apiError.httpStatus === 429) {
          const retryAfter = apiError.retryAfter || 5; // Default to 5 seconds if not specified
          console.log(`[${new Date().toISOString()}] Rate limited, waiting ${retryAfter}s before retry`);
          
          await statusMessage.edit(`Member Database Import Status\n` +
            `⏳ Rate limited by Discord. Waiting ${retryAfter}s before continuing...\n` +
            `🔄 Fetched ${totalFetched} members so far`);
          
          await new Promise(resolve => setTimeout(resolve, retryAfter * 1000 + 100));
          continue; // Try again
        }
        
        // For other errors, wait a bit and try again, but only up to 3 times
        if (apiError.httpStatus >= 500 && fetchCount < 3) {
          console.log(`[${new Date().toISOString()}] Server error, retrying in 5s...`);
          await new Promise(resolve => setTimeout(resolve, 5000));
          continue;
        }
        
        // If we get here, it's a serious error we can't recover from
        throw apiError;
      }
      
      // If no members returned, we're done
      if (!response || response.length === 0) {
        console.log(`[${new Date().toISOString()}] No more members returned, fetch complete`);
        done = true;
        continue;
      }
      
      // Batch processing with immediate storage
      console.log(`[${new Date().toISOString()}] Processing ${response.length} members from chunk #${fetchCount}`);
      
      // Start transaction
      db.run('BEGIN TRANSACTION');
      
      // Process each member and store immediately
      let processedInBatch = 0;
      const currentTime = Date.now();
      
      try {
        for (const memberData of response) {
          // Skip if invalid data
          if (!memberData || !memberData.user || !memberData.user.id) {
            continue;
          }
          
          // Extract just what we need to minimize memory usage
          const user = memberData.user;
          const memberId = user.id;
          
          // Skip if we've already processed this member
          if (processedMemberIds.has(memberId)) {
            console.log(`[${new Date().toISOString()}] Skipping already processed member: ${memberId}`);
            continue;
          }
          
          // Add to processed set
          processedMemberIds.add(memberId);
          
          const joinedTimestamp = memberData.joined_at ? new Date(memberData.joined_at).getTime() : null;
          const joinedAt = joinedTimestamp ? new Date(joinedTimestamp).toISOString() : null;
          
          // Store member data
          memberStmt.run([
            memberId,
            user.username,
            memberData.nick || user.username,
            user.avatar ? `https://cdn.discordapp.com/avatars/${user.id}/${user.avatar}.png` : null,
            joinedAt,
            joinedTimestamp,
            user.bot ? 1 : 0,
            currentTime,
            0 // Not left guild
          ]);
          
          // Process roles - first delete existing
          deleteRolesStmt.run(memberId);
          
          // Track roles we've inserted for this member to avoid duplicates
          const insertedRoles = new Set();
          
          // Then add current roles
          if (memberData.roles && Array.isArray(memberData.roles)) {
            // Skip @everyone role which isn't included in the roles array from API
            // Process each role
            for (const roleId of memberData.roles) {
              // Skip if we've already processed this role for this member
              if (insertedRoles.has(roleId)) {
                continue;
              }
              
              // Get role data from cache if available
              let role = roleCache.get(roleId);
              
              // If not in cache, get from guild
              if (!role) {
                const guildRole = guild.roles.cache.get(roleId);
                if (guildRole) {
                  // Minimize the data we store in memory
                  role = {
                    id: guildRole.id,
                    name: guildRole.name,
                    hexColor: guildRole.hexColor,
                    position: guildRole.position
                  };
                  // Store in cache
                  roleCache.set(roleId, role);
                }
              }
              
              // Insert role if we have data
              if (role) {
                roleStmt.run([
                  memberId,
                  role.id,
                  role.name,
                  role.hexColor,
                  role.position,
                  currentTime
                ]);
                
                // Mark this role as inserted for this member
                insertedRoles.add(roleId);
              }
            }
          }
          
          processedInBatch++;
          totalFetched++;
          
          // Update lastId to highest ID seen
          if (!lastId || BigInt(memberId) > BigInt(lastId)) {
            lastId = memberId;
          }
        }
        
        // Commit transaction
        db.run('COMMIT');
        console.log(`[${new Date().toISOString()}] Successfully committed ${processedInBatch} members to database`);
        
      } catch (dbError) {
        // Rollback transaction on error
        console.error(`[${new Date().toISOString()}] Database error:`, dbError);
        db.run('ROLLBACK');
        
        // Throw to outer catch
        throw dbError;
      }
      
      // If we got fewer members than requested, we've reached the end
      if (response.length < 1000) {
        console.log(`[${new Date().toISOString()}] Reached end of member list (${response.length} < 1000)`);
        done = true;
      }
      
      // Add a small delay to prevent rate limits and allow GC to work
      await new Promise(resolve => setTimeout(resolve, 300));
      
    } catch (error) {
      console.error(`[${new Date().toISOString()}] Error in member processing loop:`, error);
      
      // Update status message with error
      await statusMessage.edit(`Member Database Import Status\n` +
        `⚠️ Error encountered: ${error.message}\n` +
        `🔄 Fetched ${totalFetched} members before error\n` +
        `🔄 Attempting to continue...`);
      
      // If we've been fetching for a while, try to continue
      if (fetchCount > 3) {
        await new Promise(resolve => setTimeout(resolve, 5000));
      } else {
        // If we're at the very beginning, this is fatal
        done = true;
        throw error;
      }
    }
  }
  
  // Success, update status
  console.log(`[${new Date().toISOString()}] Completed member fetch: ${totalFetched} members`);
  await statusMessage.edit(`Member Database Import Status\n` +
    `✅ Successfully fetched ${totalFetched} members from ${guild.name}\n` +
    `💾 Final memory usage: ${Math.round(process.memoryUsage().heapUsed / 1024 / 1024)}MB`);
  
  // Clean up prepared statements
  memberStmt.finalize();
  deleteRolesStmt.finalize();
  roleStmt.finalize();
  
  // Return summary
  return {
    success: true,
    memberCount: totalFetched,
    fetchOperations: fetchCount
  };
}

// Fetch all members for a guild and store them in database (for exportguild command)
async function fetchAndStoreMembersForGuild(guild, statusMessage) {
  try {
    const db = monitor.getDatabase();
    if (!db) {
      console.error("Cannot fetch members: Database not initialized");
      return { success: false, error: "Database not initialized" };
    }
    
    // Add database optimizations
    db.run('PRAGMA journal_mode = WAL');
    db.run('PRAGMA synchronous = NORMAL');
    db.run('PRAGMA cache_size = 10000');
    db.run('CREATE INDEX IF NOT EXISTS idx_member_roles_member_id ON member_roles(memberId)');
    
    // Update status message if provided
    if (statusMessage) {
      await statusMessage.edit(`Member Database Import Status\n` +
                           `🔄 Fetching members for ${guild.name}...`);
    }
    
    // Check if the guild is large and use the memory-efficient approach
    const isLargeGuild = guild.memberCount > 10000; // Threshold for large guilds
    
    if (isLargeGuild) {
      console.log(`Guild ${guild.name} has ${guild.memberCount} members - using memory-efficient processing`);
      return await fetchMembersInChunks(guild, statusMessage);
    } else {
      console.log(`Guild ${guild.name} has ${guild.memberCount} members - using standard processing`);
    }
    
    // Fetch and store all roles first
    try {
      await statusMessage.edit(`Member Database Import Status\n` +
                           `🔄 Fetching roles for ${guild.name}...`);
      
      const roleResult = await fetchAndStoreGuildRoles(guild);
      if (roleResult.success) {
        console.log(`Successfully stored ${roleResult.roleCount} roles for guild ${guild.name}`);
        await statusMessage.edit(`Member Database Import Status\n` +
                              `✅ Stored ${roleResult.roleCount} roles\n` +
                              `🔄 Now fetching members...`);
      } else {
        console.error('Error storing roles:', roleResult.error);
        await statusMessage.edit(`Member Database Import Status\n` +
                              `⚠️ Error storing roles: ${roleResult.error}\n` +
                              `🔄 Proceeding with member fetch...`);
      }
    } catch (roleError) {
      console.error('Error fetching roles:', roleError);
    }
    
    console.log(`Starting to fetch all members for guild ${guild.name} (${guild.id})`);
    
    let memberCount = 0;
    let roleCount = 0;
    
    try {
      await guild.members.fetch();
      console.log(`Fetched ${guild.members.cache.size} members from ${guild.name}`);
    } catch (error) {
      console.error(`Error fetching members for guild ${guild.name}:`, error);
      if (statusMessage) {
        await statusMessage.edit(`Member Database Import Status\n` +
                             `❌ Error fetching members: ${error.message}\n` +
                             `⚠️ Will proceed with ${guild.members.cache.size} cached members`);
      }
    }
    
    // Process members in parallel batches
    const members = Array.from(guild.members.cache.values());
    const batchSize = config.getConfig('memberBatchSize', 'MEMBER_BATCH_SIZE') || 100;
    const concurrentBatchCount = config.getConfig('concurrentBatches', 'CONCURRENT_BATCHES') || 5;
    
    // Process batches of members concurrently
    for (let i = 0; i < members.length; i += (batchSize * concurrentBatchCount)) {
      const batchPromises = [];
      
      for (let j = 0; j < concurrentBatchCount; j++) {
        const startIndex = i + (j * batchSize);
        if (startIndex >= members.length) break;
        
        const endIndex = Math.min(startIndex + batchSize, members.length);
        const currentBatch = members.slice(startIndex, endIndex);
        
        if (currentBatch.length > 0) {
          batchPromises.push(processMemberBatch(currentBatch));
        }
      }
      
      if (batchPromises.length > 0) {
        const batchResults = await Promise.all(batchPromises);
        
        // Sum up the results
        for (const result of batchResults) {
          memberCount += result.members;
          roleCount += result.roles;
        }
        
        // Update status message
        if (statusMessage) {
          await statusMessage.edit(`Member Database Import Status\n` +
                               `🔄 Processed ${memberCount}/${members.length} members with ${roleCount} roles...`);
        }
      }
    }
    
    // Final status update
    console.log(`Completed storing ${memberCount} members with ${roleCount} roles for guild ${guild.name}`);
    
    if (statusMessage) {
      await statusMessage.edit(`Member Database Import Status\n` +
                           `✅ Completed! Stored data for ${memberCount} members with ${roleCount} total roles`);
    }
    
    return {
      success: true,
      memberCount,
      roleCount
    };
  } catch (error) {
    console.error(`Error in fetchAndStoreMembersForGuild:`, error);
    if (statusMessage) {
      await statusMessage.edit(`Member Database Import Status\n` +
                           `❌ Error: ${error.message}`);
    }
    return {
      success: false,
      error: error.message
    };
  }
  
  // Helper function to process a batch of members
  async function processMemberBatch(memberBatch) {
    let batchMemberCount = 0;
    let batchRoleCount = 0;
    
    // Store all members first
    const memberInsertPromises = memberBatch.map(member => storeMemberInDb(member));
    await Promise.all(memberInsertPromises);
    batchMemberCount += memberBatch.length;
    
    // Group members for batch role processing
    const batchesOf50 = [];
    for (let i = 0; i < memberBatch.length; i += 50) {
      batchesOf50.push(memberBatch.slice(i, i + 50));
    }
    
    // Process roles in smaller batches
    for (const smallBatch of batchesOf50) {
      try {
        // Use the new batch processing function
        const roleResult = await storeMemberRolesInDbBatch(smallBatch);
        batchRoleCount += roleResult;
      } catch (error) {
        console.error('Error in batch role processing:', error);
        
        // Fallback to individual processing
        for (const member of smallBatch) {
          try {
            const roleResult = await storeMemberRolesInDb(member);
            batchRoleCount += roleResult;
          } catch (memberError) {
            console.error(`Error storing roles for member ${member.user.username}:`, memberError);
          }
        }
      }
    }
    
    return { members: batchMemberCount, roles: batchRoleCount };
  }
}

// Get formatted date time for logs
function getFormattedDateTime() {
  const now = new Date();
  return `${now.getUTCFullYear()}-${String(now.getUTCMonth() + 1).padStart(2, '0')}-${String(now.getUTCDate()).padStart(2, '0')} ${String(now.getUTCHours()).padStart(2, '0')}:${String(now.getUTCMinutes()).padStart(2, '0')}:${String(now.getUTCSeconds()).padStart(2, '0')}`;
}

// Initialize member tracking
function initializeMemberTracking(client) {
  console.log(`[${getFormattedDateTime()}] Member tracking initialized`);
  
  // Listen for guildMemberAdd events
  client.on('guildMemberAdd', async (member) => {
    try {
      // Only process if database is initialized for this guild
      const dbExists = monitor.checkDatabaseExists(member.guild);
      if (!dbExists) {
        console.log(`[${getFormattedDateTime()}] Skipping new member ${member.user.username}: no database for guild ${member.guild.name}`);
        return;
      }
      
      console.log(`[${getFormattedDateTime()}] New member detected: ${member.user.username} (${member.id})`);
      
      // Store the member in database
      await storeMemberInDb(member);
      
      // Store any initial roles
      await storeMemberRolesInDb(member);
      
    } catch (error) {
      console.error(`[${getFormattedDateTime()}] Error processing new member:`, error);
    }
  });
  
  // Listen for guildMemberRemove events
  client.on('guildMemberRemove', async (member) => {
    try {
      // Only process if database is initialized for this guild
      const dbExists = monitor.checkDatabaseExists(member.guild);
      if (!dbExists) {
        console.log(`[${getFormattedDateTime()}] Skipping member leave ${member.user.username}: no database for guild ${member.guild.name}`);
        return;
      }
      
      console.log(`[${getFormattedDateTime()}] Member left: ${member.user.username} (${member.id})`);
      
      // Mark member as having left the guild
      await markMemberLeftGuild(member.id, member.user.username);
      
    } catch (error) {
      console.error(`[${getFormattedDateTime()}] Error processing member leave:`, error);
    }
  });
  
  // Listen for guildMemberUpdate events to track role changes
  client.on('guildMemberUpdate', async (oldMember, newMember) => {
    try {
      // Only process if database is initialized for this guild
      const dbExists = monitor.checkDatabaseExists(newMember.guild);
      if (!dbExists) {
        return;
      }
      
      // Check for role changes
      const oldRoles = oldMember.roles.cache;
      const newRoles = newMember.roles.cache;
      
      // Find added roles (in new but not in old)
      for (const [roleId, role] of newRoles) {
        // Skip @everyone role
        if (roleId === newMember.guild.id) continue;
        
        if (!oldRoles.has(roleId)) {
          console.log(`[${getFormattedDateTime()}] Role added to ${newMember.user.username}: ${role.name}`);
          
          // Add to role history
          await addRoleHistoryEntry(newMember.id, roleId, role.name, 'added');
        }
      }
      
      // Find removed roles (in old but not in new)
      for (const [roleId, role] of oldRoles) {
        // Skip @everyone role
        if (roleId === newMember.guild.id) continue;
        
        if (!newRoles.has(roleId)) {
          console.log(`[${getFormattedDateTime()}] Role removed from ${newMember.user.username}: ${role.name}`);
          
          // Add to role history
          await addRoleHistoryEntry(newMember.id, roleId, role.name, 'removed');
        }
      }
      
      // Update member data in database with any changes in username/nickname
      if (oldMember.displayName !== newMember.displayName || 
          oldMember.user.username !== newMember.user.username) {
        await storeMemberInDb(newMember);
      }
      
      // Always update roles to ensure the database has the current state
      await storeMemberRolesInDb(newMember);
      
    } catch (error) {
      console.error(`[${getFormattedDateTime()}] Error processing member update:`, error);
    }
  });
  
  // Listen for roleCreate events
  client.on('roleCreate', async (role) => {
    try {
      // Only process if database is initialized for this guild
      const dbExists = monitor.checkDatabaseExists(role.guild);
      if (!dbExists) {
        console.log(`[${getFormattedDateTime()}] Skipping new role ${role.name}: no database for guild ${role.guild.name}`);
        return;
      }
      
      console.log(`[${getFormattedDateTime()}] New role created: ${role.name} (${role.id})`);
      
      // Store the role in database
      await storeRoleInDb(role);
      
    } catch (error) {
      console.error(`[${getFormattedDateTime()}] Error processing new role:`, error);
    }
  });
  
  // Listen for roleDelete events
  client.on('roleDelete', async (role) => {
    try {
      // Only process if database is initialized for this guild
      const dbExists = monitor.checkDatabaseExists(role.guild);
      if (!dbExists) {
        console.log(`[${getFormattedDateTime()}] Skipping role deletion ${role.name}: no database for guild ${role.guild.name}`);
        return;
      }
      
      console.log(`[${getFormattedDateTime()}] Role deleted: ${role.name} (${role.id})`);
      
      // Mark the role as deleted in database
      await markRoleDeleted(role);
      
    } catch (error) {
      console.error(`[${getFormattedDateTime()}] Error processing role deletion:`, error);
    }
  });
  
  // Listen for roleUpdate events
  client.on('roleUpdate', async (oldRole, newRole) => {
    try {
      // Only process if database is initialized for this guild
      const dbExists = monitor.checkDatabaseExists(newRole.guild);
      if (!dbExists) {
        console.log(`[${getFormattedDateTime()}] Skipping role update ${newRole.name}: no database for guild ${newRole.guild.name}`);
        return;
      }
      
      console.log(`[${getFormattedDateTime()}] Role updated: ${newRole.name} (${newRole.id})`);
      
      // Store the updated role in database
      await storeRoleInDb(newRole);
      
    } catch (error) {
      console.error(`[${getFormattedDateTime()}] Error processing role update:`, error);
    }
  });
}

// Export functions
// Export functions
module.exports = {
  initializeMemberTracking,
  initializeMemberDatabase,
  fetchAndStoreMembersForGuild,
  fetchMembersInChunks,
  storeMemberInDb,
  storeMemberRolesInDb,
  storeMemberRolesInDbBatch,
  addRoleHistoryEntry,
  markMemberLeftGuild,
  storeRoleInDb,
  markRoleDeleted,
  fetchAndStoreGuildRoles
};