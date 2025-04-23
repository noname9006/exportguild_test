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
            
            console.log('Member tracking database tables initialized successfully');
            resolve(true);
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

// Fetch all members for a guild and store them in database (for exportguild command)
async function fetchAndStoreMembersForGuild(guild, statusMessage) {
  try {
    const db = monitor.getDatabase();
    if (!db) {
      console.error("Cannot fetch members: Database not initialized");
      return { success: false, error: "Database not initialized" };
    }
    
    // Update status message if provided
    if (statusMessage) {
      await statusMessage.edit(`Member Database Import Status\n` +
                           `🔄 Fetching members for ${guild.name}...`);
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
    
    // First ensure we have fetched all members for the guild
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
    
    // Process members in batches to avoid memory issues
    const members = Array.from(guild.members.cache.values());
    const batchSize = config.getConfig('memberBatchSize', 'MEMBER_BATCH_SIZE') || 100;
    let processedCount = 0;
    
    // Process in batches
    for (let i = 0; i < members.length; i += batchSize) {
      const batch = members.slice(i, i + batchSize);
      
      // Update status message for each batch
      if (statusMessage && i % 500 === 0) {
        await statusMessage.edit(`Member Database Import Status\n` +
                             `🔄 Processing ${i}/${members.length} members...`);
      }
      
      // Process each member in the batch
      for (const member of batch) {
        try {
          // Store member data
          await storeMemberInDb(member);
          memberCount++;
          
          // Store member roles
          const roleResult = await storeMemberRolesInDb(member);
          roleCount += roleResult;
          
          processedCount++;
        } catch (memberError) {
          console.error(`Error storing member ${member.user.username}:`, memberError);
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
module.exports = {
  initializeMemberTracking,
  initializeMemberDatabase,
  fetchAndStoreMembersForGuild,
  storeMemberInDb,
  storeMemberRolesInDb,
  addRoleHistoryEntry,
  markMemberLeftGuild,
  storeRoleInDb,
  markRoleDeleted,
  fetchAndStoreGuildRoles
};