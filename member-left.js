// member-left.js - Module to process left members based on message history
const monitor = require('./monitor');

/**
 * Identifies users from messages who are not present in the guild anymore
 * and adds them to the guild_members table with appropriate timestamps
 * @param {Object} guild - The Discord guild object
 */
async function processLeftMembers(guild) {
    console.log(`[${getFormattedDateTime()}] Processing left members for guild ${guild.name} (${guild.id})`);
    
    const db = monitor.getDatabase();
    if (!db) {
        console.error("Cannot process left members: Database not initialized");
        return { success: false, error: "Database not initialized" };
    }

    try {
        // Get all message authors from the database who aren't in guild_members
        const missingMembers = await findMissingMembers(db);
        console.log(`[${getFormattedDateTime()}] Found ${missingMembers.length} users in messages who might have left the guild`);

        let addedCount = 0;

        // Process each missing member
        for (const member of missingMembers) {
            try {
                // Check if the user is actually in the guild (just to be safe)
                let guildMember = null;
                try {
                    guildMember = await guild.members.fetch(member.authorId);
                } catch (fetchError) {
                    // If we can't fetch the member, they're likely not in the guild anymore
                    // This is expected for left members
                }

                // If the member is not in the guild, add them to the guild_members table as left
                if (!guildMember) {
                    // Get their first and last message timestamps for join/leave estimates
                    const memberTimestamps = await getMemberMessageTimestamps(db, member.authorId);
                    
                    if (!memberTimestamps) {
                        console.log(`[${getFormattedDateTime()}] Could not determine timestamps for ${member.authorUsername} (${member.authorId}), skipping`);
                        continue;
                    }

                    // Add member to guild_members table
                    const currentTime = Date.now();
                    await addLeftMemberToDb(
                        db, 
                        member.authorId, 
                        member.authorUsername, 
                        memberTimestamps.firstMessageTime, // Use first message as joinedTimestamp estimate
                        memberTimestamps.lastMessageTime,  // Use last message as leftTimestamp estimate
                        currentTime
                    );

                    // Add their roles to member_roles database table if we find any in messages
                    await addMemberRolesFromMessages(db, member.authorId);
                    
                    addedCount++;
                }
            } catch (memberError) {
                console.error(`[${getFormattedDateTime()}] Error processing member ${member.authorUsername} (${member.authorId}):`, memberError);
            }
        }

        console.log(`[${getFormattedDateTime()}] Added ${addedCount} left members to the database`);
        return { success: true, addedCount };
    } catch (error) {
        console.error(`[${getFormattedDateTime()}] Error processing left members:`, error);
        return { success: false, error: error.message };
    }
}

/**
 * Finds message authors who aren't in the guild_members table
 * @param {Object} db - Database connection
 * @returns {Array} Array of member objects with authorId and authorUsername
 */
function findMissingMembers(db) {
    return new Promise((resolve, reject) => {
        const sql = `
            SELECT DISTINCT authorId, authorUsername, authorBot
            FROM messages
            WHERE authorBot = 0
            AND authorId NOT IN (SELECT id FROM guild_members)
        `;

        db.all(sql, [], (err, rows) => {
            if (err) {
                console.error(`[${getFormattedDateTime()}] Error finding missing members:`, err);
                reject(err);
                return;
            }
            
            resolve(rows);
        });
    });
}

/**
 * Gets the first and last message timestamps for a member
 * @param {Object} db - Database connection
 * @param {String} memberId - Member ID
 * @returns {Object} Object with firstMessageTime and lastMessageTime
 */
function getMemberMessageTimestamps(db, memberId) {
    return new Promise((resolve, reject) => {
        const sql = `
            SELECT MIN(timestamp) as firstMessageTime, MAX(timestamp) as lastMessageTime
            FROM messages
            WHERE authorId = ?
        `;

        db.get(sql, [memberId], (err, row) => {
            if (err) {
                console.error(`[${getFormattedDateTime()}] Error getting message timestamps for ${memberId}:`, err);
                reject(err);
                return;
            }
            
            if (!row || !row.firstMessageTime || !row.lastMessageTime) {
                resolve(null);
                return;
            }
            
            resolve({
                firstMessageTime: row.firstMessageTime,
                lastMessageTime: row.lastMessageTime
            });
        });
    });
}

/**
 * Adds a left member to the guild_members database table
 * @param {Object} db - Database connection
 * @param {String} memberId - Member ID
 * @param {String} username - Member username
 * @param {Number} joinedTimestamp - Estimated join timestamp
 * @param {Number} leftTimestamp - Estimated leave timestamp
 * @param {Number} lastUpdated - Current timestamp for the lastUpdated field
 * @returns {Promise} Promise that resolves when the member is added
 */
function addLeftMemberToDb(db, memberId, username, joinedTimestamp, leftTimestamp, lastUpdated) {
    return new Promise((resolve, reject) => {
        const joinedAt = joinedTimestamp ? new Date(joinedTimestamp).toISOString() : null;
        
        const sql = `
            INSERT OR REPLACE INTO guild_members (
                id, username, displayName, avatarURL, joinedAt, joinedTimestamp,
                bot, lastUpdated, leftGuild, leftTimestamp
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        `;
        
        db.run(sql, [
            memberId,
            username,
            username, // Use username as displayName since we don't have nickname
            null,     // No avatar URL available
            joinedAt,
            joinedTimestamp,
            0,        // Not a bot (we filtered those out)
            lastUpdated,
            1,        // Marked as left guild
            leftTimestamp
        ], function(err) {
            if (err) {
                console.error(`[${getFormattedDateTime()}] Error adding left member ${username} (${memberId}) to database:`, err);
                reject(err);
                return;
            }
            
            console.log(`[${getFormattedDateTime()}] Added left member ${username} (${memberId}) to database (estimated join: ${joinedAt}, left: ${new Date(leftTimestamp).toISOString()})`);
            resolve(this.changes);
        });
    });
}

/**
 * Tries to find roles from message mentions to add to member_roles table
 * This is an estimate since we can only find roles that were mentioned
 * @param {Object} db - Database connection
 * @param {String} memberId - Member ID
 * @returns {Promise} Promise that resolves when roles are added
 */
async function addMemberRolesFromMessages(db, memberId) {
    try {
        // Find messages that mention the member
        const messages = await findMemberMentions(db, memberId);
        
        // Set to track roles we've already added
        const addedRoleIds = new Set();
        const currentTime = Date.now();
        let roleCount = 0;
        
        // Extract roles from mentions
        for (const message of messages) {
            try {
                // Try to parse mention_roles JSON data
                if (message.mention_roles) {
                    const roles = JSON.parse(message.mention_roles);
                    
                    // For each role mentioned
                    for (const roleId of roles) {
                        // Skip if we've already added this role
                        if (addedRoleIds.has(roleId)) continue;
                        
                        // Get role details
                        const role = await getRoleDetails(db, roleId);
                        if (!role) continue;
                        
                        // Add to member_roles table
                        await addRoleToMember(db, memberId, role, currentTime);
                        addedRoleIds.add(roleId);
                        roleCount++;
                    }
                }
            } catch (parseError) {
                console.error(`[${getFormattedDateTime()}] Error parsing role mentions for ${memberId}:`, parseError);
            }
        }
        
        console.log(`[${getFormattedDateTime()}] Added ${roleCount} roles for left member ${memberId} from message mentions`);
        return roleCount;
    } catch (error) {
        console.error(`[${getFormattedDateTime()}] Error adding roles from messages for ${memberId}:`, error);
        return 0;
    }
}

/**
 * Finds messages that mention a specific member
 * @param {Object} db - Database connection
 * @param {String} memberId - Member ID
 * @returns {Promise<Array>} Promise that resolves with an array of messages
 */
function findMemberMentions(db, memberId) {
    return new Promise((resolve, reject) => {
        const sql = `
            SELECT mentions, mention_roles
            FROM messages
            WHERE mentions LIKE ? AND mention_roles IS NOT NULL
            LIMIT 100
        `;
        
        db.all(sql, [`%${memberId}%`], (err, rows) => {
            if (err) {
                console.error(`[${getFormattedDateTime()}] Error finding messages mentioning ${memberId}:`, err);
                reject(err);
                return;
            }
            
            resolve(rows);
        });
    });
}

/**
 * Gets role details from the guild_roles table
 * @param {Object} db - Database connection
 * @param {String} roleId - Role ID
 * @returns {Promise<Object|null>} Promise that resolves with role details or null
 */
function getRoleDetails(db, roleId) {
    return new Promise((resolve, reject) => {
        const sql = `
            SELECT id, name, color, position
            FROM guild_roles
            WHERE id = ?
        `;
        
        db.get(sql, [roleId], (err, row) => {
            if (err) {
                console.error(`[${getFormattedDateTime()}] Error getting role details for ${roleId}:`, err);
                reject(err);
                return;
            }
            
            resolve(row);
        });
    });
}

/**
 * Adds a role to a member in the member_roles table
 * @param {Object} db - Database connection
 * @param {String} memberId - Member ID
 * @param {Object} role - Role details
 * @param {Number} timestamp - Current timestamp for addedAt field
 * @returns {Promise} Promise that resolves when the role is added
 */
function addRoleToMember(db, memberId, role, timestamp) {
    return new Promise((resolve, reject) => {
        const sql = `
            INSERT OR IGNORE INTO member_roles (
                memberId, roleId, roleName, roleColor, rolePosition, addedAt
            ) VALUES (?, ?, ?, ?, ?, ?)
        `;
        
        db.run(sql, [
            memberId,
            role.id,
            role.name,
            role.color,
            role.position,
            timestamp
        ], function(err) {
            if (err) {
                console.error(`[${getFormattedDateTime()}] Error adding role ${role.name} for member ${memberId}:`, err);
                reject(err);
                return;
            }
            
            if (this.changes > 0) {
                console.log(`[${getFormattedDateTime()}] Added role ${role.name} for left member ${memberId}`);
            }
            resolve(this.changes);
        });
    });
}

// Utility function for formatted date-time
function getFormattedDateTime() {
    const now = new Date();
    return `${now.getUTCFullYear()}-${String(now.getUTCMonth() + 1).padStart(2, '0')}-${String(now.getUTCDate()).padStart(2, '0')} ${String(now.getUTCHours()).padStart(2, '0')}:${String(now.getUTCMinutes()).padStart(2, '0')}:${String(now.getUTCSeconds()).padStart(2, '0')}`;
}

module.exports = {
    processLeftMembers
};