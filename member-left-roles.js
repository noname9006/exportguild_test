// member-left-roles.js - Module to track roles of members who have left the guild
const monitor = require('./monitor');
const sqlite3 = require('sqlite3');

// Cache for roles to avoid repeated lookups
const roleCache = new Map();

/**
 * Processes role information for members who have left the guild
 * and adds their role history to the database with timestamps
 * @param {Object} guild - The Discord guild object
 * @param {Boolean} includeTimestamps - Whether to include timestamps of when roles were assigned (default: true)
 * @param {Boolean} includeRemovedRoles - Whether to include roles that were removed before leaving (default: false)
 * @returns {Promise<Object>} Processing result
 */
async function processLeftMemberRoles(guild, includeTimestamps = true, includeRemovedRoles = false) {
    console.log(`[${getFormattedDateTime()}] Processing roles for left members in guild ${guild.name} (${guild.id})`);
    
    const db = monitor.getDatabase();
    if (!db) {
        console.error("Cannot process left member roles: Database not initialized");
        return { success: false, error: "Database not initialized" };
    }
    
    try {
        // Ensure indexes exist for better performance
        await ensureDatabaseIndexes(db);
        
        // Find members who have left but don't have role history recorded
        const leftMembers = await findLeftMembersWithoutRoleHistory(db);
        console.log(`[${getFormattedDateTime()}] Found ${leftMembers.length} left members needing role history processing`);
        
        if (leftMembers.length === 0) {
            return { success: true, processedCount: 0, message: "No left members require role history processing" };
        }
        
        let totalRolesAdded = 0;
        let membersProcessed = 0;
        const batchSize = 50;
        
        // Process in batches to avoid memory issues with large datasets
        for (let i = 0; i < leftMembers.length; i += batchSize) {
            const batch = leftMembers.slice(i, i + batchSize);
            console.log(`[${getFormattedDateTime()}] Processing batch ${Math.floor(i / batchSize) + 1}/${Math.ceil(leftMembers.length / batchSize)} (${batch.length} members)`);
            
            try {
                // Begin transaction for this batch
                await beginTransaction(db);
                
                for (const member of batch) {
                    try {
                        // Get role history for this member
                        const roleHistory = await getMemberRoleHistory(db, member.id);
                        
                        // Get member roles from mentions in messages
                        const rolesFromMentions = await getRolesFromMentions(db, member.id);
                        
                        // Combine all unique roles
                        const allRoles = mergeRoleData(roleHistory, rolesFromMentions);
                        
                        if (allRoles.length > 0) {
                            // Store role history in database
                            const addedCount = await storeLeftMemberRoles(db, member.id, allRoles, member.leftTimestamp);
                            totalRolesAdded += addedCount;
                            
                            console.log(`[${getFormattedDateTime()}] Added ${addedCount} roles for left member ${member.username} (${member.id})`);
                        } else {
                            console.log(`[${getFormattedDateTime()}] No role data found for member ${member.username} (${member.id})`);
                        }
                    } catch (memberError) {
                        console.error(`[${getFormattedDateTime()}] Error processing left member roles for ${member.id}:`, memberError);
                    }
                }
                
                // Commit transaction
                await commitTransaction(db);
                membersProcessed += batch.length;
                
                console.log(`[${getFormattedDateTime()}] Completed processing batch: ${batch.length} members processed with ${totalRolesAdded} total roles`);
            } catch (batchError) {
                // Rollback on error
                await rollbackTransaction(db);
                console.error(`[${getFormattedDateTime()}] Batch processing failed, rolled back:`, batchError);
            }
        }
        
        console.log(`[${getFormattedDateTime()}] Completed processing ${membersProcessed} left members with ${totalRolesAdded} total role entries`);
        
        return {
            success: true,
            processedCount: membersProcessed,
            roleCount: totalRolesAdded
        };
    } catch (error) {
        console.error(`[${getFormattedDateTime()}] Error processing left member roles:`, error);
        return { success: false, error: error.message };
    }
}

/**
 * Processes role information for members who have just been added to the database as left members
 * This is designed to be called immediately after left members are processed
 * @param {Object} guild - The Discord guild object
 * @param {Array} processedMembers - Array of member objects that were just processed as left
 * @returns {Promise<Object>} Processing result
 */
async function processNewlyLeftMemberRoles(guild, processedMembers) {
    console.log(`[${getFormattedDateTime()}] Processing roles for ${processedMembers.length} newly added left members in guild ${guild.name}`);
    
    const db = monitor.getDatabase();
    if (!db) {
        console.error("Cannot process left member roles: Database not initialized");
        return { success: false, error: "Database not initialized" };
    }
    
    if (!processedMembers || processedMembers.length === 0) {
        return { success: true, processedCount: 0, message: "No newly left members to process" };
    }
    
    try {
        // Ensure indexes exist for better performance
        await ensureDatabaseIndexes(db);
        
        let totalRolesAdded = 0;
        let membersProcessed = 0;
        const batchSize = 50;
        
        // Process in batches to avoid memory issues with large datasets
        for (let i = 0; i < processedMembers.length; i += batchSize) {
            const batch = processedMembers.slice(i, i + batchSize);
            console.log(`[${getFormattedDateTime()}] Processing batch ${Math.floor(i / batchSize) + 1}/${Math.ceil(processedMembers.length / batchSize)} (${batch.length} members)`);
            
            try {
                // Begin transaction for this batch
                await beginTransaction(db);
                
                for (const member of batch) {
                    try {
                        // Get role history for this member
                        const roleHistory = await getMemberRoleHistory(db, member.id);
                        
                        // Get member roles from mentions in messages
                        const rolesFromMentions = await getRolesFromMentions(db, member.id);
                        
                        // Combine all unique roles
                        const allRoles = mergeRoleData(roleHistory, rolesFromMentions);
                        
                        if (allRoles.length > 0) {
                            // Store role history in database
                            const addedCount = await storeLeftMemberRoles(db, member.id, allRoles, member.leftTimestamp);
                            totalRolesAdded += addedCount;
                            
                            console.log(`[${getFormattedDateTime()}] Added ${addedCount} roles for newly left member ${member.username} (${member.id})`);
                        } else {
                            console.log(`[${getFormattedDateTime()}] No role data found for member ${member.username} (${member.id})`);
                        }
                    } catch (memberError) {
                        console.error(`[${getFormattedDateTime()}] Error processing left member roles for ${member.id}:`, memberError);
                    }
                }
                
                // Commit transaction
                await commitTransaction(db);
                membersProcessed += batch.length;
                
                console.log(`[${getFormattedDateTime()}] Completed processing batch: ${batch.length} members processed with ${totalRolesAdded} total roles`);
            } catch (batchError) {
                // Rollback on error
                await rollbackTransaction(db);
                console.error(`[${getFormattedDateTime()}] Batch processing failed, rolled back:`, batchError);
            }
        }
        
        console.log(`[${getFormattedDateTime()}] Completed processing ${membersProcessed} newly left members with ${totalRolesAdded} total role entries`);
        
        return {
            success: true,
            processedCount: membersProcessed,
            roleCount: totalRolesAdded
        };
    } catch (error) {
        console.error(`[${getFormattedDateTime()}] Error processing newly left member roles:`, error);
        return { success: false, error: error.message };
    }
}

/**
 * Ensures necessary database indexes exist
 * @param {Object} db - Database connection
 */
async function ensureDatabaseIndexes(db) {
    return new Promise((resolve, reject) => {
        // Create index on role_history if it doesn't exist
        db.run(`CREATE INDEX IF NOT EXISTS idx_role_history_member_id ON role_history(memberId)`, (err) => {
            if (err) {
                console.error(`[${getFormattedDateTime()}] Error creating role_history index:`, err);
                // Non-fatal, continue
            }
            
            // Create index on member_roles if it doesn't exist
            db.run(`CREATE INDEX IF NOT EXISTS idx_member_roles_left_member ON member_roles(memberId)`, (err) => {
                if (err) {
                    console.error(`[${getFormattedDateTime()}] Error creating member_roles index:`, err);
                    // Non-fatal, continue
                }
                resolve();
            });
        });
    });
}

/**
 * Find members who have left but don't have role history recorded
 * @param {Object} db - Database connection
 * @returns {Promise<Array>} Array of members who have left
 */
async function findLeftMembersWithoutRoleHistory(db) {
    return new Promise((resolve, reject) => {
        // Find members who have left (leftGuild=1) but don't have entries in member_roles
        const sql = `
            SELECT m.id, m.username, m.leftTimestamp
            FROM guild_members m
            WHERE m.leftGuild = 1
            AND m.id NOT IN (
                SELECT DISTINCT memberId FROM member_roles
            )
            LIMIT 1000
        `;
        
        db.all(sql, [], (err, rows) => {
            if (err) {
                console.error(`[${getFormattedDateTime()}] Error finding left members without role history:`, err);
                reject(err);
                return;
            }
            
            resolve(rows);
        });
    });
}

/**
 * Get role history entries for a member
 * @param {Object} db - Database connection
 * @param {String} memberId - The ID of the member
 * @returns {Promise<Array>} Array of role history entries
 */
async function getMemberRoleHistory(db, memberId) {
    return new Promise((resolve, reject) => {
        const sql = `
            SELECT roleId, roleName, action, timestamp
            FROM role_history
            WHERE memberId = ?
            ORDER BY timestamp ASC
        `;
        
        db.all(sql, [memberId], (err, rows) => {
            if (err) {
                console.error(`[${getFormattedDateTime()}] Error getting role history for member ${memberId}:`, err);
                resolve([]);
                return;
            }
            
            resolve(rows);
        });
    });
}

/**
 * Get roles from message mentions for a member who has left
 * @param {Object} db - Database connection
 * @param {String} memberId - The ID of the member
 * @returns {Promise<Array>} Array of role objects derived from mentions
 */
async function getRolesFromMentions(db, memberId) {
    return new Promise((resolve, reject) => {
        // This query finds role IDs from message mentions and joins with guild_roles
        const sql = `
            WITH member_mentions AS (
                SELECT DISTINCT
                    m.authorId,
                    json_each.value as role_id,
                    MIN(m.timestamp) as first_mention_time
                FROM messages m, json_each(m.mention_roles)
                WHERE m.authorId = ?
                AND m.mention_roles IS NOT NULL
                AND m.mention_roles != '[]'
                GROUP BY m.authorId, json_each.value
                LIMIT 1000
            )
            SELECT 
                mm.authorId,
                r.id as roleId,
                r.name as roleName,
                r.color as roleColor,
                r.position as rolePosition,
                mm.first_mention_time as timestamp
            FROM member_mentions mm
            JOIN guild_roles r ON mm.role_id = r.id
        `;
        
        db.all(sql, [memberId], (err, rows) => {
            if (err) {
                console.error(`[${getFormattedDateTime()}] Error getting roles from mentions for member ${memberId}:`, err);
                resolve([]);
                return;
            }
            
            resolve(rows);
        });
    });
}

/**
 * Merge role data from different sources, prioritizing timing information
 * @param {Array} roleHistory - Role history entries
 * @param {Array} mentionRoles - Roles from message mentions
 * @returns {Array} Merged unique role data with timestamps
 */
function mergeRoleData(roleHistory, mentionRoles) {
    // Map to track roles by ID and their earliest timestamp
    const roleMap = new Map();
    
    // Process role history - only use 'added' actions
    roleHistory.forEach(entry => {
        if (entry.action === 'added') {
            if (!roleMap.has(entry.roleId) || entry.timestamp < roleMap.get(entry.roleId).timestamp) {
                roleMap.set(entry.roleId, {
                    roleId: entry.roleId,
                    roleName: entry.roleName,
                    timestamp: entry.timestamp,
                    source: 'role_history'
                });
            }
        }
    });
    
    // Process mention roles - only add if it's earlier than what we have
    mentionRoles.forEach(role => {
        if (!roleMap.has(role.roleId) || role.timestamp < roleMap.get(role.roleId).timestamp) {
            roleMap.set(role.roleId, {
                roleId: role.roleId,
                roleName: role.roleName,
                roleColor: role.roleColor,
                rolePosition: role.rolePosition,
                timestamp: role.timestamp,
                source: 'message_mention'
            });
        }
    });
    
    // Convert map back to array
    return Array.from(roleMap.values());
}

/**
 * Store role data for a member who has left
 * @param {Object} db - Database connection
 * @param {String} memberId - The ID of the member
 * @param {Array} roles - Array of role objects with timestamps
 * @param {Number} leftTimestamp - When the member left
 * @returns {Promise<Number>} Number of roles added
 */
async function storeLeftMemberRoles(db, memberId, roles, leftTimestamp) {
    return new Promise((resolve, reject) => {
        if (!roles || roles.length === 0) {
            resolve(0);
            return;
        }
        
        // Build SQL statement for bulk insert
        let sql = `
            INSERT OR IGNORE INTO member_roles 
            (memberId, roleId, roleName, roleColor, rolePosition, addedAt)
            VALUES 
        `;
        
        const values = [];
        const params = [];
        
        roles.forEach(role => {
            values.push(`(?, ?, ?, ?, ?, ?)`);
            params.push(
                memberId,
                role.roleId,
                role.roleName,
                role.roleColor || '#000000',
                role.rolePosition || 0,
                role.timestamp
            );
        });
        
        sql += values.join(', ');
        
        db.run(sql, params, function(err) {
            if (err) {
                console.error(`[${getFormattedDateTime()}] Error storing roles for left member ${memberId}:`, err);
                reject(err);
                return;
            }
            
            console.log(`[${getFormattedDateTime()}] Added ${this.changes} role entries for left member ${memberId}`);
            resolve(this.changes);
        });
    });
}

/**
 * Begin a database transaction
 * @param {Object} db - Database connection
 */
function beginTransaction(db) {
    return new Promise((resolve, reject) => {
        db.run('BEGIN TRANSACTION', function(err) {
            if (err) {
                reject(err);
            } else {
                resolve();
            }
        });
    });
}

/**
 * Commit a database transaction
 * @param {Object} db - Database connection
 */
function commitTransaction(db) {
    return new Promise((resolve, reject) => {
        db.run('COMMIT', function(err) {
            if (err) {
                reject(err);
            } else {
                resolve();
            }
        });
    });
}

/**
 * Rollback a database transaction
 * @param {Object} db - Database connection
 */
function rollbackTransaction(db) {
    return new Promise((resolve, reject) => {
        db.run('ROLLBACK', function(err) {
            if (err) {
                reject(err);
            } else {
                resolve();
            }
        });
    });
}

/**
 * Get formatted date-time string for logging
 * @returns {String} Formatted date-time string
 */
function getFormattedDateTime() {
    const now = new Date();
    return `${now.getUTCFullYear()}-${String(now.getUTCMonth() + 1).padStart(2, '0')}-${String(now.getUTCDate()).padStart(2, '0')} ${String(now.getUTCHours()).padStart(2, '0')}:${String(now.getUTCMinutes()).padStart(2, '0')}:${String(now.getUTCSeconds()).padStart(2, '0')}`;
}

// Export module functions
module.exports = {
    processLeftMemberRoles,
    processNewlyLeftMemberRoles
};