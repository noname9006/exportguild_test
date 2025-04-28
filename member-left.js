// member-left.js - Module to process left members based on message history
const monitor = require('./monitor');
const sqlite3 = require('sqlite3');

// Cache for roles to avoid repeated lookups
const roleCache = new Map();

// Prepared statements
let addMemberStmt = null;
let addRoleStmt = null;

/**
 * Identifies users from messages who are not present in the guild anymore
 * and adds them to the guild_members table with appropriate timestamps
 * @param {Object} guild - The Discord guild object
 * @param {Number} batchSize - Number of members to process in parallel (default: 100)
 * @param {Boolean} skipRoles - Skip role processing entirely (default: true)
 */
async function processLeftMembers(guild, batchSize = 100, skipRoles = true) {
    console.log(`[${getFormattedDateTime()}] Processing left members for guild ${guild.name} (${guild.id})`);
    
    const db = monitor.getDatabase();
    if (!db) {
        console.error("Cannot process left members: Database not initialized");
        return { success: false, error: "Database not initialized" };
    }

    try {
        // Prepare statements for reuse
        prepareStatements(db);
        
        // Add required indexes if they don't exist
        await ensureDatabaseIndexes(db);
        
        // Get all message authors not in guild_members
        const startTime = Date.now();
        const missingMembers = await findMissingMembers(db);
        console.log(`[${getFormattedDateTime()}] Found ${missingMembers.length} users in messages who might have left the guild (query took ${Date.now() - startTime}ms)`);

        // Process in larger batches
        let addedCount = 0;
        let processedCount = 0;
        let errorCount = 0;
        const currentTime = Date.now();
        
        // Process in chunks of batchSize
        for (let i = 0; i < missingMembers.length; i += batchSize) {
            const batchStartTime = Date.now();
            const memberBatch = missingMembers.slice(i, i + batchSize);
            const memberIds = memberBatch.map(m => m.authorId);
            
            console.log(`[${getFormattedDateTime()}] Processing batch ${Math.floor(i / batchSize) + 1}/${Math.ceil(missingMembers.length / batchSize)} (${memberBatch.length} members)`);
            
            try {
                // Begin transaction
                await beginTransaction(db);
                
                // Preload all timestamp data in a single query
                const timestampsMap = await preloadMemberTimestamps(db, memberIds);
                
                // Prepare members for bulk insert
                const membersToAdd = [];
                
                for (const member of memberBatch) {
                    const timestamps = timestampsMap[member.authorId];
                    if (!timestamps) continue;
                    
                    membersToAdd.push({
                        id: member.authorId,
                        username: member.authorUsername,
                        joinedTimestamp: timestamps.firstMessageTime,
                        leftTimestamp: timestamps.lastMessageTime
                    });
                }
                
                // Bulk add members
                const result = await addLeftMembersToBulk(db, membersToAdd, currentTime);
                addedCount += result;
                processedCount += memberBatch.length;
                
                // Skip role processing if requested
                if (!skipRoles) {
                    // Preload all role data for the batch
                    const rolesMap = await preloadRolesForMembers(db, memberIds);
                    await bulkAddRolesToMembers(db, rolesMap, currentTime);
                }
                
                // Commit transaction
                await commitTransaction(db);
                
                const batchTime = Date.now() - batchStartTime;
                console.log(`[${getFormattedDateTime()}] Batch completed in ${batchTime}ms: ${result}/${memberBatch.length} members added successfully (${(batchTime / memberBatch.length).toFixed(2)}ms per member)`);
            } catch (batchError) {
                // Rollback on error
                await rollbackTransaction(db);
                console.error(`[${getFormattedDateTime()}] Batch failed, rolled back:`, batchError);
                errorCount += memberBatch.length;
            }
        }
        
        console.log(`[${getFormattedDateTime()}] Added ${addedCount}/${processedCount} left members to database (${errorCount} errors) in ${(Date.now() - startTime)/1000} seconds`);
        return { success: true, addedCount, errorCount };
    } catch (error) {
        console.error(`[${getFormattedDateTime()}] Error processing left members:`, error);
        return { success: false, error: error.message };
    } finally {
        // Clean up prepared statements
        finalizeStatements();
    }
}

/**
 * Prepare SQLite statements for reuse
 */
function prepareStatements(db) {
    try {
        if (addMemberStmt === null) {
            addMemberStmt = db.prepare(`
                INSERT OR REPLACE INTO guild_members (
                    id, username, displayName, avatarURL, joinedAt, joinedTimestamp,
                    bot, lastUpdated, leftGuild, leftTimestamp
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            `);
        }
        
        if (addRoleStmt === null) {
            addRoleStmt = db.prepare(`
                INSERT OR IGNORE INTO member_roles (
                    memberId, roleId, roleName, roleColor, rolePosition, addedAt
                ) VALUES (?, ?, ?, ?, ?, ?)
            `);
        }
    } catch (error) {
        console.error(`[${getFormattedDateTime()}] Error preparing statements:`, error);
    }
}

/**
 * Clean up prepared statements
 */
function finalizeStatements() {
    try {
        if (addMemberStmt) {
            addMemberStmt.finalize();
            addMemberStmt = null;
        }
        if (addRoleStmt) {
            addRoleStmt.finalize();
            addRoleStmt = null;
        }
    } catch (error) {
        console.error(`[${getFormattedDateTime()}] Error finalizing statements:`, error);
    }
}

/**
 * Ensures necessary database indexes exist
 * @param {Object} db - Database connection
 */
async function ensureDatabaseIndexes(db) {
    const indexes = [
        "CREATE INDEX IF NOT EXISTS idx_messages_authorId ON messages(authorId)",
        "CREATE INDEX IF NOT EXISTS idx_messages_mentions ON messages(mentions)",
        "CREATE INDEX IF NOT EXISTS idx_guild_roles_id ON guild_roles(id)",
        "CREATE INDEX IF NOT EXISTS idx_member_roles_member_role ON member_roles(memberId, roleId)"
    ];
    
    for (const indexSql of indexes) {
        await new Promise((resolve, reject) => {
            db.run(indexSql, err => {
                if (err) {
                    console.warn(`[${getFormattedDateTime()}] Could not create index: ${err.message}`);
                }
                resolve();
            });
        });
    }
}

/**
 * Preload member message timestamps in a single query for a batch
 * @param {Object} db - Database connection
 * @param {Array} memberIds - Array of member IDs
 * @returns {Object} Map of member IDs to timestamp objects
 */
async function preloadMemberTimestamps(db, memberIds) {
    return new Promise((resolve, reject) => {
        if (!memberIds.length) {
            resolve({});
            return;
        }
        
        const placeholders = memberIds.map(() => '?').join(',');
        const sql = `
            SELECT authorId,
                MIN(timestamp) as firstMessageTime,
                MAX(timestamp) as lastMessageTime
            FROM messages
            WHERE authorId IN (${placeholders})
            GROUP BY authorId
        `;
        
        db.all(sql, memberIds, (err, rows) => {
            if (err) {
                console.error(`[${getFormattedDateTime()}] Error preloading timestamps:`, err);
                resolve({});
                return;
            }
            
            const result = {};
            rows.forEach(row => {
                result[row.authorId] = {
                    firstMessageTime: row.firstMessageTime,
                    lastMessageTime: row.lastMessageTime
                };
            });
            
            resolve(result);
        });
    });
}

/**
 * Preload role information for a batch of members
 * @param {Object} db - Database connection
 * @param {Array} memberIds - Array of member IDs
 * @returns {Object} Map of member IDs to arrays of role objects
 */
async function preloadRolesForMembers(db, memberIds) {
    if (!memberIds.length) {
        return {};
    }
    
    // First check which members have role mentions
    const membersWithRoles = await findMembersWithRoleMentions(db, memberIds);
    if (membersWithRoles.length === 0) {
        return {};
    }
    
    // Only query roles for members that have them
    const placeholders = membersWithRoles.map(() => '?').join(',');
    
    return new Promise((resolve, reject) => {
        // This query finds role IDs from message mentions and joins with guild_roles
        // Using a special syntax to extract from JSON arrays in SQLite
        const sql = `
            WITH member_mentions AS (
                SELECT DISTINCT
                    m.authorId,
                    json_each.value as role_id
                FROM messages m, json_each(m.mention_roles)
                WHERE m.authorId IN (${placeholders})
                AND m.mention_roles IS NOT NULL
                AND m.mention_roles != '[]'
                LIMIT 1000
            )
            SELECT 
                mm.authorId,
                r.id as roleId,
                r.name,
                r.color,
                r.position
            FROM member_mentions mm
            JOIN guild_roles r ON mm.role_id = r.id
            GROUP BY mm.authorId, r.id
        `;
        
        db.all(sql, membersWithRoles, (err, rows) => {
            if (err) {
                console.error(`[${getFormattedDateTime()}] Error preloading roles:`, err);
                resolve({});
                return;
            }
            
            const rolesByMember = {};
            rows.forEach(row => {
                if (!rolesByMember[row.authorId]) {
                    rolesByMember[row.authorId] = [];
                }
                rolesByMember[row.authorId].push({
                    id: row.roleId,
                    name: row.name,
                    color: row.color,
                    position: row.position
                });
            });
            
            resolve(rolesByMember);
        });
    });
}

/**
 * Find which members have role mentions in their messages
 * @param {Object} db - Database connection
 * @param {Array} memberIds - Array of member IDs
 * @returns {Array} Array of member IDs that have role mentions
 */
async function findMembersWithRoleMentions(db, memberIds) {
    return new Promise((resolve, reject) => {
        const placeholders = memberIds.map(() => '?').join(',');
        const sql = `
            SELECT DISTINCT authorId
            FROM messages 
            WHERE authorId IN (${placeholders})
            AND mention_roles IS NOT NULL 
            AND mention_roles != '[]'
        `;
        
        db.all(sql, memberIds, (err, rows) => {
            if (err) {
                resolve([]);
                return;
            }
            
            resolve(rows.map(r => r.authorId));
        });
    });
}

/**
 * Add multiple members to the database in a single operation
 * @param {Object} db - Database connection
 * @param {Array} members - Array of member objects with id, username, etc.
 * @param {Number} currentTime - Current timestamp
 * @returns {Promise<Number>} Number of members added
 */
function addLeftMembersToBulk(db, members, currentTime) {
    return new Promise((resolve, reject) => {
        if (members.length === 0) {
            resolve(0);
            return;
        }
        
        // Create a single SQL statement with multiple VALUES clauses
        let sql = `
            INSERT OR REPLACE INTO guild_members 
            (id, username, displayName, avatarURL, joinedAt, joinedTimestamp, bot, lastUpdated, leftGuild, leftTimestamp)
            VALUES `;
        
        const values = [];
        const params = [];
        
        members.forEach(member => {
            const joinedAt = member.joinedTimestamp ? new Date(member.joinedTimestamp).toISOString() : null;
            
            values.push(`(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`);
            params.push(
                member.id,
                member.username,
                member.username,
                null,
                joinedAt,
                member.joinedTimestamp,
                0,
                currentTime,
                1,
                member.leftTimestamp
            );
        });
        
        sql += values.join(', ');
        
        db.run(sql, params, function(err) {
            if (err) {
                console.error(`[${getFormattedDateTime()}] Error bulk adding ${members.length} left members:`, err);
                reject(err);
                return;
            }
            
            // Log added members in batches to reduce console output
            if (members.length > 20) {
                console.log(`[${getFormattedDateTime()}] Bulk added ${this.changes} left members to database`);
            } else {
                members.forEach(m => {
                    const joinedAt = m.joinedTimestamp ? new Date(m.joinedTimestamp).toISOString() : null;
                    console.log(`[${getFormattedDateTime()}] Added left member ${m.username} (${m.id}) to database (estimated join: ${joinedAt}, left: ${new Date(m.leftTimestamp).toISOString()})`);
                });
            }
            
            resolve(this.changes || members.length);
        });
    });
}

/**
 * Add roles to members in bulk
 * @param {Object} db - Database connection
 * @param {Object} rolesByMember - Map of member IDs to arrays of role objects
 * @param {Number} timestamp - Current timestamp
 * @returns {Promise<Number>} Number of roles added
 */
async function bulkAddRolesToMembers(db, rolesByMember, timestamp) {
    return new Promise((resolve, reject) => {
        const memberIds = Object.keys(rolesByMember);
        if (memberIds.length === 0) {
            resolve(0);
            return;
        }
        
        let sql = `
            INSERT OR IGNORE INTO member_roles 
            (memberId, roleId, roleName, roleColor, rolePosition, addedAt)
            VALUES `;
        
        const values = [];
        const params = [];
        
        let totalRoles = 0;
        
        memberIds.forEach(memberId => {
            const roles = rolesByMember[memberId] || [];
            totalRoles += roles.length;
            
            roles.forEach(role => {
                values.push(`(?, ?, ?, ?, ?, ?)`);
                params.push(
                    memberId,
                    role.id,
                    role.name,
                    role.color,
                    role.position,
                    timestamp
                );
            });
        });
        
        if (values.length === 0) {
            resolve(0);
            return;
        }
        
        sql += values.join(', ');
        
        db.run(sql, params, function(err) {
            if (err) {
                console.error(`[${getFormattedDateTime()}] Error bulk adding roles:`, err);
                resolve(0);
				                return;
            }
            
            console.log(`[${getFormattedDateTime()}] Bulk added ${this.changes} roles for ${memberIds.length} members`);
            resolve(this.changes);
        });
    });
}

/**
 * Finds message authors who aren't in the guild_members table
 * @param {Object} db - Database connection
 * @returns {Array} Array of member objects with authorId and authorUsername
 */
function findMissingMembers(db) {
    return new Promise((resolve, reject) => {
        // Optimized query with limit and filter for non-bot users
        const sql = `
            SELECT DISTINCT authorId, authorUsername, authorBot
            FROM messages
            WHERE authorBot = 0
            AND authorId NOT IN (
                SELECT id FROM guild_members
            )
            LIMIT 10000
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
 * Begins a database transaction
 * @param {Object} db - Database connection
 * @returns {Promise} Promise that resolves when transaction begins
 */
function beginTransaction(db) {
    return new Promise((resolve, reject) => {
        db.run("BEGIN TRANSACTION", (err) => {
            if (err) reject(err);
            else resolve();
        });
    });
}

/**
 * Commits a database transaction
 * @param {Object} db - Database connection
 * @returns {Promise} Promise that resolves when transaction commits
 */
function commitTransaction(db) {
    return new Promise((resolve, reject) => {
        db.run("COMMIT", (err) => {
            if (err) reject(err);
            else resolve();
        });
    });
}

/**
 * Rolls back a database transaction
 * @param {Object} db - Database connection
 * @returns {Promise} Promise that resolves when transaction rolls back
 */
function rollbackTransaction(db) {
    return new Promise((resolve, reject) => {
        db.run("ROLLBACK", (err) => {
            if (err) reject(err);
            else resolve();
        });
    });
}

/**
 * Gets role details from the guild_roles table
 * Uses an in-memory cache to avoid repeated lookups
 * @param {Object} db - Database connection
 * @param {String} roleId - Role ID
 * @returns {Promise<Object|null>} Promise that resolves with role details or null
 */
function getRoleDetails(db, roleId) {
    // Check cache first
    if (roleCache.has(roleId)) {
        return Promise.resolve(roleCache.get(roleId));
    }
    
    return new Promise((resolve, reject) => {
        const sql = `
            SELECT id, name, color, position
            FROM guild_roles
            WHERE id = ?
        `;
        
        db.get(sql, [roleId], (err, row) => {
            if (err) {
                resolve(null);
                return;
            }
            
            if (row) {
                roleCache.set(roleId, row);
            }
            resolve(row);
        });
    });
}

/**
 * Utility function to get formatted date-time string
 * @returns {String} Formatted date-time string
 */
function getFormattedDateTime() {
    const now = new Date();
    return `${now.getUTCFullYear()}-${String(now.getUTCMonth() + 1).padStart(2, '0')}-${String(now.getUTCDate()).padStart(2, '0')} ${String(now.getUTCHours()).padStart(2, '0')}:${String(now.getUTCMinutes()).padStart(2, '0')}:${String(now.getUTCSeconds()).padStart(2, '0')}`;
}

/**
 * For backward compatibility - simple wrapper around processLeftMembers
 * @deprecated Use processLeftMembers with explicit parameters instead
 * @param {Object} guild - The Discord guild object
 * @returns {Promise<Object>} Processing result
 */
async function processLeftMembersLegacy(guild) {
    return processLeftMembers(guild, 100, true);
}

// Export the module functions
module.exports = {
    processLeftMembers,
    processLeftMembersLegacy
};