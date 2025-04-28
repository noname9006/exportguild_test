// member-left.js - Module to process left members based on message history
const monitor = require('./monitor');
const sqlite3 = require('sqlite3').verbose(); // Adding .verbose() for better error reporting

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
        // First ensure all required tables exist
        await ensureTablesExist(db);
        
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
        
        // Store processed members for role processing later
        const processedLeftMembers = [];
        
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
                    
                    const memberInfo = {
                        id: member.authorId,
                        username: member.authorUsername,
                        joinedTimestamp: timestamps.firstMessageTime,
                        leftTimestamp: timestamps.lastMessageTime
                    };
                    
                    membersToAdd.push(memberInfo);
                    processedLeftMembers.push(memberInfo); // Save for role processing
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
        
        // Process role information for newly added left members
        if (!skipRoles && processedLeftMembers.length > 0) {
            try {
                console.log(`[${getFormattedDateTime()}] Processing role information for ${processedLeftMembers.length} left members`);
                
                let totalRolesAdded = 0;
                let roleProcessCount = 0;
                
                // Use smaller batch size for role processing
                const roleBatchSize = 50;
                
                for (let i = 0; i < processedLeftMembers.length; i += roleBatchSize) {
                    const batch = processedLeftMembers.slice(i, i + roleBatchSize);
                    console.log(`[${getFormattedDateTime()}] Processing role batch ${Math.floor(i / roleBatchSize) + 1}/${Math.ceil(processedLeftMembers.length / roleBatchSize)} (${batch.length} members)`);
                    
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
                        roleProcessCount += batch.length;
                        
                    } catch (batchError) {
                        // Rollback on error
                        await rollbackTransaction(db);
                        console.error(`[${getFormattedDateTime()}] Role batch processing failed, rolled back:`, batchError);
                    }
                }
                
                console.log(`[${getFormattedDateTime()}] Successfully processed roles: added ${totalRolesAdded} roles for ${roleProcessCount} left members`);
                
            } catch (roleError) {
                console.error(`[${getFormattedDateTime()}] Exception processing roles for left members:`, roleError);
            }
        }
        
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
 * Check if database tables exist, creating them if needed
 * This ensures all required tables are available before trying to process left members
 * @param {Object} db - Database connection
 * @returns {Promise<Boolean>} Whether tables were created or already exist
 */
async function ensureTablesExist(db) {
    return new Promise((resolve, reject) => {
        // Check if the required tables exist
        db.get("SELECT COUNT(*) as tableCount FROM sqlite_master WHERE type='table' AND name IN ('guild_members', 'member_roles', 'role_history')", (err, row) => {
            if (err) {
                console.error(`[${getFormattedDateTime()}] Error checking for tables:`, err);
                reject(err);
                return;
            }
            
            // If all three tables exist, we're good
            if (row && row.tableCount === 3) {
                resolve(true);
                return;
            }
            
            // Import the necessary table creation function from member-tracker
            try {
                const memberTracker = require('./member-tracker');
                memberTracker.initializeMemberDatabase(db)
                    .then(() => {
                        console.log(`[${getFormattedDateTime()}] Member tracking tables created successfully`);
                        resolve(true);
                    })
                    .catch(error => {
                        console.error(`[${getFormattedDateTime()}] Error creating member tracking tables:`, error);
                        reject(error);
                    });
            } catch (error) {
                console.error(`[${getFormattedDateTime()}] Error importing member-tracker module:`, error);
                reject(error);
            }
        });
    });
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
    return new Promise((resolve, reject) => {
        // First ensure the message author index exists
        db.run(`CREATE INDEX IF NOT EXISTS idx_messages_author_id ON messages(authorId)`, (err) => {
            if (err) {
                console.error(`[${getFormattedDateTime()}] Error creating messages index:`, err);
                // Non-fatal, continue
            }
            
            // Then add index on leftGuild for faster queries
            db.run(`CREATE INDEX IF NOT EXISTS idx_guild_members_left ON guild_members(leftGuild)`, (err) => {
                if (err) {
                    console.error(`[${getFormattedDateTime()}] Error creating guild_members index:`, err);
                    // Non-fatal, continue
                }
                
                // Create index on role_history if it doesn't exist
                db.run(`CREATE INDEX IF NOT EXISTS idx_role_history_member_id ON role_history(memberId)`, (err) => {
                    if (err) {
                        console.error(`[${getFormattedDateTime()}] Error creating role_history index:`, err);
                        // Non-fatal, continue
                    }
                    
                    // Create index on member_roles if it doesn't exist
                    db.run(`CREATE INDEX IF NOT EXISTS idx_member_roles_member_id ON member_roles(memberId)`, (err) => {
                        if (err) {
                            console.error(`[${getFormattedDateTime()}] Error creating member_roles index:`, err);
                            // Non-fatal, continue
                        }
                        
                        // Create index on message mentions if it doesn't exist
                        db.run(`CREATE INDEX IF NOT EXISTS idx_messages_mentions ON messages(mention_roles) WHERE mention_roles IS NOT NULL AND mention_roles != '[]'`, (err) => {
                            if (err) {
                                console.error(`[${getFormattedDateTime()}] Error creating messages mentions index:`, err);
                                // Non-fatal, continue
                            }
                            
                            // Create additional indexes for possible complex queries
                            db.run(`CREATE INDEX IF NOT EXISTS idx_role_history_timestamp ON role_history(timestamp)`, (err) => {
                                if (err) {
                                    console.error(`[${getFormattedDateTime()}] Error creating role_history timestamp index:`, err);
                                    // Non-fatal, continue
                                }
                                
                                // Create index on member_roles addedAt for timestamp queries
                                db.run(`CREATE INDEX IF NOT EXISTS idx_member_roles_added_at ON member_roles(addedAt)`, (err) => {
                                    if (err) {
                                        console.error(`[${getFormattedDateTime()}] Error creating member_roles addedAt index:`, err);
                                        // Non-fatal, continue
                                    }
                                    
                                    resolve();
                                });
                            });
                        });
                    });
                });
            });
        });
    });
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
                    json_each.value as role_id,
                    MIN(m.timestamp) as first_mention_time
                FROM messages m, json_each(m.mention_roles)
                WHERE m.authorId IN (${placeholders})
                AND m.mention_roles IS NOT NULL
                AND m.mention_roles != '[]'
                GROUP BY m.authorId, json_each.value
                LIMIT 1000
            )
            SELECT 
                mm.authorId,
                r.id as roleId,
                r.name,
                r.color,
                r.position,
                mm.first_mention_time as timestamp
            FROM member_mentions mm
            JOIN guild_roles r ON mm.role_id = r.id
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
                    position: row.position,
                    timestamp: row.timestamp // Ensure timestamp is properly passed
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
                console.error(`[${getFormattedDateTime()}] Error finding members with role mentions:`, err);
                // Return empty array on error instead of rejecting
                resolve([]);
                return;
            }
            
            // Extract member IDs from the result
            const membersWithRoles = rows.map(row => row.authorId);
            resolve(membersWithRoles);
        });
    });
}

/**
 * Add multiple left members to the database in a single operation
 * @param {Object} db - Database connection
 * @param {Array} members - Array of member objects with id, username, etc.
 * @param {Number} currentTime - Current timestamp
 * @returns {Promise<Number>} Number of members added
 */
function addLeftMembersToBulk(db, members, currentTime) {
    return new Promise((resolve, reject) => {
        if (!members || members.length === 0) {
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
            const leftTimestamp = member.leftTimestamp || currentTime;
            
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
                leftTimestamp
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
                    const leftAt = m.leftTimestamp ? new Date(m.leftTimestamp).toISOString() : new Date(currentTime).toISOString();
                    console.log(`[${getFormattedDateTime()}] Added left member ${m.username} (${m.id}) to database (estimated join: ${joinedAt}, left: ${leftAt})`);
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
                    role.color || '#000000',
                    role.position || 0,
                    role.timestamp || timestamp // Use role's timestamp if available
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
                resolve(0); // Resolve with 0 on error to allow processing to continue
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
 */
function beginTransaction(db) {
    return new Promise((resolve, reject) => {
        db.run('BEGIN TRANSACTION', (err) => {
            if (err) reject(err);
            else resolve();
        });
    });
}

/**
 * Commits a database transaction
 * @param {Object} db - Database connection
 */
function commitTransaction(db) {
    return new Promise((resolve, reject) => {
        db.run('COMMIT', (err) => {
            if (err) reject(err);
            else resolve();
        });
    });
}

/**
 * Rolls back a database transaction
 * @param {Object} db - Database connection
 */
function rollbackTransaction(db) {
    return new Promise((resolve, reject) => {
        db.run('ROLLBACK', (err) => {
            if (err) reject(err);
            else resolve();
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
    return processLeftMembers(guild, 100, false); // Changed from true to false to process roles by default
}

// Export the module functions
module.exports = {
    processLeftMembers,
    processLeftMembersLegacy,
    ensureTablesExist
};