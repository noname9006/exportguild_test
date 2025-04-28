// member-left.js - Module to process left members based on message history
const monitor = require('./monitor');

/**
 * Identifies users from messages who are not present in the guild anymore
 * and adds them to the guild_members table with appropriate timestamps
 * @param {Object} guild - The Discord guild object
 */
async function processLeftMembers(guild) {
    console.log(`========================================`);
    console.log(`[${getFormattedDateTime()}] START: Processing left members for guild ${guild.name} (${guild.id})`);
    console.log(`[${getFormattedDateTime()}] Database connection available: ${monitor.getDatabase() !== null}`);
    console.log(`========================================`);
    
    const db = monitor.getDatabase();
    if (!db) {
        console.error("Cannot process left members: Database not initialized");
        return { success: false, error: "Database not initialized" };
    }

    try {
        console.log(`[${getFormattedDateTime()}] Finding missing members - this may take some time...`);
        
        // Use a single SQL query to get all missing members with their message timestamps
        const missingMembers = await findMissingMembersWithTimestamps(db);
        console.log(`[${getFormattedDateTime()}] Found ${missingMembers.length} users in messages who might have left the guild`);
        
        // Log first few members for debugging
        if (missingMembers.length > 0) {
            console.log(`[${getFormattedDateTime()}] Sample of missing members:`);
            for (let i = 0; i < Math.min(5, missingMembers.length); i++) {
                const member = missingMembers[i];
                console.log(`   - ${member.authorUsername} (${member.authorId}): First msg: ${new Date(member.firstMessageTime).toISOString()}, Last msg: ${new Date(member.lastMessageTime).toISOString()}`);
            }
        }

        // Batch process members instead of one by one
        const BATCH_SIZE = 100; // Process 100 members at a time
        let addedCount = 0;
        let processedCount = 0;
        let skippedCount = 0;
        let roleCount = 0;
        
        // Use a transaction for better performance
        console.log(`[${getFormattedDateTime()}] Starting database transaction for bulk processing...`);
        await executeTransaction(db, async () => {
            // Process members in batches
            for (let i = 0; i < missingMembers.length; i += BATCH_SIZE) {
                const batch = missingMembers.slice(i, i + BATCH_SIZE);
                const batchNum = Math.floor(i/BATCH_SIZE) + 1;
                const totalBatches = Math.ceil(missingMembers.length/BATCH_SIZE);
                console.log(`[${getFormattedDateTime()}] Processing batch ${batchNum}/${totalBatches} (${batch.length} members)`);
                
                // Process batch in parallel
                const results = await Promise.all(batch.map(async (member) => {
                    try {
                        processedCount++;
                        // Check if the user is actually in the guild
                        let guildMember = null;
                        try {
                            guildMember = await guild.members.fetch(member.authorId);
                        } catch (fetchError) {
                            // Expected for left members - this is normal
                        }
                        
                        // Skip if member is actually in the guild
                        if (guildMember) {
                            console.log(`[${getFormattedDateTime()}] User ${member.authorUsername} (${member.authorId}) is still in guild, skipping`);
                            skippedCount++;
                            return null;
                        }
                        
                        const currentTime = Date.now();
                        
                        // Add to guild_members
                        await addLeftMemberToDb(
                            db, 
                            member.authorId, 
                            member.authorUsername, 
                            member.firstMessageTime, 
                            member.lastMessageTime,
                            currentTime
                        );
                        
                        return member;
                    } catch (error) {
                        console.error(`[${getFormattedDateTime()}] Error processing member ${member.authorUsername} (${member.authorId}):`, error);
                        return null;
                    }
                }));
                
                // Count successfully added members
                const addedMembers = results.filter(m => m !== null);
                addedCount += addedMembers.length;
                
                // Process roles in bulk after adding members
                if (addedMembers.length > 0) {
                    const batchRoleCount = await addBulkMemberRoles(db, addedMembers.map(m => m.authorId));
                    roleCount += batchRoleCount;
                }
                
                // Log progress
                console.log(`[${getFormattedDateTime()}] Batch ${batchNum}/${totalBatches} complete: Added ${addedMembers.length} members in this batch`);
                console.log(`[${getFormattedDateTime()}] Progress: ${processedCount}/${missingMembers.length} (${Math.round(processedCount/missingMembers.length*100)}%) processed, ${addedCount} added, ${skippedCount} skipped`);
            }
        });
        
        console.log(`========================================`);
        console.log(`[${getFormattedDateTime()}] LEFT MEMBER PROCESSING SUMMARY:`);
        console.log(`[${getFormattedDateTime()}] - Total processed: ${processedCount}`);
        console.log(`[${getFormattedDateTime()}] - Added to database: ${addedCount}`);
        console.log(`[${getFormattedDateTime()}] - Skipped (still in guild): ${skippedCount}`);
        console.log(`[${getFormattedDateTime()}] - Roles recovered: ${roleCount}`);
        console.log(`========================================`);
        
        return { 
            success: true, 
            addedCount,
            processedCount,
            skippedCount,
            roleCount
        };
    } catch (error) {
        console.error(`[${getFormattedDateTime()}] ERROR: Failed to process left members:`, error);
        console.error(`[${getFormattedDateTime()}] Stack trace:`, error.stack);
        return { success: false, error: error.message };
    }
}

/**
 * Finds message authors who aren't in the guild_members table and also gets their message timestamps
 * @param {Object} db - Database connection
 * @returns {Array} Array of member objects with authorId, authorUsername, firstMessageTime, and lastMessageTime
 */
function findMissingMembersWithTimestamps(db) {
    return new Promise((resolve, reject) => {
        console.log(`[${getFormattedDateTime()}] Executing SQL to find missing members...`);
        
        // Single optimized query to get members and their message timestamps
        const sql = `
            SELECT 
                m.authorId, 
                m.authorUsername,
                MIN(m.timestamp) as firstMessageTime,
                MAX(m.timestamp) as lastMessageTime,
                COUNT(m.id) as messageCount
            FROM messages m
            LEFT JOIN guild_members gm ON m.authorId = gm.id
            WHERE m.authorBot = 0 AND gm.id IS NULL
            GROUP BY m.authorId, m.authorUsername
            ORDER BY messageCount DESC
        `;

        db.all(sql, [], (err, rows) => {
            if (err) {
                console.error(`[${getFormattedDateTime()}] Error finding missing members:`, err);
                reject(err);
                return;
            }
            
            console.log(`[${getFormattedDateTime()}] Found ${rows.length} missing members from message history`);
            resolve(rows);
        });
    });
}

/**
 * Execute a database transaction
 * @param {Object} db - Database connection
 * @param {Function} operations - Function containing the operations to perform
 * @returns {Promise} Promise that resolves when the transaction completes
 */
function executeTransaction(db, operations) {
    return new Promise((resolve, reject) => {
        db.serialize(() => {
            db.run('BEGIN TRANSACTION', (beginErr) => {
                if (beginErr) {
                    console.error(`[${getFormattedDateTime()}] Error beginning transaction:`, beginErr);
                    return reject(beginErr);
                }
                
                console.log(`[${getFormattedDateTime()}] Transaction started successfully`);
                
                try {
                    // Execute the operations
                    operations()
                        .then(() => {
                            // Commit the transaction
                            console.log(`[${getFormattedDateTime()}] Operations complete, committing transaction...`);
                            db.run('COMMIT', (commitErr) => {
                                if (commitErr) {
                                    console.error(`[${getFormattedDateTime()}] Error committing transaction:`, commitErr);
                                    db.run('ROLLBACK', () => reject(commitErr));
                                } else {
                                    console.log(`[${getFormattedDateTime()}] Transaction committed successfully`);
                                    resolve();
                                }
                            });
                        })
                        .catch(operationErr => {
                            console.error(`[${getFormattedDateTime()}] Error during transaction operations:`, operationErr);
                            db.run('ROLLBACK', () => {
                                console.log(`[${getFormattedDateTime()}] Transaction rolled back due to error`);
                                reject(operationErr);
                            });
                        });
                } catch (operationErr) {
                    console.error(`[${getFormattedDateTime()}] Unexpected error during transaction:`, operationErr);
                    db.run('ROLLBACK', () => {
                        console.log(`[${getFormattedDateTime()}] Transaction rolled back due to unexpected error`);
                        reject(operationErr);
                    });
                }
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
        const leftAt = leftTimestamp ? new Date(leftTimestamp).toISOString() : null;
        
        console.log(`[${getFormattedDateTime()}] Adding left member ${username} (${memberId}) with join: ${joinedAt}, left: ${leftAt}`);
        
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
            
            console.log(`[${getFormattedDateTime()}] Successfully added left member ${username} (${memberId}) to database`);
            resolve(this.changes);
        });
    });
}

/**
 * Process roles for multiple members in bulk
 * @param {Object} db - Database connection
 * @param {Array} memberIds - Array of member IDs
 * @returns {Promise} Promise that resolves when roles are processed
 */
async function addBulkMemberRoles(db, memberIds) {
    if (!memberIds || memberIds.length === 0) return 0;
    
    try {
        console.log(`[${getFormattedDateTime()}] Bulk processing roles for ${memberIds.length} members`);
        const currentTime = Date.now();
        let roleCount = 0;
        
        // Create a prepared statement for better performance
        const stmt = db.prepare(`
            INSERT OR IGNORE INTO member_roles (
                memberId, roleId, roleName, roleColor, rolePosition, addedAt
            ) VALUES (?, ?, ?, ?, ?, ?)
        `);
        
        // Process each member
        for (const memberId of memberIds) {
            // Find roles from mentions for this member
            const roles = await findRolesForMember(db, memberId);
            
            if (roles.length > 0) {
                console.log(`[${getFormattedDateTime()}] Found ${roles.length} roles for member ${memberId}`);
            }
            
            // Add each role
            for (const role of roles) {
                try {
                    stmt.run(
                        memberId,
                        role.id,
                        role.name,
                        role.color,
                        role.position,
                        currentTime
                    );
                    roleCount++;
                } catch (error) {
                    console.error(`[${getFormattedDateTime()}] Error adding role ${role.name} for member ${memberId}:`, error);
                }
            }
        }
        
        // Finalize the statement
        stmt.finalize();
        
        console.log(`[${getFormattedDateTime()}] Added ${roleCount} roles for ${memberIds.length} members`);
        return roleCount;
    } catch (error) {
        console.error(`[${getFormattedDateTime()}] Error adding bulk roles:`, error);
        return 0;
    }
}

/**
 * Find roles for a member from message mentions and role tables
 * @param {Object} db - Database connection
 * @param {String} memberId - Member ID
 * @returns {Promise<Array>} Promise resolving to an array of roles
 */
async function findRolesForMember(db, memberId) {
    return new Promise((resolve, reject) => {
        // First try the optimized query that joins message mentions with guild_roles table
        const sql = `
            WITH member_mentions AS (
                SELECT m.mention_roles
                FROM messages m
                WHERE m.mentions LIKE ? AND m.mention_roles IS NOT NULL
                LIMIT 100
            )
            SELECT DISTINCT gr.id, gr.name, gr.color, gr.position
            FROM guild_roles gr
            JOIN member_mentions mm ON mm.mention_roles LIKE '%' || gr.id || '%'
        `;
        
        db.all(sql, [`%${memberId}%`], (err, rows) => {
            if (err) {
                console.error(`[${getFormattedDateTime()}] Error finding roles for member ${memberId}:`, err);
                reject(err);
                return;
            }
            
            resolve(rows || []);
        });
    });
}

/**
 * Fallback method to extract roles from message mentions
 * @param {Object} db - Database connection
 * @param {String} memberId - Member ID
 * @returns {Promise<Array>} Promise resolving to an array of roles
 */
async function extractRolesFromMentions(db, memberId) {
    try {
        // Get messages that mention the member
        const messages = await new Promise((resolve, reject) => {
            db.all(
                `SELECT mention_roles FROM messages WHERE mentions LIKE ? AND mention_roles IS NOT NULL LIMIT 50`, 
                [`%${memberId}%`], 
                (err, rows) => err ? reject(err) : resolve(rows)
            );
        });
        
        if (messages.length === 0) {
            return [];
        }
        
        // Extract role IDs from mentions
        const roleIds = new Set();
        for (const message of messages) {
            try {
                const roles = JSON.parse(message.mention_roles);
                if (Array.isArray(roles)) {
                    roles.forEach(roleId => roleIds.add(roleId));
                }
            } catch (e) {
                // Ignore JSON parse errors
            }
        }
        
        // Get role details for each ID
        const roles = [];
        for (const roleId of roleIds) {
            try {
                const role = await new Promise((resolve, reject) => {
                    db.get(
                        `SELECT id, name, color, position FROM guild_roles WHERE id = ?`,
                        [roleId],
                        (err, row) => err ? reject(err) : resolve(row)
                    );
                });
                
                if (role) {
                    roles.push(role);
                }
            } catch (e) {
                // Ignore errors for individual role lookups
            }
        }
        
        return roles;
    } catch (error) {
        console.error(`[${getFormattedDateTime()}] Error extracting roles from mentions for ${memberId}:`, error);
        return [];
    }
}

// Utility function for formatted date-time
function getFormattedDateTime() {
    const now = new Date();
    return `${now.getUTCFullYear()}-${String(now.getUTCMonth() + 1).padStart(2, '0')}-${String(now.getUTCDate()).padStart(2, '0')} ${String(now.getUTCHours()).padStart(2, '0')}:${String(now.getUTCMinutes()).padStart(2, '0')}:${String(now.getUTCSeconds()).padStart(2, '0')}`;
}

module.exports = {
    processLeftMembers
};