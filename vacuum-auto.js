const cron = require('node-cron');
const monitor = require('./monitor');
const vacuum = require('./vacuum');
const config = require('./config');

// To track the vacuum job
let vacuumJob = null;

/**
 * Initialize the automatic vacuum schedule
 * @param {Client} client - Discord client
 * @param {Object} options - Configuration options
 * @returns {boolean} Whether scheduling was successful
 */
function initializeAutoVacuum(client, options = {}) {
  // Stop any existing job
  stopVacuumSchedule();

  // Return early if auto-vacuum is disabled
  if (!config.getConfig('autoVacuumEnabled', 'AUTO_VACUUM_ENABLED')) {
    console.log('Automatic vacuum operations are disabled');
    return false;
  }

  // Get the cron schedule from config
  const cronSchedule = config.getConfig('vacuumCronSchedule', 'VACUUM_CRON_SCHEDULE');
  if (!cronSchedule || !cron.validate(cronSchedule)) {
    console.error(`Invalid cron schedule: ${cronSchedule}. Automatic vacuum not scheduled.`);
    return false;
  }

  try {
    // Schedule the vacuum operation
    vacuumJob = cron.schedule(cronSchedule, async () => {
      console.log(`[${getFormattedDateTime()}] Starting automatic vacuum operation`);

      // Get all guilds
      for (const [guildId, guild] of client.guilds.cache) {
        try {
          // Check if we have a database for this guild
          const dbPath = monitor.getCurrentDatabasePath();
          if (!dbPath) {
            console.log(`No database found for guild ${guild.name}, skipping automatic vacuum`);
            continue;
          }

          console.log(`Performing automatic vacuum for database: ${dbPath}`);
          
          const result = await vacuum.vacuumDatabase(dbPath);

          console.log(`[${getFormattedDateTime()}] Automatic vacuum for ${guild.name} complete: ${result.spaceSaved} MB saved`);
          
          // Optional: Send notification to a log channel if configured
          if (options.logChannelId) {
            try {
              const channel = await client.channels.fetch(options.logChannelId);
              if (channel) {
                await channel.send(
                  `✅ **Automatic Database Vacuum Complete**\n` +
                  `• Database: \`${path.basename(dbPath)}\`\n` +
                  `• Size before: \`${result.sizeBefore} MB\`\n` +
                  `• Size after: \`${result.sizeAfter} MB\`\n` +
                  `• Space saved: \`${result.spaceSaved} MB (${result.percentSaved}%)\`\n` +
                  `• Time: ${getFormattedDateTime()}`
                );
              }
            } catch (notifyError) {
              console.error('Error sending vacuum notification:', notifyError);
            }
          }
        } catch (guildError) {
          console.error(`Error during automatic vacuum for guild ${guild.name}:`, guildError);
        }
      }

      console.log(`[${getFormattedDateTime()}] Automatic vacuum operations completed`);
    });

    console.log(`Automatic vacuum scheduled with cron: ${cronSchedule}`);
    return true;
  } catch (error) {
    console.error('Error setting up vacuum schedule:', error);
    return false;
  }
}

/**
 * Stop the vacuum schedule if running
 */
function stopVacuumSchedule() {
  if (vacuumJob) {
    vacuumJob.stop();
    vacuumJob = null;
    console.log('Stopped automatic vacuum schedule');
    return true;
  }
  return false;
}

/**
 * Format date and time for logs
 */
function getFormattedDateTime() {
  const now = new Date();
  return `${now.getUTCFullYear()}-${String(now.getUTCMonth() + 1).padStart(2, '0')}-${String(now.getUTCDate()).padStart(2, '0')} ${String(now.getUTCHours()).padStart(2, '0')}:${String(now.getUTCMinutes()).padStart(2, '0')}:${String(now.getUTCSeconds()).padStart(2, '0')}`;
}

module.exports = {
  initializeAutoVacuum,
  stopVacuumSchedule
};