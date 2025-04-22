const fs = require('fs');
const path = require('path');
const sqlite3 = require('sqlite3').verbose();
const { PermissionFlagsBits } = require('discord.js');

function getFileSize(filePath) {
  const stats = fs.statSync(filePath);
  const fileSizeInBytes = stats.size;
  return fileSizeInBytes / (1024 * 1024); // Convert to MB
}

function formatNumber(num) {
  return num.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",");
}

async function vacuumDatabase(dbPath) {
  return new Promise((resolve, reject) => {
    // Get size before vacuum
    const sizeBefore = getFileSize(dbPath);
    const sizeBeforeMB = sizeBefore.toFixed(2);
    
    console.log(`Vacuuming database at ${dbPath} (current size: ${sizeBeforeMB} MB)`);
    
    // Connect to database 
    const db = new sqlite3.Database(dbPath, (err) => {
      if (err) {
        console.error('Error connecting to database for vacuum:', err);
        reject(err);
        return;
      }
      
      // Run vacuum command
      db.run('VACUUM', function(err) {
        // Close the database connection
        db.close();
        
        if (err) {
          console.error('Error vacuuming database:', err);
          reject(err);
          return;
        }
        
        // Get size after vacuum
        const sizeAfter = getFileSize(dbPath);
        const sizeAfterMB = sizeAfter.toFixed(2);
        
        // Calculate space saved
        const spaceSaved = sizeBefore - sizeAfter;
        const spaceSavedMB = spaceSaved.toFixed(2);
        const percentSaved = sizeBefore > 0 ? ((spaceSaved / sizeBefore) * 100).toFixed(2) : 0;
        
        console.log(`Vacuum complete: Before: ${sizeBeforeMB} MB, After: ${sizeAfterMB} MB, Saved: ${spaceSavedMB} MB (${percentSaved}%)`);
        
        resolve({
          sizeBefore: sizeBeforeMB,
          sizeAfter: sizeAfterMB,
          spaceSaved: spaceSavedMB,
          percentSaved
        });
      });
    });
  });
}

async function handleVacuumCommand(message, monitor) {
  try {
    // Check if user has administrator permissions
    if (!message.member.permissions.has(PermissionFlagsBits.Administrator)) {
      return message.reply('❌ You need administrator permissions to use the vacuum command.');
    }
    
    // Get current database path
    const dbPath = monitor.getCurrentDatabasePath();
    if (!dbPath || !fs.existsSync(dbPath)) {
      return message.reply('❌ Database not found. Please run the export command first.');
    }
    
    // Send initial status message
    const statusMessage = await message.channel.send(
      `🔄 Database Vacuum Operation\n` +
      `Starting vacuum process on database: ${path.basename(dbPath)}\n` +
      `This operation may take some time for large databases...`
    );
    
    // Perform the vacuum operation
    const result = await vacuumDatabase(dbPath);
    
    // Format the result for display
    const sizeBeforeFormatted = formatNumber(parseFloat(result.sizeBefore));
    const sizeAfterFormatted = formatNumber(parseFloat(result.sizeAfter));
    const spaceSavedFormatted = formatNumber(parseFloat(result.spaceSaved));
    
    // Update the status message with results
    await statusMessage.edit(
      `✅ Database Vacuum Complete!\n\n` +
      `📊 **Results:**\n` +
      `• Database: \`${path.basename(dbPath)}\`\n` +
      `• Size before: \`${sizeBeforeFormatted} MB\`\n` +
      `• Size after: \`${sizeAfterFormatted} MB\`\n` +
      `• Space saved: \`${spaceSavedFormatted} MB (${result.percentSaved}%)\`\n\n` +
      `The database has been optimized and redundant space has been reclaimed.`
    );
    
  } catch (error) {
    console.error('Error during database vacuum:', error);
    message.channel.send(`❌ Error during database vacuum: ${error.message}`);
  }
}

module.exports = {
  handleVacuumCommand
};