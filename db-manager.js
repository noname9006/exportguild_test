const transactionQueue = [];
let isProcessingQueue = false;
let isTransactionActive = false;  // Track globally if a transaction is active

// Function to execute a transaction safely
function executeTransaction(db, transactionFn) {
  return new Promise((resolve, reject) => {
    if (isTransactionActive) {
      console.error("Transaction already active! Aborting to prevent nested transactions");
      return reject(new Error("Transaction already active"));
    }
    
    try {
      isTransactionActive = true;
      console.log("Starting transaction...");
      
      db.run('BEGIN TRANSACTION', (beginErr) => {
        if (beginErr) {
          isTransactionActive = false;
          console.error("Failed to begin transaction:", beginErr);
          return reject(beginErr);
        }
        
        // Execute the transaction function
        Promise.resolve()
          .then(() => transactionFn(db))
          .then(result => {
            // Commit the transaction
            db.run('COMMIT', (commitErr) => {
              isTransactionActive = false;
              
              if (commitErr) {
                console.error("Failed to commit transaction:", commitErr);
                // Attempt to roll back
                try {
                  db.run('ROLLBACK');
                } catch (rollbackErr) {
                  console.error("Failed to rollback:", rollbackErr);
                }
                return reject(commitErr);
              }
              
              console.log("Transaction committed successfully");
              resolve(result);
            });
          })
          .catch(err => {
            console.error("Error during transaction execution:", err);
            // Roll back the transaction
            db.run('ROLLBACK', (rollbackErr) => {
              isTransactionActive = false;
              
              if (rollbackErr) {
                console.error("Failed to rollback transaction:", rollbackErr);
              }
              
              reject(err);
            });
          });
      });
    } catch (error) {
      isTransactionActive = false;
      console.error("Unexpected error in transaction wrapper:", error);
      reject(error);
    }
  });
}

// Queue system for database operations
function queueDatabaseOperation(operationFn) {
  return new Promise((resolve, reject) => {
    transactionQueue.push({
      fn: operationFn,
      resolve,
      reject
    });
    
    processQueue();
  });
}

function processQueue() {
  if (isProcessingQueue || transactionQueue.length === 0) return;
  
  isProcessingQueue = true;
  const item = transactionQueue.shift();
  
  Promise.resolve()
    .then(() => item.fn())
    .then(result => item.resolve(result))
    .catch(err => item.reject(err))
    .finally(() => {
      isProcessingQueue = false;
      processQueue();  // Process next item
    });
}