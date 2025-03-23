import getLogger from '../utils/logger.js';

const logger = getLogger('core/StateStorage');

/**
 * StateStorage manages the persistent storage of message final states,
 * providing verification that a message has completed a specific state transition.
 * This implementation includes an in-memory storage option for testing.
 */
class StateStorage {
  /**
   * Create a new StateStorage instance
   * @param {Object|null} dbClient - Database client instance (Knex or similar), null for in-memory
   * @param {Object} options - Configuration options
   */
  constructor(dbClient, options = {}) {
    if (StateStorage.instance) {
      return StateStorage.instance;
    }

    this._dbClient = dbClient;
    this._options = {
      tableName: 'message_states',
      messageIdColumn: 'message_id',
      currentStateColumn: 'current_state',
      nextStateColumn: 'next_state',
      completedAtColumn: 'completed_at',
      metadataColumn: 'metadata',
      useInMemory: dbClient === null,
      ...options,
    };

    // Initialize in-memory storage if needed
    if (this._options.useInMemory) {
      this._inMemoryStore = [];
      logger.info('StateStorage initialized with in-memory storage (for testing)');
    } else {
      logger.info('StateStorage initialized with database storage');
    }

    StateStorage.instance = this;
  }

  /**
   * Check if a message has already reached its final state
   * @param {string|number} messageId - Message ID
   * @param {string} currentState - Current state to check
   * @returns {Promise<Object|null>} - Final state record or null if not found
   */
  async getFinalState(messageId, currentState) {
    try {
      if (this._options.useInMemory) {
        return this.#getInMemoryFinalState(messageId, currentState);
      }

      const result = await this._dbClient(this._options.tableName)
        .where({
          [this._options.messageIdColumn]: messageId,
          [this._options.currentStateColumn]: currentState,
        })
        .first();

      return result || null;
    } catch (error) {
      logger.error(`Error getting final state for message ${messageId}:`, error);
      return null;
    }
  }

  /**
   * Store the final state of a message
   * @param {string|number} messageId - Message ID
   * @param {string} currentState - Current state
   * @param {string} nextState - Next (final) state
   * @param {Object} options - Additional options
   * @returns {Promise<boolean>} - Success indicator
   */
  async storeFinalState(messageId, currentState, nextState, options = {}) {
    const { completedAt = new Date(), metadata = null } = options;

    try {
      if (this._options.useInMemory) {
        return this.#storeInMemoryFinalState(
          messageId,
          currentState,
          nextState,
          completedAt,
          metadata
        );
      }

      await this._dbClient(this._options.tableName)
        .insert({
          [this._options.messageIdColumn]: messageId,
          [this._options.currentStateColumn]: currentState,
          [this._options.nextStateColumn]: nextState,
          [this._options.completedAtColumn]: completedAt,
          [this._options.metadataColumn]:
            typeof metadata === 'string' ? metadata : JSON.stringify(metadata),
        })
        .onConflict([this._options.messageIdColumn, this._options.currentStateColumn])
        .merge(); // Update if already exists

      logger.debug(`Stored final state for message ${messageId}: ${currentState} -> ${nextState}`);
      return true;
    } catch (error) {
      logger.error(`Error storing final state for message ${messageId}:`, error);
      return false;
    }
  }

  /**
   * Check if a batch of messages have already reached their final state
   * @param {Array<Object>} messages - Array of message objects with id and currentState
   * @returns {Promise<Array<Object>>} - Messages that haven't reached final state
   */
  async filterCompletedMessages(messages) {
    if (!messages || !messages.length) return [];

    try {
      if (this._options.useInMemory) {
        return this.#filterInMemoryCompletedMessages(messages);
      }

      // Extract message IDs and states
      const criteria = messages.map(msg => ({
        messageId: msg.id,
        currentState: msg.currentState,
      }));

      // Build query to find all matching records
      const query = this._dbClient(this._options.tableName).whereIn(
        [this._options.messageIdColumn, this._options.currentStateColumn],
        criteria.map(c => [c.messageId, c.currentState])
      );

      // Execute query
      const results = await query;

      // Create map of completed messages
      const completedMap = {};
      for (const result of results) {
        const key = `${result[this._options.messageIdColumn]}:${result[this._options.currentStateColumn]}`;
        completedMap[key] = true;
      }

      // Filter out messages that have already completed
      return messages.filter(msg => {
        const key = `${msg.id}:${msg.currentState}`;
        return !completedMap[key];
      });
    } catch (error) {
      logger.error('Error filtering completed messages:', error);
      return messages; // Return all messages on error to be safe
    }
  }

  /**
   * Get message processing history
   * @param {string|number} messageId - Message ID
   * @returns {Promise<Array<Object>>} - Array of state transitions
   */
  async getMessageHistory(messageId) {
    try {
      if (this._options.useInMemory) {
        return this.#getInMemoryMessageHistory(messageId);
      }

      const results = await this._dbClient(this._options.tableName)
        .where({ [this._options.messageIdColumn]: messageId })
        .orderBy(this._options.completedAtColumn, 'asc');

      return results;
    } catch (error) {
      logger.error(`Error getting message history for ${messageId}:`, error);
      return [];
    }
  }

  /**
   * Get statistics for message processing
   * @param {Object} options - Query options
   * @returns {Promise<Object>} - Statistics
   */
  async getProcessingStats(options = {}) {
    const { startDate = null, endDate = null, limit = 10, groupBy = null } = options;

    try {
      if (this._options.useInMemory) {
        return this.#getInMemoryProcessingStats(options);
      }

      let query = this._dbClient(this._options.tableName);

      if (startDate) {
        query = query.where(this._options.completedAtColumn, '>=', startDate);
      }

      if (endDate) {
        query = query.where(this._options.completedAtColumn, '<=', endDate);
      }

      if (groupBy === 'state') {
        // Group by state transition
        const stateStats = await query
          .select(
            this._options.currentStateColumn,
            this._options.nextStateColumn,
            this._dbClient.raw('COUNT(*) as count'),
            this._dbClient.raw(`MIN(${this._options.completedAtColumn}) as first_seen`),
            this._dbClient.raw(`MAX(${this._options.completedAtColumn}) as last_seen`)
          )
          .groupBy(this._options.currentStateColumn, this._options.nextStateColumn)
          .orderBy('count', 'desc')
          .limit(limit);

        return { stateStats };
      } else if (groupBy === 'time') {
        // Group by time periods (hourly)
        const timeStats = await query
          .select(
            this._dbClient.raw(`DATE_TRUNC('hour', ${this._options.completedAtColumn}) as period`),
            this._dbClient.raw('COUNT(*) as count')
          )
          .groupBy('period')
          .orderBy('period', 'desc')
          .limit(limit);

        return { timeStats };
      } else {
        // Overall statistics
        const totalCount = await query.count('* as count').first();
        const stateCount = await query
          .select(this._options.currentStateColumn, this._dbClient.raw('COUNT(*) as count'))
          .groupBy(this._options.currentStateColumn)
          .orderBy('count', 'desc');

        const recentMessages = await query
          .select('*')
          .orderBy(this._options.completedAtColumn, 'desc')
          .limit(limit);

        return {
          totalCount: totalCount.count,
          stateCount,
          recentMessages,
        };
      }
    } catch (error) {
      logger.error('Error getting processing statistics:', error);
      return { error: error.message };
    }
  }

  /**
   * Initialize the database table if it doesn't exist
   * @returns {Promise<boolean>} - Success indicator
   */
  async initializeTable() {
    if (this._options.useInMemory) {
      this._inMemoryStore = [];
      logger.info('Initialized in-memory storage');
      return true;
    }

    try {
      // Check if table exists
      const tableExists = await this._dbClient.schema.hasTable(this._options.tableName);

      if (!tableExists) {
        // Create table
        await this._dbClient.schema.createTable(this._options.tableName, table => {
          table.string(this._options.messageIdColumn).notNullable();
          table.string(this._options.currentStateColumn).notNullable();
          table.string(this._options.nextStateColumn).notNullable();
          table.timestamp(this._options.completedAtColumn).defaultTo(this._dbClient.fn.now());
          table.json(this._options.metadataColumn).nullable();

          // Primary key on message ID and current state
          table.primary([this._options.messageIdColumn, this._options.currentStateColumn]);

          // Index on completion time for stats queries
          table.index(this._options.completedAtColumn);
        });

        logger.info(`Created ${this._options.tableName} table`);
      } else {
        logger.debug(`Table ${this._options.tableName} already exists`);
      }

      return true;
    } catch (error) {
      logger.error(`Error initializing ${this._options.tableName} table:`, error);
      return false;
    }
  }

  /**
   * Purge old message state records
   * @param {Object} options - Purge options
   * @returns {Promise<number>} - Number of records purged
   */
  async purgeOldRecords(options = {}) {
    const {
      olderThan = new Date(Date.now() - 30 * 24 * 60 * 60 * 1000), // 30 days
      batchSize = 1000,
      maxRecords = null,
    } = options;

    try {
      if (this._options.useInMemory) {
        return this.#purgeInMemoryOldRecords(olderThan, maxRecords);
      }

      let totalPurged = 0;
      let continueDeleting = true;

      while (continueDeleting) {
        // Delete in batches
        const purgeCount = await this._dbClient(this._options.tableName)
          .where(this._options.completedAtColumn, '<', olderThan)
          .limit(batchSize)
          .delete();

        totalPurged += purgeCount;

        // Stop if we've reached the max or if last batch was smaller than batch size
        if ((maxRecords && totalPurged >= maxRecords) || purgeCount < batchSize) {
          continueDeleting = false;
        }
      }

      logger.info(`Purged ${totalPurged} records older than ${olderThan}`);
      return totalPurged;
    } catch (error) {
      logger.error('Error purging old records:', error);
      return 0;
    }
  }

  /**
   * Clear all storage (mainly for testing)
   * @returns {Promise<boolean>} - Success indicator
   */
  async clear() {
    try {
      if (this._options.useInMemory) {
        this._inMemoryStore = [];
        logger.info('Cleared in-memory storage');
        return true;
      }

      await this._dbClient(this._options.tableName).truncate();
      logger.info(`Truncated ${this._options.tableName} table`);
      return true;
    } catch (error) {
      logger.error('Error clearing storage:', error);
      return false;
    }
  }

  /**
   * Get the in-memory final state for a message
   * @param {string|number} messageId - Message ID
   * @param {string} currentState - Current state
   * @returns {Object|null} - Final state record or null
   * @private
   */
  #getInMemoryFinalState(messageId, currentState) {
    const record = this._inMemoryStore.find(
      entry =>
        entry[this._options.messageIdColumn] === messageId &&
        entry[this._options.currentStateColumn] === currentState
    );

    return record || null;
  }

  /**
   * Store a final state in memory
   * @param {string|number} messageId - Message ID
   * @param {string} currentState - Current state
   * @param {string} nextState - Next state
   * @param {Date} completedAt - Completion timestamp
   * @param {Object|string} metadata - Additional metadata
   * @returns {boolean} - Success indicator
   * @private
   */
  #storeInMemoryFinalState(messageId, currentState, nextState, completedAt, metadata) {
    // Check if record already exists
    const existingIndex = this._inMemoryStore.findIndex(
      entry =>
        entry[this._options.messageIdColumn] === messageId &&
        entry[this._options.currentStateColumn] === currentState
    );

    const record = {
      [this._options.messageIdColumn]: messageId,
      [this._options.currentStateColumn]: currentState,
      [this._options.nextStateColumn]: nextState,
      [this._options.completedAtColumn]: completedAt,
      [this._options.metadataColumn]:
        typeof metadata === 'object' ? JSON.stringify(metadata) : metadata,
    };

    if (existingIndex >= 0) {
      // Update existing record
      this._inMemoryStore[existingIndex] = record;
    } else {
      // Add new record
      this._inMemoryStore.push(record);
    }

    logger.debug(
      `Stored in-memory final state for message ${messageId}: ${currentState} -> ${nextState}`
    );
    return true;
  }

  /**
   * Filter messages that have already been completed in memory
   * @param {Array<Object>} messages - Messages to filter
   * @returns {Array<Object>} - Messages that haven't been completed
   * @private
   */
  #filterInMemoryCompletedMessages(messages) {
    // Create map of completed messages
    const completedMap = {};

    this._inMemoryStore.forEach(record => {
      const key = `${record[this._options.messageIdColumn]}:${record[this._options.currentStateColumn]}`;
      completedMap[key] = true;
    });

    // Filter messages that haven't been completed
    return messages.filter(msg => {
      const key = `${msg.id}:${msg.currentState}`;
      return !completedMap[key];
    });
  }

  /**
   * Get message history from in-memory storage
   * @param {string|number} messageId - Message ID
   * @returns {Array<Object>} - State transition history
   * @private
   */
  #getInMemoryMessageHistory(messageId) {
    const records = this._inMemoryStore.filter(
      entry => entry[this._options.messageIdColumn] === messageId
    );

    // Sort by completion time
    return records.sort((a, b) => {
      const dateA = new Date(a[this._options.completedAtColumn]);
      const dateB = new Date(b[this._options.completedAtColumn]);
      return dateA - dateB;
    });
  }

  /**
   * Get processing statistics from in-memory storage
   * @param {Object} options - Stat options
   * @returns {Object} - Processing statistics
   * @private
   */
  #getInMemoryProcessingStats(options) {
    const { startDate = null, endDate = null, limit = 10, groupBy = null } = options;

    // Filter records by date range if specified
    let filteredRecords = [...this._inMemoryStore];

    if (startDate) {
      filteredRecords = filteredRecords.filter(record => {
        const recordDate = new Date(record[this._options.completedAtColumn]);
        return recordDate >= new Date(startDate);
      });
    }

    if (endDate) {
      filteredRecords = filteredRecords.filter(record => {
        const recordDate = new Date(record[this._options.completedAtColumn]);
        return recordDate <= new Date(endDate);
      });
    }

    if (groupBy === 'state') {
      // Group by state transition
      const stateGroups = {};

      filteredRecords.forEach(record => {
        const key = `${record[this._options.currentStateColumn]}:${record[this._options.nextStateColumn]}`;

        if (!stateGroups[key]) {
          stateGroups[key] = {
            [this._options.currentStateColumn]: record[this._options.currentStateColumn],
            [this._options.nextStateColumn]: record[this._options.nextStateColumn],
            count: 0,
            first_seen: record[this._options.completedAtColumn],
            last_seen: record[this._options.completedAtColumn],
          };
        }

        stateGroups[key].count++;

        const recordDate = new Date(record[this._options.completedAtColumn]);
        const firstSeen = new Date(stateGroups[key].first_seen);
        const lastSeen = new Date(stateGroups[key].last_seen);

        if (recordDate < firstSeen) {
          stateGroups[key].first_seen = record[this._options.completedAtColumn];
        }

        if (recordDate > lastSeen) {
          stateGroups[key].last_seen = record[this._options.completedAtColumn];
        }
      });

      // Convert to array and sort by count
      const stateStats = Object.values(stateGroups)
        .sort((a, b) => b.count - a.count)
        .slice(0, limit);

      return { stateStats };
    } else if (groupBy === 'time') {
      // Group by hour
      const timeGroups = {};

      filteredRecords.forEach(record => {
        const date = new Date(record[this._options.completedAtColumn]);
        // Truncate to hour
        const hourKey = new Date(
          date.getFullYear(),
          date.getMonth(),
          date.getDate(),
          date.getHours()
        ).toISOString();

        if (!timeGroups[hourKey]) {
          timeGroups[hourKey] = {
            period: hourKey,
            count: 0,
          };
        }

        timeGroups[hourKey].count++;
      });

      // Convert to array and sort by period
      const timeStats = Object.values(timeGroups)
        .sort((a, b) => new Date(b.period) - new Date(a.period))
        .slice(0, limit);

      return { timeStats };
    } else {
      // Overall statistics
      const totalCount = filteredRecords.length;

      // Group by current state
      const stateGroups = {};
      filteredRecords.forEach(record => {
        const state = record[this._options.currentStateColumn];

        if (!stateGroups[state]) {
          stateGroups[state] = {
            [this._options.currentStateColumn]: state,
            count: 0,
          };
        }

        stateGroups[state].count++;
      });

      const stateCount = Object.values(stateGroups).sort((a, b) => b.count - a.count);

      // Get recent messages
      const recentMessages = [...filteredRecords]
        .sort((a, b) => {
          const dateA = new Date(a[this._options.completedAtColumn]);
          const dateB = new Date(b[this._options.completedAtColumn]);
          return dateB - dateA;
        })
        .slice(0, limit);

      return {
        totalCount,
        stateCount,
        recentMessages,
      };
    }
  }

  /**
   * Purge old records from in-memory storage
   * @param {Date} olderThan - Date threshold
   * @param {number} maxRecords - Maximum records to purge
   * @returns {number} - Number of records purged
   * @private
   */
  #purgeInMemoryOldRecords(olderThan, maxRecords) {
    const initialCount = this._inMemoryStore.length;

    // Filter records to keep only newer ones
    const filteredRecords = this._inMemoryStore.filter(record => {
      const recordDate = new Date(record[this._options.completedAtColumn]);
      return recordDate >= olderThan;
    });

    const purgedCount = initialCount - filteredRecords.length;

    // If maxRecords is specified, limit the purge
    if (maxRecords !== null && purgedCount > maxRecords) {
      // Sort by date (oldest first)
      this._inMemoryStore.sort((a, b) => {
        const dateA = new Date(a[this._options.completedAtColumn]);
        const dateB = new Date(b[this._options.completedAtColumn]);
        return dateA - dateB;
      });

      // Remove only maxRecords from the beginning
      this._inMemoryStore = this._inMemoryStore.slice(maxRecords);

      logger.info(`Purged ${maxRecords} oldest records (limited by maxRecords)`);
      return maxRecords;
    } else {
      // Replace with filtered records
      this._inMemoryStore = filteredRecords;

      logger.info(`Purged ${purgedCount} records older than ${olderThan}`);
      return purgedCount;
    }
  }
}

export default StateStorage;
