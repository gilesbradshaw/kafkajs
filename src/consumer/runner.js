const limitConcurrency = require('../utils/concurrency')
const { KafkaJSError } = require('../errors')
const {
  events: { GROUP_JOIN, FETCH, FETCH_START, START_BATCH_PROCESS, END_BATCH_PROCESS },
} = require('./instrumentationEvents')

const isTestMode = process.env.NODE_ENV === 'test'

const isRebalancing = e =>
  e.type === 'REBALANCE_IN_PROGRESS' || e.type === 'NOT_COORDINATOR_FOR_GROUP'

const isKafkaJSError = e => e instanceof KafkaJSError

module.exports = class Runner {
  constructor({
    logger,
    consumerGroup,
    instrumentationEmitter,
    eachBatchAutoResolve = true,
    partitionsConsumedConcurrently,
    eachBatch,
    eachMessage,
    heartbeatInterval,
    onCrash,
    retry,
    autoCommit = true,
  }) {
    this.logger = logger.namespace('Runner')
    this.consumerGroup = consumerGroup
    this.instrumentationEmitter = instrumentationEmitter
    this.eachBatchAutoResolve = eachBatchAutoResolve
    this.eachBatch = eachBatch
    this.eachMessage = eachMessage
    this.heartbeatInterval = heartbeatInterval
    this.onCrash = onCrash
    this.autoCommit = autoCommit
    this.partitionsConsumedConcurrently = partitionsConsumedConcurrently

    this.running = false
    this.consuming = false
  }

  async join() {
    const startJoin = Date.now()
    await this.consumerGroup.join()
    await this.consumerGroup.sync()

    this.running = true

    const memberAssignment = this.consumerGroup
      .assigned()
      .reduce((result, { topic, partitions }) => ({ ...result, [topic]: partitions }), {})

    const payload = {
      groupId: this.consumerGroup.groupId,
      memberId: this.consumerGroup.memberId,
      leaderId: this.consumerGroup.leaderId,
      isLeader: this.consumerGroup.isLeader(),
      memberAssignment,
      groupProtocol: this.consumerGroup.groupProtocol,
      duration: Date.now() - startJoin,
    }

    this.instrumentationEmitter.emit(GROUP_JOIN, payload)
    this.logger.info('Consumer has joined the group', payload)
  }

  async scheduleJoin() {
    if (!this.running) {
      this.logger.debug('consumer not running, exiting', {
        groupId: this.consumerGroup.groupId,
        memberId: this.consumerGroup.memberId,
      })
      return
    }

    return this.join().catch(this.onCrash)
  }

  async start() {
    if (this.running) {
      return
    }

    try {
      await this.consumerGroup.connect()
      await this.join()

      this.running = true
      while (true) {
        await this.scheduleFetch()
      }
    } catch (e) {
      throw e
      this.onCrash(e)
    }
  }

  async stop() {
    if (!this.running) {
      return
    }

    this.logger.debug('stop consumer group', {
      groupId: this.consumerGroup.groupId,
      memberId: this.consumerGroup.memberId,
    })

    this.running = false

    try {
      if (!isTestMode) {
        await this.waitForConsumer()
      }
      await this.consumerGroup.leave()
    } catch (e) {}
  }

  waitForConsumer() {
    return new Promise(resolve => {
      const scheduleWait = () => {
        this.logger.debug('waiting for consumer to finish...', {
          groupId: this.consumerGroup.groupId,
          memberId: this.consumerGroup.memberId,
        })

        setTimeout(() => (!this.consuming ? resolve() : scheduleWait()), 1000)
      }

      if (!this.consuming) {
        return resolve()
      }

      scheduleWait()
    })
  }

  async processEachMessage(batch) {
    const { topic, partition } = batch

    for (const message of batch.messages) {
      if (!this.running || this.consumerGroup.hasSeekOffset({ topic, partition })) {
        break
      }

      try {
        await this.eachMessage({ topic, partition, message })
      } catch (e) {
        console.log('ERRORRRRR')
        if (!isKafkaJSError(e)) {
          this.logger.error(`Error when calling eachMessage`, {
            topic,
            partition,
            offset: message.offset,
            stack: e.stack,
          })
        }

        // In case of errors, commit the previously consumed offsets
        await this.consumerGroup.commitOffsets()
        throw e
      }

      this.consumerGroup.resolveOffset({ topic, partition, offset: message.offset })
      await this.consumerGroup.heartbeat({ interval: this.heartbeatInterval })
      await this.consumerGroup.commitOffsetsIfNecessary()
    }
  }

  async processEachBatch(batch) {
    const { topic, partition } = batch

    try {
      await this.eachBatch({
        batch,
        resolveOffset: offset => {
          this.consumerGroup.resolveOffset({ topic, partition, offset })
        },
        heartbeat: async () => {
          await this.consumerGroup.heartbeat({ interval: this.heartbeatInterval })
        },
        /**
         * Commit offsets if provided. Otherwise commit most recent resolved offsets
         * if the autoCommit conditions are met.
         *
         * @param {OffsetsByTopicPartition} [offsets] Optional.
         */
        commitOffsetsIfNecessary: async offsets => {
          return offsets
            ? this.consumerGroup.commitOffsets(offsets)
            : this.consumerGroup.commitOffsetsIfNecessary()
        },
        uncommittedOffsets: () => this.consumerGroup.uncommittedOffsets(),
        isRunning: () => this.running,
        isStale: () => this.consumerGroup.hasSeekOffset({ topic, partition }),
      })
    } catch (e) {
      if (!isKafkaJSError(e)) {
        this.logger.error(`Error when calling eachBatch`, {
          topic,
          partition,
          offset: batch.firstOffset(),
          stack: e.stack,
        })
      }

      // eachBatch has a special resolveOffset which can be used
      // to keep track of the messages
      await this.autoCommitOffsets()
      throw e
    }

    // resolveOffset for the last offset can be disabled to allow the users of eachBatch to
    // stop their consumers without resolving unprocessed offsets (issues/18)
    if (this.eachBatchAutoResolve) {
      this.consumerGroup.resolveOffset({ topic, partition, offset: batch.lastOffset() })
    }
  }

  async fetch() {
    const startFetch = Date.now()

    this.instrumentationEmitter.emit(FETCH_START, {})

    const batches = await this.consumerGroup.fetch()

    this.instrumentationEmitter.emit(FETCH, {
      numberOfBatches: batches.length,
      duration: Date.now() - startFetch,
    })

    const onBatch = async batch => {
      const startBatchProcess = Date.now()
      const payload = {
        topic: batch.topic,
        partition: batch.partition,
        highWatermark: batch.highWatermark,
        offsetLag: batch.offsetLag(),
        /**
         * @since 2019-06-24 (>= 1.8.0)
         *
         * offsetLag returns the lag based on the latest offset in the batch, to
         * keep the event backward compatible we just introduced "offsetLagLow"
         * which calculates the lag based on the first offset in the batch
         */
        offsetLagLow: batch.offsetLagLow(),
        batchSize: batch.messages.length,
        firstOffset: batch.firstOffset(),
        lastOffset: batch.lastOffset(),
      }

      this.instrumentationEmitter.emit(START_BATCH_PROCESS, payload)

      if (this.eachMessage) {
        await this.processEachMessage(batch)
      } else if (this.eachBatch) {
        await this.processEachBatch(batch)
      }

      this.instrumentationEmitter.emit(END_BATCH_PROCESS, {
        ...payload,
        duration: Date.now() - startBatchProcess,
      })
    }

    const concurrently = limitConcurrency({ limit: this.partitionsConsumedConcurrently })
    await Promise.all(
      batches.map(batch =>
        concurrently(async () => {
          if (!this.running) {
            return
          }

          if (batch.isEmpty()) {
            return
          }

          await onBatch(batch)
        })
      )
    )

    await this.autoCommitOffsets()
    await this.consumerGroup.heartbeat({ interval: this.heartbeatInterval })
  }

  async scheduleFetch() {
    if (!this.running) {
      this.logger.debug('consumer not running, exiting', {
        groupId: this.consumerGroup.groupId,
        memberId: this.consumerGroup.memberId,
      })
      return
    }
    this.consuming = true
    await this.fetch()
    this.consuming = false
  }

  autoCommitOffsets() {
    if (this.autoCommit) {
      return this.consumerGroup.commitOffsets()
    }
  }

  autoCommitOffsetsIfNecessary() {
    if (this.autoCommit) {
      return this.consumerGroup.commitOffsetsIfNecessary()
    }
  }

  commitOffsets(offsets) {
    if (!this.running) {
      this.logger.debug('consumer not running, exiting', {
        groupId: this.consumerGroup.groupId,
        memberId: this.consumerGroup.memberId,
        offsets,
      })
      return
    }
    this.consumerGroup.commitOffsets(offsets)
  }
}
