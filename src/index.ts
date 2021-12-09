#!/usr/bin/env node

import { Bee, BeeError, BeeRequest, BeeResponse, FeedWriter, Reference, Utils } from '@ethersphere/bee-js'
import { ChunkReference } from '@ethersphere/bee-js/dist/src/feed'
import { FetchFeedUpdateResponse } from '@ethersphere/bee-js/dist/src/modules/feed'
import ora from 'ora'
import yargs from 'yargs'
import { feedIndexBeeResponse, incrementBytes, makeBytes, randomByteArray } from './utils'
import crypto from 'crypto'
import fs from 'fs'

const zeros64 = '0000000000000000000000000000000000000000000000000000000000000000'
const syncPollingTime = 1000 //in ms
const syncPollingTrials = 15

export const testIdentity = {
  privateKey: '634fb5a872396d9693e5c9f9d7233cfa93f395c093371017ff44aa9ae6564cdd',
  publicKey: '03c32bb011339667a487b6c1c35061f15f7edc36aa9a0f8648aba07a4b8bd741b4',
  address: '8d3766440f0d7b949a5e32995d09619a7f86e632',
}

function formatDateTime(date = new Date()) {
  return date.toISOString().replace('T', ' ').slice(0, 19)
}

/**
 * Checks whether the fetched resource is the same as expected
 *
 * @returns false if there were no errors or the error message
 */
function fetchDataCheck(
  updateFetch: FetchFeedUpdateResponse,
  expectedFeedRef: ChunkReference,
  expectedFeedIndex: number,
  beeNodeUrl: string
): number {
  // const beeFeedIndex = feedIndexBeeResponse(expectedFeedIndex)
  // const feedRef =  Utils.bytesToHex(expectedFeedRef)

  // if(updateFetch.feedIndex !== beeFeedIndex || feedRef !== updateFetch.reference) {
  //   return `\tDownloaded feed payload or index has not the expected result at Bee node "${beeNodeUrl}".`
  //     + `\n\t\tindex| expected: "${beeFeedIndex}" got: "${updateFetch.feedIndex}"`
  //     + `\n\t\treference| expected: "${feedRef}" got: "${updateFetch.reference}"`
  // }
  const feedIndex = parseInt(updateFetch.feedIndex, 16)
  if (feedIndex !== expectedFeedIndex) {
    console.error(`Feed index mismatch on ${beeNodeUrl}`, { feedIndex, expectedFeedIndex })
  }

  return feedIndex
}

async function waitSyncing(bee: Bee, tagUid: number): Promise<void | never> {
  const pollingTime = syncPollingTime
  const pollingTrials = syncPollingTrials
  let synced = false
  let syncStatus = 0

  for (let i = 0; i < pollingTrials; i++) {
    const tag = await bee.retrieveTag(tagUid)

    if (syncStatus !== tag.synced) {
      i = 0
      syncStatus = tag.synced
    }

    if (syncStatus >= tag.total) {
      synced = true
      // FIXME: after successful syncing the chunk is still not available.
      await new Promise(resolve => setTimeout(resolve, 500))
      break
    }
  }

  if (!synced) {
    throw new Error('Data syncing timeout.')
  }
}

interface MeasureAyncReturnable {
  measuredTime: number,
  returnValue: any,
}

async function feedWriterUpload(feedWriter: FeedWriter, stamp: string, reference: ChunkReference) {
  try {
    const response = await feedWriter.upload(stamp, reference)
    return response
  } catch (e: any) {
    if (e.status === 409) {
      return reference
    }
  }
}

async function measureAync(hookFunction: () => Promise<any>): Promise<MeasureAyncReturnable> {
  let startTime = new Date().getTime()
  const returnValue = await hookFunction()
  return {
    returnValue,
    measuredTime: new Date().getTime() - startTime
  }
}

/** Used for console log */
function beeWriterResults(urls: string[], measuredTimes: number[]): string {
  const results: string[] = []
  urls.forEach((url, i) => {
    const time = measuredTimes[i]
    results.push(`\tUpload Time on "${url}": ${time / 1000}s`)
  })

  return results.join('\n')
}

/** Used for console log */
function beeReaderResults(urls: string[], measuredTimes: number[]): string {
  const results: string[] = []
  urls.forEach((url, i) => {
    const time = measuredTimes[i]
    results.push(`\tFetch Time on "${url}": ${time / 1000}s`)
  })

  return results.join('\n')
}

function onRequest(request: BeeRequest) {
  // console.debug({ request })
}

function onResponse(response: BeeResponse) {
  // console.debug({ response })
}

function sleep(waitTime: number) {
  return new Promise(resolve => setTimeout(resolve, waitTime))
}

function makeDownloadReport(downloads: MeasureAyncReturnable[], beeReaderUrls: string[], reference: ChunkReference, index: number) {
  const downloadTimes: number[] = []

  // check data correctness
  const checks: number[] = []
  downloads.forEach((download, j) => {
    downloadTimes.push(download.measuredTime)

    const url = beeReaderUrls[j]
    checks.push(fetchDataCheck(download.returnValue, reference, index, url))
  })

  return {
    downloadTimes,
    checks,
  }
}

// eslint-disable-next-line @typescript-eslint/no-extra-semi
;(async function root() {
  const argv = await yargs(process.argv.slice(2))
    .usage('Usage: <some STDOUT producing command> | $0 [options]')
    .option('bee-writer', {
      alias: 'bw',
      type: 'array',
      describe: 'Writer Bee node URL. By default Gateway 7-9 are used.',
      default: [
        'https://bee-7.gateway.ethswarm.org',
        'https://bee-8.gateway.ethswarm.org',
        'https://bee-9.gateway.ethswarm.org',
      ]
    })
    .option('bee-reader', {
      alias: 'br',
      type: 'array',
      describe: 'Reader Bee node URL. By default Gateway 4-6 are used.',
      default: [
        'https://bee-4.gateway.ethswarm.org',
        'https://bee-5.gateway.ethswarm.org',
        'https://bee-6.gateway.ethswarm.org',
      ]
    })
    .option('stamp', {
      alias: 'st',
      type: 'array',
      describe: 'Postage Batch Stamp ID for bee-writers. By default it is array of zeros',
      default: [
        zeros64,
        zeros64,
        zeros64
      ]
    })
    .option('updates', {
      alias: 'x',
      type: 'number',
      describe: 'How many updates the script will do',
      default: 2
    })
    .option('topic-seed', {
      alias: 't',
      type: 'number',
      describe: 'From what seed the random topic will be generated',
      default: 10
    })
    .option('download-iteration', {
      alias: 'di',
      type: 'number',
      describe: 'Attempt to download the feed from the other Bee client on every given amount of feed update',
      default: 1
    })
    .option('sync-time', {
      alias: 's',
      type: 'number',
      describe: 'Time to wait until data is synced in seconds',
      default: 40
    })
    .option('wait-time', {
      alias: 'w',
      type: 'number',
      describe: 'Time to wait between updates in seconds',
      default: 3,
    })
    .help('h')
    .alias('h', 'help').epilog(`Testing Ethereum Swarm Feed lookup time`).argv

  const beeWriterUrls = process.env.BEE_API_URLS?.split(',') || argv['bee-writer']
  const beeReaderUrls = process.env.BEE_PEER_API_URL?.split(',') || argv['bee-reader']
  const stamps = process.env.BEE_STAMP?.split(',') || argv.stamp
  const updates = argv.updates
  const topicSeed = argv['topic-seed']
  const downloadIteration = argv['download-iteration']
  if(downloadIteration > updates) {
    throw new Error(`Download iteration ${downloadIteration} is higher than the feed update count: ${updates}`)
  }
  if(stamps.length !== beeWriterUrls.length) {
    throw new Error(`Got different amount of bee writer ${beeWriterUrls.length} than stamps ${stamps.length}`)
  }
  const syncTime = argv['sync-time'] * 1000
  const waitTime = argv['wait-time'] * 1000

  const beeWriters: Bee[] = beeWriterUrls.map(url => new Bee(url, { onRequest, onResponse }))
  const beeReaders: Bee[] = beeReaderUrls.map(url => new Bee(url))

  const report: any = {}
  report.startDate = formatDateTime()

  // const topic = randomByteArray(32, topicSeed)
  const topic = crypto.randomBytes(32)
  report.topic = Utils.bytesToHex(topic)

  const feedWriters = beeWriters.map(beeWriter => beeWriter.makeFeedWriter('sequence', topic, testIdentity.privateKey))
  const feedReaders = beeReaders.map(beeReader => beeReader.makeFeedReader('sequence', topic, testIdentity.address))

  const manifestReference = await beeWriters[0].createFeedManifest(stamps[0], 'sequence', topic, feedWriters[0].owner)
  console.log({ report, manifestReference })

  // reference that the feed refers to
  const reference = makeBytes(32) // all zeroes
  let downloadIterationIndex = 0

  for(let i = 0; i < updates; i++) {
    console.log(`Upload feed for index ${i}`)

    // create tag for the full sync
    // const tag = await beeWriter.createTag()
    // await feedWriter.upload(stamp, reference, { tag: tag.uid })
    const uploads = await Promise.all(feedWriters.map((feedWriter, i) => {
      const stamp = stamps[i]
      return measureAync(() => feedWriterUpload(feedWriter, stamp, reference))
    }))
    const uploadTimes = uploads.map(upload => upload.measuredTime)
    console.log(`Waiting for ${Math.floor(waitTime / 1000)} secs`)
    await sleep(waitTime)
    incrementBytes(reference)
  }
  {
    downloadIterationIndex = 0

    const index = updates - 1
    console.log(`Waiting for ${Math.floor(syncTime / 1000)} secs`)
    await sleep(syncTime)

    console.log(`Download feed for index ${index}`)

    const downloads = await Promise.all(feedReaders.map(feedReader => measureAync(() => feedReader.download())))
    let { checks, downloadTimes } = makeDownloadReport(downloads, beeReaderUrls, reference, index)

    const reportLine = [report.startDate, report.topic, index, ...checks, ...downloadTimes].join(',') + '\n'
    console.log(reportLine)
    fs.appendFileSync('report.csv', reportLine)

    while (!checks.every(value => value === index)) {
      await sleep(waitTime)
      const downloadsAgain = await Promise.all(feedReaders.map(feedReader => measureAync(() => feedReader.download())))
      const again = makeDownloadReport(downloadsAgain, beeReaderUrls, reference, index)
      checks = again.checks
      console.log('After check again: ', {checks, downloadTimes})
    }

  }
})().catch(error => console.error({ error }))
