const stream = require('stream')
const readline = require('readline')
const AWS = require('aws-sdk')
const S3 = new AWS.S3()

const processors = require('./processors')

function createReadline(Bucket, Key) {

    // s3 read stream
    const input = S3
        .getObject({
            Bucket,
            Key
        })
        .createReadStream()

    // node readline with stream
    return readline
        .createInterface({
            input,
            terminal: false
        })
}

function createWriteStream(Bucket, Key) {
    const writeStream = new stream.PassThrough()
    const uploadPromise = S3
        .upload({
            Bucket,
            Key,
            Body: writeStream
        })
        .promise()
    return { writeStream, uploadPromise }
}

async function processLine(processor, line, totalLineCount) {
    const result = processors[processor].process(line, totalLineCount)
    if (typeof result.then === 'function') {
        return await result
    }
    return result
}

exports.handler = (event, context) => {
    console.log(JSON.stringify(event, null, 2))
    var totalLineCount = 0
    var totalProcessingCount = 0

    // create input stream from S3
    const readStream = createReadline(event.inputBucket, event.inputKey)

    // create output stream to S3
    const { writeStream, uploadPromise } = createWriteStream(event.outputBucket, event.outputKey)

    // create close function
    async function close() {
        
        // close processor
        const p = processors[event.processor]
        const processorClose = await p.close()
        if (processorClose) {
            writeStream.write(processorClose)
        }

        // end write stream
        writeStream.end()
    
        // wait for upload
        const uploadResponse = await uploadPromise
    
        // return processing insights
        context.succeed({
            totalLineCount,
            uploadResponse,
            processorDetails: p.details
        })
    }

    // read each line
    readStream.on('line', async line => {
        totalLineCount++

        try {
            totalProcessingCount++
            line = await processLine(event.processor, line, totalLineCount)
            if (line) {
                writeStream.write(line)
            }
            totalProcessingCount--
            
            // finished
            if (totalProcessingCount === 0) {
                close()
            }
        } catch (err) {
            console.error(`Error processing line:`, err)
        }
    })
}