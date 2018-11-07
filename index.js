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

function processLine(processor, line, totalLineCount) {
    return processors[processor].process(line, totalLineCount)
}

exports.handler = (event, context) => {
    console.log(JSON.stringify(event, null, 2))
    var totalLineCount = 0

    // create input stream from S3
    const readStream = createReadline(event.inputBucket, event.inputKey)

    // create output stream to S3
    const { writeStream, uploadPromise } = createWriteStream(event.outputBucket, event.outputKey)

    // read each line
    readStream.on('line', line => {
        totalLineCount++
        line = processLine(event.processor, line, totalLineCount)
        console.log(`Line #${totalLineCount}: ${line}`)
        if (line) {
            writeStream.write(line)
        }
    })

    // end stream
    readStream.on('end', async () => {

        // end write stream
        writeStream.end()
    
        // wait for upload
        const uploadResponse = await uploadPromise
    
        // return processing insights
        context.succeed({
            totalLineCount,
            uploadResponse
        })
    })
}