const fs = require('fs')
const readline = require('readline')

const util = require('./util')
const AWS = require('aws-sdk')
const input = process.env.INPUT_FILE
var outputTxt = process.env.OUTPUT_FILE_TXT
var outputCsv = process.env.OUTPUT_FILE_CSV
const MAX_BYTES_PER_FILE = 5000000 // 5MB
var bytesProcessed = 0
var fileParts = 0

const writeTxt = fs.createWriteStream(outputTxt)
const writeCsv = fs.createWriteStream(outputCsv)

function getWriteStream() {
    // const nameSplit = output.split('.')
    // const name = `${nameSplit[0]}_${fileParts}.${nameSplit[1]}`
    return fs.createWriteStream(output, {flags:'a'})
}

var businesses = {}
readline.createInterface({
    input: fs.createReadStream('results.csv')
})
.on('line', line => {
    businesses[line] = true
})
.on('close', () => {
    var r = readline.createInterface({
        input: fs.createReadStream(input)
    })

    function write(string) {
        bytesProcessed += Buffer.byteLength(string)
        // if (bytesProcessed >= MAX_BYTES_PER_FILE) {
        //     console.log('finished writing file:', fileParts)
        //     bytesProcessed = 0
        //     fileParts++
        //     w = getWriteStream()
        // }
        writeCsv.write(string)
    }

    var textBytesProcessed = 0
    function writeLine(line) {
        var string = ''
        if (bytesProcessed === 0) {
            // string += 'review_id,text\n'
        }
        textBytesProcessed += Buffer.byteLength(line.text)
        string += `${util.csvFromArray([line.review_id, lineCountTotal, line.text])}\n`
        writeCsv.write(string)
        writeTxt.write(line.text)
    }

    var lineCountTotal = lineCountSkipped = lineCountProcessed = 0
    r.on('line', function(line) {
        if (lineCountTotal > 25) {
            return r.close()
        }
        lineCountTotal++
        line = JSON.parse(line)
        
        if (businesses[line.business_id]) {
            lineCountProcessed++
            return writeLine(line)
        }
        lineCountSkipped++
    })

    r.on('close', function() {
        console.log(`Processed output to ${output}`)
        console.log(`Total: ${lineCountTotal}`)
        console.log(`Skipped: ${lineCountSkipped}`)
        console.log(`Processed: ${lineCountProcessed}`)
        console.log(`Bytes: ${bytesProcessed}`)
        console.log(`Bytes text: ${textBytesProcessed}`)
    })
})