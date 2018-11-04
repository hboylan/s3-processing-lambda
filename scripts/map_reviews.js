const fs = require('fs')
const readline = require('readline')
const writeTxt = fs.createWriteStream(process.env.OUTPUT_FILE_TXT)
const writeCsv = fs.createWriteStream(process.env.OUTPUT_FILE_CSV)
const reader = readline.createInterface({
    input: fs.createReadStream(process.env.INPUT_FILE)
})

function writeLine(line) {
    writeCsv.write(`${line.review_id},${lineCount}\n`)
    writeTxt.write(`${line.text}\n`)
}

var lineCount = 0
reader.on('line', function(line) {
    writeLine(JSON.parse(line))
    lineCount++
})

reader.on('close', function() {
    console.log(`Total: ${lineCount}`)
})