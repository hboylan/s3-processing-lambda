const fs = require('fs')
const readline = require('readline')
const writer = fs.createWriteStream(process.env.OUTPUT_FILE)
const reader = readline.createInterface({
    input: fs.createReadStream(process.env.INPUT_FILE)
})

writer.write('line,sentiment,sentiment_positive,sentiment_negative,sentiment_neutral,sentiment_mixed\n')
function writeLine(line) {
    writer.write(`${line.Line},${line.Sentiment},${line.SentimentScore.Positive},${line.SentimentScore.Negative},${line.SentimentScore.Neutral},${line.SentimentScore.Mixed}\n`)
}

var lineCount = 0
reader.on('line', function(line) {
    writeLine(JSON.parse(line))
    lineCount++
})

reader.on('close', function() {
    console.log(`Total: ${lineCount}`)
})