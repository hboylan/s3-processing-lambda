const fs = require('fs')
const readline = require('readline')

const keywords = require('keyword-extractor')
const LanguageDetect = require('languagedetect')
const AWS = require('aws-sdk')

const util = require('./util')
const lang = new LanguageDetect()
const comprehend = new AWS.Comprehend({ region: 'us-east-2' })
const input = process.env.INPUT_FILE
const output = process.env.OUTPUT_FILE

var w = fs.createWriteStream(output, {flags:'a'})
var r = readline.createInterface({
    input: fs.createReadStream(input)
})

function processLine(line) {
    line = JSON.parse(line)
    line.text = line.text.replace(/\\n|\\r/g, ' ')
    line.text = line.text.replace(/\s+/g, ' ')
    return line
}

var languages = {}
var bytesProcessed = 0
const MAX_BYTES = 12000
const countLanguage = (l = ['unknown']) => { languages[l[0]] = languages[l[0]] ? languages[l[0]] + 1 : 1 }

function writeLine(line, comprehend = { Sentiment: null, SentimentScore: { Positive: null, Negative: null, Neutral: null, Mixed: null } }) {
    line.s = comprehend.Sentiment ? comprehend.Sentiment.toLowerCase() : comprehend.Sentiment
    line.s_pos = comprehend.SentimentScore.Positive
    line.s_neg = comprehend.SentimentScore.Negative
    line.s_neu = comprehend.SentimentScore.Neutral
    line.s_mix = comprehend.SentimentScore.Mixed
    delete line.cool
    delete line.funny
    delete line.useful
    w.write(`${JSON.stringify(line)}\n`)
}

function isValidLanguage(line) {
    const language = lang.detect(line.text, 1)[0]

    // count languages
    countLanguage(language)

    // only use english/pidgin
    return (language && ['english', 'pidgin'].indexOf(language[0]) !== -1)
}

var comprehendCount = 0
function batchComprehend(lines) {
    comprehendCount++
    const params = {
        TextList: lines.map(l => l.text),
        LanguageCode: 'en'
    }
    comprehend.batchDetectSentiment(params, (err, data) => {
        if (err) {
            return console.error(err)
        }
        for (var result of data.ResultList) {
            writeLine(lines[result.Index], result)
        }
    })
}

function extractWords(line) {
    const text = line.text
        .replace(/[\+\-\/\,\.\!\?\$\^\*\#\@\=\%\_\\"\(\)\[\]\\0-9]|http[a-zA-Z]*/g, '')
        .toLowerCase()
    return keywords.extract(text, { language: 'english' })
}

var reviews = []
var lineCountTotal = lineCountSkipped = lineCountProcessed = 0
r.on('line', function(line) {
    lineCountTotal++
    console.log(line)
    const split = line.split(/,(.+)/)
    const id = split[0]
    console.log(split[1])
    const text = util.getTextInQuotes(split[1])
    console.log(text)
        // line = JSON.parse(line)
        // w.write(`${line.review_id},${line.word}\n`)
        lineCountProcessed++
    // bytesProcessed += byteLength
    // writeLine(line)

    // queue for comprehend
    // const bytes = bytesProcessed + byteLength
    // if (bytes < MAX_BYTES) {
    //     bytesProcessed = bytes
    //     reviews.push(line)
    //     if (reviews.length === 25) {
    //         batchComprehend(reviews)
    //         reviews = []
    //     }
    // }

    // // remaining queue for comprehend
    // else if (reviews.length) {
    //     writeLine(line)
    //     reviews.forEach(r => writeLine(r))
    //     reviews = []
    // }
})

r.on('close', function() {
})

process.on('exit', () => {
    console.log(`Processed output to ${output}`)
    console.log(`Total: ${lineCountTotal}`)
    console.log(`Skipped: ${lineCountSkipped}`)
    console.log(`Processed: ${lineCountProcessed}\n`)
    console.log(`Comprehend calls: ${comprehendCount}`)
    console.log(`Comprehend bytes: ${bytesProcessed}\n`)
    console.log(`Languages: ${JSON.stringify(languages)}`)
})