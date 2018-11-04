const AWS = require('aws-sdk')

const util = require('../util')
const comprehend = new AWS.Comprehend({ region: 'us-east-2' })

function getLine(line) {
    const split = line.split(/,(.+)/)
    const review_id = split[0]
    const text = util.getTextInQuotes(split[1])
    return { review_id, text }
}

function getHeader() {
    return 'review_id,sentiment,sentiment_positive,sentiment_negative,sentiment_neutral,sentiment_mixed\n'
}

function setLine(line, comprehend) {
    return util.csvFromArray([
        line.review_id,
        comprehend.Sentiment,
        comprehend.SentimentScore.Positive,
        comprehend.SentimentScore.Negative,
        comprehend.SentimentScore.Neutral,
        comprehend.SentimentScore.Mixed
    ])
}

exports.details = {}
exports.details.comprehendCount = exports.details.comprehendBytes = 0
async function batchComprehend(lines) {
    exports.details.comprehendCount++

    // async request
    const params = {
        TextList: lines.map(l => l.text),
        LanguageCode: 'en'
    }
    try {
        const data = await comprehend
            .batchDetectSentiment(params)
            .promise()
        return data.ResultList
            .map(result => setLine(lines[result.Index], result))
            .join('\n')
    } catch (err) {
        console.error(`Error with batch comprehend:`, err)
    }
}

var reviews = []
exports.process = async (line, lineNumber) => {
    if (lineNumber === 1) return getHeader()
    line = getLine(line)
    exports.details.comprehendBytes += Buffer.byteLength(line.text)

    // queue for comprehend
    reviews.push(line)
    if (reviews.length === 25) {
        const lines = await batchComprehend(reviews)
        reviews = []
        return lines + '\n'
    }
}

exports.close = async () => {
    if (reviews.length) {
        return await batchComprehend(reviews)
    }
}