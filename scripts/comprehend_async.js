const fs = require('fs')
const LineByLineReader = require('line-by-line')
const comprehendReview = require('../processors/comprehendReview')

const input = process.env.INPUT_FILE
const output = process.env.OUTPUT_FILE
const reader = new LineByLineReader(input)
const writer = fs.createWriteStream(output)

setInterval(() => {
    const d = comprehendReview.details
    console.log(`Total: ${count}, Batches: ${d.comprehendCount}, Batches/s: ${d.comprehendCount - lastCount}`)
    lastCount = d.comprehendCount
}, 1000)

var count = lastCount = 0
reader.on('line', async line => {
    reader.pause()
    count++

    const sentiments = await comprehendReview.process(line, count)
    reader.resume()
    
    if (sentiments) {
        writer.write(sentiments)
    }
})

reader.on('end', async () => {
    const sentiments = await comprehendReview.close()
    if (sentiments) {
        writer.write(sentiments)
    }
    console.log(`Count:`, count)
    console.log(`Comprehend details:`, comprehendReview.details)
    writer.end()
})