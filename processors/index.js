const fs = require('fs')

const processors = {}
fs
    .readdirSync(__dirname)
    .forEach(file => {
        if (file === 'index.js') return
        const processor = require(`./${file}`)
        const processorName = file.split('.')[0]
        processors[processorName] = processor
    })

module.exports = processors