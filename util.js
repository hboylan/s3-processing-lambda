
exports.csvFromArray = array => array
    .map(value => {

        // only for strings
        if (typeof value === 'string') {

            // escape double quotes
            value = value.replace(/"/g, '""')

            // escape commas and double quotes
            if (value.match(/,|"/)) {
                value = `"${value}"`
            }
        }
        return value
    })
    .join(',')

exports.getTextInQuotes = text => {
    const matches = text.match(/"(.*?)"/)
    return matches ? matches[1] : text
}