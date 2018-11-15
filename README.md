# S3 Processing Lambda

Efficiently process files line by line from S3 using Lambda function

## Introduction

This processing tool is useful to clean and flatten data before loading it into a data warehouse. It can efficiently process source files from S3 by line to limit the amount of memory used by the serverless function.

## Requirements

1. NodeJS
2. ClaudisJS
3. AWS account
    - S3
    - Lambda

## Build

- Clone repository:<br/>
`git clone git@github.com:hboylan/s3-processing-lambda.git`
- Create serverless function:<br/>
`npm run create`
- Create event configuration:<br/>
`cp events/flattenBusinesses.json events/myProcessingEvent.json`
- Execute serverless function:<br/>
`claudia test-lambda --event events/myProcessingEvent.json`

## Configuration

<table>
    <tr>
        <th>Key</th>
        <th>Required</th>
        <th>Notes</th>
    </tr>
    <tr>
        <td><tt>inputBucket</tt></td>
        <td>Yes</td>
        <td>Input bucket for source data</td>
    </tr>
    <tr>
        <td><tt>inputKey</tt></td>
        <td>Yes</td>
        <td>Input key for source data file</td>
    </tr>
    <tr>
        <td><tt>outputBucket</tt></td>
        <td>Yes</td>
        <td>Output bucket for destination data</td>
    </tr>
    <tr>
        <td><tt>outputKey</tt></td>
        <td>Yes</td>
        <td>Output key for destination data file</td>
    </tr>
    <tr>
        <td><tt>processor</tt></td>
        <td>Yes</td>
        <td>Processing function located in <tt>processors/</tt></td>
    </tr>
</table>