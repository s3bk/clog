# Compressed Log

## What is this?
This is a collection of libraries and tools to effectively record, store, search and view access logs.

## Motivation
Plain text logs take up space and searching in them requires parsing the text, which is slower than it could be.
Especially when the logs need to be searched on the server using a web-interface.

The idea is to be able to send large amounts of logs to the client and filter them there, reducing UI latency to milliseconds.

## Tricks
To archive this several tricks are used:
 - data is stored in a columnar format,
 - large common values and prefixes are de-duplicated,
 - each data column is compressed for storage and transmission,

## Demo
coming soon
