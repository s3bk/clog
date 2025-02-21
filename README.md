## Compressed Log

## What is this?

This is a collection of libraries and tools to effectively record, store, search and view access logs.

## Motivation

Plain text logs take up space and searching in them requires parsing the text, which is slower than it could be.  
Especially when the logs need to be searched on the server using a web-interface.

The idea is to be able to send large amounts of logs to the client and filter them there, reducing UI latency to milliseconds.

## Tricks

To archive this several tricks are used:

*   data is stored in a columnar format,
*   large common values and prefixes are de-duplicated,
*   each data column is compressed for storage and transmission,

## Demo

coming soon

## Filter syntax

### Number filter

*   `N`Match the given number N
*   `N ..`: match any numbers great or equal than N
*   `N .. M` match any numbers great or equal than N but less than M
*   `> N` match any number greater than N
*   `>= N`match the given number N and any higher value
*   `< N` match any number less than N
*   `<= N` match any number less or equal to N

### String syntax

*   `foo` matche the string “foo” (does not allow whitespaces, escapes or quotation marks)
*   `"foo bar"` matches the string “foo bar” (allows whitespaces and escape sequences)

### String filter

*   `S` matches the string S exactly
*   `S *` matches any string beginning with S
*   `* S` matches any string ending with S
*   `r"RE"` matches the regular expression “RE”

### Fields

The following fields can be searched for:

#### Numeric fields:

*   `port` the client's port
*   `status` response status

#### String fields:

*   `uri` the URI
*   `ua` user agent
*   `method` method
*   `referer`

#### Other fields:

*   `ip` The client ip (Ip filter)
*   `time` (time filter)

### Ip filter

*   `192.168.1.1` matches the given ip exactly
*   `192.168.*.*` matches any IP where the first byte is 192 and the seond byte is 168

### Time filter

The time filter can be build from durations, date and time:

#### Durations:

*   `1w` one week
*   `5d` 5 days
*   `10h` 10 hours
*   `5m` 5 minutes
*   `30s` 30 seconds

#### Date:

*   `2024-01-30` January 30, 2024

#### Time:

*   `09:00`
*   `09:05:30`

A date and time can be used to specify a point in time. If the time is omitted, 0:00 is assumed.

*   `2024-01-30 09:00` 9:00 on January 30, 2024
*   `2024-01-30` Midnight on January 30, 2024

A duration can be used to specify a point in time, relative to the current time.

*   `- 15m` 15 minutes ago
*   One or two points in time can be used to specify the time filter:
*   `2024-01-30 09:00:15 ..` matches any time starting at January 30, 2024,  9am and 15 seconds
*   `.. 2024-01-30 09:00` matches any time on before January 30, 2024, 9am
*   `2024-01-28 .. 2024-01-30` matches the time beginning on January 28, 2024 and before January 30, 2024
*   `-15m ..` matches time starting at 15 minutes ago
*   `-5h .. 1h` between one and five hours ago
*   \``2024-01-28 .. - 5min`between midnight on Jan. 28 2024 and 5 minutes ago

### Field Filters

Field filters have the form `F V` where F is the field name and V is a number, string, ip or time filter

*   `port 80` matches the field port with the number 80
*   `status 200 .. 300` matches the status field with numbers between 200 and 300
*   `time 2024-08-01` matches the time field with values on Aug. 1, 2024

#### Logical combinations

*   `F & G` matches entries that match F and G
*   `F | G` matches entries that match F or G (or both)
*   `F ^ G` matches entries that match F xor G (only F, or only G)
*   `A | (B & C)` matches if A matches, or when both B and C match.
