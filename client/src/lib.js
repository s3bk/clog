export function make_entry(status, method, uri, ua, referer, ip, port, time, body, headers, host, proto) {
    return {
        status,
        method,
        uri,
        ua,
        referer,
        ip,
        port,
        time,
        body,
        headers,
        host,
        proto
    };
}
