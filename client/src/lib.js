export function make_entry(status, method, uri, ua, referer, ip, port, time, body, headers, host, proto, location, tls_fp) {
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
        proto,
        location,
        tls_fp
    };
}
