export function make_entry(status, method, uri, ua, referer, ip, port, time, body) {
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
    };
}
