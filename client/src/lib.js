export function make_entry(status, method, uri, ua, referer, ip, port, time) {
    return {
        status,
        method,
        uri,
        ua,
        referer,
        ip,
        port,
        time
    };
}
