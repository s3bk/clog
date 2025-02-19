import wasm, { FilterView, ScrollView, Client, PacketRange } from "./pkg/client.js";
import { Accessor, createEffect, createSignal, For, JSX, JSXElement, onCleanup, onMount, Setter, splitProps } from "solid-js";
import { render } from "solid-js/web";


type BatchEntry = {
    status: number,
    method: string,
    uri: string,
    ua: string | null,
    referer: string | null,
    ip: string,
    port: number,
    time: string
};

function init_ws(update: (c: Client, start: bigint, end: bigint) => void): Client {
    let ws = new WebSocket("ws://127.0.0.1:3000/ws");
    let client = new Client(ws);
    
    function handle_range(range: PacketRange) {
        update(client, range.start, range.end);
        range.free();
    }

    ws.addEventListener("open", (e) => client.on_open(e));
    ws.addEventListener("message", (e) => {
        let r = client.on_message(e);
        if (r !== undefined) {
            handle_range(r);
        }
    });
    ws.addEventListener("error", (e) => console.log(e));
    return client;
}

interface View {
    pos(): bigint;
    scroll_by(client: Client, delta: number): boolean;
    scroll_to_end(client: Client);
    render(client: Client);
}


function App() {
    const [list, updateList] = createSignal<JSXElement[]>([]);
    const [filtered, updateFilteredList] = createSignal<JSXElement[]>([]);

    const [filterStr, setFilterStr] = createSignal<string | null>("status 400..500");
    const [filterStrError, setFilterStrError] = createSignal<string | null>(null);

    let view: ScrollView | null = null;
    let filter_view: FilterView | null = null;
    let client: Client | null = null;
    let follow = true;

    onMount(() => wasm({
        
    }).then(() => {
        function update(c: Client, start: bigint, end: bigint) {
            if (follow) {
                view.scroll_to_end(c);
                filter_view.scroll_to_end(c);
            }
            updateList(view.render(c));
            updateFilteredList(filter_view.render(c));
        }
        function produce(n: bigint, e: BatchEntry): JSXElement {
            return <tr>
                <td>{n.toString()}</td>
                <td>{e.time}</td>
                <td>{e.status}</td>
                <td>{e.uri}</td>
                <td>{e.ip}</td>
                <td>{e.port}</td>
                <td>{e.ua}</td>
            </tr>
        }
        client = init_ws(update);
        view = new ScrollView(produce, 20);
        filter_view = new FilterView(produce, 20);
        
        createEffect(() => {
            try {
                filter_view.set_filter(filterStr());
                updateFilteredList(filter_view.render(client));

                setFilterStrError(null);
            } catch(e) {
                console.log(e);
                setFilterStrError(display_error(e));
            }
        });
    }));

    let lastTarget = null;
    let acc = 0.0;
    const handleScroll = (event: WheelEvent, view: View | null, update: Setter<JSXElement[]>) => {
        if (view != lastTarget) {
            acc = 0.0;
            lastTarget = view;
        }
        let delta;
        switch (event.deltaMode) {
            case event.DOM_DELTA_PIXEL:
                delta = event.deltaY / 20;
                break;
            case event.DOM_DELTA_LINE:
            case event.DOM_DELTA_PAGE:
                delta = event.deltaY;
        }
        acc += delta;
        const by = Math.round(acc);
        if (by != 0 && view !== null && client !== null) {
            const old_pos = view.pos();
            follow = view.scroll_by(client, by) && by > 0;
            update(view.render(client));
            acc -= by;

            if (by < 0) {
                follow = false;
            } else if (by > 0) {
                const new_pos = view.pos();
                if (old_pos === new_pos) {
                    follow = true;
                }
            }
        }
        event.preventDefault();
    };
    return <div>
        <Table on:wheel={(e) => handleScroll(e, view, updateList)} list={list} />

        <input type="text" value={filterStr()} oninput={(e) => setFilterStr(e.target.value)} />
        <div>{filterStrError()}</div>
        <Table on:wheel={(e) => handleScroll(e, filter_view, updateFilteredList)} list={filtered} />
    </div>
}

function display_error(e: string | Error): string {
    if (typeof e == "string") return e;
    return e.message;
}

function Table(p: JSX.HTMLAttributes<HTMLDivElement> & { list: Accessor<JSXElement[]> }) {
    const [{list}, tableProps] = splitProps(p, ["list"]);
    return <div class="table" {...tableProps}>
        <table>
        <thead>
            <tr>
                <th>N</th>
                <th>Time</th>
                <th>Status</th>
                <th>URI</th>
                <th>IP</th>
                <th>Port</th>
                <th>User Agent</th>
            </tr>
        </thead>
        <tbody>
            <For each={list()}>
                {(e) => e}
            </For>
        </tbody>
    </table>
    </div>
}

render(() => (
    <App />
), document.body);
