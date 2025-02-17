import wasm, { FilterView, ScrollView } from "./pkg/client.js";
import { Client, PacketRange } from "./pkg/client.js";
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
    time: number
};
function init_ws(update: (c: Client, start: bigint, end: bigint) => void): Client {

    let ws = new WebSocket("ws://127.0.0.1:3000/ws");
    let client = new Client(ws);
    
    function handle_range(range: PacketRange) {
        console.log("recieved", range.start, range.end);
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
    scroll_by(client: Client, delta: number);
    render(client: Client);
}

function App() {
    const [list, updateList] = createSignal<JSXElement[]>([]);
    const [filtered, updateFilteredList] = createSignal<JSXElement[]>([]);

    const [filterStr, setFilterStr] = createSignal<string | null>(JSON.stringify({"Status": { "Range": [400, 600] }}));
    const [filterStrError, setFilterStrError] = createSignal<string | null>(null);

    let view: ScrollView | null = null;
    let filter_view: FilterView | null = null;
    let client: Client | null = null;
    onMount(() => wasm().then(() => {
        function update(c: Client, start: bigint, end: bigint) {
            if (end > 20) {
                view.scroll_to(end - 20n);
            }
            updateList(view.render(c));
            updateFilteredList(filter_view.render(c));
        }
        client = init_ws(update);
        function produce(n: bigint, e: BatchEntry): JSXElement {
            return <tr>
                <td>{e.uri}</td>
                <td>{e.status}</td>
                <td>{e.ip}</td>
                <td>{e.port}</td>
                <td>{e.ua}</td>
            </tr>
        }
        view = new ScrollView(produce, 20);
        filter_view = new FilterView(produce, 20);
        
        createEffect(() => {
            try {
                filter_view.set_filter(JSON.parse(filterStr()));
                updateFilteredList(filter_view.render(client));

                setFilterStrError(null);
            } catch(e) {
                console.log(e);
                setFilterStrError(e.message);
            }
        });
    }));

    
    const handleScroll = (event: WheelEvent, view: View | null, update: Setter<JSXElement[]>) => {
        let delta;
        switch (event.deltaMode) {
            case event.DOM_DELTA_PIXEL:
                delta = event.deltaY / 20;
                break;
            case event.DOM_DELTA_LINE:
            case event.DOM_DELTA_PAGE:
                delta = event.deltaY;
        }

        if (view !== null && client !== null) {
            view.scroll_by(client, delta);
            update(view.render(client));
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

function Table(p: JSX.HTMLAttributes<HTMLTableElement> & { list: Accessor<JSXElement[]> }) {
    const [{list}, tableProps] = splitProps(p, ["list"]);
    return <table {...tableProps}>
        <thead>
            <tr>
                <th>URI</th>
                <th>Status</th>
                <th>IP</th>
                <th>Port</th>
                <th>User Agent</th>
            </tr>
        </thead>
        <For each={list()}>
            {(e) => e}
        </For>
    </table>
}

render(() => (
    <App />
), document.body);
