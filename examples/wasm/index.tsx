import * as React from "react";
import { useState, useEffect } from "react";
import * as ReactDOM from "react-dom";
import { useDropzone } from "react-dropzone";

const CID_KEY = "/cid/default";
const ADDR_KEY = "/maddr/default";
const MAX_CHUNK_SIZE = 262144;

function fileIterator(file: File): AsyncIterable<Uint8Array> {
  let index = 0;

  const iterator = {
    next: (): Promise<IteratorResult<Uint8Array>> => {
      if (index > file.size) {
        return Promise.resolve({
          done: true,
          value: null,
        });
      }

      return new Promise((resolve, reject) => {
        const chunk = file.slice(index, (index += MAX_CHUNK_SIZE));

        const reader = new global.FileReader();

        const handleLoad = (ev) => {
          // @ts-ignore No overload matches this call.
          reader.removeEventListener("loadend", handleLoad, false);

          if (ev.error) {
            return reject(ev.error);
          }

          resolve({
            done: false,
            value: new Uint8Array(reader.result as ArrayBuffer),
          });
        };

        // @ts-ignore No overload matches this call.
        reader.addEventListener("loadend", handleLoad);
        reader.readAsArrayBuffer(chunk);
      });
    },
  };

  return {
    [Symbol.asyncIterator]: () => {
      return iterator;
    },
  };
}

function Spinner() {
  return (
    <div className="spin" role="progressbar">
      <svg height="100%" viewBox="0 0 32 32" width="100%">
        <circle
          cx="16"
          cy="16"
          fill="none"
          r="14"
          strokeWidth="4"
          style={{
            stroke: "#000",
            opacity: 0.2,
          }}
        />
        <circle
          cx="16"
          cy="16"
          fill="none"
          r="14"
          strokeWidth="4"
          style={{
            stroke: "#000",
            strokeDasharray: 80,
            strokeDashoffset: 60,
          }}
        />
      </svg>
    </div>
  );
}

class Client {
  inner: any;
  constructor() {
    const { Client } = wasm_bindgen;
    this.inner = new Client();
  }
  fetch(path: string, maddr: string): Promise<void> {
    return Promise.resolve();
  }
  push(path: string, maddr: string): Promise<void> {
    return Promise.resolve();
  }

  import(file: File): Promise<any> {
    return this.inner.import_content(
      fileIterator(file)[Symbol.asyncIterator]()
    );
  }
}

function App() {
  const [root, setRoot] = useState(localStorage.getItem(CID_KEY) ?? "");
  const [maddr, setMaddr] = useState(localStorage.getItem(ADDR_KEY) ?? "");
  const [img, setImg] = useState("");
  const [vid, setVid] = useState("");
  const [loading, setLoading] = useState(false);
  const [client, setClient] = useState<Client | null>(null);
  const [uproot, setUproot] = useState<CID | null>(null);

  const onDrop = async (files: File[]) => {
    if (!client) {
      console.error("not client initialized");
      return;
    }
    files.forEach((file) =>
      client.import(file).then((root) => console.log(root))
    );
  };

  const { getRootProps, getInputProps, isDragActive } = useDropzone({ onDrop });

  const disabled = !root || !maddr || loading;

  function upload() {
    if (!client || !uproot) {
      return;
    }
    console.log("uploading");
    client
      .push(uproot.toString(), maddr)
      .then(() => console.log("uploaded"))
      .catch(console.error);
  }
  function sendRequest() {
    if (disabled || !client) {
      return;
    }
    setLoading(true);
    localStorage.setItem(CID_KEY, root);
    localStorage.setItem(ADDR_KEY, maddr);
    const start = performance.now();
    client
      .fetch(root, maddr)
      .then((res) => res.blob())
      .then((blob) => {
        const url = URL.createObjectURL(blob);
        setLoading(false);
        if (/image/.test(blob.type)) {
          setImg(url);
        }
        if (/video/.test(blob.type)) {
          setVid(url);
        }
        const done = performance.now();
        const duration = done - start;
        console.log(`done in ${duration}ms (${blob.size / duration}bps)`);
      })
      .catch(console.error);
  }
  useEffect(() => {
    wasm_bindgen("graphsync_wasm_example_bg.wasm").then(() => {
      console.log("wasm loaded");
      setClient(new Client());
    });
  }, []);
  return (
    <div className="app">
      {img ? (
        <img className="img" src={img} alt="Retrieved image" />
      ) : vid ? (
        <video controls className="img" autoPlay loop>
          <source src={vid} type="video/mp4" />
        </video>
      ) : (
        <div className="img">{loading && <Spinner />}</div>
      )}
      <input
        id="root"
        type="text"
        autoComplete="off"
        spellCheck="false"
        placeholder="root CID"
        className="ipt"
        value={root}
        onChange={(e) => setRoot(e.target.value)}
      />
      <input
        id="maddr"
        type="text"
        autoComplete="off"
        spellCheck="false"
        placeholder="multi address"
        className="ipt"
        value={maddr}
        onChange={(e) => setMaddr(e.target.value)}
      />
      <button className="btn" onClick={sendRequest} disabled={disabled}>
        request
      </button>
      <div {...getRootProps({ className: "drp" })}>
        <input {...getInputProps()} />
        {uproot ? (
          <p>{uproot.toString()}</p>
        ) : (
          <p>Drag or click to add files</p>
        )}
      </div>
      <button className="btn" onClick={upload}>
        upload
      </button>

      <p className="p">{!!client && "wasm loaded"}</p>
    </div>
  );
}

ReactDOM.render(
  <React.StrictMode>
    <App />
  </React.StrictMode>,
  document.getElementById("root")
);
