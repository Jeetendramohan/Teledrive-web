// TeleCloud Drive v10.0 - Hybrid Streaming Download Service Worker
const activeDownloads = new Map();

self.addEventListener("install", () => {
    self.skipWaiting();
});

self.addEventListener("activate", (event) => {
    event.waitUntil(self.clients.claim());
});

self.addEventListener("message", (event) => {
    const data = event.data;
    if (!data) return;

    if (data.type === "REGISTER_DOWNLOAD") {
        activeDownloads.set(data.id, data.payload);
        if (event.ports && event.ports[0]) {
            event.ports[0].postMessage({ ok: true });
        }
    }
});

self.addEventListener("fetch", (event) => {
    const url = new URL(event.request.url);
    const match = url.pathname.match(/\/sw-download\/([^/]+)\//);
    if (!match) return;

    const dlId = match[1];
    const meta = activeDownloads.get(dlId);
    if (!meta) {
        event.respondWith(new Response("Download session expired. Please try again.", { status: 404 }));
        return;
    }

    activeDownloads.delete(dlId);

    const stream = new ReadableStream({
        async start(controller) {
            try {
                const workerBase = (meta.workerUrl || "").replace(/\/+$/, "");

                for (let i = 0; i < meta.chunks.length; i++) {
                    const chunk = meta.chunks[i];
                    let chunkRes;

                    if (meta.authMode === "private") {
                        const privateUrl = `${workerBase}/api/file?fileId=${encodeURIComponent(chunk.fileId)}&name=${encodeURIComponent(`part_${i + 1}`)}`;
                        chunkRes = await fetch(privateUrl, {
                            headers: { "X-App-Password": meta.appPassword || "" }
                        });
                    } else {
                        const tgRes = await fetch(
                            `https://api.telegram.org/bot${meta.botToken}/getFile?file_id=${encodeURIComponent(chunk.fileId)}`
                        );
                        const tgData = await tgRes.json();
                        if (!tgData.ok || !tgData.result || !tgData.result.file_path) {
                            throw new Error(`Chunk ${i + 1} unavailable`);
                        }

                        const tgFileUrl = `https://api.telegram.org/file/bot${meta.botToken}/${tgData.result.file_path}`;
                        const workerFetchUrl = `${meta.workerUrl}?url=${encodeURIComponent(tgFileUrl)}&name=${encodeURIComponent(`part_${i + 1}`)}`;
                        chunkRes = await fetch(workerFetchUrl);
                    }

                    if (!chunkRes.ok || !chunkRes.body) {
                        throw new Error(`Failed to fetch chunk ${i + 1}`);
                    }

                    const reader = chunkRes.body.getReader();
                    while (true) {
                        const { done, value } = await reader.read();
                        if (done) break;
                        controller.enqueue(value);
                    }
                }
                controller.close();
            } catch (err) {
                controller.error(err);
            }
        }
    });

    const headers = new Headers({
        "Content-Type": "application/octet-stream",
        "Content-Disposition": `attachment; filename*=UTF-8''${encodeURIComponent(meta.name)}`
    });
    if (meta.size && meta.size > 0) {
        headers.set("Content-Length", String(meta.size));
    }

    event.respondWith(new Response(stream, { status: 200, headers }));
});
