require('dotenv').config();
const { MongoClient } = require('mongodb');
const { TelegramClient } = require('telegram');
const { StringSession } = require('telegram/sessions');
const { CustomFile } = require('telegram/client/uploads');
const { v2: webdav } = require('webdav-server');
const express = require('express');

// --- Environment & Config ---
const PORT = process.env.PORT || 8080;
const MONGO_URI = process.env.MONGO_URI;
const DB_NAME = process.env.DB_NAME || 'telegram_drive';

const apiId = parseInt(process.env.TELEGRAM_API_ID);
const apiHash = process.env.TELEGRAM_API_HASH;
const stringSession = new StringSession(process.env.SESSION_STRING || "");
const channelId = BigInt(process.env.TELEGRAM_CHANNEL_ID);

let db, nodesCol;
const client = new TelegramClient(stringSession, apiId, apiHash, { connectionRetries: 5 });

// --- Custom WebDAV File System mapped to MongoDB & Telegram ---
class TelegramFileSystem extends webdav.FileSystem {
    constructor() {
        super();
    }

    async _lock(path, ctx, callback) { callback(null, true); }
    async _unlock(path, ctx, callback) { callback(null); }

    async _type(path, ctx, callback) {
        try {
            const cleanPath = path.toString();
            console.log(`[WebDAV] _type requested for path: "${cleanPath}"`);
            if (cleanPath === '/' || cleanPath === '') {
                return callback(null, webdav.ResourceType.DIRECTORY);
            }
            const parentPath = cleanPath.substring(0, cleanPath.lastIndexOf('/')) || '/';
            const name = cleanPath.substring(cleanPath.lastIndexOf('/') + 1);

            const node = await nodesCol.findOne({ parent_path: parentPath, name: name });
            if (!node) return callback(webdav.Errors.ResourceNotFound);

            const type = node.is_folder ? webdav.ResourceType.DIRECTORY : webdav.ResourceType.FILE;
            callback(null, type);
        } catch (err) {
            console.error("[WebDAV] _type error:", err);
            callback(err);
        }
    }

    async _exists(path, ctx, callback) {
        this._type(path, ctx, (err, type) => {
            callback(null, !err && type !== undefined);
        });
    }

    async _fastExistCheck(ctx, path, callback) {
        this._exists(path, ctx, (err, exists) => {
            callback(exists);
        });
    }

    async _stat(path, ctx, callback) {
        try {
            const cleanPath = path.toString();
            console.log(`[WebDAV] _stat requested for path: "${cleanPath}"`);
            if (cleanPath === '/' || cleanPath === '') {
                return callback(null, {
                    size: 0,
                    isDirectory: () => true,
                    isFile: () => false,
                    mimeType: 'httpd/unix-directory',
                    dev: 0,
                    mode: webdav.ResourceType.DIRECTORY,
                    ino: 0,
                    nlink: 1,
                    uid: 0,
                    gid: 0,
                    rdev: 0,
                    blksize: 0,
                    blocks: 0,
                    atime: new Date(),
                    mtime: new Date(),
                    ctime: new Date()
                });
            }

            const parentPath = cleanPath.substring(0, cleanPath.lastIndexOf('/')) || '/';
            const name = cleanPath.substring(cleanPath.lastIndexOf('/') + 1);

            const node = await nodesCol.findOne({ parent_path: parentPath, name: name });
            if (!node) return callback(webdav.Errors.ResourceNotFound);

            callback(null, {
                size: node.size || 0,
                isDirectory: () => node.is_folder,
                isFile: () => !node.is_folder,
                mimeType: node.mime_type || 'application/octet-stream',
                atime: node.updated_at || new Date(),
                mtime: node.updated_at || new Date(),
                ctime: node.created_at || new Date()
            });
        } catch (err) {
            callback(err);
        }
    }

    async _readDir(path, ctx, callback) {
        try {
            const cleanPath = path.toString() === '' ? '/' : path.toString();
            console.log(`[WebDAV] _readDir requested for path: "${cleanPath}"`);
            const items = await nodesCol.find({ parent_path: cleanPath }).toArray();
            const filenames = items.map(item => item.name);
            callback(null, filenames);
        } catch (err) {
            callback(err);
        }
    }

    async _createFolder(path, ctx, callback) {
        try {
            const cleanPath = path.toString();
            console.log(`[WebDAV] _createFolder requested for path: "${cleanPath}"`);
            const parentPath = cleanPath.substring(0, cleanPath.lastIndexOf('/')) || '/';
            const name = cleanPath.substring(cleanPath.lastIndexOf('/') + 1);

            await nodesCol.updateOne(
                { parent_path: parentPath, name: name },
                {
                    $set: {
                        parent_path: parentPath,
                        name: name,
                        is_folder: true,
                        size: 0,
                        created_at: new Date(),
                        updated_at: new Date()
                    }
                },
                { upsert: true }
            );
            callback(null);
        } catch (err) {
            callback(err);
        }
    }

    async _openReadStream(path, ctx, callback) {
        try {
            const cleanPath = path.toString();
            console.log(`[WebDAV] _openReadStream requested for path: "${cleanPath}"`);
            const parentPath = cleanPath.substring(0, cleanPath.lastIndexOf('/')) || '/';
            const name = cleanPath.substring(cleanPath.lastIndexOf('/') + 1);

            const node = await nodesCol.findOne({ parent_path: parentPath, name: name });
            if (!node || node.is_folder) return callback(webdav.Errors.ResourceNotFound);

            const { PassThrough } = require('stream');
            const passThroughStream = new PassThrough();

            (async () => {
                try {
                    for (const chunk of node.telegram_chunks) {
                        const messages = await client.getMessages(channelId, { ids: [chunk.message_id] });
                        if (messages && messages.length > 0) {
                            const buffer = await client.downloadMedia(messages[0], { streaming: false });
                            passThroughStream.write(buffer);
                        }
                    }
                    passThroughStream.end();
                } catch (streamErr) {
                    passThroughStream.destroy(streamErr);
                }
            })();

            callback(null, passThroughStream);
        } catch (err) {
            callback(err);
        }
    }

    async _openWriteStream(path, ctx, callback) {
        try {
            const cleanPath = path.toString();
            console.log(`[WebDAV] _openWriteStream requested for path: "${cleanPath}"`);
            const parentPath = cleanPath.substring(0, cleanPath.lastIndexOf('/')) || '/';
            const name = cleanPath.substring(cleanPath.lastIndexOf('/') + 1);

            const { Writable } = require('stream');
            const chunksBuffer = [];

            const writableStream = new Writable({
                write(chunk, encoding, next) {
                    chunksBuffer.push(chunk);
                    next();
                },
                final: async (next) => {
                    try {
                        const fullBuffer = Buffer.concat(chunksBuffer);
                        const chunkSize = 1.5 * 1024 * 1024;
                        const telegramChunks = [];

                        for (let i = 0; i < fullBuffer.length; i += chunkSize) {
                            const slice = fullBuffer.slice(i, i + chunkSize);
                            const customFile = new CustomFile(`chunk_${i}_${name}`, slice.length, slice);
                            
                            const sentMsg = await client.sendFile(channelId, {
                                file: customFile,
                                caption: `file:${name} | chunk:${i}`
                            });

                            telegramChunks.push({
                                chunk_index: telegramChunks.length,
                                message_id: sentMsg.id,
                                size: slice.length
                            });
                        }

                        await nodesCol.updateOne(
                            { parent_path: parentPath, name: name },
                            {
                                $set: {
                                    parent_path: parentPath,
                                    name: name,
                                    is_folder: false,
                                    size: fullBuffer.length,
                                    telegram_chunks: telegramChunks,
                                    created_at: new Date(),
                                    updated_at: new Date()
                                }
                            },
                            { upsert: true }
                        );

                        next();
                    } catch (uploadErr) {
                        next(uploadErr);
                    }
                }
            });

            callback(null, writableStream);
        } catch (err) {
            callback(err);
        }
    }

    async _delete(path, ctx, callback) {
        try {
            const cleanPath = path.toString();
            console.log(`[WebDAV] _delete requested for path: "${cleanPath}"`);
            const parentPath = cleanPath.substring(0, cleanPath.lastIndexOf('/')) || '/';
            const name = cleanPath.substring(cleanPath.lastIndexOf('/') + 1);

            await nodesCol.deleteOne({ parent_path: parentPath, name: name });
            callback(null);
        } catch (err) {
            callback(err);
        }
    }
}

// --- Main Startup Sequence ---
async function main() {
    // 1. Connect MongoDB Atlas
    const mongoClient = new MongoClient(MONGO_URI);
    await mongoClient.connect();
    db = mongoClient.db(DB_NAME);
    nodesCol = db.collection('nodes');
    console.log("Connected to MongoDB Atlas.");

    // 2. Connect Telegram Client via Bot Token
    await client.start({
        botAuthToken: process.env.TELEGRAM_BOT_TOKEN
    });
    console.log("Telegram Bot Client connected.");

    // 3. Setup Express + WebDAV Server integration
    const app = express();
    const webdavServer = new webdav.WebDAVServer({
        rootFileSystem: new TelegramFileSystem()
    });

    // Dedicated Health Check Endpoint (handles browser /health pings cleanly)
    app.get('/health', (req, res) => {
        res.status(200).json({
            status: 'online',
            service: 'Telegram WebDAV Bridge',
            database: db ? 'connected' : 'disconnected',
            telegram: client.connected ? 'connected' : 'disconnected',
            timestamp: new Date().toISOString()
        });
    });

    // Express middleware to log incoming raw HTTP requests
    app.use((req, res, next) => {
        console.log(`[HTTP] ${req.method} request received for URL: ${req.url}`);
        next();
    });

    // Route all other traffic to the WebDAV server engine explicitly at root
    app.use((req, res) => {
        webdavServer.executeRequest(req, res);
    });

    app.listen(PORT, () => {
        console.log(`WebDAV proxy server running on port ${PORT}`);
    });
}

main().catch((err) => {
    console.error("Startup error:", err);
    process.exit(1);
});
