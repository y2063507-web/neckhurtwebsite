const http = require("http");
const express = require("express");
const { Server } = require("socket.io");
const { Pool } = require("pg");

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
    cors: { origin: "*" },
    maxHttpBufferSize: 30 * 1024 * 1024 // 30MB file upload support
});

// PostgreSQL Database Connection
const pool = new Pool({
    connectionString: process.env.DATABASE_URL,
    ssl: process.env.DATABASE_URL ? { rejectUnauthorized: false } : false
});

// Automatically ensure tables and columns exist on startup
pool.query(`
    CREATE TABLE IF NOT EXISTS servers (
        id TEXT PRIMARY KEY,
        name TEXT,
        owner TEXT,
        channels TEXT[],
        stickers TEXT[],
        invite_code TEXT,
        members JSONB DEFAULT '[]'::jsonb
    );
    ALTER TABLE servers ADD COLUMN IF NOT EXISTS members JSONB DEFAULT '[]'::jsonb;

    CREATE TABLE IF NOT EXISTS messages (
        id TEXT PRIMARY KEY,
        server_id TEXT,
        channel TEXT,
        username TEXT,
        text TEXT,
        file_url TEXT,
        file_name TEXT,
        file_type TEXT,
        time TEXT
    );
`).catch(err => console.error("Database initialization error:", err));

app.use(express.json());

app.get("/", (req, res) => {
    res.send("pnig backend is running.");
});

// Active session tracking per socket
const activeUsers = {};

io.on("connection", (socket) => {
    console.log(`User connected: ${socket.id}`);

    socket.on("auth_user", (session) => {
        activeUsers[socket.id] = session;
        // Default join general lobby
        socket.join("general_main");
        sendServerList(socket);
        loadHistory(socket, "general", "main");
    });

    socket.on("join_server", async (serverId) => {
        try {
            const res = await pool.query("SELECT * FROM servers WHERE id = $1", [serverId]);
            if (res.rows.length === 0) return;
            const srv = res.rows[0];

            // Leave current rooms
            const rooms = Array.from(socket.rooms);
            rooms.forEach(r => { if (r !== socket.id) socket.leave(r); });

            const defaultChannel = srv.channels && srv.channels.length > 0 ? srv.channels[0] : "main";
            socket.join(`${serverId}_${defaultChannel}`);

            socket.emit("server_switched", {
                serverId: srv.id,
                name: srv.name,
                channels: srv.channels || ["main"],
                channel: defaultChannel,
                stickers: srv.stickers || [],
                inviteCode: srv.invite_code
            });

            loadHistory(socket, srv.id, defaultChannel);
        } catch (err) {
            console.error("Error joining server:", err);
        }
    });

    socket.on("join_channel", async (channelName) => {
        let currentServerId = "general";
        for (const room of socket.rooms) {
            if (room !== socket.id && room.includes("_")) {
                currentServerId = room.split("_")[0];
                socket.leave(room);
            }
        }

        socket.join(`${currentServerId}_${channelName}`);
        socket.emit("channel_switched", channelName);
        loadHistory(socket, currentServerId, channelName);
    });

    socket.on("create_server", async (name) => {
        const user = activeUsers[socket.id];
        if (!user) return;

        const serverId = 'srv_' + Math.random().toString(36).substr(2, 9);
        const inviteCode = Math.random().toString(36).substr(2, 8);
        const channels = ["main", "general"];
        const stickers = [];
        const members = [user.username];

        try {
            await pool.query(
                `INSERT INTO servers (id, name, owner, channels, stickers, invite_code, members) VALUES ($1, $2, $3, $4, $5, $6, $7)`,
                [serverId, name, user.username, channels, stickers, inviteCode, JSON.stringify(members)]
            );

            socket.emit("server_created_success", serverId);
            broadcastServerList();
        } catch (err) {
            console.error("Error creating server:", err);
        }
    });

    socket.on("join_server_by_invite", async (inviteCode) => {
        const user = activeUsers[socket.id];
        if (!user) return;

        try {
            const res = await pool.query("SELECT * FROM servers WHERE invite_code = $1", [inviteCode]);
            if (res.rows.length === 0) return;
            const srv = res.rows[0];

            let members = srv.members || [];
            if (!members.includes(user.username)) {
                members.push(user.username);
                await pool.query("UPDATE servers SET members = $1 WHERE id = $2", [JSON.stringify(members), srv.id]);
            }

            socket.emit("force_switch_server", srv.id);
        } catch (err) {
            console.error("Error joining by invite:", err);
        }
    });

    socket.on("chat_message", async (text) => {
        const user = activeUsers[socket.id];
        if (!user) return;

        const { serverId, channel } = getSocketServerAndChannel(socket);
        const msgId = 'msg_' + Math.random().toString(36).substr(2, 9);
        const time = new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });

        const msgObj = {
            id: msgId,
            serverId,
            channel,
            username: user.username,
            text,
            fileUrl: null,
            fileName: null,
            fileType: null,
            time
        };

        try {
            await pool.query(
                `INSERT INTO messages (id, server_id, channel, username, text, time) VALUES ($1, $2, $3, $4, $5, $6)`,
                [msgId, serverId, channel, user.username, text, time]
            );
            io.to(`${serverId}_${channel}`).emit("message", msgObj);
        } catch (err) {
            console.error("Error saving message:", err);
        }
    });

    socket.on("upload_file_msg", async (data) => {
        const user = activeUsers[socket.id];
        if (!user) return;

        const { serverId, channel } = getSocketServerAndChannel(socket);
        const msgId = 'msg_' + Math.random().toString(36).substr(2, 9);
        const time = new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });

        // Convert binary buffer to data URI for simple storage/rendering
        const base64Data = data.file.toString("base64");
        const fileUrl = `data:${data.fileType};base64,${base64Data}`;

        const msgObj = {
            id: msgId,
            serverId,
            channel,
            username: user.username,
            text: data.text || "",
            fileUrl,
            fileName: data.fileName,
            fileType: data.fileType,
            time
        };

        try {
            await pool.query(
                `INSERT INTO messages (id, server_id, channel, username, text, file_url, file_name, file_type, time) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
                [msgId, serverId, channel, user.username, data.text || "", fileUrl, data.fileName, data.fileType, time]
            );
            io.to(`${serverId}_${channel}`).emit("message", msgObj);
        } catch (err) {
            console.error("Error saving file message:", err);
        }
    });

    socket.on("upload_server_sticker_binary", async (data) => {
        try {
            const base64Data = data.file.toString("base64");
            const fileUrl = `data:${data.fileType};base64,${base64Data}`;

            const res = await pool.query("SELECT stickers FROM servers WHERE id = $1", [data.serverId]);
            if (res.rows.length === 0) return;
            let stickers = res.rows[0].stickers || [];
            stickers.push(fileUrl);

            await pool.query("UPDATE servers SET stickers = $1 WHERE id = $2", [stickers, data.serverId]);
            io.to(`${data.serverId}_main`).emit("stickers_updated", stickers);
        } catch (err) {
            console.error("Error uploading sticker:", err);
        }
    });

    socket.on("create_channel", async (channelName) => {
        const { serverId } = getSocketServerAndChannel(socket);
        try {
            const res = await pool.query("SELECT channels FROM servers WHERE id = $1", [serverId]);
            if (res.rows.length === 0) return;
            let channels = res.rows[0].channels || [];
            if (!channels.includes(channelName)) {
                channels.push(channelName);
                await pool.query("UPDATE servers SET channels = $1 WHERE id = $2", [channels, serverId]);
                io.to(`${serverId}_${channels[0]}`).emit("channels_updated", channels);
            }
        } catch (err) {
            console.error("Error creating channel:", err);
        }
    });

    socket.on("delete_message", async (msgId) => {
        try {
            await pool.query("DELETE FROM messages WHERE id = $1", [msgId]);
            io.emit("message_deleted", msgId);
        } catch (err) {
            console.error("Error deleting message:", err);
        }
    });

    socket.on("disconnect", () => {
        delete activeUsers[socket.id];
        console.log(`User disconnected: ${socket.id}`);
    });
});

function getSocketServerAndChannel(socket) {
    for (const room of socket.rooms) {
        if (room !== socket.id && room.includes("_")) {
            const parts = room.split("_");
            return { serverId: parts[0], channel: parts[1] };
        }
    }
    return { serverId: "general", channel: "main" };
}

async function sendServerList(socket) {
    try {
        const res = await pool.query("SELECT * FROM servers");
        const servers = {
            "general": { name: "General Lobby", owner: "System", channels: ["main", "random"], stickers: [] }
        };
        res.rows.forEach(row => {
            servers[row.id] = {
                name: row.name,
                owner: row.owner,
                channels: row.channels,
                stickers: row.stickers,
                inviteCode: row.invite_code,
                members: row.members
            };
        });
        socket.emit("server_list", servers);
    } catch (err) {
        console.error("Error fetching servers:", err);
    }
}

async function broadcastServerList() {
    const sockets = await io.fetchSockets();
    sockets.forEach(s => sendServerList(s));
}

async function loadHistory(socket, serverId, channel) {
    try {
        const res = await pool.query(
            "SELECT * FROM messages WHERE server_id = $1 AND channel = $2 ORDER BY ctid ASC LIMIT 50",
            [serverId, channel]
        );
        const history = res.rows.map(row => ({
            id: row.id,
            username: row.username,
            text: row.text,
            fileUrl: row.file_url,
            fileName: row.file_name,
            fileType: row.file_type,
            time: row.time
        }));
        socket.emit("load_history", history);
    } catch (err) {
        console.error("Error loading history:", err);
    }
}

const PORT = process.env.PORT || 10000;
server.listen(PORT, () => {
    console.log(`pnig backend running on port ${PORT}`);
});
