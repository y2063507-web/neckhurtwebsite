const express = require("express");
const http = require("http");
const { Server } = require("socket.io");
const cors = require("cors");
const { Pool } = require("pg");

const app = express();
app.use(cors());

const server = http.createServer(app);
const io = new Server(server, {
    cors: { origin: "*" },
    maxHttpBufferSize: 30 * 1024 * 1024 // 30MB limit
});

// Setup PostgreSQL Cloud Database Connection
const pool = new Pool({
    connectionString: process.env.DATABASE_URL,
    ssl: { rejectUnauthorized: false }
});

// Automatically create tables on startup if they don't exist
pool.query(`
    CREATE TABLE IF NOT EXISTS messages (
        id VARCHAR(50) PRIMARY KEY,
        username VARCHAR(100),
        text TEXT,
        file_url TEXT,
        file_name TEXT,
        file_type TEXT,
        channel VARCHAR(50),
        server_id VARCHAR(50),
        time VARCHAR(50)
    )
`).catch(err => console.error("Database initialization error:", err));

const ADMIN_USER_ID = "usr_kbyc5yhe2";

const servers = {
    "general": { 
        name: "General Lobby", 
        owner: "System", 
        channels: ["main", "random"],
        stickers: []
    }
};

const announcedCustomUsers = new Set();

io.on("connection", (socket) => {
    console.log(`User connected: ${socket.id}`);

    socket.on("auth_user", async (sessionData) => {
        socket.username = sessionData.username;
        socket.userId = sessionData.userId;
        
        socket.currentServer = "general";
        socket.currentChannel = "main";
        
        const roomKey = "general_main";
        socket.join(roomKey);
        
        socket.emit("server_list", servers);

        try {
            const result = await pool.query(
                `SELECT id, username, text, file_url as "fileUrl", file_name as "fileName", file_type as "fileType", time FROM messages WHERE server_id = $1 AND channel = $2 ORDER BY ctid ASC LIMIT 50`,
                ["general", "main"]
            );
            socket.emit("load_history", result.rows);
        } catch (err) {
            console.error("Error loading history:", err);
            socket.emit("load_history", []);
        }
    });

    socket.on("join_server", async (serverId) => {
        if (!servers[serverId]) return;

        if (socket.currentServer && socket.currentChannel) {
            socket.leave(`${socket.currentServer}_${socket.currentChannel}`);
        }

        socket.currentServer = serverId;
        socket.currentChannel = servers[serverId].channels[0] || "main";
        
        const roomKey = `${socket.currentServer}_${socket.currentChannel}`;
        socket.join(roomKey);

        socket.emit("server_switched", { 
            serverId, 
            channel: socket.currentChannel, 
            channels: servers[serverId].channels,
            stickers: servers[serverId].stickers || []
        });

        try {
            const result = await pool.query(
                `SELECT id, username, text, file_url as "fileUrl", file_name as "fileName", file_type as "fileType", time FROM messages WHERE server_id = $1 AND channel = $2 ORDER BY ctid ASC LIMIT 50`,
                [socket.currentServer, socket.currentChannel]
            );
            socket.emit("load_history", result.rows);
        } catch (err) {
            console.error("Error loading history:", err);
            socket.emit("load_history", []);
        }

        if (serverId !== "general" && socket.username) {
            const userKey = `${serverId}-${socket.username}`;
            if (!announcedCustomUsers.has(userKey)) {
                announcedCustomUsers.add(userKey);
                io.to(roomKey).emit("message", {
                    id: "sys_" + Math.random(),
                    system: true,
                    text: `${socket.username} entered the server.`
                });
            }
        }
    });

    socket.on("join_channel", async (channelName) => {
        if (!servers[socket.currentServer]) return;
        if (!servers[socket.currentServer].channels.includes(channelName)) return;

        socket.leave(`${socket.currentServer}_${socket.currentChannel}`);
        socket.currentChannel = channelName;
        
        const roomKey = `${socket.currentServer}_${socket.currentChannel}`;
        socket.join(roomKey);

        socket.emit("channel_switched", channelName);

        try {
            const result = await pool.query(
                `SELECT id, username, text, file_url as "fileUrl", file_name as "fileName", file_type as "fileType", time FROM messages WHERE server_id = $1 AND channel = $2 ORDER BY ctid ASC LIMIT 50`,
                [socket.currentServer, socket.currentChannel]
            );
            socket.emit("load_history", result.rows);
        } catch (err) {
            console.error("Error loading history:", err);
            socket.emit("load_history", []);
        }
    });

    socket.on("create_channel", (channelName) => {
        const cleanName = channelName.toLowerCase().replace(/[^a-z0-9-_]/g, '-').trim();
        if (!cleanName || !servers[socket.currentServer]) return;

        const srv = servers[socket.currentServer];

        if (socket.currentServer === "general") {
            if (socket.userId !== ADMIN_USER_ID) {
                socket.emit("message", { system: true, text: "Permission denied: Only admin can add channels here." });
                return;
            }
        } else {
            if (srv.owner !== socket.username) {
                socket.emit("message", { system: true, text: "Permission denied: Only server owner can create channels." });
                return;
            }
        }

        if (!srv.channels.includes(cleanName)) {
            srv.channels.push(cleanName);
            io.emit("server_list", servers);
            srv.channels.forEach(ch => {
                io.to(`${socket.currentServer}_${ch}`).emit("channels_updated", srv.channels);
            });
        }
    });

    socket.on("create_server", (serverName) => {
        const serverId = "srv_" + Math.random().toString(36).substr(2, 6);
        servers[serverId] = { 
            name: serverName, 
            owner: socket.username, 
            channels: ["main", "general"],
            stickers: []
        };

        io.emit("server_list", servers);
        socket.emit("server_created_success", serverId);
    });

    socket.on("update_server", ({ serverId, newName }) => {
        if (!servers[serverId]) return;

        if (serverId === "general") {
            if (socket.userId !== ADMIN_USER_ID) {
                socket.emit("message", { system: true, text: "Permission denied: Only admin can customize General Lobby." });
                return;
            }
        } else {
            if (servers[serverId].owner !== socket.username) {
                socket.emit("message", { system: true, text: "Permission denied." });
                return;
            }
        }

        if (newName && newName.trim()) {
            servers[serverId].name = newName.trim();
        }

        io.emit("server_list", servers);
    });

    // High-Performance Binary Sticker Upload Handler
    socket.on("upload_server_sticker_binary", (data) => {
        const serverId = data.serverId;
        if (!servers[serverId] || !data.file) return;
        
        const isGeneralAdmin = (serverId === "general" && socket.userId === ADMIN_USER_ID);
        const isCustomOwner = (serverId !== "general" && servers[serverId].owner === socket.username);

        if (!isGeneralAdmin && !isCustomOwner) return;

        const base64Data = data.file.toString("base64");
        const stickerUrl = `data:${data.fileType};base64,${base64Data}`;

        if (!servers[serverId].stickers) {
            servers[serverId].stickers = [];
        }

        servers[serverId].stickers.push(stickerUrl);

        servers[serverId].channels.forEach(ch => {
            io.to(`${serverId}_${ch}`).emit("stickers_updated", servers[serverId].stickers);
        });
    });

    // Standard Text Messages
    socket.on("chat_message", async (textInput) => {
        if (!socket.currentServer || !socket.currentChannel) return;
        const text = typeof textInput === "string" ? textInput.trim() : "";
        if (!text) return;

        const roomKey = `${socket.currentServer}_${socket.currentChannel}`;
        const msgData = {
            id: "msg_" + Math.random().toString(36).substr(2, 9),
            username: socket.username,
            text: text,
            fileUrl: null,
            fileName: null,
            fileType: null,
            time: new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })
        };

        await saveAndBroadcastMessage(roomKey, socket.currentServer, socket.currentChannel, msgData);
    });

    // High-Performance Binary File + Optional Caption Handler
    socket.on("upload_file_msg", async (data) => {
        if (!socket.currentServer || !socket.currentChannel) return;

        let fileUrl = null;
        if (data.file) {
            const base64Data = data.file.toString("base64");
            fileUrl = `data:${data.fileType};base64,${base64Data}`;
        }

        const roomKey = `${socket.currentServer}_${socket.currentChannel}`;
        const msgData = {
            id: "msg_" + Math.random().toString(36).substr(2, 9),
            username: socket.username,
            text: data.text ? data.text.trim() : "",
            fileUrl: fileUrl,
            fileName: data.fileName || "attachment",
            fileType: data.fileType || "application/octet-stream",
            time: new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })
        };

        await saveAndBroadcastMessage(roomKey, socket.currentServer, socket.currentChannel, msgData);
    });

    socket.on("delete_message", async (msgId) => {
        if (!socket.currentServer || !socket.currentChannel) return;
        const roomKey = `${socket.currentServer}_${socket.currentChannel}`;

        try {
            const checkRes = await pool.query(`SELECT username FROM messages WHERE id = $1`, [msgId]);
            if (checkRes.rows.length > 0 && checkRes.rows[0].username === socket.username) {
                await pool.query(`DELETE FROM messages WHERE id = $1`, [msgId]);
                io.to(roomKey).emit("message_deleted", msgId);
            }
        } catch (err) {
            console.error("Error deleting message:", err);
        }
    });

    socket.on("disconnect", () => {
        console.log(`User disconnected: ${socket.id}`);
    });
});

async function saveAndBroadcastMessage(roomKey, serverId, channel, msgData) {
    try {
        await pool.query(
            `INSERT INTO messages (id, username, text, file_url, file_name, file_type, channel, server_id, time) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
            [msgData.id, msgData.username, msgData.text, msgData.fileUrl, msgData.fileName, msgData.fileType, channel, serverId, msgData.time]
        );
        io.to(roomKey).emit("message", msgData);
    } catch (err) {
        console.error("Error saving message to database:", err);
    }
}

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
    console.log(`pnig backend running on port ${PORT}`);
});
