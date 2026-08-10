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
    );

    CREATE TABLE IF NOT EXISTS servers (
        id VARCHAR(50) PRIMARY KEY,
        name VARCHAR(100),
        owner VARCHAR(100),
        channels TEXT[],
        stickers TEXT[],
        invite_code VARCHAR(50) UNIQUE,
        members TEXT[] DEFAULT '{}',
        banned_users TEXT[] DEFAULT '{}'
    );
`).catch(err => console.error("Database initialization error:", err));

const ADMIN_USER_ID = "usr_kbyc5yhe2";

// In-memory cache for servers, loaded from DB on startup
const servers = {};

async function loadServersIntoMemory() {
    try {
        const result = await pool.query(`SELECT * FROM servers`);
        if (result.rows.length === 0) {
            // Default General Lobby setup if database is brand new
            await pool.query(
                `INSERT INTO servers (id, name, owner, channels, stickers, invite_code, members, banned_users) VALUES ($1, $2, $3, $4, $5, $6, $7, $8) ON CONFLICT (id) DO NOTHING`,
                ["general", "General Lobby", "System", ["main", "random"], [], "general-lobby", [], []]
            );
            servers["general"] = { 
                name: "General Lobby", 
                owner: "System", 
                channels: ["main", "random"], 
                stickers: [], 
                invite_code: "general-lobby",
                members: [],
                bannedUsers: []
            };
        } else {
            result.rows.forEach(row => {
                servers[row.id] = {
                    name: row.name,
                    owner: row.owner,
                    channels: row.channels,
                    stickers: row.stickers || [],
                    invite_code: row.invite_code || row.id,
                    members: row.members || [],
                    bannedUsers: row.banned_users || []
                };
            });
        }
    } catch (err) {
        console.error("Error loading servers:", err);
        servers["general"] = { name: "General Lobby", owner: "System", channels: ["main", "random"], stickers: [], invite_code: "general-lobby", members: [], bannedUsers: [] };
    }
}
loadServersIntoMemory();

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

        // Check if user is banned from this server
        if (servers[serverId].bannedUsers && servers[serverId].bannedUsers.includes(socket.userId)) {
            socket.emit("message", { system: true, text: "Access denied: You are banned from this server." });
            return;
        }

        if (socket.currentServer && socket.currentChannel) {
            socket.leave(`${socket.currentServer}_${socket.currentChannel}`);
        }

        socket.currentServer = serverId;
        socket.currentChannel = servers[serverId].channels[0] || "main";
        
        const roomKey = `${socket.currentServer}_${socket.currentChannel}`;
        socket.join(roomKey);

        // Automatically track user as member if not already
        if (serverId !== "general" && socket.userId) {
            if (!servers[serverId].members.includes(socket.userId)) {
                servers[serverId].members.push(socket.userId);
                try {
                    await pool.query(`UPDATE servers SET members = $1 WHERE id = $2`, [servers[serverId].members, serverId]);
                } catch (err) {
                    console.error("Error adding member to DB:", err);
                }
            }
        }

        socket.emit("server_switched", { 
            serverId, 
            channel: socket.currentChannel, 
            channels: servers[serverId].channels,
            stickers: servers[serverId].stickers || [],
            inviteCode: servers[serverId].invite_code,
            members: servers[serverId].members || [],
            bannedUsers: servers[serverId].bannedUsers || []
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

    // Join Server via Invite Code
    socket.on("join_server_by_invite", async (inviteCode) => {
        let targetServerId = null;
        for (const [sId, sData] of Object.entries(servers)) {
            if (sData.invite_code === inviteCode) {
                targetServerId = sId;
                break;
            }
        }

        if (!targetServerId) {
            socket.emit("message", { system: true, text: "Invalid invite code or server no longer exists." });
            return;
        }

        // Check if banned
        if (servers[targetServerId].bannedUsers && servers[targetServerId].bannedUsers.includes(socket.userId)) {
            socket.emit("message", { system: true, text: "You are banned from this server." });
            return;
        }

        socket.emit("force_switch_server", targetServerId);
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

    socket.on("create_channel", async (channelName) => {
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
            
            try {
                await pool.query(`UPDATE servers SET channels = $1 WHERE id = $2`, [srv.channels, socket.currentServer]);
            } catch (err) {
                console.error("Error updating channels in DB:", err);
            }

            io.emit("server_list", servers);
            srv.channels.forEach(ch => {
                io.to(`${socket.currentServer}_${ch}`).emit("channels_updated", srv.channels);
            });
        }
    });

    // Rename Channel Handler
    socket.on("rename_channel", async ({ oldName, newName }) => {
        const srv = servers[socket.currentServer];
        if (!srv || oldName === "main") return;

        const cleanNewName = newName.toLowerCase().replace(/[^a-z0-9-_]/g, '-').trim();
        if (!cleanNewName || srv.channels.includes(cleanNewName)) return;

        if (socket.currentServer === "general" && socket.userId !== ADMIN_USER_ID) return;
        if (socket.currentServer !== "general" && srv.owner !== socket.username) return;

        const index = srv.channels.indexOf(oldName);
        if (index !== -1) {
            srv.channels[index] = cleanNewName;

            try {
                await pool.query(`UPDATE servers SET channels = $1 WHERE id = $2`, [srv.channels, socket.currentServer]);
                await pool.query(`UPDATE messages SET channel = $1 WHERE server_id = $2 AND channel = $3`, [cleanNewName, socket.currentServer, oldName]);
            } catch (err) {
                console.error("Error renaming channel in DB:", err);
            }

            io.emit("server_list", servers);
            srv.channels.forEach(ch => {
                io.to(`${socket.currentServer}_${ch}`).emit("channels_updated", srv.channels);
            });
        }
    });

    socket.on("create_server", async (serverName) => {
        const serverId = "srv_" + Math.random().toString(36).substr(2, 6);
        const inviteCode = Math.random().toString(36).substr(2, 8);
        const defaultChannels = ["main", "general"];
        const defaultStickers = [];
        const initialMembers = [socket.userId];

        servers[serverId] = { 
            name: serverName, 
            owner: socket.username, 
            channels: defaultChannels,
            stickers: defaultStickers,
            invite_code: inviteCode,
            members: initialMembers,
            bannedUsers: []
        };

        try {
            await pool.query(
                `INSERT INTO servers (id, name, owner, channels, stickers, invite_code, members, banned_users) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
                [serverId, serverName, socket.username, defaultChannels, defaultStickers, inviteCode, initialMembers, []]
            );
        } catch (err) {
            console.error("Error saving new server:", err);
        }

        io.emit("server_list", servers);
        socket.emit("server_created_success", serverId);
    });

    // Delete Custom Server Handler
    socket.on("delete_server", async (serverId) => {
        if (serverId === "general" || !servers[serverId]) return;
        if (servers[serverId].owner !== socket.username && socket.userId !== ADMIN_USER_ID) {
            socket.emit("message", { system: true, text: "Permission denied: Only the server owner or admin can delete this server." });
            return;
        }

        try {
            await pool.query(`DELETE FROM servers WHERE id = $1`, [serverId]);
            await pool.query(`DELETE FROM messages WHERE server_id = $1`, [serverId]);
        } catch (err) {
            console.error("Error deleting server from DB:", err);
        }

        delete servers[serverId];
        io.emit("server_list", servers);
        socket.emit("force_switch_server", "general");
    });

    socket.on("update_server", async ({ serverId, newName }) => {
        if (!servers[serverId]) return;

        if (serverId === "general") {
            if (socket.userId !== ADMIN_USER_ID) {
                socket.emit("message", { system: true, text: "Permission denied: Only admin can customize General Lobby." });
                return;
            }
        } else {
            if (servers[serverId].owner !== socket.username && socket.userId !== ADMIN_USER_ID) {
                socket.emit("message", { system: true, text: "Permission denied." });
                return;
            }
        }

        if (newName && newName.trim()) {
            servers[serverId].name = newName.trim();
            try {
                await pool.query(`UPDATE servers SET name = $1 WHERE id = $2`, [servers[serverId].name, serverId]);
            } catch (err) {
                console.error("Error updating server name in DB:", err);
            }
        }

        io.emit("server_list", servers);
    });

    // --- CUSTOM SERVER MODERATION: KICK MEMBER ---
    socket.on("kick_member", async ({ serverId, targetUserId }) => {
        const srv = servers[serverId];
        if (!srv || serverId === "general") return;

        const isOwner = (srv.owner === socket.username);
        const isAdmin = (socket.userId === ADMIN_USER_ID);

        if (!isOwner && !isAdmin) {
            socket.emit("message", { system: true, text: "Permission denied: Only the server owner can kick members." });
            return;
        }

        srv.members = srv.members.filter(id => id !== targetUserId);
        try {
            await pool.query(`UPDATE servers SET members = $1 WHERE id = $2`, [srv.members, serverId]);
        } catch (err) {
            console.error("Error updating members after kick:", err);
        }

        io.to(serverId).emit("member_list_updated", srv.members);
        // Force target user out of the room if connected
        io.in(targetUserId).socketsLeave(`${serverId}_${srv.channels[0]}`);
        io.to(targetUserId).emit("force_switch_server", "general");
        socket.emit("message", { system: true, text: "Member kicked successfully." });
    });

    // --- CUSTOM SERVER MODERATION: BAN MEMBER ---
    socket.on("ban_member", async ({ serverId, targetUserId }) => {
        const srv = servers[serverId];
        if (!srv || serverId === "general") return;

        const isOwner = (srv.owner === socket.username);
        const isAdmin = (socket.userId === ADMIN_USER_ID);

        if (!isOwner && !isAdmin) {
            socket.emit("message", { system: true, text: "Permission denied: Only the server owner can ban members." });
            return;
        }

        srv.members = srv.members.filter(id => id !== targetUserId);
        if (!srv.bannedUsers.includes(targetUserId)) {
            srv.bannedUsers.push(targetUserId);
        }

        try {
            await pool.query(`UPDATE servers SET members = $1, banned_users = $2 WHERE id = $3`, [srv.members, srv.bannedUsers, serverId]);
        } catch (err) {
            console.error("Error updating ban list in DB:", err);
        }

        io.to(serverId).emit("member_list_updated", srv.members);
        io.to(serverId).emit("ban_list_updated", srv.bannedUsers);
        
        io.in(targetUserId).socketsLeave(`${serverId}_${srv.channels[0]}`);
        io.to(targetUserId).emit("force_switch_server", "general");
        socket.emit("message", { system: true, text: "Member banned and added to ban list." });
    });

    // --- CUSTOM SERVER MODERATION: UNBAN MEMBER ---
    socket.on("unban_member", async ({ serverId, targetUserId }) => {
        const srv = servers[serverId];
        if (!srv || serverId === "general") return;

        const isOwner = (srv.owner === socket.username);
        const isAdmin = (socket.userId === ADMIN_USER_ID);

        if (!isOwner && !isAdmin) {
            socket.emit("message", { system: true, text: "Permission denied." });
            return;
        }

        srv.bannedUsers = srv.bannedUsers.filter(id => id !== targetUserId);
        try {
            await pool.query(`UPDATE servers SET banned_users = $1 WHERE id = $2`, [srv.bannedUsers, serverId]);
        } catch (err) {
            console.error("Error updating ban list during unban:", err);
        }

        io.to(serverId).emit("ban_list_updated", srv.bannedUsers);
        socket.emit("message", { system: true, text: "User unbanned successfully." });
    });

    // --- GLOBAL PNIG OWNER POWER: WIPE / DELETE ACCOUNT ANYTIME ---
    socket.on("pnig_admin_delete_account", async (targetUserId) => {
        if (socket.userId !== ADMIN_USER_ID) {
            socket.emit("message", { system: true, text: "Unauthorized: Master Admin command only." });
            return;
        }

        try {
            // Delete all messages authored by this user across the entire platform
            await pool.query(`DELETE FROM messages WHERE username IN (SELECT username FROM messages WHERE id = $1)`, [targetUserId]).catch(() => {});
            // Clear them out from all server member/ban configurations
            for (const sId of Object.keys(servers)) {
                servers[sId].members = servers[sId].members.filter(id => id !== targetUserId);
                servers[sId].bannedUsers = servers[sId].bannedUsers.filter(id => id !== targetUserId);
                await pool.query(`UPDATE servers SET members = $1, banned_users = $2 WHERE id = $3`, [servers[sId].members, servers[sId].bannedUsers, sId]);
            }
            // Force disconnect their active socket sessions
            io.in(targetUserId).disconnectSockets(true);
            console.log(`Master admin wiped account/user ID: ${targetUserId}`);
        } catch (err) {
            console.error("Error executing global account wipe:", err);
        }
    });

    // High-Performance Binary Sticker Upload Handler
    socket.on("upload_server_sticker_binary", async (data) => {
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

        try {
            await pool.query(`UPDATE servers SET stickers = $1 WHERE id = $2`, [servers[serverId].stickers, serverId]);
        } catch (err) {
            console.error("Error updating stickers in DB:", err);
        }

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

    // Delete Message Handler (Updated to allow Custom Server Owner & Global Admin override)
    socket.on("delete_message", async (msgId) => {
        if (!socket.currentServer || !socket.currentChannel) return;
        const roomKey = `${socket.currentServer}_${socket.currentChannel}`;

        try {
            const checkRes = await pool.query(`SELECT username, server_id FROM messages WHERE id = $1`, [msgId]);
            if (checkRes.rows.length > 0) {
                const msg = checkRes.rows[0];
                const srv = servers[msg.server_id];
                
                const isAuthor = (msg.username === socket.username);
                const isServerOwner = srv && (srv.owner === socket.username);
                const isAdmin = (socket.userId === ADMIN_USER_ID);

                if (isAuthor || isServerOwner || isAdmin) {
                    await pool.query(`DELETE FROM messages WHERE id = $1`, [msgId]);
                    io.to(roomKey).emit("message_deleted", msgId);
                }
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
