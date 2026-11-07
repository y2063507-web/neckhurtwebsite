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
pool.on("error", (err) => console.error("Unexpected pool error:", err));

const ADMIN_USER_ID = process.env.ADMIN_USER_ID || "usr_kbyc5yhe2";

// Automatically ensure tables and default General lobby exist on startup
async function initializeDatabase() {
    try {
        await pool.query(`
            CREATE TABLE IF NOT EXISTS servers (
                id TEXT PRIMARY KEY,
                name TEXT,
                owner TEXT,
                channels TEXT[],
                stickers TEXT[],
                invite_code TEXT,
                members JSONB DEFAULT '[]'::jsonb
            );
        `);
        await pool.query(`ALTER TABLE servers ADD COLUMN IF NOT EXISTS members JSONB DEFAULT '[]'::jsonb;`);
        await pool.query(`ALTER TABLE servers ADD COLUMN IF NOT EXISTS bans JSONB DEFAULT '[]'::jsonb;`);
        await pool.query(`ALTER TABLE servers ADD COLUMN IF NOT EXISTS owner_id TEXT;`);
        await pool.query(`
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
        `);
        await pool.query(`ALTER TABLE messages ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ DEFAULT now();`);
        await pool.query(`CREATE INDEX IF NOT EXISTS messages_server_channel_idx ON messages (server_id, channel, created_at);`);
        await pool.query(`
            INSERT INTO servers (id, name, owner, channels, stickers, invite_code, members, bans)
            VALUES ('general', 'General Lobby', 'System', ARRAY['main', 'random'], ARRAY[]::text[], '', '[]'::jsonb, '[]'::jsonb)
            ON CONFLICT (id) DO NOTHING;
        `);
        console.log("Database initialized successfully.");
    } catch (err) {
        console.error("Database initialization error:", err);
    }
}
initializeDatabase();

app.use(express.json());
app.get("/", (req, res) => res.send("pnig backend is running."));

// ---------- helpers ----------

// socket.id -> { username, userId, serverId, channel }
const activeUsers = {};

const str = (v) => (typeof v === "string" ? v : "");
const serverRoom = (serverId) => `s:${serverId}`;
const channelRoom = (serverId, channel) => `c:${serverId}|${channel}`;

// Members/bans used to be plain username strings; the frontend expects { username, userId }.
function normalizeList(list) {
    if (!Array.isArray(list)) return [];
    return list
        .map((m) => (typeof m === "string" ? { username: m, userId: null } : m))
        .filter((m) => m && m.username);
}

// Match by userId when we have one, otherwise fall back to username (legacy entries).
function sameUser(entry, target) {
    if (target.userId && entry.userId) return entry.userId === target.userId;
    return !!target.username && entry.username === target.username;
}

function isManager(srv, user) {
    if (!srv || !user) return false;
    if (srv.id === "general") return user.userId === ADMIN_USER_ID;
    if (srv.owner_id) return srv.owner_id === user.userId;
    return srv.owner === user.username; // servers created before owner_id existed
}

function isProtectedTarget(srv, target) {
    if (target.userId === ADMIN_USER_ID) return true;
    if (srv.owner_id && target.userId === srv.owner_id) return true;
    return !!target.username && target.username === srv.owner;
}

function safeFileType(t) {
    return /^[a-z0-9.+-]+\/[a-z0-9.+-]+$/i.test(str(t)) ? t : "application/octet-stream";
}

function safeFileName(n) {
    return str(n).replace(/["'<>\\\/\r\n]/g, "").slice(0, 100) || "file";
}

const STICKER_URL_RE = /^data:image\/[a-z0-9.+-]+;base64,[A-Za-z0-9+/=]+$/i;

function nowTime() {
    return new Date().toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });
}

async function getServer(serverId) {
    const res = await pool.query("SELECT * FROM servers WHERE id = $1", [str(serverId)]);
    return res.rows[0] || null;
}

// Move a socket into a server + channel (tracked explicitly, not parsed from room names,
// because server ids like "srv_ab12cd" contain underscores).
function moveSocket(socket, serverId, channel) {
    const st = activeUsers[socket.id];
    if (!st) return;
    if (st.serverId) {
        socket.leave(serverRoom(st.serverId));
        socket.leave(channelRoom(st.serverId, st.channel));
    }
    st.serverId = serverId;
    st.channel = channel;
    socket.join(serverRoom(serverId));
    socket.join(channelRoom(serverId, channel));
}

function buildServerList(rows, user) {
    const servers = {};
    rows.forEach((row) => {
        servers[row.id] = {
            name: row.name,
            owner: row.owner,
            channels: row.channels || ["main"],
            stickers: row.stickers || [],
            inviteCode: row.invite_code || "",
            members: normalizeList(row.members),
            bans: isManager(row, user) ? normalizeList(row.bans) : []
        };
    });
    return servers;
}

async function sendServerList(socket) {
    try {
        const res = await pool.query("SELECT * FROM servers");
        socket.emit("server_list", buildServerList(res.rows, activeUsers[socket.id]));
    } catch (err) {
        console.error("Error fetching servers:", err);
    }
}

async function broadcastServerList() {
    try {
        const res = await pool.query("SELECT * FROM servers");
        const sockets = await io.fetchSockets();
        sockets.forEach((s) => s.emit("server_list", buildServerList(res.rows, activeUsers[s.id])));
    } catch (err) {
        console.error("Error broadcasting servers:", err);
    }
}

async function emitBansToManagers(srv, bans) {
    const sockets = await io.in(serverRoom(srv.id)).fetchSockets();
    sockets.forEach((s) => {
        if (isManager(srv, activeUsers[s.id])) s.emit("bans_updated", bans);
    });
}

async function loadHistory(socket, serverId, channel) {
    try {
        const res = await pool.query(
            "SELECT * FROM messages WHERE server_id = $1 AND channel = $2 ORDER BY created_at DESC, ctid DESC LIMIT 50",
            [serverId, channel]
        );
        const history = res.rows.reverse().map((row) => ({
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

async function addMember(serverId, user) {
    const srv = await getServer(serverId);
    if (!srv) return null;
    const members = normalizeList(srv.members);
    const existing = members.find((m) => sameUser(m, user));
    if (existing) {
        if (existing.userId === user.userId && existing.username === user.username) return members;
        existing.userId = user.userId;
        existing.username = user.username;
    } else {
        members.push({ username: user.username, userId: user.userId });
    }
    await pool.query("UPDATE servers SET members = $1 WHERE id = $2", [JSON.stringify(members), serverId]);
    return members;
}

// Removes a member (and optionally bans them), then kicks any online sockets back to General.
async function removeFromServer(srv, target, ban) {
    const members = normalizeList(srv.members);
    const found = members.find((m) => sameUser(m, target));
    const remaining = members.filter((m) => !sameUser(m, target));
    const bans = normalizeList(srv.bans);

    if (ban) {
        const entry = found || { username: target.username || "unknown", userId: target.userId || null };
        if (!bans.some((b) => sameUser(b, entry))) bans.push({ username: entry.username, userId: entry.userId });
    }

    await pool.query("UPDATE servers SET members = $1, bans = $2 WHERE id = $3", [
        JSON.stringify(remaining),
        JSON.stringify(bans),
        srv.id
    ]);

    const sockets = await io.in(serverRoom(srv.id)).fetchSockets();
    sockets.forEach((s) => {
        const u = activeUsers[s.id];
        if (u && sameUser(u, target)) {
            moveSocket(s, "general", "main");
            s.emit("force_switch_server", "general");
        }
    });

    io.to(serverRoom(srv.id)).emit("members_updated", remaining);
    await emitBansToManagers(srv, bans);
    broadcastServerList();
}

// ---------- sockets ----------

io.on("connection", (socket) => {
    console.log(`User connected: ${socket.id}`);

    socket.on("auth_user", async (session) => {
        if (!session || typeof session.username !== "string" || typeof session.userId !== "string") return;
        const username = session.username.trim().slice(0, 32);
        if (!username) return;

        // Register + place in General synchronously so events that follow immediately are handled correctly
        activeUsers[socket.id] = { username, userId: session.userId.slice(0, 64) };
        moveSocket(socket, "general", "main");
        const user = activeUsers[socket.id];

        try {
            const general = await getServer("general");
            if (general && normalizeList(general.bans).some((b) => sameUser(b, user))) {
                socket.disconnect(true);
                return;
            }
            const members = await addMember("general", user);
            await sendServerList(socket);
            loadHistory(socket, "general", "main");
            if (members) io.to(serverRoom("general")).emit("members_updated", members);
        } catch (err) {
            console.error("Error authenticating user:", err);
        }
    });

    socket.on("join_server", async (serverId) => {
        const user = activeUsers[socket.id];
        if (!user) return;
        try {
            const srv = await getServer(serverId);
            if (!srv) return;

            if (normalizeList(srv.bans).some((b) => sameUser(b, user))) {
                if (srv.id !== "general") socket.emit("force_switch_server", "general");
                return;
            }

            const defaultChannel = srv.channels && srv.channels.length > 0 ? srv.channels[0] : "main";
            moveSocket(socket, srv.id, defaultChannel);
            const members = (await addMember(srv.id, user)) || normalizeList(srv.members);

            socket.emit("server_switched", {
                serverId: srv.id,
                name: srv.name,
                channels: srv.channels || ["main"],
                channel: defaultChannel,
                stickers: srv.stickers || [],
                inviteCode: srv.invite_code || "",
                members,
                bans: isManager(srv, user) ? normalizeList(srv.bans) : []
            });

            io.to(serverRoom(srv.id)).emit("members_updated", members);
            loadHistory(socket, srv.id, defaultChannel);
        } catch (err) {
            console.error("Error joining server:", err);
        }
    });

    socket.on("join_channel", async (channelName) => {
        const st = activeUsers[socket.id];
        if (!st || typeof channelName !== "string") return;
        try {
            const srv = await getServer(st.serverId);
            if (!srv || !(srv.channels || []).includes(channelName)) return;

            socket.leave(channelRoom(st.serverId, st.channel));
            st.channel = channelName;
            socket.join(channelRoom(st.serverId, channelName));

            socket.emit("channel_switched", channelName);
            loadHistory(socket, st.serverId, channelName);
        } catch (err) {
            console.error("Error joining channel:", err);
        }
    });

    socket.on("create_server", async (name) => {
        const user = activeUsers[socket.id];
        if (!user) return;
        const serverName = str(name).trim().slice(0, 50);
        if (!serverName) return;

        const serverId = "srv_" + Math.random().toString(36).slice(2, 11);
        const inviteCode = Math.random().toString(36).slice(2, 10);
        const members = [{ username: user.username, userId: user.userId }];

        try {
            await pool.query(
                `INSERT INTO servers (id, name, owner, owner_id, channels, stickers, invite_code, members, bans)
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, '[]'::jsonb)`,
                [serverId, serverName, user.username, user.userId, ["main", "general"], [], inviteCode, JSON.stringify(members)]
            );
            // Send the updated list first so the client knows the server before it switches to it
            await broadcastServerList();
            socket.emit("server_created_success", serverId);
        } catch (err) {
            console.error("Error creating server:", err);
        }
    });

    socket.on("join_server_by_invite", async (inviteCode) => {
        const user = activeUsers[socket.id];
        if (!user || typeof inviteCode !== "string" || !inviteCode) return;
        try {
            const res = await pool.query("SELECT * FROM servers WHERE invite_code = $1", [inviteCode]);
            const srv = res.rows[0];
            if (!srv) return;
            if (normalizeList(srv.bans).some((b) => sameUser(b, user))) return;

            await addMember(srv.id, user);
            await broadcastServerList();
            socket.emit("force_switch_server", srv.id);
        } catch (err) {
            console.error("Error joining by invite:", err);
        }
    });

    socket.on("update_server", async (data) => {
        const user = activeUsers[socket.id];
        if (!user || !data) return;
        const newName = str(data.newName).trim().slice(0, 50);
        if (!newName) return;
        try {
            const srv = await getServer(data.serverId);
            if (!srv || !isManager(srv, user)) return;
            await pool.query("UPDATE servers SET name = $1 WHERE id = $2", [newName, srv.id]);
            broadcastServerList();
        } catch (err) {
            console.error("Error updating server:", err);
        }
    });

    socket.on("delete_server", async (serverId) => {
        const user = activeUsers[socket.id];
        if (!user || serverId === "general") return;
        try {
            const srv = await getServer(serverId);
            if (!srv || !isManager(srv, user)) return;

            await pool.query("DELETE FROM servers WHERE id = $1", [srv.id]);
            await pool.query("DELETE FROM messages WHERE server_id = $1", [srv.id]);

            const sockets = await io.in(serverRoom(srv.id)).fetchSockets();
            sockets.forEach((s) => {
                moveSocket(s, "general", "main");
                s.emit("force_switch_server", "general");
            });
            await broadcastServerList();
        } catch (err) {
            console.error("Error deleting server:", err);
        }
    });

    // --- KICK / BAN / UNBAN (frontend sends userId) ---
    async function handleRemoval(data, ban) {
        const user = activeUsers[socket.id];
        if (!user || !data) return;
        try {
            const srv = await getServer(data.serverId);
            if (!srv || !isManager(srv, user)) return;
            const target = { userId: str(data.userId), username: str(data.username) };
            if (!target.userId && !target.username) return;
            if (sameUser(user, target) || isProtectedTarget(srv, target)) return;
            await removeFromServer(srv, target, ban);
        } catch (err) {
            console.error(`Error ${ban ? "banning" : "kicking"} member:`, err);
        }
    }
    socket.on("kick_member", (data) => handleRemoval(data, false));
    socket.on("ban_member", (data) => handleRemoval(data, true));

    socket.on("unban_member", async (data) => {
        const user = activeUsers[socket.id];
        if (!user || !data) return;
        try {
            const srv = await getServer(data.serverId);
            if (!srv || !isManager(srv, user)) return;
            const target = { userId: str(data.userId), username: str(data.username) };
            const bans = normalizeList(srv.bans).filter((b) => !sameUser(b, target));
            await pool.query("UPDATE servers SET bans = $1 WHERE id = $2", [JSON.stringify(bans), srv.id]);
            await emitBansToManagers(srv, bans);
            broadcastServerList();
        } catch (err) {
            console.error("Error unbanning member:", err);
        }
    });

    // Admin: remove a user from every server and disconnect them.
    // Note: accounts only exist in each user's browser, so this can't stop them signing up again.
    socket.on("admin_delete_account", async (targetUserId) => {
        const user = activeUsers[socket.id];
        if (!user || user.userId !== ADMIN_USER_ID) return;
        if (typeof targetUserId !== "string" || !targetUserId || targetUserId === ADMIN_USER_ID) return;
        try {
            await pool.query(
                `UPDATE servers SET members = COALESCE(
                    (SELECT jsonb_agg(m) FROM jsonb_array_elements(members) m WHERE m->>'userId' IS DISTINCT FROM $1),
                    '[]'::jsonb)`,
                [targetUserId]
            );
            const sockets = await io.fetchSockets();
            sockets.forEach((s) => {
                if (activeUsers[s.id]?.userId === targetUserId) s.disconnect(true);
            });
            await broadcastServerList();
        } catch (err) {
            console.error("Error deleting account:", err);
        }
    });

    socket.on("create_channel", async (channelName) => {
        const st = activeUsers[socket.id];
        const name = str(channelName).trim().slice(0, 30);
        if (!st || !name) return;
        try {
            const srv = await getServer(st.serverId);
            if (!srv || !isManager(srv, st)) return;

            const channels = srv.channels || ["main"];
            if (channels.includes(name)) return;
            channels.push(name);
            await pool.query("UPDATE servers SET channels = $1 WHERE id = $2", [channels, srv.id]);
            io.to(serverRoom(srv.id)).emit("channels_updated", channels);
            broadcastServerList();
        } catch (err) {
            console.error("Error creating channel:", err);
        }
    });

    socket.on("rename_channel", async (data) => {
        const st = activeUsers[socket.id];
        if (!st || !data) return;
        const oldName = str(data.oldName);
        const newName = str(data.newName).trim().slice(0, 30);
        if (!newName || oldName === "main") return;
        try {
            const srv = await getServer(st.serverId);
            if (!srv || !isManager(srv, st)) return;

            let channels = srv.channels || ["main", "random"];
            if (!channels.includes(oldName) || channels.includes(newName)) return;
            channels = channels.map((c) => (c === oldName ? newName : c));

            await pool.query("UPDATE servers SET channels = $1 WHERE id = $2", [channels, srv.id]);
            await pool.query("UPDATE messages SET channel = $1 WHERE server_id = $2 AND channel = $3", [newName, srv.id, oldName]);

            // Move anyone currently sitting in the renamed channel
            const sockets = await io.in(serverRoom(srv.id)).fetchSockets();
            sockets.forEach((s) => {
                const u = activeUsers[s.id];
                if (u && u.channel === oldName) {
                    s.leave(channelRoom(srv.id, oldName));
                    u.channel = newName;
                    s.join(channelRoom(srv.id, newName));
                    s.emit("channel_switched", newName);
                }
            });

            io.to(serverRoom(srv.id)).emit("channels_updated", channels);
            broadcastServerList();
        } catch (err) {
            console.error("Error renaming channel:", err);
        }
    });

    // payload is a plain string (typed message) or { text, fileUrl, fileName, fileType } (stickers)
    socket.on("chat_message", async (payload) => {
        const st = activeUsers[socket.id];
        if (!st || !st.serverId) return;

        const isObj = payload && typeof payload === "object";
        const text = (isObj ? str(payload.text) : str(payload)).slice(0, 2000);
        let fileUrl = null, fileName = null, fileType = null;
        if (isObj && STICKER_URL_RE.test(str(payload.fileUrl))) {
            fileUrl = payload.fileUrl;
            fileName = safeFileName(payload.fileName || "sticker.gif");
            fileType = safeFileType(payload.fileType || "image/gif");
        }
        if (!text.trim() && !fileUrl) return;

        const msgId = "msg_" + Math.random().toString(36).slice(2, 11);
        const time = nowTime();
        const msgObj = {
            id: msgId, serverId: st.serverId, channel: st.channel,
            username: st.username, text, fileUrl, fileName, fileType, time
        };

        try {
            await pool.query(
                `INSERT INTO messages (id, server_id, channel, username, text, file_url, file_name, file_type, time)
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
                [msgId, st.serverId, st.channel, st.username, text, fileUrl, fileName, fileType, time]
            );
            io.to(channelRoom(st.serverId, st.channel)).emit("message", msgObj);
        } catch (err) {
            console.error("Error saving message:", err);
        }
    });

    socket.on("upload_file_msg", async (data) => {
        const st = activeUsers[socket.id];
        if (!st || !st.serverId || !data || !Buffer.isBuffer(data.file)) return;

        const text = str(data.text).slice(0, 2000);
        const fileType = safeFileType(data.fileType);
        const fileName = safeFileName(data.fileName);
        const fileUrl = `data:${fileType};base64,${data.file.toString("base64")}`;

        const msgId = "msg_" + Math.random().toString(36).slice(2, 11);
        const time = nowTime();
        const msgObj = {
            id: msgId, serverId: st.serverId, channel: st.channel,
            username: st.username, text, fileUrl, fileName, fileType, time
        };

        try {
            await pool.query(
                `INSERT INTO messages (id, server_id, channel, username, text, file_url, file_name, file_type, time)
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
                [msgId, st.serverId, st.channel, st.username, text, fileUrl, fileName, fileType, time]
            );
            io.to(channelRoom(st.serverId, st.channel)).emit("message", msgObj);
        } catch (err) {
            console.error("Error saving file message:", err);
        }
    });

    socket.on("upload_server_sticker_binary", async (data) => {
        const user = activeUsers[socket.id];
        if (!user || !data || !Buffer.isBuffer(data.file)) return;
        if (data.file.length > 2 * 1024 * 1024) return;
        const fileType = safeFileType(data.fileType);
        if (!fileType.startsWith("image/")) return;

        try {
            const srv = await getServer(data.serverId);
            if (!srv || !isManager(srv, user)) return;

            const stickers = srv.stickers || [];
            stickers.push(`data:${fileType};base64,${data.file.toString("base64")}`);
            await pool.query("UPDATE servers SET stickers = $1 WHERE id = $2", [stickers, srv.id]);

            io.to(serverRoom(srv.id)).emit("stickers_updated", stickers);
            broadcastServerList();
        } catch (err) {
            console.error("Error uploading sticker:", err);
        }
    });

    socket.on("delete_message", async (msgId) => {
        const user = activeUsers[socket.id];
        if (!user || typeof msgId !== "string") return;
        try {
            const res = await pool.query("SELECT * FROM messages WHERE id = $1", [msgId]);
            const msg = res.rows[0];
            if (!msg) return;

            const srv = await getServer(msg.server_id);
            const allowed =
                msg.username === user.username ||
                user.userId === ADMIN_USER_ID ||
                (srv && srv.id !== "general" && isManager(srv, user));
            if (!allowed) return;

            await pool.query("DELETE FROM messages WHERE id = $1", [msgId]);
            io.to(serverRoom(msg.server_id)).emit("message_deleted", msgId);
        } catch (err) {
            console.error("Error deleting message:", err);
        }
    });

    socket.on("disconnect", () => {
        delete activeUsers[socket.id];
        console.log(`User disconnected: ${socket.id}`);
    });
});

process.on("unhandledRejection", (err) => console.error("Unhandled rejection:", err));

const PORT = process.env.PORT || 10000;
server.listen(PORT, () => {
    console.log(`pnig backend running on port ${PORT}`);
});
