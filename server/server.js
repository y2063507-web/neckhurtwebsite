const express = require("express");
const http = require("http");
const { Server } = require("socket.io");
const cors = require("cors");

const app = express();
app.use(cors());

const server = http.createServer(app);
const io = new Server(server, {
    cors: { origin: "*" },
    maxHttpBufferSize: 25 * 1024 * 1024 // 25MB limit for files/videos
});

const ADMIN_USER_ID = "usr_kbyc5yhe2";

const servers = {
    "general": { 
        name: "General Lobby", 
        owner: "System", 
        channels: ["main", "random"],
        stickers: []
    }
};

const chatHistories = {};
const MAX_HISTORY = 50;
const announcedCustomUsers = new Set();

io.on("connection", (socket) => {
    console.log(`User connected: ${socket.id}`);

    socket.on("auth_user", (sessionData) => {
        socket.username = sessionData.username;
        socket.userId = sessionData.userId;
        
        socket.currentServer = "general";
        socket.currentChannel = "main";
        
        const roomKey = "general_main";
        socket.join(roomKey);
        
        socket.emit("server_list", servers);

        if (chatHistories[roomKey]) {
            socket.emit("load_history", chatHistories[roomKey]);
        } else {
            socket.emit("load_history", []);
        }
    });

    socket.on("join_server", (serverId) => {
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

        if (chatHistories[roomKey]) {
            socket.emit("load_history", chatHistories[roomKey]);
        } else {
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

    socket.on("join_channel", (channelName) => {
        if (!servers[socket.currentServer]) return;
        if (!servers[socket.currentServer].channels.includes(channelName)) return;

        socket.leave(`${socket.currentServer}_${socket.currentChannel}`);
        socket.currentChannel = channelName;
        
        const roomKey = `${socket.currentServer}_${socket.currentChannel}`;
        socket.join(roomKey);

        socket.emit("channel_switched", channelName);

        if (chatHistories[roomKey]) {
            socket.emit("load_history", chatHistories[roomKey]);
        } else {
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

    // Handle Custom Server Sticker Uploads (Server Owner Only)
    socket.on("upload_server_sticker", ({ serverId, stickerUrl }) => {
        if (!servers[serverId] || !stickerUrl) return;
        
        const isGeneralAdmin = (serverId === "general" && socket.userId === ADMIN_USER_ID);
        const isCustomOwner = (serverId !== "general" && servers[serverId].owner === socket.username);

        if (!isGeneralAdmin && !isCustomOwner) return;

        if (!servers[serverId].stickers) {
            servers[serverId].stickers = [];
        }

        servers[serverId].stickers.push(stickerUrl);

        // Broadcast updated stickers to everyone on that server
        servers[serverId].channels.forEach(ch => {
            io.to(`${serverId}_${ch}`).emit("stickers_updated", servers[serverId].stickers);
        });
    });

    socket.on("chat_message", (content) => {
        if (!socket.currentServer || !socket.currentChannel) return;

        let text = "";
        let fileUrl = null;
        let fileName = null;
        let fileType = null;

        if (typeof content === "string") {
            text = content.trim();
        } else if (typeof content === "object" && content !== null) {
            text = content.text ? content.text.trim() : "";
            fileUrl = content.fileUrl || null;
            fileName = content.fileName || null;
            fileType = content.fileType || null;
        }

        if (!text && !fileUrl) return;

        const roomKey = `${socket.currentServer}_${socket.currentChannel}`;
        const msgData = {
            id: "msg_" + Math.random().toString(36).substr(2, 9),
            username: socket.username,
            text: text,
            fileUrl: fileUrl,
            fileName: fileName,
            fileType: fileType,
            time: new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })
        };

        if (!chatHistories[roomKey]) {
            chatHistories[roomKey] = [];
        }
        chatHistories[roomKey].push(msgData);
        if (chatHistories[roomKey].length > MAX_HISTORY) {
            chatHistories[roomKey].shift();
        }

        io.to(roomKey).emit("message", msgData);
    });

    // Handle Message Deletion (Author Only)
    socket.on("delete_message", (msgId) => {
        if (!socket.currentServer || !socket.currentChannel) return;
        const roomKey = `${socket.currentServer}_${socket.currentChannel}`;
        if (!chatHistories[roomKey]) return;

        const index = chatHistories[roomKey].findIndex(m => m.id === msgId);
        if (index !== -1) {
            const msg = chatHistories[roomKey][index];
            if (msg.username === socket.username) {
                chatHistories[roomKey].splice(index, 1);
                io.to(roomKey).emit("message_deleted", msgId);
            }
        }
    });

    socket.on("disconnect", () => {
        console.log(`User disconnected: ${socket.id}`);
    });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
    console.log(`pnig backend running on port ${PORT}`);
});
