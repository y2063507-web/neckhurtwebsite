const express = require("express");
const http = require("http");
const { Server } = require("socket.io");
const cors = require("cors");

const app = express();
app.use(cors());

const server = http.createServer(app);
const io = new Server(server, {
    cors: { origin: "*" },
    maxHttpBufferSize: 5 * 1024 * 1024 // Allow up to 5MB file payloads over sockets
});

// Admin User ID allowed to manage the General Lobby
const ADMIN_USER_ID = "usr_kbyc5yhe2";

// Store active custom servers in memory with an array of channels
const servers = {
    "general": { 
        name: "General Lobby", 
        owner: "System", 
        channels: ["main", "random"] 
    }
};

// Store chat history per room: { "server_channel": [ { username, text, fileUrl, fileName, fileType, time }, ... ] }
const chatHistories = {};
const MAX_HISTORY = 50;

// Track users who have already been announced on custom servers to avoid repeat spam
const announcedCustomUsers = new Set();

io.on("connection", (socket) => {
    console.log(`User connected: ${socket.id}`);

    // Handle user authentication handshake via session token
    socket.on("auth_user", (sessionData) => {
        socket.username = sessionData.username;
        socket.userId = sessionData.userId;
        
        socket.currentServer = "general";
        socket.currentChannel = "main";
        
        const roomKey = "general_main";
        socket.join(roomKey);
        
        // Send list of available servers to the user
        socket.emit("server_list", servers);

        // Send past history for default channel
        if (chatHistories[roomKey]) {
            socket.emit("load_history", chatHistories[roomKey]);
        } else {
            socket.emit("load_history", []);
        }
    });

    // Handle switching or joining servers
    socket.on("join_server", (serverId) => {
        if (!servers[serverId]) return;

        // Leave old room
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
            channels: servers[serverId].channels 
        });

        // Send past history for the channel
        if (chatHistories[roomKey]) {
            socket.emit("load_history", chatHistories[roomKey]);
        } else {
            socket.emit("load_history", []);
        }

        // Only announce on custom servers once per session
        if (serverId !== "general" && socket.username) {
            const userKey = `${serverId}-${socket.username}`;
            if (!announcedCustomUsers.has(userKey)) {
                announcedCustomUsers.add(userKey);
                io.to(roomKey).emit("message", {
                    system: true,
                    text: `${socket.username} entered the server.`
                });
            }
        }
    });

    // Handle switching channels inside the current server
    socket.on("join_channel", (channelName) => {
        if (!servers[socket.currentServer]) return;
        if (!servers[socket.currentServer].channels.includes(channelName)) return;

        socket.leave(`${socket.currentServer}_${socket.currentChannel}`);
        socket.currentChannel = channelName;
        
        const roomKey = `${socket.currentServer}_${socket.currentChannel}`;
        socket.join(roomKey);

        socket.emit("channel_switched", channelName);

        // Send history for the new channel
        if (chatHistories[roomKey]) {
            socket.emit("load_history", chatHistories[roomKey]);
        } else {
            socket.emit("load_history", []);
        }
    });

    // Handle creating a new channel inside the current server
    socket.on("create_channel", (channelName) => {
        const cleanName = channelName.toLowerCase().replace(/[^a-z0-9-_]/g, '-').trim();
        if (!cleanName || !servers[socket.currentServer]) return;

        const srv = servers[socket.currentServer];

        // Security permissions
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

    // Handle creation of a new custom server
    socket.on("create_server", (serverName) => {
        const serverId = "srv_" + Math.random().toString(36).substr(2, 6);
        servers[serverId] = { 
            name: serverName, 
            owner: socket.username, 
            channels: ["main", "general"] 
        };

        io.emit("server_list", servers);
        socket.emit("server_created_success", serverId);
    });

    // Handle server customization
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
        servers[serverId].channels.forEach(ch => {
            io.to(`${serverId}_${ch}`).emit("message", {
                system: true,
                text: `Server settings updated by (${socket.username}).`
            });
        });
    });

    // Handle incoming chat messages (supports text and file/meme uploads)
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

    socket.on("disconnect", () => {
        console.log(`User disconnected: ${socket.id}`);
    });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
    console.log(`pnig backend running on port ${PORT}`);
});
