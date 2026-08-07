const express = require("express");
const http = require("http");
const { Server } = require("socket.io");
const cors = require("cors");

const app = express();
app.use(cors());

const server = http.createServer(app);
const io = new Server(server, {
    cors: { origin: "*" }
});

// Store active custom servers in memory with an array of channels
const servers = {
    "general": { 
        name: "General Lobby", 
        owner: "System", 
        channels: ["main", "random"] 
    }
};

const announcedCustomUsers = new Set();

io.on("connection", (socket) => {
    console.log(`User connected: ${socket.id}`);

    socket.on("auth_user", (sessionData) => {
        socket.username = sessionData.username;
        socket.userId = sessionData.userId;
        
        socket.currentServer = "general";
        socket.currentChannel = "main";
        socket.join("general_main");
        
        socket.emit("server_list", servers);
    });

    // Handle switching servers (defaults to the first channel)
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

        socket.emit("server_switched", { serverId, channel: socket.currentChannel, channels: servers[serverId].channels });

        // Announce user entry once on custom servers
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

    // Handle switching between channels inside the current server
    socket.on("join_channel", (channelName) => {
        if (!servers[socket.currentServer]) return;
        if (!servers[socket.currentServer].channels.includes(channelName)) return;

        socket.leave(`${socket.currentServer}_${socket.currentChannel}`);
        socket.currentChannel = channelName;
        
        const roomKey = `${socket.currentServer}_${socket.currentChannel}`;
        socket.join(roomKey);

        socket.emit("channel_switched", channelName);
    });

    // Handle creating a new channel inside the current server
    socket.on("create_channel", (channelName) => {
        const cleanName = channelName.toLowerCase().replace(/[^a-z0-9-_]/g, '-').trim();
        if (!cleanName || !servers[socket.currentServer]) return;

        const srv = servers[socket.currentServer];
        if (!srv.channels.includes(cleanName)) {
            srv.channels.push(cleanName);
            io.emit("server_list", servers);
            // Broadcast updated channel list to everyone currently in that server
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

    // Handle server customization (Owner only)
    socket.on("update_server", ({ serverId, newName }) => {
        if (!servers[serverId]) return;
        if (servers[serverId].owner !== socket.username) {
            socket.emit("message", { system: true, text: "Permission denied." });
            return;
        }

        if (newName && newName.trim()) {
            servers[serverId].name = newName.trim();
        }

        io.emit("server_list", servers);
        servers[serverId].channels.forEach(ch => {
            io.to(`${serverId}_${ch}`).emit("message", {
                system: true,
                text: `Server settings updated by owner (${socket.username}).`
            });
        });
    });

    // Handle chat messages scoped to current server and channel
    socket.on("chat_message", (text) => {
        if (!socket.currentServer || !socket.currentChannel || !text.trim()) return;

        const roomKey = `${socket.currentServer}_${socket.currentChannel}`;
        io.to(roomKey).emit("message", {
            username: socket.username,
            text: text.trim(),
            time: new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })
        });
    });

    socket.on("disconnect", () => {
        console.log(`User disconnected: ${socket.id}`);
    });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
    console.log(`pnig backend running on port ${PORT}`);
});
