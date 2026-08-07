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

// Store active custom servers in memory: { serverId: { name, owner } }
const servers = {
    "general": { name: "General Lobby", owner: "System" }
};

// Track users who have already been announced on custom servers to avoid repeat spam
const announcedCustomUsers = new Set();

io.on("connection", (socket) => {
    console.log(`User connected: ${socket.id}`);

    // Handle user authentication handshake via session token
    socket.on("auth_user", (sessionData) => {
        socket.username = sessionData.username;
        socket.userId = sessionData.userId;
        
        // Default join to general lobby (silently, no spam message)
        socket.join("general");
        socket.currentServer = "general";
        
        // Send list of available servers to the user
        socket.emit("server_list", servers);
    });

    // Handle switching or joining custom servers
    socket.on("join_server", (serverId) => {
        if (!servers[serverId]) return;

        socket.leave(socket.currentServer);
        socket.join(serverId);
        socket.currentServer = serverId;

        socket.emit("server_switched", serverId);

        // Only announce on custom servers, and make sure it appears ONLY ONCE
        if (serverId !== "general" && socket.username) {
            const userKey = `${serverId}-${socket.username}`;
            if (!announcedCustomUsers.has(userKey)) {
                announcedCustomUsers.add(userKey);
                io.to(serverId).emit("message", {
                    system: true,
                    text: `${socket.username} entered the server.`
                });
            }
        }
    });

    // Handle creation of a new custom server
    socket.on("create_server", (serverName) => {
        const serverId = "srv_" + Math.random().toString(36).substr(2, 6);
        servers[serverId] = { name: serverName, owner: socket.username };

        // Broadcast updated server list to everyone connected
        io.emit("server_list", servers);
        
        // Automatically join the creator to the new server
        socket.emit("server_created_success", serverId);
    });

    // Handle server customization (Owner only)
    socket.on("update_server", ({ serverId, newName }) => {
        if (!servers[serverId]) return;

        // Verify that the user sending the request is actually the owner of the server
        if (servers[serverId].owner !== socket.username) {
            socket.emit("message", {
                system: true,
                text: "Permission denied: Only the server owner can customize this server."
            });
            return;
        }

        // Update properties if provided
        if (newName && newName.trim()) {
            servers[serverId].name = newName.trim();
        }

        // Broadcast updated server list to everyone so titles/settings refresh instantly
        io.emit("server_list", servers);

        // Notify the specific server room
        io.to(serverId).emit("message", {
            system: true,
            text: `Server settings were updated by owner (${socket.username}).`
        });
    });

    // Handle incoming chat messages within the current server room
    socket.on("chat_message", (text) => {
        if (!socket.currentServer || !text.trim()) return;

        io.to(socket.currentServer).emit("message", {
            username: socket.username,
            text: text.trim(),
            time: new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })
        });
    });

    socket.on("disconnect", () => {
        console.log(`User disconnected: ${socket.id}`);
        // Disconnect leave notifications are removed entirely to prevent chat clutter
    });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
    console.log(`pnig backend running on port ${PORT}`);
});
