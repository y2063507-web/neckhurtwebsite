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

io.on("connection", (socket) => {
    console.log(`User connected: ${socket.id}`);

    // Handle user authentication handshake via session token
    socket.on("auth_user", (sessionData) => {
        socket.username = sessionData.username;
        socket.userId = sessionData.userId;
        
        // Default join to general lobby
        socket.join("general");
        socket.currentServer = "general";
        
        // Send list of available servers to the user
        socket.emit("server_list", servers);
        
        io.to("general").emit("message", {
            system: true,
            text: `${socket.username} joined the general lobby.`
        });
    });

    // Handle switching or joining custom servers
    socket.on("join_server", (serverId) => {
        if (!servers[serverId]) return;

        socket.leave(socket.currentServer);
        socket.join(serverId);
        socket.currentServer = serverId;

        socket.emit("server_switched", serverId);
        io.to(serverId).emit("message", {
            system: true,
            text: `${socket.username} entered the server.`
        });
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
        if (socket.username && socket.currentServer) {
            io.to(socket.currentServer).emit("message", {
                system: true,
                text: `${socket.username} left.`
            });
        }
    });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log(`pnig backend running on port ${PORT}`));