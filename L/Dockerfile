# Dockerfile for L (Node.js backend)
FROM node:18-alpine

WORKDIR /app

# Install dependencies
COPY package.json package-lock.json* .
RUN npm install --production

# Copy the rest of the backend code
COPY . .

# Expose the port your server listens on (adjust if needed)
EXPOSE 3001

# Start the backend server
CMD ["node", "server.js"]
