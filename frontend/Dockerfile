# ---------------------------------------------------------
# Stage 1: Build (Node.js)
# ---------------------------------------------------------
FROM node:18-alpine as builder

WORKDIR /app

# Copy package files first for better cache utilization
COPY package.json package-lock.json ./

# Install dependencies (use npm ci for reproducible, faster installs)
RUN npm ci

# Copy source code
COPY . .

# Build the production bundle
RUN npm run build

# ---------------------------------------------------------
# Stage 2: Serve (Nginx)
# ---------------------------------------------------------
FROM nginx:alpine-slim

# Copy built assets from builder stage
COPY --from=builder /app/dist /usr/share/nginx/html

# (Optional) Copy custom nginx config for SPA routing
# COPY nginx.conf /etc/nginx/conf.d/default.conf

# Expose port 80 (standard HTTP port for nginx)
EXPOSE 80

# Run nginx in foreground (Docker requirement)
CMD ["nginx", "-g", "daemon off;"]