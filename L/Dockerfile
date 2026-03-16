FROM node:18-alpine

WORKDIR /app

# Set production environment (affects Node.js behavior & npm)
ENV NODE_ENV=production


# Copy package files first for Docker layer caching
COPY package.json package-lock.json ./

# Install ONLY production dependencies
# npm ci: clean install from lockfile (faster, deterministic)
# --omit=dev: excludes devDependencies (replaces deprecated --production)
RUN npm ci --omit=dev --no-audit --no-fund

# Copy application source code
COPY . .

# 🔒 Security: Create non-root user and switch to it
RUN addgroup -g 1001 -S nodejs && \
    adduser -S nodejs -u 1001 -G nodejs && \
    chown -R nodejs:nodejs /app
USER nodejs

# Expose the application port
EXPOSE 3001


CMD ["node", "server.js"]