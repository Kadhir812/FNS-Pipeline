# Dockerfile for frontend (React + Vite)
FROM node:18-alpine

WORKDIR /app

# Install dependencies
COPY package.json package-lock.json* .
RUN npm install

# Copy the rest of the frontend code
COPY . .

# Build the frontend
RUN npm run build

# Expose the port Vite/production build serves on (adjust if needed)
EXPOSE 5173

# Start the dev server (for development) or serve the build (for production)
CMD ["npm", "run", "dev"]
