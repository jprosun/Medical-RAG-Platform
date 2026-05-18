FROM node:24-alpine

WORKDIR /app

COPY services/web-ui/package*.json ./
RUN npm ci

COPY services/web-ui/ ./

EXPOSE 5173

CMD ["npm", "run", "dev"]
