# 使用輕量級的 Node.js 環境
FROM node:18-alpine

# 設定工作目錄
WORKDIR /app

# 先複製 package 設定檔 (利用 Docker 快取層)
COPY package.json ./

# 安裝套件
RUN npm install

# 複製所有程式碼
COPY . .

# 開放 Port
EXPOSE 3000

# 啟動指令
CMD ["npm", "start"]