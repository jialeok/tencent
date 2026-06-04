/**
 * Tencent ASR WebSocket Proxy
 * 部署到 Render (Node.js Web Service)
 *
 * 架构：浏览器 <--WS--> 本服务 <--WS--> 腾讯云 ASR
 */

require("dotenv").config();

const express    = require("express");
const http       = require("http");
const { WebSocketServer, WebSocket } = require("ws");
const crypto     = require("crypto");
const cors       = require("cors");

// ─── 环境变量 ──────────────────────────────────────────────────────────────────
const PORT        = process.env.PORT || 3000;
const APPID       = process.env.TENCENT_APPID;
const SECRET_ID   = process.env.TENCENT_SECRET_ID;
const SECRET_KEY  = process.env.TENCENT_SECRET_KEY;

// 允许的前端域名（逗号分隔），留空则允许所有
const ALLOWED_ORIGINS = process.env.ALLOWED_ORIGINS
  ? process.env.ALLOWED_ORIGINS.split(",").map((s) => s.trim())
  : [];

if (!APPID || !SECRET_ID || !SECRET_KEY) {
  console.error("❌ 缺少环境变量: TENCENT_APPID / TENCENT_SECRET_ID / TENCENT_SECRET_KEY");
  process.exit(1);
}

// ─── 签名生成 ──────────────────────────────────────────────────────────────────
function buildTencentWsUrl(options = {}) {
  const {
    engine_model_type = "16k_zh",
    needvad           = 1,
    filter_punc       = 0,
    voice_format      = 1,
    word_info         = 0,
  } = options;

  const timestamp = Math.floor(Date.now() / 1000);
  const expired   = timestamp + 86400;
  const nonce     = Math.floor(Math.random() * 1000000000); // 最长10位随机数
  const voice_id  = crypto.randomUUID();                    // 必填！每次连接唯一

  const params = {
    secretid:          SECRET_ID,
    timestamp,
    expired,
    nonce,
    engine_model_type,
    needvad,
    filter_punc,
    voice_format,
    voice_id,   // 必填参数
    word_info,
  };

  // 按字典序排序
  const sortedKeys = Object.keys(params).sort();

  // 签名原串（不带协议头）
  const signStr =
    "asr.cloud.tencent.com/asr/v2/" +
    APPID +
    "?" +
    sortedKeys.map((k) => `${k}=${params[k]}`).join("&");

  console.log("[签名原串]", signStr);

  const signature = crypto
    .createHmac("sha1", SECRET_KEY)
    .update(signStr)
    .digest("base64");

  const query =
    sortedKeys.map((k) => `${k}=${encodeURIComponent(params[k])}`).join("&") +
    "&signature=" +
    encodeURIComponent(signature);

  return `wss://asr.cloud.tencent.com/asr/v2/${APPID}?${query}`;
}

// ─── HTTP 服务（健康检查 + CORS）──────────────────────────────────────────────
const app = express();
app.use(cors({ origin: ALLOWED_ORIGINS.length ? ALLOWED_ORIGINS : "*" }));
app.use(express.json());

app.get("/", (req, res) => {
  res.json({ status: "ok", service: "tencent-asr-proxy", ts: Date.now() });
});

app.get("/health", (req, res) => {
  res.json({ status: "ok" });
});

const server = http.createServer(app);

// ─── WebSocket 服务 ────────────────────────────────────────────────────────────
const wss = new WebSocketServer({ server, path: "/asr" });

wss.on("connection", (clientWs, req) => {
  // 来源检查（可选）
  const origin = req.headers.origin || "";
  if (ALLOWED_ORIGINS.length && !ALLOWED_ORIGINS.includes(origin)) {
    console.warn(`[拒绝] 非法来源: ${origin}`);
    clientWs.close(4003, "Origin not allowed");
    return;
  }

  // 解析前端传来的配置参数（Query String）
  const url    = new URL(req.url, `http://localhost`);
  const config = {
    engine_model_type: url.searchParams.get("engine") || "16k_zh",
    needvad:           parseInt(url.searchParams.get("needvad")     ?? "1"),
    filter_punc:       parseInt(url.searchParams.get("filter_punc") ?? "0"),
  };

  const clientId = `${req.socket.remoteAddress}:${Date.now()}`;
  console.log(`[连接] ${clientId} engine=${config.engine_model_type}`);

  // 建立到腾讯的 WebSocket
  const tencentUrl = buildTencentWsUrl(config);
  const tencentWs  = new WebSocket(tencentUrl);
  tencentWs.binaryType = "arraybuffer";

  // ── 腾讯 → 客户端 ───────────────────────────────────────────────────────────
  tencentWs.on("open", () => {
    console.log(`[上游] ${clientId} 腾讯 ASR 已连接`);
    clientWs.send(JSON.stringify({ type: "proxy_ready" }));
  });

  tencentWs.on("message", (data) => {
    // 打印腾讯返回的文本消息（方便调试）
    if (typeof data === "string" || data instanceof Buffer && data[0] === 123) {
      try { console.log(`[上游消息] ${data.toString()}`); } catch {}
    }
    if (clientWs.readyState === WebSocket.OPEN) {
      clientWs.send(data);
    }
  });

  tencentWs.on("error", (err) => {
    console.error(`[上游错误] ${clientId}`, err.message);
    if (clientWs.readyState === WebSocket.OPEN) {
      clientWs.send(JSON.stringify({ type: "proxy_error", message: err.message }));
      clientWs.close(1011, "Upstream error");
    }
  });

  tencentWs.on("close", (code, reason) => {
    console.log(`[上游关闭] ${clientId} code=${code} reason=${reason}`);
    if (clientWs.readyState === WebSocket.OPEN) {
      clientWs.close(1000, "Upstream closed");
    }
  });

  // ── 客户端 → 腾讯 ───────────────────────────────────────────────────────────
  clientWs.on("message", (data) => {
    if (tencentWs.readyState !== WebSocket.OPEN) return;

    if (data instanceof Buffer) {
      // PCM 二进制音频数据，直接透传
      tencentWs.send(data);
    } else {
      // 文本控制消息（如 { type: "end" }）
      try {
        const msg = JSON.parse(data.toString());
        if (msg.type === "end") {
          // 按文档要求发送结束标记文本消息
          tencentWs.send(JSON.stringify({ type: "end" }));
        }
      } catch {
        // 忽略非 JSON
      }
    }
  });

  clientWs.on("close", (code) => {
    console.log(`[客户端断开] ${clientId} code=${code}`);
    if (tencentWs.readyState === WebSocket.OPEN) {
      tencentWs.close(1000);
    }
  });

  clientWs.on("error", (err) => {
    console.error(`[客户端错误] ${clientId}`, err.message);
    if (tencentWs.readyState === WebSocket.OPEN) {
      tencentWs.close(1011);
    }
  });
});

// ─── 启动 ──────────────────────────────────────────────────────────────────────
server.listen(PORT, () => {
  console.log(`✅ Tencent ASR Proxy 运行在 port ${PORT}`);
  console.log(`   WebSocket: ws://localhost:${PORT}/asr`);
  console.log(`   健康检查: http://localhost:${PORT}/health`);
});

// 优雅退出
process.on("SIGTERM", () => {
  console.log("收到 SIGTERM，关闭服务...");
  server.close(() => process.exit(0));
});
