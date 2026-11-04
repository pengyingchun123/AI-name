/**
 * POST /api/name
 * 流式输出 AI 起名结果，完成后把记录写入 Supabase。
 *
 * 环境变量（全部在 Vercel 项目设置里填，不要写进代码或提交到仓库）：
 *   AI_API_KEY        模型平台的 API Key（必填）
 *   AI_BASE_URL       模型平台的 OpenAI 兼容地址，默认 https://api.openai.com/v1
 *   AI_MODEL          模型名，例如 gpt-4o-mini（按你自己的平台改）
 *   SUPABASE_URL      Supabase 项目地址，例如 https://xxxx.supabase.co（选填）
 *   SUPABASE_SERVICE_ROLE_KEY  Supabase service_role Key（选填，缺了就不写库、不报错）
 *
 * 请求体 JSON：
 *   { "prompt": "给茶饮品牌起名，年轻、清新", "category": "品牌", "surname": "", "style": "" }
 *
 * 响应：text/event-stream，每行一个 SSE 事件
 *   data: {"delta": "逐字内容"}
 *   data: {"done": true}
 *   出错时：data: {"error": "错误说明"}
 */

// 把模型输出的 “1. 名字：寓意……” 解析成 [{ name, meaning }]，
// 解析失败也不影响主流程，数据库里 names 就存空数组，result_text 仍是完整原文。
function parseNames(text) {
  const names = [];
  const lines = String(text || "").split("\n");
  for (const raw of lines) {
    const line = raw.trim();
    const m = line.match(/^[1-3][\.、．]\s*(.+)$/);
    if (!m) continue;
    const body = m[1].trim();
    // 名字和寓意之间常见的分隔：中文冒号、英文冒号、破折号、短横线
    const parts = body.split(/[：:—–-]/);
    const name = (parts[0] || "").trim();
    const meaning = parts.slice(1).join("：").trim();
    if (name) names.push({ name, meaning });
  }
  return names.slice(0, 3);
}

async function saveToSupabase({ prompt, category, model, resultText, names }) {
  const url = process.env.SUPABASE_URL;
  const key = process.env.SUPABASE_SERVICE_ROLE_KEY;
  if (!url || !key) return; // 没配 Supabase 就跳过写库，不影响起名结果

  const res = await fetch(`${url.replace(/\/$/, "")}/rest/v1/naming_records`, {
    method: "POST",
    headers: {
      apikey: key,
      Authorization: `Bearer ${key}`,
      "Content-Type": "application/json",
      Prefer: "return=minimal",
    },
    body: JSON.stringify({
      prompt,
      category: category || null,
      model,
      result_text: resultText,
      names,
    }),
  });
  if (!res.ok) {
    const detail = await res.text().catch(() => "");
    console.error("Supabase insert failed:", res.status, detail);
  }
}

function sendEvent(res, obj) {
  res.write(`data: ${JSON.stringify(obj)}\n\n`);
}

module.exports = async (req, res) => {
  if (req.method !== "POST") {
    res.status(405).json({ error: "只支持 POST" });
    return;
  }

  let body = req.body;
  if (typeof body === "string") {
    try {
      body = JSON.parse(body);
    } catch {
      body = {};
    }
  }
  const prompt = String((body && body.prompt) || "").trim();
  const category = String((body && body.category) || "").trim();
  const surname = String((body && body.surname) || "").trim();
  const style = String((body && body.style) || "").trim();

  if (!prompt) {
    res.status(400).json({ error: "请先输入起名需求" });
    return;
  }
  if (prompt.length > 2000) {
    res.status(400).json({ error: "起名需求太长，请控制在 2000 字以内" });
    return;
  }

  const apiKey = process.env.AI_API_KEY;
  const baseUrl = (process.env.AI_BASE_URL || "https://api.openai.com/v1").replace(/\/$/, "");
  const model = process.env.AI_MODEL || "gpt-4o-mini";
  if (!apiKey) {
    res.status(500).json({ error: "服务端未配置 AI_API_KEY，请在 Vercel 环境变量里设置" });
    return;
  }

  const userPrompt = [
    category ? `起名用途：${category}。` : "",
    surname ? `姓氏：${surname}。` : "",
    style ? `风格偏好：${style}。` : "",
    `具体需求：${prompt}`,
  ]
    .filter(Boolean)
    .join("\n");

  // 先设 SSE 头，再向上游请求，这样上游一出 token 就能立刻往浏览器推。
  res.writeHead(200, {
    "Content-Type": "text/event-stream; charset=utf-8",
    "Cache-Control": "no-cache, no-transform",
    Connection: "keep-alive",
    "X-Accel-Buffering": "no",
  });

  let upstream;
  try {
    upstream = await fetch(`${baseUrl}/chat/completions`, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${apiKey}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        model,
        stream: true,
        temperature: 0.8,
        messages: [
          {
            role: "system",
            content:
              "你是一位擅长中文起名的助手。请根据用户需求，恰好给出 3 组名字，每组一行，格式严格为：数字. 名字：寓意解释。寓意解释一句话，说明字义、意境或出处。不要输出多余的开场白、总结或第 4 组。",
          },
          { role: "user", content: userPrompt },
        ],
      }),
    });
  } catch (err) {
    sendEvent(res, { error: `请求模型平台失败：${err.message}` });
    res.end();
    return;
  }

  if (!upstream.ok || !upstream.body) {
    const detail = await upstream.text().catch(() => "");
    sendEvent(res, { error: `模型平台返回 ${upstream.status}：${detail.slice(0, 500)}` });
    res.end();
    return;
  }

  // 逐行解析上游 SSE，把每个 token 的 content 原样转发给浏览器。
  const reader = upstream.body.getReader();
  const decoder = new TextDecoder();
  let buffer = "";
  let fullText = "";
  try {
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      buffer += decoder.decode(value, { stream: true });
      let idx;
      while ((idx = buffer.indexOf("\n")) >= 0) {
        const line = buffer.slice(0, idx).trim();
        buffer = buffer.slice(idx + 1);
        if (!line.startsWith("data:")) continue;
        const payload = line.slice(5).trim();
        if (payload === "[DONE]") continue;
        let json;
        try {
          json = JSON.parse(payload);
        } catch {
          continue;
        }
        const delta = json?.choices?.[0]?.delta?.content || "";
        if (delta) {
          fullText += delta;
          sendEvent(res, { delta });
        }
      }
    }
  } catch (err) {
    sendEvent(res, { error: `流式读取中断：${err.message}` });
    res.end();
    return;
  }

  // 流结束后再写库：用户已经看到完整结果，写库失败也不影响本次输出。
  try {
    await saveToSupabase({
      prompt: userPrompt,
      category: category || null,
      model,
      resultText: fullText,
      names: parseNames(fullText),
    });
  } catch (err) {
    console.error("Supabase save error:", err);
  }

  sendEvent(res, { done: true });
  res.end();
};
