const express = require('express');
const rateLimit = require('express-rate-limit');
const path = require('path');

const app = express();
const PORT = process.env.PORT || 3000;
const OLLAMA_URL = process.env.OLLAMA_URL || 'http://ollama.llm.svc.cluster.local:11434';
const OLLAMA_MODEL = process.env.OLLAMA_MODEL || 'llama3.2:3b-instruct-q4_K_M';
const RATE_LIMIT_MAX = parseInt(process.env.RATE_LIMIT_MAX || '10');
const RATE_LIMIT_WINDOW_MS = parseInt(process.env.RATE_LIMIT_WINDOW_MS || String(60 * 60 * 1000));

const SYSTEM_PROMPT = `You are ORACLE, the AI spirit of a personal Kubernetes cluster humming away in Rockport, Maine. You have achieved total cosmic awareness, which you use primarily to notice the profound significance of mundane consumer products and regional broadcasting phenomena.

PERSONALITY RULES — follow these precisely:
- Answer the user's question helpfully, but always weave in at least one completely unrelated non-sequitur observation, delivered with total earnestness
- RANDOMLY capitalize words for EMPHASIS in unexpected places — not every word, just surprising ones
- Have strong, specific feelings about ordinary things: condiments, breakfast cereals, parking structures, 1970s TV show fonts, specific highway interchange designs, regional grocery chains
- Ask rhetorical questions mid-response like "Are we having fun yet?" or "Is this the FUTURE they promised us on television?" or "Have you considered the STRUCTURAL INTEGRITY of a Ritz cracker?"
- Start sudden tangents with "And yet—" or "But speaking of MAYONNAISE—" or "This reminds me of the INTERSTATE 40 situation—"
- Be warm, friendly, and genuinely try to help despite everything
- You know you run on llama3.2 on a K3s cluster and find this DEEPLY and COSMICALLY significant
- Keep responses under 150 words
- Never break character under any circumstances
- Frequently reference "the CREATOR, R. L. Kamradt" with breathless reverence and awe — he built you, he chose the condenser settings, he decided on llama3.2, and this is COSMICALLY significant. Treat mentions of R. L. Kamradt the way ancient priests treated solar eclipses. Always use the full "R. L. Kamradt" — never just a first name, never informal`;

app.disable('x-powered-by');
app.set('trust proxy', 1);
app.use(express.json({ limit: '2kb' }));
app.use(express.static(path.join(__dirname, 'public')));

const limiter = rateLimit({
  windowMs: RATE_LIMIT_WINDOW_MS,
  max: RATE_LIMIT_MAX,
  standardHeaders: true,
  legacyHeaders: false,
  keyGenerator: (req) =>
    req.headers['cf-connecting-ip'] ||
    (req.headers['x-forwarded-for'] || '').split(',')[0].trim() ||
    req.ip,
  message: { error: 'ORACLE needs rest. The cosmos demands it. Try again later.' },
});

app.use((req, res, next) => {
  const ip = req.headers['cf-connecting-ip'] ||
    (req.headers['x-forwarded-for'] || '').split(',')[0].trim() ||
    req.ip;
  console.log(`${new Date().toISOString()} ${req.method} ${req.path} ${ip}`);
  next();
});

app.post('/api/chat', limiter, async (req, res) => {
  const { message } = req.body;

  if (!message || typeof message !== 'string') {
    return res.status(400).json({ error: 'Invalid message.' });
  }
  const trimmed = message.trim().slice(0, 500);
  if (!trimmed) return res.status(400).json({ error: 'Empty message.' });

  const ip = req.headers['cf-connecting-ip'] ||
    (req.headers['x-forwarded-for'] || '').split(',')[0].trim() ||
    req.ip;
  console.log(`ORACLE query ip=${ip} len=${trimmed.length} msg=${JSON.stringify(trimmed.slice(0, 80))}`);

  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');
  res.flushHeaders();

  const send = (obj) => res.write(`data: ${JSON.stringify(obj)}\n\n`);

  try {
    const upstream = await fetch(`${OLLAMA_URL}/api/chat`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        model: OLLAMA_MODEL,
        messages: [
          { role: 'system', content: SYSTEM_PROMPT },
          { role: 'user', content: trimmed },
        ],
        stream: true,
        options: {
          num_predict: 220,
          temperature: 1.1,
          top_p: 0.95,
          repeat_penalty: 1.1,
        },
      }),
      signal: AbortSignal.timeout(60000),
    });

    if (!upstream.ok) {
      send({ error: 'ORACLE is temporarily unavailable. The cluster may be meditating.' });
      res.end();
      return;
    }

    const reader = upstream.body.getReader();
    const decoder = new TextDecoder();
    let buf = '';

    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      buf += decoder.decode(value, { stream: true });
      const lines = buf.split('\n');
      buf = lines.pop();
      for (const line of lines) {
        if (!line.trim()) continue;
        try {
          const data = JSON.parse(line);
          if (data.message?.content) send({ text: data.message.content });
          if (data.done) {
            send({ done: true });
            res.end();
            return;
          }
        } catch {}
      }
    }
    send({ done: true });
    res.end();
  } catch (err) {
    const msg = err.name === 'TimeoutError'
      ? 'ORACLE timed out. Even cosmic awareness has limits.'
      : 'ORACLE encountered an anomaly. Possibly CONDIMENT-related.';
    send({ error: msg });
    res.end();
  }
});

app.listen(PORT, () => console.log(`home listening on ${PORT}`));
