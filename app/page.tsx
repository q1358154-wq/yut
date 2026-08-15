import { DurableObject } from "cloudflare:workers";

interface Env {
  VERCEL_ORIGIN: string;
  GATEWAY_SECRET: string;
  CHAT_RATE_LIMITER: RateLimit;
  CLIENT_GATE: DurableObjectNamespace<ClientGate>;
}

const MAX_CLIENT_CONCURRENT = 2;
const LEASE_TTL_MS = 150000;

function json(data: unknown, status = 200, headers: HeadersInit = {}) {
  return new Response(JSON.stringify(data), {
    status,
    headers: {
      "Content-Type": "application/json; charset=utf-8",
      "Cache-Control": "no-store",
      "X-Content-Type-Options": "nosniff",
      ...headers,
    },
  });
}

function getClientCookie(request: Request) {
  const cookie = request.headers.get("Cookie") || "";

  const match = cookie.match(
    /(?:^|;\s*)cqs_client_id=([^;]+)/
  );

  return match?.[1] || null;
}

function createClientId() {
  return crypto.randomUUID();
}

async function sha256(value: string) {
  const data = new TextEncoder().encode(value);

  const digest = await crypto.subtle.digest(
    "SHA-256",
    data
  );

  return [...new Uint8Array(digest)]
    .map((b) => b.toString(16).padStart(2, "0"))
    .join("");
}

function getClientIdentity(request: Request, clientId: string) {
  const ip =
    request.headers.get("CF-Connecting-IP") ||
    request.headers.get("X-Forwarded-For") ||
    "unknown";

  const ua =
    request.headers.get("User-Agent") ||
    "unknown";

  return `${clientId}|${ip}|${ua}`;
}

function sanitizeOrigin(origin: string) {
  return origin.replace(/\/+$/, "");
}

function buildUpstreamRequest(
  request: Request,
  env: Env
) {
  const url = new URL(request.url);

  const upstreamUrl =
    sanitizeOrigin(env.VERCEL_ORIGIN) +
    url.pathname +
    url.search;

  const headers = new Headers(request.headers);

  headers.set(
    "X-CQS-Gateway-Key",
    env.GATEWAY_SECRET
  );

  headers.set(
    "X-Forwarded-Host",
    url.host
  );

  headers.set(
    "X-CQS-Edge",
    "cloudflare"
  );

  headers.delete("Cookie");
  headers.delete("Host");

  return new Request(upstreamUrl, {
    method: request.method,
    headers,
    body:
      request.method === "GET" ||
      request.method === "HEAD"
        ? undefined
        : request.body,
    redirect: "manual",
  });
}

function proxyResponse(
  upstream: Response,
  clientId: string,
  gate: DurableObjectStub<ClientGate>
) {
  const headers = new Headers(upstream.headers);

  headers.set(
    "Cache-Control",
    "no-cache, no-store, must-revalidate"
  );

  headers.set(
    "X-CQS-Edge",
    "cloudflare"
  );

  headers.set(
    "X-CQS-Client",
    "protected"
  );

  if (!headers.has("X-Request-ID")) {
    headers.set(
      "X-Request-ID",
      crypto.randomUUID()
    );
  }

  let released = false;

  const release = async () => {
    if (released) return;

    released = true;

    try {
      await gate.fetch(
        new Request(
          "https://gate.local/release",
          {
            method: "POST",
            body: JSON.stringify({
              lease: clientId,
            }),
          }
        )
      );
    } catch {}
  };

  if (!upstream.body) {
    void release();

    return new Response(null, {
      status: upstream.status,
      headers,
    });
  }

  const reader =
    upstream.body.getReader();

  const stream =
    new ReadableStream<Uint8Array>({
      async pull(controller) {
        try {
          const { done, value } =
            await reader.read();

          if (done) {
            await release();
            controller.close();
            return;
          }

          controller.enqueue(value);
        } catch (error) {
          await release();

          try {
            controller.error(error);
          } catch {}
        }
      },

      async cancel(reason) {
        try {
          await reader.cancel(reason);
        } catch {}

        await release();
      },
    });

  return new Response(stream, {
    status: upstream.status,
    statusText: upstream.statusText,
    headers,
  });
}

export default {
  async fetch(
    request: Request,
    env: Env
  ): Promise<Response> {

    const url =
      new URL(request.url);

    if (
      url.pathname !== "/api/chat"
    ) {
      return json(
        {
          error:
            "Not Found",
        },
        404
      );
    }

    if (request.method === "OPTIONS") {
      return new Response(null, {
        status: 204,
        headers: {
          "Access-Control-Allow-Methods":
            "POST, OPTIONS",
          "Access-Control-Allow-Headers":
            "Content-Type, Authorization",
          "Access-Control-Max-Age":
            "86400",
        },
      });
    }

    if (request.method !== "POST") {
      return json(
        {
          error:
            "Method Not Allowed",
        },
        405,
        {
          Allow: "POST, OPTIONS",
        }
      );
    }

    if (!env.VERCEL_ORIGIN) {
      return json(
        {
          error:
            "Cloudflare VERCEL_ORIGIN 未配置",
        },
        500
      );
    }

    if (!env.GATEWAY_SECRET) {
      return json(
        {
          error:
            "Cloudflare GATEWAY_SECRET 未配置",
        },
        500
      );
    }

    const ip =
      request.headers.get(
        "CF-Connecting-IP"
      ) || "unknown";

    const rateKey =
      `chat:${ip}`;

    const rate =
      await env.CHAT_RATE_LIMITER.limit({
        key: rateKey,
      });

    if (!rate.success) {
      return json(
        {
          error:
            "请求过于频繁，请稍后再试。",
        },
        429,
        {
          "Retry-After": "60",
        }
      );
    }

    let clientId =
      getClientCookie(request);

    let newClientCookie = false;

    if (!clientId) {
      clientId = createClientId();
      newClientCookie = true;
    }

    const identity =
      await sha256(
        getClientIdentity(
          request,
          clientId
        )
      );

    const gate =
      env.CLIENT_GATE.getByName(
        identity
      );

    const leaseId =
      crypto.randomUUID();

    const acquireResponse =
      await gate.fetch(
        new Request(
          "https://gate.local/acquire",
          {
            method: "POST",
            body: JSON.stringify({
              lease: leaseId,
              max:
                MAX_CLIENT_CONCURRENT,
              ttl:
                LEASE_TTL_MS,
            }),
          }
        )
      );

    if (!acquireResponse.ok) {
      return json(
        {
          error:
            "当前请求较多，请稍后再试。",
        },
        429,
        {
          "Retry-After": "3",
        }
      );
    }

    let upstream: Response;

    try {
      const upstreamRequest =
        buildUpstreamRequest(
          request,
          env
        );

      upstream =
        await fetch(
          upstreamRequest
        );
    } catch {
      await gate.fetch(
        new Request(
          "https://gate.local/release",
          {
            method: "POST",
            body: JSON.stringify({
              lease: leaseId,
            }),
          }
        )
      );

      return json(
        {
          error:
            "上游服务暂时无法连接。",
        },
        502
      );
    }

    if (!upstream.ok) {
      await gate.fetch(
        new Request(
          "https://gate.local/release",
          {
            method: "POST",
            body: JSON.stringify({
              lease: leaseId,
            }),
          }
        )
      );
    }

    const response =
      proxyResponse(
        upstream,
        leaseId,
        gate
      );

    if (newClientCookie) {
      response.headers.append(
        "Set-Cookie",
        `cqs_client_id=${clientId}; Path=/; Max-Age=31536000; HttpOnly; Secure; SameSite=Lax`
      );
    }

    return response;
  },
};

export class ClientGate
  extends DurableObject {

  private leases =
    new Map<
      string,
      number
    >();

  constructor(
    ctx: DurableObjectState,
    env: Env
  ) {
    super(ctx, env);

    this.ctx.blockConcurrencyWhile(
      async () => {
        this.cleanExpired();
      }
    );
  }

  private cleanExpired() {
    const now = Date.now();

    for (
      const [
        lease,
        expiresAt,
      ] of this.leases
    ) {
      if (
        expiresAt <= now
      ) {
        this.leases.delete(
          lease
        );
      }
    }
  }

  async fetch(
    request: Request
  ): Promise<Response> {

    const url =
      new URL(request.url);

    let body: any = {};

    try {
      body =
        await request.json();
    } catch {}

    this.cleanExpired();

    if (
      url.pathname ===
      "/acquire"
    ) {
      const max =
        Number(body.max) ||
        MAX_CLIENT_CONCURRENT;

      const ttl =
        Math.min(
          Math.max(
            Number(body.ttl) ||
              LEASE_TTL_MS,
            10000
          ),
          180000
        );

      const lease =
        String(
          body.lease || ""
        );

      if (!lease) {
        return new Response(
          "missing lease",
          {
            status: 400,
          }
        );
      }

      if (
        this.leases.size >= max
      ) {
        return new Response(
          "busy",
          {
            status: 429,
          }
        );
      }

      this.leases.set(
        lease,
        Date.now() + ttl
      );

      await this.ctx.storage.setAlarm(
        Date.now() + ttl
      );

      return new Response(
        "ok",
        {
          status: 200,
        }
      );
    }

    if (
      url.pathname ===
      "/release"
    ) {
      const lease =
        String(
          body.lease || ""
        );

      if (lease) {
        this.leases.delete(
          lease
        );
      }

      return new Response(
        "ok"
      );
    }

    return new Response(
      "Not Found",
      {
        status: 404,
      }
    );
  }

  async alarm() {
    this.cleanExpired();

    if (
      this.leases.size > 0
    ) {
      const next =
        Math.min(
          ...this.leases.values()
        );

      await this.ctx.storage.setAlarm(
        next
      );
    }
  }
}
