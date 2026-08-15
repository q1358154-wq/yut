import { NextResponse } from 'next/server';

/*
 * ============================================================
 * CQS AI / Universal Invoice
 * Industrial Streaming Gateway V3
 *
 * 设计目标：
 * - Cloudflare / Vercel 友好
 * - SSE Streaming
 * - Provider Failover
 * - Retry + Exponential Backoff + Jitter
 * - Circuit Breaker
 * - Timeout
 * - Request Abort
 * - Duplicate Request Protection
 * - Input Validation
 * - Request ID
 * - Provider concurrency protection
 * - 不保存 API Key 到客户端
 * ============================================================
 */

/*
 * Cloudflare / Edge 环境更安全。
 *
 * 如果你的当前 Cloudflare Next.js 适配器明确要求
 * nodejs runtime，可以删除这一行；
 * 正常情况下 Edge/Workers 更适合高并发 I/O。
 */
export const runtime = 'edge';

export const dynamic = 'force-dynamic';
export const revalidate = 0;

/* ============================================================
   Provider Configuration
   ============================================================ */

const PROVIDER_CONFIG = {
  deepseek: {
    name: 'DeepSeek',
    url: 'https://api.deepseek.com/v1/chat/completions',
    keyEnv: 'DEEPSEEK_API_KEY',
    model: 'deepseek-chat',

    /*
     * DeepSeek 支持 JSON Object。
     */
    jsonMode: true
  },

  still: {
    name: 'Still',
    urlEnv: 'STILL_API_URL',
    keyEnv: 'STILL_API_KEY',
    modelEnv: 'STILL_MODEL',
    fallbackModel: 'still',

    /*
     * 不假定第三方 Provider 支持
     * OpenAI response_format。
     */
    jsonMode: false
  },

  agent: {
    name: 'Agent',
    urlEnv: 'AGENT_API_URL',
    keyEnv: 'AGENT_API_KEY',
    modelEnv: 'AGENT_MODEL',
    fallbackModel: 'agent',

    jsonMode: false
  }
} as const;

/* ============================================================
   Limits
   ============================================================ */

const REQUEST_TIMEOUT_MS = 120000;

const MAX_MESSAGES = 100;

const MAX_MESSAGE_LENGTH = 20000;

const MAX_SYSTEM_PROMPT_LENGTH = 30000;

/*
 * JSON Body 大小保护。
 *
 * 这里只在解析后做字符串级保护；
 * Cloudflare 还应该在边缘层设置 Request Size / WAF。
 */
const MAX_BODY_LENGTH = 150000;

/*
 * Provider 单实例保护。
 *
 * 注意：
 * 这不是“全球百万并发限制”。
 * 它只保护当前 Worker/Server 实例。
 */
const LOCAL_MAX_CONCURRENT = 64;

/* ============================================================
   Retry
   ============================================================ */

const MAX_RETRIES = 2;

const RETRY_BASE_MS = 350;

const RETRY_JITTER_MS = 250;

/* ============================================================
   Circuit Breaker
   ============================================================ */

const CIRCUIT_FAILURE_THRESHOLD = 5;

const CIRCUIT_OPEN_MS = 30000;

/* ============================================================
   In-memory State
   ============================================================ */

type ProviderState = {
  failures: number;
  successes: number;
  openedAt: number;
  halfOpenProbe: boolean;
  active: number;
};

const providerState = new Map<
  string,
  ProviderState
>();

/*
 * 只用于防止同一个实例上的重复请求。
 *
 * Cloudflare / Vercel 横向扩容后，
 * 每个实例都有自己的 Map。
 *
 * 真正全球级 Rate Limit 应交给 Cloudflare。
 */
const activeRequests = new Map<
  string,
  number
>();

/* ============================================================
   Provider State
   ============================================================ */

function getProviderState(
  provider: string
): ProviderState {
  let state = providerState.get(provider);

  if (!state) {
    state = {
      failures: 0,
      successes: 0,
      openedAt: 0,
      halfOpenProbe: false,
      active: 0
    };

    providerState.set(
      provider,
      state
    );
  }

  return state;
}

function isCircuitOpen(
  provider: string
) {
  const state =
    getProviderState(provider);

  if (!state.openedAt) {
    return false;
  }

  const elapsed =
    Date.now() - state.openedAt;

  /*
   * 熔断时间结束。
   *
   * 允许一个探针请求进入 Half-Open。
   */
  if (
    elapsed >= CIRCUIT_OPEN_MS
  ) {
    if (!state.halfOpenProbe) {
      state.halfOpenProbe = true;
      return false;
    }

    return true;
  }

  return true;
}

function markProviderSuccess(
  provider: string
) {
  const state =
    getProviderState(provider);

  state.successes += 1;
  state.failures = 0;
  state.openedAt = 0;
  state.halfOpenProbe = false;
}

function markProviderFailure(
  provider: string
) {
  const state =
    getProviderState(provider);

  state.failures += 1;

  if (
    state.failures >=
    CIRCUIT_FAILURE_THRESHOLD
  ) {
    state.openedAt = Date.now();
    state.halfOpenProbe = false;
  }
}

function releaseProvider(
  provider: string
) {
  const state =
    getProviderState(provider);

  state.active = Math.max(
    0,
    state.active - 1
  );

  /*
   * Half-open 探针结束后，
   * 如果没有继续保持熔断，
   * 允许下一次正常请求。
   */
  if (
    state.openedAt &&
    Date.now() -
      state.openedAt >=
      CIRCUIT_OPEN_MS
  ) {
    state.halfOpenProbe = false;
  }
}

function acquireProvider(
  provider: string
) {
  const state =
    getProviderState(provider);

  if (
    state.active >=
    LOCAL_MAX_CONCURRENT
  ) {
    return false;
  }

  if (
    isCircuitOpen(provider)
  ) {
    return false;
  }

  state.active += 1;

  return true;
}

/* ============================================================
   Request ID
   ============================================================ */

function createRequestId() {
  const random =
    Math.random()
      .toString(36)
      .slice(2, 10)
      .toUpperCase();

  return `CQS-${Date.now()}-${random}`;
}

/* ============================================================
   Error Response
   ============================================================ */

function jsonError(
  message: string,
  status: number,
  requestId: string
) {
  return NextResponse.json(
    {
      error: message,
      requestId
    },
    {
      status,
      headers: {
        'Cache-Control':
          'no-store, no-cache, must-revalidate',
        'X-Request-ID':
          requestId,
        'X-Content-Type-Options':
          'nosniff'
      }
    }
  );
}

/* ============================================================
   Body Reader
   ============================================================ */

async function readBody(
  request: Request
) {
  const contentLength =
    request.headers.get(
      'content-length'
    );

  if (contentLength) {
    const length =
      Number(contentLength);

    if (
      Number.isFinite(length) &&
      length > MAX_BODY_LENGTH
    ) {
      throw new Error(
        '请求数据过大。'
      );
    }
  }

  try {
    const body =
      await request.json();

    return body;
  } catch {
    throw new Error(
      '请求数据不是有效的 JSON。'
    );
  }
}

/* ============================================================
   Validation
   ============================================================ */

function validateBody(
  body: any
) {
  if (
    !body ||
    typeof body !== 'object'
  ) {
    throw new Error(
      '请求数据格式无效。'
    );
  }

  const {
    provider,
    messages,
    systemPrompt
  } = body;

  if (
    typeof provider !==
      'string' ||
    !Object.prototype.hasOwnProperty.call(
      PROVIDER_CONFIG,
      provider
    )
  ) {
    throw new Error(
      '不支持的 AI Provider。'
    );
  }

  if (
    !Array.isArray(messages) ||
    messages.length === 0 ||
    messages.length > MAX_MESSAGES
  ) {
    throw new Error(
      `messages 数量必须为 1-${MAX_MESSAGES}。`
    );
  }

  if (
    typeof systemPrompt !==
      'string' ||
    !systemPrompt.trim() ||
    systemPrompt.length >
      MAX_SYSTEM_PROMPT_LENGTH
  ) {
    throw new Error(
      'systemPrompt 无效或超出长度限制。'
    );
  }

  for (const message of messages) {
    if (
      !message ||
      typeof message !==
        'object' ||
      typeof message.role !==
        'string' ||
      typeof message.content !==
        'string'
    ) {
      throw new Error(
        'messages 中存在无效结构。'
      );
    }

    if (
      message.content.length >
      MAX_MESSAGE_LENGTH
    ) {
      throw new Error(
        '单条消息超出长度限制。'
      );
    }
  }

  /*
   * 最后进行序列化大小检查。
   */
  try {
    const serialized =
      JSON.stringify(body);

    if (
      serialized.length >
      MAX_BODY_LENGTH
    ) {
      throw new Error(
        '请求数据过大。'
      );
    }
  } catch {
    throw new Error(
      '请求数据无法处理。'
    );
  }
}

/* ============================================================
   Provider Resolver
   ============================================================ */

function resolveProvider(
  provider: string
) {
  const config =
    PROVIDER_CONFIG[
      provider as keyof typeof PROVIDER_CONFIG
    ];

  if (!config) {
    throw new Error(
      'Provider 不存在。'
    );
  }

  if (provider === 'deepseek') {
    const key =
      process.env.DEEPSEEK_API_KEY;

    if (!key) {
      throw new Error(
        '服务器未配置 DeepSeek API Key。'
      );
    }

    return {
      ...config,
      url: config.url,
      key,
      model: config.model
    };
  }

  const dynamicConfig =
    config as any;

  const url =
    process.env[
      dynamicConfig.urlEnv
    ];

  const key =
    process.env[
      dynamicConfig.keyEnv
    ];

  const model =
    process.env[
      dynamicConfig.modelEnv
    ] ||
    dynamicConfig.fallbackModel;

  if (!url || !key) {
    throw new Error(
      `${config.name} Provider 尚未配置。`
    );
  }

  return {
    ...config,
    url,
    key,
    model
  };
}

/* ============================================================
   Retry Policy
   ============================================================ */

function shouldRetryStatus(
  status: number
) {
  return [
    408,
    425,
    429,
    500,
    502,
    503,
    504
  ].includes(status);
}

function sleep(
  ms: number
) {
  return new Promise(
    resolve =>
      setTimeout(
        resolve,
        ms
      )
  );
}

function retryDelay(
  attempt: number
) {
  const exponential =
    RETRY_BASE_MS *
    Math.pow(
      2,
      attempt
    );

  const jitter =
    Math.floor(
      Math.random() *
        RETRY_JITTER_MS
    );

  return (
    exponential +
    jitter
  );
}

/* ============================================================
   Upstream Error
   ============================================================ */

async function readUpstreamError(
  response: Response
) {
  let text = '';

  try {
    text =
      await response.text();
  } catch {}

  if (
    text.length > 4000
  ) {
    text =
      text.slice(0, 4000) +
      '...';
  }

  return text;
}

/* ============================================================
   Provider Fetch
   ============================================================ */

async function fetchProvider(
  provider: string,
  requestBody: any,
  requestSignal: AbortSignal
) {
  const config =
    resolveProvider(provider);

  let lastError:
    | Error
    | null = null;

  for (
    let attempt = 0;
    attempt <= MAX_RETRIES;
    attempt++
  ) {
    if (
      requestSignal.aborted
    ) {
      throw new DOMException(
        '请求已取消。',
        'AbortError'
      );
    }

    const controller =
      new AbortController();

    const onAbort = () => {
      try {
        controller.abort();
      } catch {}
    };

    requestSignal.addEventListener(
      'abort',
      onAbort,
      { once: true }
    );

    const timeoutId =
      setTimeout(() => {
        try {
          controller.abort();
        } catch {}
      }, REQUEST_TIMEOUT_MS);

    try {
      /*
       * 基础请求体。
       */
      const upstreamBody: any = {
        model: config.model,

        messages: [
          {
            role: 'system',
            content:
              requestBody.systemPrompt
          },
          ...requestBody.messages
        ],

        stream: true
      };

      /*
       * 只有明确知道 Provider 支持时，
       * 才开启 response_format。
       *
       * 避免 Still / Agent 因为不支持
       * OpenAI JSON Mode 而直接 400。
       */
      if (config.jsonMode) {
        upstreamBody.response_format = {
          type: 'json_object'
        };
      }

      const response =
        await fetch(
          config.url,
          {
            method: 'POST',

            headers: {
              'Content-Type':
                'application/json',

              'Authorization':
                `Bearer ${config.key}`,

              'Accept':
                'text/event-stream',

              'Cache-Control':
                'no-cache'
            },

            body:
              JSON.stringify(
                upstreamBody
              ),

            signal:
              controller.signal,

            cache:
              'no-store'
          }
        );

      /*
       * HTTP 成功。
       *
       * 注意：
       * 不在这里读取 body。
       * 必须把流交给下游 SSE。
       */
      if (response.ok) {
        return response;
      }

      const errorText =
        await readUpstreamError(
          response
        );

      if (
        !shouldRetryStatus(
          response.status
        ) ||
        attempt >=
          MAX_RETRIES
      ) {
        const error: any =
          new Error(
            `${config.name} API 报错: ${
              errorText ||
              response.statusText
            }`
          );

        error.status =
          response.status;

        throw error;
      }

      lastError =
        new Error(
          `${config.name} API 暂时不可用`
        );
    } catch (
      error: any
    ) {
      if (
        error?.name ===
        'AbortError'
      ) {
        /*
         * 用户主动取消。
         */
        if (
          requestSignal.aborted
        ) {
          throw error;
        }

        /*
         * Provider 超时。
         */
        lastError =
          new Error(
            `${config.name} API 请求超时。`
          );
      } else {
        lastError =
          error;

        /*
         * 400 / 401 / 403
         * 不应该盲目重试。
         */
        if (
          [
            400,
            401,
            403
          ].includes(
            error?.status
          )
        ) {
          throw error;
        }

        if (
          attempt >=
          MAX_RETRIES
        ) {
          throw error;
        }
      }
    } finally {
      clearTimeout(
        timeoutId
      );

      requestSignal.removeEventListener(
        'abort',
        onAbort
      );
    }

    if (
      attempt <
      MAX_RETRIES
    ) {
      await sleep(
        retryDelay(attempt)
      );
    }
  }

  throw (
    lastError ||
    new Error(
      'AI Provider 请求失败。'
    )
  );
}

/* ============================================================
   Provider Order
   ============================================================ */

function getProviderOrder(
  requested: string
) {
  const order: string[] =
    [];

  if (
    Object.prototype.hasOwnProperty.call(
      PROVIDER_CONFIG,
      requested
    )
  ) {
    order.push(
      requested
    );
  }

  /*
   * 用户指定的 Provider 优先。
   *
   * 如果它不可用，
   * 再尝试其他已经配置好的 Provider。
   */
  for (
    const provider of [
      'deepseek',
      'still',
      'agent'
    ]
  ) {
    if (
      !order.includes(
        provider
      )
    ) {
      order.push(
        provider
      );
    }
  }

  return order;
}

/* ============================================================
   SSE Stream
   ============================================================ */

function createStreamResponse(
  upstreamResponse: Response,
  provider: string,
  requestId: string,
  requestSignal: AbortSignal,
  release: () => void
) {
  const encoder =
    new TextEncoder();

  const decoder =
    new TextDecoder(
      'utf-8'
    );

  let released =
    false;

  const releaseOnce =
    () => {
      if (released) {
        return;
      }

      released = true;

      release();
    };

  const stream =
    new ReadableStream<Uint8Array>({
      async start(
        controller
      ) {
        let reader:
          | ReadableStreamDefaultReader<Uint8Array>
          | null =
          null;

        let pending = '';

        let closed =
          false;

        const close =
          () => {
            if (closed) {
              return;
            }

            closed = true;

            releaseOnce();

            try {
              controller.close();
            } catch {}
          };

        const send =
          (payload: any) => {
            if (closed) {
              return;
            }

            try {
              controller.enqueue(
                encoder.encode(
                  `data: ${JSON.stringify(
                    payload
                  )}\n\n`
                )
              );
            } catch {
              closed = true;

              releaseOnce();
            }
          };

        const processLine =
          (line: string) => {
            if (closed) {
              return;
            }

            const trimmed =
              line.trim();

            if (
              !trimmed ||
              trimmed.startsWith(
                ':'
              ) ||
              !trimmed.startsWith(
                'data:'
              )
            ) {
              return;
            }

            const dataStr =
              trimmed
                .slice(5)
                .trim();

            if (!dataStr) {
              return;
            }

            if (
              dataStr ===
                '[DONE]' ||
              dataStr ===
                '"[DONE]"'
            ) {
              send({
                type: 'done'
              });

              return;
            }

            let json: any;

            try {
              json =
                JSON.parse(
                  dataStr
                );
            } catch {
              /*
               * 某些 Provider 的 SSE
               * 可能出现非 JSON 行。
               * 忽略，不让整个连接崩掉。
               */
              return;
            }

            /*
             * Provider Error
             */
            if (
              json?.error
            ) {
              send({
                type: 'error',
                error:
                  json.error
                    ?.message ||
                  'AI Provider 返回错误。',
                requestId,
                provider
              });

              return;
            }

            /*
             * OpenAI-compatible SSE
             */
            const content =
              json
                ?.choices?.[0]
                ?.delta
                ?.content;

            if (
              typeof content ===
                'string' &&
              content.length > 0
            ) {
              send({
                type: 'delta',
                content
              });
            }
          };

        try {
          if (
            !upstreamResponse.body
          ) {
            throw new Error(
              '上游没有返回流式响应。'
            );
          }

          reader =
            upstreamResponse.body.getReader();

          while (
            !closed
          ) {
            if (
              requestSignal.aborted
            ) {
              break;
            }

            const {
              done,
              value
            } =
              await reader.read();

            if (done) {
              break;
            }

            if (!value) {
              continue;
            }

            pending +=
              decoder.decode(
                value,
                {
                  stream: true
                }
              );

            const lines =
              pending.split(
                /\r?\n/
              );

            pending =
              lines.pop() ||
              '';

            for (
              const line of lines
            ) {
              processLine(
                line
              );

              if (closed) {
                break;
              }
            }
          }

          /*
           * flush decoder
           */
          pending +=
            decoder.decode();

          if (
            pending.trim() &&
            !closed
          ) {
            processLine(
              pending
            );
          }

          if (
            !closed &&
            !requestSignal.aborted
          ) {
            send({
              type: 'done'
            });
          }
        } catch (
          error: any
        ) {
          if (
            error?.name !==
              'AbortError' &&
            !requestSignal.aborted
          ) {
            send({
              type: 'error',
              error:
                error?.message ||
                'AI 流式连接发生错误。',
              requestId,
              provider
            });
          }
        } finally {
          if (reader) {
            try {
              reader.releaseLock();
            } catch {}
          }

          close();
        }
      },

      cancel() {
        releaseOnce();

        try {
          upstreamResponse.body?.cancel();
        } catch {}
      }
    });

  return new Response(
    stream,
    {
      status: 200,

      headers: {
        'Content-Type':
          'text/event-stream; charset=utf-8',

        /*
         * 禁止缓存。
         */
        'Cache-Control':
          'no-cache, no-store, must-revalidate, no-transform',

        /*
         * Nginx / Proxy 不缓冲。
         */
        'X-Accel-Buffering':
          'no',

        'X-Content-Type-Options':
          'nosniff',

        'X-Request-ID':
          requestId,

        'X-CQS-Provider':
          provider,

        /*
         * 保持长连接。
         */
        'Connection':
          'keep-alive'
      }
    }
  );
}

/* ============================================================
   POST
   ============================================================ */

export async function POST(
  request: Request
) {
  const requestId =
    createRequestId();

  let body: any =
    null;

  let requestAborted =
    false;

  let timeoutId:
    | ReturnType<typeof setTimeout>
    | null =
    null;

  let acquiredProvider:
    | string
    | null =
    null;

  let activeRequestKey:
    | string
    | null =
    null;

  const requestController =
    new AbortController();

  /*
   * Client Disconnect
   */
  const abort =
    () => {
      requestAborted =
        true;

      if (
        !requestController
          .signal.aborted
      ) {
        try {
          requestController.abort();
        } catch {}
      }
    };

  if (
    request.signal
  ) {
    if (
      request.signal.aborted
    ) {
      abort();

      return new Response(
        null,
        {
          status: 499,
          headers: {
            'X-Request-ID':
              requestId
          }
        }
      );
    }

    request.signal.addEventListener(
      'abort',
      abort,
      {
        once: true
      }
    );
  }

  try {
    /*
     * --------------------------
     * 读取请求
     * --------------------------
     */

    body =
      await readBody(
        request
      );

    /*
     * --------------------------
     * 参数验证
     * --------------------------
     */

    try {
      validateBody(
        body
      );
    } catch (
      error: any
    ) {
      return jsonError(
        error?.message ||
          '请求参数无效。',
        400,
        requestId
      );
    }

    /*
     * --------------------------
     * Duplicate Request
     * --------------------------
     *
     * 使用请求内容产生一个简单指纹。
     */
    let serializedMessages =
      '';

    try {
      serializedMessages =
        JSON.stringify(
          body.messages
        );
    } catch {
      throw new Error(
        '请求消息无法序列化。'
      );
    }

    activeRequestKey =
      [
        body.provider,
        serializedMessages,
        body.systemPrompt
      ].join('|');

    if (
      activeRequests.has(
        activeRequestKey
      )
    ) {
      return jsonError(
        '相同请求正在处理中，请勿重复提交。',
        409,
        requestId
      );
    }

    activeRequests.set(
      activeRequestKey,
      Date.now()
    );

    /*
     * --------------------------
     * Global Request Timeout
     * --------------------------
     */

    timeoutId =
      setTimeout(
        abort,
        REQUEST_TIMEOUT_MS
      );

    /*
     * --------------------------
     * Provider Failover
     * --------------------------
     */

    const providerOrder =
      getProviderOrder(
        body.provider
      );

    let lastProviderError:
      | Error
      | null =
      null;

    for (
      const currentProvider of providerOrder
    ) {
      if (
        requestAborted
      ) {
        throw new DOMException(
          '请求已取消。',
          'AbortError'
        );
      }

      /*
       * Provider 环境变量检查。
       *
       * 未配置的 Provider
       * 直接跳过。
       */
      try {
        resolveProvider(
          currentProvider
        );
      } catch (
        error: any
      ) {
        lastProviderError =
          error;

        continue;
      }

      /*
       * 熔断保护。
       */
      if (
        isCircuitOpen(
          currentProvider
        )
      ) {
        lastProviderError =
          new Error(
            `${currentProvider} 当前处于保护状态。`
          );

        continue;
      }

      /*
       * 当前实例并发保护。
       */
      if (
        !acquireProvider(
          currentProvider
        )
      ) {
        lastProviderError =
          new Error(
            `${currentProvider} 当前并发负载较高。`
          );

        continue;
      }

      acquiredProvider =
        currentProvider;

      try {
        /*
         * 请求 Provider。
         *
         * 只有在真正拿到 upstream response
         * 后才算成功。
         */
        const upstream =
          await fetchProvider(
            currentProvider,
            body,
            requestController.signal
          );

        markProviderSuccess(
          currentProvider
        );

        /*
         * 一旦把 stream 返回给客户端，
         * 由 stream 生命周期负责 release。
         */
        const release =
          () => {
            releaseProvider(
              currentProvider
            );

            acquiredProvider =
              null;

            if (
              activeRequestKey
            ) {
              activeRequests.delete(
                activeRequestKey
              );
            }
          };

        return createStreamResponse(
          upstream,
          currentProvider,
          requestId,
          requestController.signal,
          release
        );
      } catch (
        error: any
      ) {
        markProviderFailure(
          currentProvider
        );

        releaseProvider(
          currentProvider
        );

        acquiredProvider =
          null;

        lastProviderError =
          error;

        /*
         * 用户取消。
         */
        if (
          error?.name ===
          'AbortError'
        ) {
          throw error;
        }

        /*
         * 认证 / 请求参数错误，
         * 不应该换 Provider 隐瞒问题。
         */
        if (
          [
            400,
            401,
            403
          ].includes(
            error?.status
          )
        ) {
          throw error;
        }

        /*
         * 其他错误：
         * 继续尝试下一个 Provider。
         */
      }
    }

    throw (
      lastProviderError ||
      new Error(
        '当前 AI 服务暂时不可用，请稍后重试。'
      )
    );
  } catch (
    error: any
  ) {
    /*
     * Client Abort / Timeout
     */
    if (
      error?.name ===
      'AbortError'
    ) {
      return new Response(
        null,
        {
          status: 499,
          headers: {
            'Cache-Control':
              'no-store',
            'X-Request-ID':
              requestId
          }
        }
      );
    }

    const status =
      Number.isInteger(
        error?.status
      )
        ? error.status
        : 503;

    return jsonError(
      error?.message ||
        '服务器内部错误。',
      status,
      requestId
    );
  } finally {
    /*
     * --------------------------
     * Cleanup
     * --------------------------
     */

    if (
      timeoutId !== null
    ) {
      clearTimeout(
        timeoutId
      );

      timeoutId = null;
    }

    /*
     * 正常情况下 stream release
     * 已经负责释放 Provider。
     *
     * 如果异常发生在 stream 创建之前，
     * 这里负责兜底。
     */
    if (
      acquiredProvider
    ) {
      releaseProvider(
        acquiredProvider
      );

      acquiredProvider =
        null;
    }

    /*
     * 清理重复请求锁。
     */
    if (
      activeRequestKey
    ) {
      activeRequests.delete(
        activeRequestKey
      );

      activeRequestKey =
        null;
    }

    request.signal?.removeEventListener(
      'abort',
      abort
    );

    /*
     * 确保上游请求被取消。
     */
    if (
      requestAborted &&
      !requestController
        .signal.aborted
    ) {
      try {
        requestController.abort();
      } catch {}
    }
  }
}