這段代碼的主要錯誤在於最前方的 Import 關鍵字第一個字母是大寫（Import），這在 JavaScript / TypeScript 中會導致編譯與語法錯誤，使得頁面或 API 無法正常加載。
以下是已經將 Import 改為全小寫 import 的完整正確代碼，您可以直接複製覆蓋使用：
import { NextResponse } from 'next/server';

export const runtime = 'nodejs';
export const dynamic = 'force-dynamic';
export const revalidate = 0;

const PROVIDER_CONFIG = {
  deepseek: {
    name: 'DeepSeek',
    url: 'https://api.deepseek.com/v1/chat/completions',
    envKey: 'DEEPSEEK_API_KEY',
    model: 'deepseek-chat'
  },

  still: {
    name: 'Still',
    urlEnv: 'STILL_API_URL',
    keyEnv: 'STILL_API_KEY',
    modelEnv: 'STILL_MODEL',
    fallbackModel: 'still'
  },

  agent: {
    name: 'Agent',
    urlEnv: 'AGENT_API_URL',
    keyEnv: 'AGENT_API_KEY',
    modelEnv: 'AGENT_MODEL',
    fallbackModel: 'agent'
  }
};

const REQUEST_TIMEOUT_MS = 120000;

const MAX_MESSAGES = 100;

const MAX_MESSAGE_LENGTH = 20000;

const MAX_SYSTEM_PROMPT_LENGTH = 30000;

const MAX_RETRIES = 2;

const RETRY_BASE_MS = 350;

const CIRCUIT_FAILURE_THRESHOLD = 5;

const CIRCUIT_OPEN_MS = 30000;

/*
 * 这是单个 Vercel runtime instance
 * 的本地 Bulkhead。
 *
 * 它不是全球限制。
 *
 * 全球边缘限制由 Cloudflare 负责。
 */
const LOCAL_MAX_CONCURRENT = 64;

const providerState = new Map<
  string,
  {
    failures: number;
    successes: number;
    openedAt: number;
    halfOpenProbe: boolean;
    active: number;
  }
>();

const activeRequests = new Map<
  string,
  number
>();

function getGatewaySecret() {
  return process.env.CLOUDFLARE_GATEWAY_SECRET || '';
}

function constantTimeEqual(
  a: string,
  b: string
) {
  if (!a || !b) return false;

  if (a.length !== b.length) {
    return false;
  }

  let result = 0;

  for (let i = 0; i < a.length; i++) {
    result |=
      a.charCodeAt(i) ^
      b.charCodeAt(i);
  }

  return result === 0;
}

function authorizeGateway(
  request: Request
) {
  const expected =
    getGatewaySecret();

  if (!expected) {
    return false;
  }

  const provided =
    request.headers.get(
      'X-CQS-Gateway-Key'
    ) || '';

  return constantTimeEqual(
    provided,
    expected
  );
}

function getProviderState(
  provider: string
) {
  let state =
    providerState.get(
      provider
    );

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
    getProviderState(
      provider
    );

  if (!state.openedAt) {
    return false;
  }

  const elapsed =
    Date.now() -
    state.openedAt;

  if (
    elapsed >=
    CIRCUIT_OPEN_MS
  ) {
    if (
      !state.halfOpenProbe
    ) {
      state.halfOpenProbe =
        true;

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
    getProviderState(
      provider
    );

  state.successes += 1;

  state.failures = 0;

  state.openedAt = 0;

  state.halfOpenProbe =
    false;
}

function markProviderFailure(
  provider: string
) {
  const state =
    getProviderState(
      provider
    );

  state.failures += 1;

  if (
    state.failures >=
    CIRCUIT_FAILURE_THRESHOLD
  ) {
    state.openedAt =
      Date.now();
  }
}

function releaseProvider(
  provider: string
) {
  const state =
    getProviderState(
      provider
    );

  state.active =
    Math.max(
      0,
      state.active - 1
    );

  if (
    state.openedAt &&
    Date.now() -
      state.openedAt >=
      CIRCUIT_OPEN_MS
  ) {
    state.halfOpenProbe =
      false;
  }
}

function acquireProvider(
  provider: string
) {
  const state =
    getProviderState(
      provider
    );

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

function createRequestId() {
  const random =
    Math.random()
      .toString(36)
      .slice(2, 10)
      .toUpperCase();

  return `CQS-${Date.now()}-${random}`;
}

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

async function readBody(
  request: Request
) {
  try {
    return await request.json();
  } catch {
    throw new Error(
      '请求数据不是有效的 JSON。'
    );
  }
}

function validateBody(
  body: any
) {
  if (
    !body ||
    typeof body !==
      'object'
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
    !PROVIDER_CONFIG[
      provider as keyof typeof PROVIDER_CONFIG
    ]
  ) {
    throw new Error(
      '不支持的 AI Provider。'
    );
  }

  if (
    !Array.isArray(messages) ||
    messages.length === 0 ||
    messages.length >
      MAX_MESSAGES
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

  for (
    const message of messages
  ) {
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
}

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

  if (
    provider ===
    'deepseek'
  ) {
    const key =
      process.env
        .DEEPSEEK_API_KEY;

    if (!key) {
      throw new Error(
        '服务器未配置 DeepSeek API Key。'
      );
    }

    return {
      ...config,
      key
    };
  }

  const url =
    process.env[
      (config as any).urlEnv
    ];

  const key =
    process.env[
      (config as any).keyEnv
    ];

  const model =
    process.env[
      (config as any).modelEnv
    ] ||
    (config as any)
      .fallbackModel;

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
    (resolve) =>
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
      Math.random() * 250
    );

  return (
    exponential +
    jitter
  );
}

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
      text.slice(
        0,
        4000
      ) + '...';
  }

  return text;
}

async function fetchProvider(
  provider: string,
  requestBody: any,
  requestSignal: AbortSignal
) {
  const config =
    resolveProvider(
      provider
    );

  let lastError:
    | any = null;

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
      {
        once: true
      }
    );

    const timeoutId =
      setTimeout(
        () => {
          try {
            controller.abort();
          } catch {}
        },
        REQUEST_TIMEOUT_MS
      );

    try {
      const body: any = {
        model:
          config.model,

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

      if (
        provider ===
        'deepseek'
      ) {
        body.response_format = {
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
                body
              ),

            signal:
              controller.signal,

            cache:
              'no-store'
          }
        );

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
        const error:
          any =
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
        if (
          requestSignal.aborted
        ) {
          throw error;
        }

        lastError =
          new Error(
            `${config.name} API 请求超时。`
          );
      } else {
        lastError =
          error;

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
        retryDelay(
          attempt
        )
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

function getProviderOrder(
  requested: string
) {
  const order: string[] =
    [];

  if (
    PROVIDER_CONFIG[
      requested as keyof typeof PROVIDER_CONFIG
    ]
  ) {
    order.push(
      requested
    );
  }

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

  let released = false;

  const releaseOnce =
    () => {
      if (released)
        return;

      released = true;

      release();
    };

  const stream =
    new ReadableStream({
      async start(
        controller
      ) {
        let reader:
          ReadableStreamDefaultReader<Uint8Array> |
          null =
            null;

        let pending = '';

        let closed = false;

        const close =
          () => {
            if (closed)
              return;

            closed = true;

            releaseOnce();

            try {
              controller.close();
            } catch {}
          };

        const send =
          (
            payload: any
          ) => {
            if (closed)
              return;

            try {
              controller.enqueue(
                encoder.encode(
                  `data: ${JSON.stringify(
                    payload
                  )}\n\n`
                )
              );
            } catch {
              closed =
                true;

              releaseOnce();
            }
          };

        const processLine =
          (
            line: string
          ) => {
            if (closed)
              return;

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

            if (!dataStr)
              return;

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
              return;
            }

            const content =
              json?.choices?.[0]
                ?.delta?.content;

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

            if (
              json?.error
            ) {
              send({
                type: 'error',
                error:
                  json.error
                    ?.message ||
                  'AI Provider 返回错误。'
              });
            }
          };

        try {
          if (
            !upstreamResponse.body
          ) {
            send({
              type: 'error',
              error:
                '上游没有返回流。'
            });

            return;
          }

          reader =
            upstreamResponse
              .body
              .getReader();

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

            if (done)
              break;

            if (!value)
              continue;

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

              if (closed)
                break;
            }
          }

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

          if (!closed) {
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

        if (
          !requestSignal.aborted
        ) {
          try {
            upstreamResponse.body?.cancel();
          } catch {}
        }
      }
    });

  return new Response(
    stream,
    {
      status: 200,

      headers: {
        'Content-Type':
          'text/event-stream; charset=utf-8',

        'Cache-Control':
          'no-cache, no-store, must-revalidate',

        'X-Accel-Buffering':
          'no',

        'X-Content-Type-Options':
          'nosniff',

        'X-Request-ID':
          requestId,

        'X-CQS-Provider':
          provider,

        'Connection':
          'keep-alive'
      }
    }
  );
}

export async function POST(
  request: Request
) {
  const requestId =
    createRequestId();

  let body: any = null;

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

  let streamOwnership =
    false;

  const requestController =
    new AbortController();

  const abort =
    () => {
      requestAborted =
        true;

      if (
        !requestController
          .signal
          .aborted
      ) {
        try {
          requestController.abort();
        } catch {}
      }
    };

  if (
    !authorizeGateway(
      request
    )
  ) {
    return jsonError(
      'Unauthorized Gateway',
      403,
      requestId
    );
  }

  if (
    request.signal
  ) {
    if (
      request.signal
        .aborted
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

  let requestKey:
    | string
    | null =
      null;

  try {
    body =
      await readBody(
        request
      );

    try {
      validateBody(
        body
      );
    } catch (
      error: any
    ) {
      return jsonError(
        error.message ||
          '请求参数无效。',
        400,
        requestId
      );
    }

    requestKey = [
      body.provider,
      JSON.stringify(
        body.messages
      ),
      body.systemPrompt
    ].join('|');

    if (
      activeRequests.size <
      10000
    ) {
      if (
        activeRequests.has(
          requestKey
        )
      ) {
        return jsonError(
          '相同请求正在处理中，请勿重复提交。',
          409,
          requestId
        );
      }

      activeRequests.set(
        requestKey,
        Date.now()
      );
    } else {
      requestKey = null;
    }

    timeoutId =
      setTimeout(
        abort,
        REQUEST_TIMEOUT_MS
      );

    const providerOrder =
      getProviderOrder(
        body.provider
      );

    let lastProviderError:
      | any =
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

      try {
        resolveProvider(
          currentProvider
        );
      } catch {
        continue;
      }

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
        const upstream =
          await fetchProvider(
            currentProvider,
            body,
            requestController.signal
          );

        markProviderSuccess(
          currentProvider
        );

        streamOwnership =
          true;

        const release =
          () => {
            if (
              acquiredProvider ===
              currentProvider
            ) {
              releaseProvider(
                currentProvider
              );

              acquiredProvider =
                null;
            }

            if (
              requestKey
            ) {
              activeRequests.delete(
                requestKey
              );

              requestKey =
                null;
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

        if (
          error?.name ===
          'AbortError'
        ) {
          throw error;
        }

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
    if (
      timeoutId !==
      null
    ) {
      clearTimeout(
        timeoutId
      );

      timeoutId = null;
    }

    if (
      acquiredProvider &&
      !streamOwnership
    ) {
      releaseProvider(
        acquiredProvider
      );

      acquiredProvider =
        null;
    }

    if (
      requestKey &&
      !streamOwnership
    ) {
      activeRequests.delete(
        requestKey
      );

      requestKey =
        null;
    }

    request.signal?.removeEventListener(
      'abort',
      abort
    );

    if (
      requestAborted &&
      !requestController
        .signal
        .aborted
    ) {
      try {
        requestController.abort();
      } catch {}
    }
  }
}

