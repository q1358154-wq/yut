'use client';

import { useCallback, useEffect, useRef, useState } from 'react';

const MAX_PROMPT_LENGTH = 20000;
const REQUEST_TIMEOUT_MS = 125000;

export default function UniversalInvoiceApp() {
  const [prompt, setPrompt] = useState('');
  const [provider, setProvider] = useState('deepseek');
  const [mode, setMode] = useState('global');

  const [loading, setLoading] = useState(false);
  const [rawText, setRawText] = useState('');
  const [invoiceData, setInvoiceData] = useState(null);
  const [errorMessage, setErrorMessage] = useState('');
  const [copied, setCopied] = useState(false);

  const abortControllerRef = useRef(null);
  const timeoutRef = useRef(null);
  const mountedRef = useRef(true);
  const requestStartedRef = useRef(0);

  useEffect(() => {
    mountedRef.current = true;

    return () => {
      mountedRef.current = false;

      if (abortControllerRef.current) {
        try {
          abortControllerRef.current.abort();
        } catch {}
      }

      if (timeoutRef.current) {
        clearTimeout(timeoutRef.current);
        timeoutRef.current = null;
      }
    };
  }, []);

  const safeSet = useCallback((setter, value) => {
    if (mountedRef.current) {
      setter(value);
    }
  }, []);

  const handleGenerate = async () => {
    const cleanPrompt = prompt.trim();

    if (!cleanPrompt) {
      safeSet(setErrorMessage, '请输入发票或报表需求。');
      return;
    }

    if (cleanPrompt.length > MAX_PROMPT_LENGTH) {
      safeSet(
        setErrorMessage,
        `输入内容过长，最多允许 ${MAX_PROMPT_LENGTH} 个字符。`
      );
      return;
    }

    /*
     * 防止极端情况下旧请求还没有完全释放，
     * 前端再次提交。
     */
    if (loading) return;

    /*
     * 如果存在旧请求，主动取消。
     */
    if (abortControllerRef.current) {
      try {
        abortControllerRef.current.abort();
      } catch {}
    }

    if (timeoutRef.current) {
      clearTimeout(timeoutRef.current);
      timeoutRef.current = null;
    }

    const controller = new AbortController();

    abortControllerRef.current = controller;
    requestStartedRef.current = Date.now();

    safeSet(setLoading, true);
    safeSet(setRawText, '');
    safeSet(setInvoiceData, null);
    safeSet(setErrorMessage, '');
    safeSet(setCopied, false);

    const systemPrompt =
      mode === 'global'
        ? `You are a professional invoice assistant.

Extract the user's request and return ONLY ONE valid JSON object.

Required structure:
{
  "invoiceNumber": "INV-2026-001",
  "clientName": "Client Name",
  "clientAddress": "Client Address",
  "date": "YYYY-MM-DD",
  "currency": "USD",
  "items": [
    {
      "description": "Service item",
      "amount": 0.00
    }
  ],
  "total": 0.00
}

Rules:
- Return pure JSON only.
- Do not use Markdown.
- Do not wrap JSON in code fences.
- date must be YYYY-MM-DD.
- amount and total must be numeric.
- items must always be an array.
- currency should normally be USD unless the user explicitly specifies another currency.`
        : `你是一个专业的中国财务发票与报表助手。

请根据用户描述提取信息，并且只返回一个合法 JSON 对象。

必须使用以下结构：
{
  "invoiceNumber": "FP-2026-001",
  "clientName": "购方/客户名称",
  "taxNumber": "统一社会信用代码/税号",
  "date": "YYYY-MM-DD",
  "currency": "CNY",
  "items": [
    {
      "description": "项目内容",
      "amount": 0.00,
      "taxRate": "6%"
    }
  ],
  "total": 0.00
}

规则：
- 只能输出纯 JSON。
- 不允许 Markdown。
- 不允许代码块。
- date 必须使用 YYYY-MM-DD。
- amount 和 total 必须是数字。
- items 必须始终为数组。
- 如果用户没有提供税号，可以使用空字符串。
- 如果用户没有提供税率，可以使用空字符串。`;

    /*
     * 前端超时保险。
     * 后端本身也有超时，两层保护。
     */
    timeoutRef.current = setTimeout(() => {
      try {
        controller.abort();
      } catch {}
    }, REQUEST_TIMEOUT_MS);

    try {
      const response = await fetch('/api/chat', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Accept': 'text/event-stream',
          'Cache-Control': 'no-cache'
        },
        body: JSON.stringify({
          provider,
          messages: [
            {
              role: 'user',
              content: cleanPrompt
            }
          ],
          systemPrompt
        }),
        signal: controller.signal,
        cache: 'no-store'
      });

      if (!response.ok) {
        let errorText = '';

        try {
          const errorData = await response.json();
          errorText =
            errorData?.error ||
            errorData?.message ||
            '';
        } catch {
          try {
            errorText = await response.text();
          } catch {}
        }

        throw new Error(
          errorText ||
            `请求失败：HTTP ${response.status}`
        );
      }

      if (!response.body) {
        throw new Error('服务器没有返回流式数据。');
      }

      const reader = response.body.getReader();
      const decoder = new TextDecoder('utf-8');

      let buffer = '';
      let accumulated = '';
      let serverError = '';

      const processSSELine = (line) => {
        const trimmed = line.trim();

        if (!trimmed) return;

        if (!trimmed.startsWith('data:')) {
          return;
        }

        const payload = trimmed.slice(5).trim();

        if (!payload) return;

        let data;

        try {
          data = JSON.parse(payload);
        } catch {
          return;
        }

        if (data.type === 'delta') {
          if (typeof data.content === 'string') {
            accumulated += data.content;

            safeSet(setRawText, accumulated);
          }

          return;
        }

        if (data.type === 'error') {
          serverError =
            data.error ||
            'AI 服务返回错误。';

          safeSet(setErrorMessage, serverError);

          return;
        }

        if (data.type === 'done') {
          return;
        }
      };

      while (true) {
        const { value, done } = await reader.read();

        if (done) break;

        if (!value) continue;

        buffer += decoder.decode(value, {
          stream: true
        });

        const lines = buffer.split(/\r?\n/);

        buffer = lines.pop() || '';

        for (const line of lines) {
          processSSELine(line);
        }
      }

      buffer += decoder.decode();

      if (buffer.trim()) {
        processSSELine(buffer);
      }

      try {
        if (serverError) {
          throw new Error(serverError);
        }

        /*
         * AI 偶尔会返回：
         * ```json
         * {...}
         * ```
         *
         * 或者前后多出空白。
         */
        let cleanJson = accumulated
          .replace(/^\s*```json\s*/i, '')
          .replace(/^\s*```\s*/i, '')
          .replace(/\s*```\s*$/i, '')
          .trim();

        /*
         * 防止模型前后多出解释文字。
         * 优先截取第一个 { 到最后一个 }。
         */
        const firstBrace = cleanJson.indexOf('{');
        const lastBrace = cleanJson.lastIndexOf('}');

        if (
          firstBrace !== -1 &&
          lastBrace !== -1 &&
          lastBrace > firstBrace
        ) {
          cleanJson = cleanJson.slice(
            firstBrace,
            lastBrace + 1
          );
        }

        const parsed = JSON.parse(cleanJson);

        /*
         * 基础数据完整性保护。
         */
        if (
          !parsed ||
          typeof parsed !== 'object'
        ) {
          throw new Error('返回的数据不是有效对象。');
        }

        if (!Array.isArray(parsed.items)) {
          parsed.items = [];
        }

        safeSet(setInvoiceData, parsed);
        safeSet(setRawText, JSON.stringify(parsed, null, 2));
      } catch (parseError) {
        if (!serverError) {
          safeSet(
            setErrorMessage,
            'AI 返回的数据不是有效发票 JSON，请重试。'
          );
        }
      }
    } catch (error) {
      if (error?.name === 'AbortError') {
        /*
         * 如果是用户主动取消或组件卸载，
         * 不显示错误。
         */
        const elapsed =
          Date.now() - requestStartedRef.current;

        if (
          mountedRef.current &&
          elapsed >= REQUEST_TIMEOUT_MS - 1000
        ) {
          safeSet(
            setErrorMessage,
            'AI 请求超时，请稍后重试。'
          );
        }
      } else {
        safeSet(
          setErrorMessage,
          error?.message ||
            '网络请求发生错误，请稍后重试。'
        );
      }
    } finally {
      if (timeoutRef.current) {
        clearTimeout(timeoutRef.current);
        timeoutRef.current = null;
      }

      if (abortControllerRef.current === controller) {
        abortControllerRef.current = null;
      }

      safeSet(setLoading, false);
    }
  };

  const handleCancel = () => {
    if (!loading) return;

    try {
      abortControllerRef.current?.abort();
    } catch {}

    if (timeoutRef.current) {
      clearTimeout(timeoutRef.current);
      timeoutRef.current = null;
    }

    safeSet(setLoading, false);
  };

  const handleCopyText = async () => {
    if (!invoiceData) return;

    const currencySymbol =
      invoiceData.currency === 'USD'
        ? '$'
        : invoiceData.currency === 'EUR'
        ? '€'
        : '￥';

    const textSummary = `--- ${
      mode === 'global'
        ? 'INVOICE'
        : '财务报表/发票'
    } ---
单号: ${invoiceData.invoiceNumber || '-'}
日期: ${invoiceData.date || '-'}
对象: ${invoiceData.clientName || '-'}
总计: ${currencySymbol}${invoiceData.total ?? 0}
币种: ${invoiceData.currency || '-'}
-----------------------------`;

    try {
      await navigator.clipboard.writeText(
        textSummary
      );

      safeSet(setCopied, true);

      setTimeout(() => {
        if (mountedRef.current) {
          setCopied(false);
        }
      }, 2000);
    } catch {
      safeSet(
        setErrorMessage,
        '复制失败，请手动复制。'
      );
    }
  };

  const formatAmount = (value) => {
    const number = Number(value);

    if (!Number.isFinite(number)) {
      return '0.00';
    }

    return number.toFixed(2);
  };

  const currencySymbol =
    invoiceData?.currency === 'USD'
      ? '$'
      : invoiceData?.currency === 'EUR'
      ? '€'
      : '￥';

  return (
    <main className="max-w-2xl mx-auto p-6 font-sans">
      <h1 className="text-2xl font-bold mb-2">
        🌐 AI 全球通用智能发票与报表生成器
      </h1>

      <p className="text-sm text-gray-500 mb-6">
        支持国内中文报表与海外英文发票自由切换，一句话极速生成。
      </p>

      <div className="bg-white p-4 border rounded-xl shadow-sm mb-6">
        <div className="flex flex-wrap gap-4 mb-3 items-center justify-between">
          <label className="text-sm font-semibold flex items-center gap-2">
            业务模式：

            <select
              value={mode}
              onChange={(e) =>
                setMode(e.target.value)
              }
              disabled={loading}
              className="border p-1.5 rounded text-sm bg-gray-50 outline-none font-bold"
            >
              <option value="global">
                🌍 海外英文发票 (USD)
              </option>

              <option value="china">
                🇨🇳 国内中文报表/发票 (CNY)
              </option>
            </select>
          </label>

          <label className="text-sm font-semibold flex items-center gap-2">
            AI 模型：

            <select
              value={provider}
              onChange={(e) =>
                setProvider(e.target.value)
              }
              disabled={loading}
              className="border p-1.5 rounded text-sm bg-gray-50 outline-none"
            >
              <option value="deepseek">
                DeepSeek
              </option>

              <option value="still">
                Still
              </option>

              <option value="agent">
                Agent
              </option>
            </select>
          </label>
        </div>

        <textarea
          className="w-full p-3 border rounded-lg shadow-sm focus:ring-2 focus:ring-black outline-none text-sm mb-3"
          rows={3}
          maxLength={MAX_PROMPT_LENGTH}
          placeholder={
            mode === 'global'
              ? 'e.g., Invoice for John 500 USD for web design...'
              : '例如：给北京客户开一张含税 3000 元的技术服务费发票...'
          }
          value={prompt}
          disabled={loading}
          onChange={(e) =>
            setPrompt(e.target.value)
          }
        />

        {!loading ? (
          <button
            onClick={handleGenerate}
            disabled={!prompt.trim()}
            className="bg-black text-white px-4 py-2 rounded-lg w-full font-medium hover:bg-gray-800 transition text-sm disabled:opacity-50 disabled:cursor-not-allowed"
          >
            ✨ 一键生成发票/报表
          </button>
        ) : (
          <button
            onClick={handleCancel}
            className="bg-red-600 text-white px-4 py-2 rounded-lg w-full font-medium hover:bg-red-700 transition text-sm"
          >
            ⏹ 停止生成
          </button>
        )}

        <div className="text-right text-[10px] text-gray-400 mt-2">
          {prompt.length}/{MAX_PROMPT_LENGTH}
        </div>
      </div>

      {errorMessage && (
        <div className="p-4 bg-red-50 border border-red-200 text-red-600 rounded-lg text-sm mb-4">
          ⚠️ {errorMessage}
        </div>
      )}

      {loading && (
        <div className="p-4 bg-gray-50 border rounded-lg text-xs text-gray-600 mb-4 font-mono">
          <p className="font-bold text-black mb-1">
            正在流式接收 AI 数据...
          </p>

          <div className="max-h-32 overflow-y-auto whitespace-pre-wrap">
            {rawText}
          </div>
        </div>
      )}

      {invoiceData && (
        <div className="p-8 border rounded-xl shadow-lg bg-white text-black">
          <div className="flex justify-between items-start border-b pb-4 mb-4">
            <div>
              <h2 className="text-2xl font-black tracking-wider">
                {mode === 'global'
                  ? 'INVOICE'
                  : '财务报表 / 发票'}
              </h2>

              <p className="text-xs text-gray-500 mt-1">
                No: {invoiceData.invoiceNumber || '-'}
              </p>
            </div>

            <div className="text-right">
              <p className="text-xs text-gray-500">
                Date: {invoiceData.date || '-'}
              </p>
            </div>
          </div>

          <div className="mb-6">
            <p className="text-xs font-bold text-gray-400 uppercase">
              {mode === 'global'
                ? 'Billed To:'
                : '购买方 / 抬头:'}
            </p>

            <p className="font-bold text-base">
              {invoiceData.clientName || '-'}
            </p>

            {invoiceData.taxNumber && (
              <p className="text-xs text-gray-600">
                税号：{invoiceData.taxNumber}
              </p>
            )}

            {invoiceData.clientAddress && (
              <p className="text-xs text-gray-600">
                {invoiceData.clientAddress}
              </p>
            )}
          </div>

          <div className="overflow-x-auto">
            <table className="w-full mb-6 border-collapse">
              <thead>
                <tr className="border-b text-left text-xs text-gray-400 uppercase">
                  <th className="py-2">
                    项目说明 (Description)
                  </th>

                  {mode === 'china' && (
                    <th className="py-2 text-center">
                      税率
                    </th>
                  )}

                  <th className="py-2 text-right">
                    金额 (Amount)
                  </th>
                </tr>
              </thead>

              <tbody>
                {invoiceData.items?.map(
                  (item, idx) => (
                    <tr
                      key={`${idx}-${item.description || 'item'}`}
                      className="border-b text-sm"
                    >
                      <td className="py-3 pr-2">
                        {item.description || '-'}
                      </td>

                      {mode === 'china' && (
                        <td className="py-3 text-center text-xs text-gray-500">
                          {item.taxRate || '-'}
                        </td>
                      )}

                      <td className="py-3 text-right font-mono">
                        {currencySymbol}
                        {formatAmount(
                          item.amount
                        )}
                      </td>
                    </tr>
                  )
                )}
              </tbody>
            </table>
          </div>

          <div className="flex justify-end mb-6">
            <div className="text-right">
              <span className="text-xs text-gray-500 mr-4 uppercase">
                Total Due (总计):
              </span>

              <span className="text-xl font-bold font-mono">
                {currencySymbol}
                {formatAmount(
                  invoiceData.total
                )}{' '}
                {invoiceData.currency || ''}
              </span>
            </div>
          </div>

          <div className="flex justify-end gap-3 pt-4 border-t">
            <button
              onClick={handleCopyText}
              className="bg-gray-100 text-gray-800 px-3 py-2 rounded-lg text-xs font-medium hover:bg-gray-200 transition"
            >
              {copied
                ? '✅ 已复制文本'
                : '📋 一键复制文本'}
            </button>

            <button
              onClick={() => window.print()}
              className="bg-gray-900 text-white px-4 py-2 rounded-lg text-xs font-medium hover:bg-black transition"
            >
              打印 / 另存为 PDF
            </button>
          </div>
        </div>
      )}
    </main>
  );
}