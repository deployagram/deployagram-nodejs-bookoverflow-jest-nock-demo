import type { DiagramLogger } from '@deployagram/api';

/**
 * Calls the application over HTTP, and records the call to the Collector as the Deployagram HTTP Proxy would.
 * Nock stubs only the application's outbound calls, so the test records its own inbound call.
 */
export async function callRecordingInbound(
  logger: DiagramLogger,
  names: { caller: string; callee: string },
  url: URL,
): Promise<{ status: number; body: string }> {
  const { caller, callee } = names;
  const endpoint = `${url.pathname.replace(/^\//, '')}${url.search}`;

  await logger.log({
    timestamp: new Date().toISOString(),
    protocol: 'HTTP',
    source: caller,
    target: callee,
    message: `GET Request from ${caller} to ${callee}`,
    body: JSON.stringify({ Headers: {}, Endpoint: endpoint }),
  });

  const response = await fetch(url);
  const body = await response.text();

  await logger.log({
    timestamp: new Date().toISOString(),
    protocol: 'HTTP',
    source: callee,
    target: caller,
    message: `GET Response from ${callee} to ${caller}`,
    body: JSON.stringify({
      Headers: { 'content-type': [response.headers.get('content-type') ?? ''] },
      Body: body || null,
      ResponseCode: response.status,
    }),
  });

  return { status: response.status, body };
}
