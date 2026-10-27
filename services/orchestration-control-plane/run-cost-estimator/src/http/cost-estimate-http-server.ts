/**
 * cost-estimate-http-server.ts
 *
 * Minimal HTTP entrypoint exposing `POST /cost-estimate` over
 * estimateCost() from src/index.ts, so pipeline-api-gateway's
 * `src/cost-estimation/cost-estimator-http-client.adapter.ts` has something
 * real to call. Deliberately implemented with Node's built-in `http` module
 * rather than pulling in NestJS/Express: this service is a single stateless
 * POST endpoint over a pure function, not a growing surface of resources
 * with routing/validation/DI needs like pipeline-api-gateway or
 * telemetry-query-api — a full framework would be more scaffolding than the
 * job requires. If this service's surface grows, migrating to NestJS
 * (matching telemetry-query-api's shape) is the natural next step.
 */
import { createServer, IncomingMessage, ServerResponse } from 'node:http';
import type { PipelineDefinition } from '@shared-libraries/dag-domain-model';
import { estimateCost } from '../index';
import type { CloudProvider } from '../estimation/cloud-instance-pricing-catalog';

interface CostEstimateRequestBody {
  pipelineDefinition: PipelineDefinition;
  cloudProvider?: CloudProvider;
}

const DEFAULT_PORT = 8095;

function readJsonBody(req: IncomingMessage): Promise<unknown> {
  return new Promise((resolve, reject) => {
    let raw = '';
    req.on('data', (chunk: Buffer) => {
      raw += chunk.toString('utf8');
    });
    req.on('end', () => {
      if (!raw) {
        resolve({});
        return;
      }
      try {
        resolve(JSON.parse(raw));
      } catch (error) {
        reject(error instanceof Error ? error : new Error(String(error)));
      }
    });
    req.on('error', reject);
  });
}

function sendJson(res: ServerResponse, statusCode: number, body: unknown): void {
  const payload = JSON.stringify(body);
  res.writeHead(statusCode, { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(payload) });
  res.end(payload);
}

async function handleCostEstimateRequest(req: IncomingMessage, res: ServerResponse): Promise<void> {
  if (req.method !== 'POST' || req.url !== '/cost-estimate') {
    sendJson(res, 404, { error: 'not found; expected POST /cost-estimate' });
    return;
  }

  let body: CostEstimateRequestBody;
  try {
    body = (await readJsonBody(req)) as CostEstimateRequestBody;
  } catch {
    sendJson(res, 400, { error: 'invalid JSON request body' });
    return;
  }

  if (!body.pipelineDefinition || !Array.isArray(body.pipelineDefinition.nodes)) {
    sendJson(res, 400, { error: 'request body must include a pipelineDefinition with a nodes array' });
    return;
  }

  try {
    const estimate = estimateCost(body.pipelineDefinition, {
      cloudProvider: body.cloudProvider ?? 'AWS',
    });
    sendJson(res, 200, estimate);
  } catch (error) {
    sendJson(res, 500, { error: error instanceof Error ? error.message : 'unknown estimation error' });
  }
}

export function startCostEstimateHttpServer(port = DEFAULT_PORT) {
  const server = createServer((req, res) => {
    handleCostEstimateRequest(req, res).catch((error) => {
      sendJson(res, 500, { error: error instanceof Error ? error.message : 'unhandled server error' });
    });
  });

  server.listen(port, () => {
    // eslint-disable-next-line no-console
    console.log(`run-cost-estimator: listening on :${port} (POST /cost-estimate)`);
  });

  return server;
}

if (require.main === module) {
  const port = process.env.PORT ? Number(process.env.PORT) : DEFAULT_PORT;
  startCostEstimateHttpServer(port);
}
